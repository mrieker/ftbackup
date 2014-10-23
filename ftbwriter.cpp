
#include "ftbackup.h"
#include "ftbwriter.h"

FTBWriter::FTBWriter ()
{
    pthread_cond_init  (&compr2write_cond,  NULL);
    pthread_cond_init  (&main2compr_cond,   NULL);
    pthread_mutex_init (&compr2write_mutex, NULL);
    pthread_mutex_init (&main2compr_mutex,  NULL);
}

FTBWriter::~FTBWriter ()
{
    uint32_t i;

    if (xorblocks != NULL) {
        for (i = 0; i < xorgc; i ++) {
            free (xorblocks[i]);
        }
        free (xorblocks);
    }

    if (inodesname != NULL) {
        for (i = 0; i < inodesused; i ++) {
            free (inodesname[i]);
        }
        free (inodesname);
    }

    if (inodeslist != NULL) {
        free (inodeslist);
    }

    if ((ssfd != STDIN_FILENO) && (ssfd != STDOUT_FILENO) && (ssfd != STDERR_FILENO)) {
        close (ssfd);
    }

    if (zisopen) {
        zstrm.avail_out = 0;
        zstrm.next_out  = NULL;
        deflateEnd (&zstrm);
    }

    while (compr2write_used > 0) {
        free (compr2write_slots[compr2write_next]);
        compr2write_next = (compr2write_next) % COMPR2WRITE_NSLOTS;
        -- compr2write_used;
    }

    while (main2compr_used > 0) {
        free (main2compr_slots[main2compr_used].buf);
        main2compr_next = (main2compr_next) % MAIN2COMPR_NSLOTS;
        -- main2compr_used;
    }
}

/**
 * @brief Write saveset.
 * @param ssname = name of saveset (or "-" to write to stdout).
 * @param rootpath = root path of files to back up
 */
int FTBWriter::write_saveset (char const *ssname, char const *rootpath)
{
    Header hdr;
    int ok, rc;
    pthread_t compr_thandl, write_thandl;

    /*
     * Open saveset file.
     */
    if (strcmp (ssname, "-") == 0) {
        ssfd = STDOUT_FILENO;
        if (isatty (ssfd)) {
            fprintf (stderr, "ftbackup: cannot write saveset to a tty\n");
            return EX_SSIO;
        }
        fflush (stdout);
    } else {
        ssfd = open (ssname, O_WRONLY | O_CREAT | O_TRUNC | ooptions, 0666);
        if (ssfd < 0) {
            fprintf (stderr, "ftbackup: open(%s) saveset error: %s\n", ssname, strerror (errno));
            return EX_SSIO;
        }
        if (isatty (ssfd)) {
            fprintf (stderr, "ftbackup: cannot write saveset to a tty\n");
            close (ssfd);
            return EX_SSIO;
        }
    }

    /*
     * Create compression and writing threads.
     */
    rc = pthread_create (&compr_thandl, NULL, compr_thread_wrapper, this);
    if (rc != 0) SYSERR (pthread_create, rc);
    rc = pthread_create (&write_thandl, NULL, write_thread_wrapper, this);
    if (rc != 0) SYSERR (pthread_create, rc);

    /*
     * Process root path of files to back up.
     */
    ok = write_file (rootpath, NULL);

    /*
     * Write EOF mark.
     */
    memset (&hdr, 0, sizeof hdr);
    write_header (&hdr);
    write_queue (NULL, 0, 0);

    /*
     * Wait for threads to finish.
     */
    rc = pthread_join (compr_thandl, NULL);
    if (rc != 0) SYSERR (pthread_join, rc);
    rc = pthread_join (write_thandl, NULL);
    if (rc != 0) SYSERR (pthread_join, rc);

    /*
     * Finished, close saveset file.
     */
    if ((strcmp (ssname, "-") != 0) && (close (ssfd) < 0)) {
        fprintf (stderr, "ftbackup: close() saveset error: %s\n", strerror (errno));
        return EX_SSIO;
    }

    return ok ? EX_OK : EX_FILIO;
}

/**
 * @brief Write the given file/directory/whatever to the currently open saveset.
 * @param path = path to file
 * @returns 1: no file io errors
 *          0: some io errors
 */
int FTBWriter::write_file (char const *path, struct stat const *dirstat)
{
    Header *hdr;
    int pathlen;
    struct stat statbuf;

    /*
     * See what type of thing we are dealing with.
     */
    if (lstat (path, &statbuf) < 0) {
        fprintf (stderr, "ftbackup: lstat(%s) error: %s\n", path, strerror (errno));
        return 0;
    }

    /*
     * We can't restore sockets so no sense trying to save them.
     */
    if (S_ISSOCK (statbuf.st_mode)) {
        fprintf (stderr, "ftbackup: skipping socket %s\n", path);
        return 1;
    }

    /*
     * No trailing '/' on path unless the path is only a '/'
     * so we have a specific convention to follow.
     */
    pathlen = strlen (path);
    while ((pathlen > 1) && (path[pathlen-1] == '/')) -- pathlen;

    /*
     * Set up file header with the file's name and attributes.
     */
    hdr = (Header *) alloca (pathlen + 1 + sizeof *hdr);
    memset (hdr, 0, sizeof *hdr);

    hdr->mtimns = NANOTIME (statbuf.st_mtim);
    hdr->ctimns = NANOTIME (statbuf.st_ctim);
    hdr->atimns = NANOTIME (statbuf.st_atim);
    hdr->size   = statbuf.st_size;
    hdr->stmode = statbuf.st_mode;
    hdr->ownuid = statbuf.st_uid;
    hdr->owngid = statbuf.st_gid;
    hdr->nameln = pathlen + 1;

    memcpy (hdr->name, path, pathlen);
    hdr->name[pathlen] = 0;

    /*
     * Mountpoints get an empty directory instead of descending into the filesystem.
     */
    if ((dirstat != NULL) && (statbuf.st_dev != dirstat->st_dev)) {
        fprintf (stderr, "ftbackup: skipping mountpoint %s\n", hdr->name);
        return write_mountpoint (hdr);
    }

    /*
     * Write it out to saveset based on what its type is.
     */
    if (S_ISREG (statbuf.st_mode)) return write_regular (hdr, &statbuf);
    if (S_ISDIR (statbuf.st_mode)) return write_directory (hdr, &statbuf);
    if (S_ISLNK (statbuf.st_mode)) return write_symlink (hdr);
    return write_special (hdr, statbuf.st_rdev);
}

/**
 * @brief Write a regular file out to the saveset.
 */
int FTBWriter::write_regular (Header *hdr, struct stat const *statbuf)
{
    int fd, ok, rc;
    struct stat statend;
    uint32_t bs, i;
    uint64_t len, ofs;
    uint8_t *buf;

    /*
     * Only back up regular files changed since the -since option value.
     */
    if (hdr->ctimns < opt_since) return 1;

    /*
     * If same inode as a previous file, say this is an hardlink.
     */
    for (i = 0; i < inodesused; i ++) {
        if (inodeslist[i] == statbuf->st_ino) break;
    }
    if ((i < inodesused) && (stat (inodesname[i], &statend) >= 0) && (NANOTIME (statend.st_mtim) == NANOTIME (statbuf->st_mtim))) {
        uint32_t fileno = i;
        hdr->flags = HFL_HDLINK;
        write_header (hdr);
        write_raw (&fileno, sizeof fileno, 0);
        return 1;
    }

    /*
     * Make sure we can open the file before writing header.
     */
    fd = open (hdr->name, O_RDONLY | O_NOATIME | ioptions);
    if (fd < 0) {
        fprintf (stderr, "ftbackup: open(%s) error: %s\n", hdr->name, strerror (errno));
        return 0;
    }

    /*
     * Write header out to saveset.
     */
    write_header (hdr);

    /*
     * Write file contents to saveset.
     * We always write the exact number of bytes shown in the header.
     */
    bs = 1 << l2bs;
    ok = 1;
    for (ofs = 0; ofs < hdr->size; ofs += rc) {
        len = hdr->size - ofs;
        if (len > 16384) len = 16384;
        len = (len + PAGESIZE - 1) & -PAGESIZE;
        rc  = posix_memalign ((void **)&buf, PAGESIZE, len);
        if (rc != 0) NOMEM ();
        if (ok) {
            rc = read (fd, buf, len);
            if (rc <= 0) {
                fprintf (stderr, "ftbackup: read(%s@0x%llX) error: %s\n", hdr->name, ofs, ((rc == 0) ? "end of file" : strerror (errno)));
                ok = 0;
            }
        }
        if (!ok) {
            memset (buf, 0x69, len);
            rc = len;
        }
        write_queue (buf, rc, 1);
    }

    /*
     * Save inode number in case there is an hardlink to the file later.
     */
    i = hdr->fileno;
    if (inodessize <= i) {
        if (inodessize == 0) inodesdevno = statbuf->st_dev;
        do inodessize += inodessize / 2 + 10;
        while (inodessize <= i);
        inodeslist = (ino_t *) realloc (inodeslist, inodessize * sizeof *inodeslist);
        inodesname = (char **) realloc (inodesname, inodessize * sizeof *inodesname);
        if ((inodeslist == NULL) || (inodesname == NULL)) NOMEM ();
    }
    if (inodesdevno != statbuf->st_dev) {
        fprintf (stderr, "ftbackup: %s different dev_t %lu than %lu\n", hdr->name, statbuf->st_dev, inodesdevno);
        ok = 0;
    } else {
        while (inodesused <= i) {
            inodeslist[inodesused] = 0;
            inodesname[inodesused] = NULL;
            inodesused ++;
        }
        inodeslist[i] = statbuf->st_ino;
        inodesname[i] = strdup (hdr->name);
        if (inodesname[i] == NULL) NOMEM ();
    }

    /*
     * If different mtime than when started, output warning message.
     */
    if (fstat (fd, &statend) < 0) {
        fprintf (stderr, "ftbackup: fstat(%s) at end of backup error: %s\n", hdr->name, strerror (errno));
    } else if (NANOTIME (statend.st_mtim) > NANOTIME (statbuf->st_mtim)) {
        fprintf (stderr, "ftbackup: file %s modified during processing\n", hdr->name);
    }

    /*
     * All done, good or bad.
     */
    close (fd);

    return ok;
}

/**
 * @brief Write a directory out to the saveset followed by all the files in the directory.
 */
int FTBWriter::write_directory (Header *hdr, struct stat const *statbuf)
{
    char *buf, *path;
    char const *name;
    int i, len, longest, nents, ok;
    struct dirent *de, **names;

    /*
     * Read and sort the directory contents.
     */
    nents = scandir (hdr->name, &names, NULL, alphasort);
    if (nents < 0) {
        fprintf (stderr, "ftbackup: scandir(%s) error: %s\n", hdr->name, strerror (errno));
        return 0;
    }

    /*
     * Total up the length of all the filenames in the directory.
     * Also get length of the longest name.
     */
    longest = 0;
    hdr->size = 0;
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;
        if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
            len = strlen (name) + 1;
            hdr->size += len;
            if (longest < len) longest = len;
        }
    }

    /*
     * Write directory contents to saveset iff changed since the -since option value.
     */
    if (hdr->ctimns >= opt_since) {

        /*
         * Write header out with hdr->size = total of all those sizes with null terminators.
         */
        write_header (hdr);

        /*
         * Write all the null terminated filenames out as the contents of the directory.
         */
        buf = (char *) malloc (hdr->size);
        if (buf == NULL) NOMEM ();
        len = 0;
        for (i = 0; i < nents; i ++) {
            de = names[i];
            name = de->d_name;
            if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
                strcpy (buf + len, name);
                len += strlen (buf + len) + 1;
            }
        }
        if (len > 0) write_queue (buf, len, 1);
    }

    /*
     * Set up a buffer that will hold the directory path + the longest filename therein.
     */
    len = strlen (hdr->name);
    if ((len > 0) && (hdr->name[len-1] == '/')) -- len;
    path = (char *) alloca (len + longest + 2);
    memcpy (path, hdr->name, len);
    path[len++] = '/';

    /*
     * Write all the files in the directory out to the saveset.
     */
    ok = 1;
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;
        if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
            strcpy (path + len, name);
            ok &= write_file (path, statbuf);
        }
        free (de);
    }
    free (names);

    return ok;
}

/**
 * @brief Write a directory used as a mountpoint, ie, write it as being empty.
 */
int FTBWriter::write_mountpoint (Header *hdr)
{
    /*
     * Only back up mountpoints changed since the -since option value.
     */
    if (hdr->ctimns < opt_since) return 1;

    /*
     * Write header out with hdr->size = 0 indicating an empty directory.
     */
    hdr->size = 0;
    write_header (hdr);
    return 1;
}

/**
 * @brief Write a symlink out to the saveset.
 */
int FTBWriter::write_symlink (Header *hdr)
{
    char *buf;
    int rc;

    if (hdr->ctimns < opt_since) return 1;

    buf = (char *) malloc (hdr->size + 1);
    while (1) {
        if (buf == NULL) NOMEM ();
        rc = readlink (hdr->name, buf, hdr->size + 1);
        if (rc < 0) {
            fprintf (stderr, "ftbackup: readlink(%s) error: %s\n", hdr->name, strerror (errno));
            return 0;
        }
        if ((uint32_t) rc <= hdr->size) break;
        hdr->size += hdr->size / 2 + 1;
        buf = (char *) realloc (buf, hdr->size + 1);
    }

    hdr->size = rc;
    write_header (hdr);
    write_queue (buf, rc, 0);
    return 1;
}

/**
 * @brief Write a special file (mknod) out to the saveset.
 */
int FTBWriter::write_special (Header *hdr, dev_t strdev)
{
    if (hdr->ctimns < opt_since) return 1;
    hdr->size = sizeof strdev;
    write_header (hdr);
    write_raw (&strdev, sizeof strdev, 0);
    return 1;
}

/**
 * @brief Write a saveset file's header block to the saveset.
 */
void FTBWriter::write_header (Header *hdr)
{
    memcpy (hdr->magic, HEADER_MAGIC, 8);
    hdr->fileno = ++ lastfileno;
    if (hdr->nameln > 0) {
        if (opt_verbose || ((opt_verbsec > 0) && (time (NULL) >= lastverbsec + opt_verbsec))) {
            lastverbsec = time (NULL);
            print_header (stderr, hdr);
        }
    }
    write_raw (hdr, (ulong_t)(&hdr->name[hdr->nameln]) - (ulong_t)hdr, 1);
}

/**
 * @brief Write some uncompressed bytes to the saveset, could be header or data.
 * @param buf = address of data to write
 * @param len = length of data to write
 * @param hdr = 1 iff it is a file header; else it is data
 */
void FTBWriter::write_raw (void const *buf, uint32_t len, int hdr)
{
    void *mem;

    if (len > 0) {
        mem = malloc (len);
        if (mem == NULL) NOMEM ();
        memcpy (mem, buf, len);
        write_queue (mem, len, hdr ? -1 : 0);
    }
}

/**
 * @brief Queue the malloc()d buffer to be written to saveset.
 * @param buf = malloc()d buffer, will be freed after processing
 * @param len = number of bytes from buf to write
 * @param dty = 1: compress data bytes before writing
 *              0: write data bytes as given without compression
 *             -1: write file header bytes as given without compression
 */
void FTBWriter::write_queue (void *buf, uint32_t len, int dty)
{
    uint32_t i;

    pthread_mutex_lock (&main2compr_mutex);
    while (main2compr_used >= MAIN2COMPR_NSLOTS) {
        pthread_cond_wait (&main2compr_cond, &main2compr_mutex);
    }
    i = (main2compr_next + main2compr_used) % MAIN2COMPR_NSLOTS;
    main2compr_slots[i].buf = buf;
    main2compr_slots[i].len = len;
    main2compr_slots[i].dty = dty;
    main2compr_used ++;
    pthread_cond_signal (&main2compr_cond);
    pthread_mutex_unlock (&main2compr_mutex);
}

/**
 * @brief Take data from main2compr queue, optionally compress,
 *        and put in blocks for compr2write queue.
 */
void *FTBWriter::compr_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->compr_thread ();
}
void *FTBWriter::compr_thread ()
{
    Block *block;
    int dty, rc;
    uint32_t bs, i, len;
    void *buf;

    block = NULL;
    bs    = 1 << l2bs;

    while (1) {

        /*
         * Get data of arbitrary length from main thread to process.
         */
        pthread_mutex_lock (&main2compr_mutex);
        while (main2compr_used == 0) {
            pthread_cond_wait (&main2compr_cond, &main2compr_mutex);
        }
        i = main2compr_next;
        buf = main2compr_slots[i].buf;
        len = main2compr_slots[i].len;
        dty = main2compr_slots[i].dty;
        main2compr_next = (i + 1) % MAIN2COMPR_NSLOTS;
        if (-- main2compr_used == MAIN2COMPR_NSLOTS - 1) {
            pthread_cond_signal (&main2compr_cond);
        }
        pthread_mutex_unlock (&main2compr_mutex);

        /*
         * Maybe compress it to fixed-size blocks.
         */
        if (dty > 0) {
            if (!zisopen) {
                void    *no = zstrm.next_out;
                uint32_t ao = zstrm.avail_out;
                memset (&zstrm, 0, sizeof zstrm);
                rc = deflateInit (&zstrm, Z_DEFAULT_COMPRESSION);
                if (rc != Z_OK) INTERR (deflateInit, rc);
                zstrm.next_out  = (Bytef *) no;
                zstrm.avail_out = ao;
                zisopen = 1;
            }
            zstrm.next_in  = (Bytef *) buf;
            zstrm.avail_in = len;
            while (zstrm.avail_in > 0) {
                if (zstrm.avail_out == 0) {
                    block = malloc_block ();
                }
                rc = deflate (&zstrm, Z_NO_FLUSH);
                if (rc != Z_OK) INTERR (deflate, rc);
                if (zstrm.avail_out == 0) {
                    queue_block (block);
                    block = NULL;
                }
            }
        }

        /*
         * Otherwise just copy as is to fixed-size blocks.
         */
        else {
            if (zisopen) {
                do {
                    if (zstrm.avail_out == 0) {
                        block = malloc_block ();
                    }
                    rc = deflate (&zstrm, Z_FINISH);
                    if (zstrm.avail_out == 0) {
                        queue_block (block);
                        block = NULL;
                    }
                } while (rc == Z_OK);
                if (rc != Z_STREAM_END) INTERR (deflate, rc);
                rc = deflateEnd (&zstrm);
                if (rc != Z_OK) INTERR (deflateEnd, rc);
                zisopen = 0;
            }

            // special case of null buffer means we are done!
            if (len == 0) break;

            // there is data, copy to block buffer
            zstrm.next_in  = (Bytef *) buf;
            zstrm.avail_in = len;
            do {
                // maybe we need a new output block
                if (zstrm.avail_out == 0) {
                    block = malloc_block ();
                }

                // if first header in the block, save its offset for recoveries
                if ((dty < 0) && (block->hdroffs == 0)) {
                    block->hdroffs = (ulong_t)zstrm.next_out - (ulong_t)block;
                }

                // we only care about setting block->hdroffs for the first byte of the header
                dty = 0;

                // see how much we can copy out and copy it out
                len = zstrm.avail_out;
                if (len > zstrm.avail_in) len = zstrm.avail_in;
                memcpy (zstrm.next_out, zstrm.next_in, len);

                // update counters to account for the copy
                zstrm.next_out  += len;
                zstrm.avail_out -= len;
                zstrm.next_in   += len;
                zstrm.avail_in  -= len;

                // if output block full, queue it for writing
                if (zstrm.avail_out == 0) {
                    queue_block (block);
                    block = NULL;
                }
            } while (zstrm.avail_in > 0);
        }

        /*
         * Either way, all done with input buffer.
         */
        free (buf);
    }

    /*
     * End of saveset, pad and queue final block.
     */
    if (zstrm.avail_out != 0) {
        memset (zstrm.next_out, 0xFF, zstrm.avail_out);
        queue_block (block);
    }
    queue_block (NULL);

    return NULL;
}

/**
 * @brief Malloc a block and set up data area descriptor.
 *        They are page aligned so O_DIRECT will work.
 */
Block *FTBWriter::malloc_block ()
{
    Block *block;
    int rc;
    uint32_t bs;

    bs = 1 << l2bs;
    rc = posix_memalign ((void **)&block, PAGESIZE, bs);
    if (rc != 0) NOMEM ();
    memset (block, 0, sizeof *block);
    zstrm.next_out  = block->data;
    zstrm.avail_out = (ulong_t)block + bs - (ulong_t)block->data;
    return block;
}

/**
 * @brief Queue block to write_thread().
 * @param block = block to queue or NULL for end-of-saveset marker
 */
void FTBWriter::queue_block (Block *block)
{
    uint32_t i;

    if (block != NULL) {
        memcpy (block->magic, BLOCK_MAGIC, 8);
        block->seqno  = ++ lastseqno;
        block->l2bs   = l2bs;
        block->xorgc  = xorgc;
        block->xorsc  = xorsc;
        block->chksum = 0;
        block->chksum = - checksumdata (block, 1 << l2bs);
    }

    pthread_mutex_lock (&compr2write_mutex);
    while (compr2write_used == COMPR2WRITE_NSLOTS) {
        pthread_cond_wait (&compr2write_cond, &compr2write_mutex);
    }
    i = (compr2write_next + compr2write_used) % COMPR2WRITE_NSLOTS;
    compr2write_slots[i] = block;
    if (++ compr2write_used == 1) {
        pthread_cond_signal (&compr2write_cond);
    }
    pthread_mutex_unlock (&compr2write_mutex);
}

/**
 * @brief Dequeue blocks from compr_thread() and write to saveset.
 */
void *FTBWriter::write_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->write_thread ();
}
void *FTBWriter::write_thread ()
{
    Block *block, *xorblock;
    int rc;
    uint32_t bs, i, ofs, seqno;
    uint8_t oldxorbc;

    bs = 1 << l2bs;

    xorblocks = (Block **) alloca (xorgc * sizeof *xorblocks);
    memset (xorblocks, 0, xorgc * sizeof *xorblocks);

    while (1) {

        /*
         * Dequeue a block, waiting if queue is empty.
         */
        pthread_mutex_lock (&compr2write_mutex);
        while (compr2write_used == 0) {
            pthread_cond_wait (&compr2write_cond, &compr2write_mutex);
        }
        i = compr2write_next;
        block = compr2write_slots[i];
        compr2write_next = (i + 1) % COMPR2WRITE_NSLOTS;
        if (-- compr2write_used == COMPR2WRITE_NSLOTS - 1) {
            pthread_cond_signal (&compr2write_cond);
        }
        pthread_mutex_unlock (&compr2write_mutex);

        /*
         * Special case of NULL block means we are done!
         */
        if (block == NULL) break;

        /*
         * It should be valid.
         */
        if (!blockisvalid (block)) {
            fprintf (stderr, "ftbackup: saveset block %u not valid\n", block->seqno);
            abort ();
        }

        /*
         * Write block to saveset file.
         */
        for (ofs = 0; ofs < bs; ofs += rc) {
            rc = write (ssfd, ((uint8_t *)block) + ofs, bs - ofs);
            if (rc <= 0) {
                fprintf (stderr, "ftbackup: write() saveset error: %s\n", (rc == 0) ? "end of file" : strerror (errno));
                exit (EX_SSIO);
            }
        }

        /*
         * If we are generating XOR blocks, XOR the data block into the XOR block.
         */
        if ((xorgc > 0) && (xorsc > 0)) {
            seqno = block->seqno;
            i = (seqno - 1) % xorgc;
            if (((seqno - 1) / xorgc) % xorsc == 0) {
                xorblocks[i] = block;
                block->xorbc = 1;
            } else {
                xorblock = xorblocks[i];
                oldxorbc = xorblock->xorbc;
                xorblockdata (xorblock, block, bs);
                xorblock->xorbc = ++ oldxorbc;
            }
            if (seqno % (xorgc * xorsc) == 0) {
                flush_xor_blocks ();
            }
        } else {
            free (block);
        }
    }

    flush_xor_blocks ();
    xorblocks = NULL;

    return NULL;
}

/**
 * @brief Write XOR blocks to saveset file.
 */
void FTBWriter::flush_xor_blocks ()
{
    Block *block;
    int rc;
    uint32_t bs, i, ofs;

    bs = 1 << l2bs;
    for (i = 0; i < xorgc; i ++) {
        block = xorblocks[i];
        if (block != NULL) {
            memcpy (block->magic, BLOCK_MAGIC, 8);
            block->xorno  = lastxorno + i + 1;
            block->chksum = 0;
            block->chksum = - checksumdata (block, bs);
            if (!blockbaseisvalid (block)) {
                fprintf (stderr, "ftbackup: xor block %u not valid\n", block->xorno);
                abort ();
            }
            for (ofs = 0; ofs < bs; ofs += rc) {
                rc = write (ssfd, ((uint8_t *)block) + ofs, bs - ofs);
                if (rc <= 0) {
                    fprintf (stderr, "ftbackup: write() saveset error: %s\n", (rc == 0) ? "end of file" : strerror (errno));
                    exit (EX_SSIO);
                }
            }
            free (block);
            xorblocks[i] = NULL;
        }
    }
    lastxorno += xorgc;
}
