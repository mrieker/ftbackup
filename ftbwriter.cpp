
#include "ftbackup.h"
#include "ftbwriter.h"

static void printthreadcputime (char const *name)
{
    int rc;
    struct timespec tp;

    rc = clock_gettime (CLOCK_THREAD_CPUTIME_ID, &tp);
    if (rc < 0) SYSERRNO (clock_gettime);
    fprintf (stderr, "ftbackup: thread %s cpu time %lu.%.9lu\n", name, (ulong_t) tp.tv_sec, (ulong_t) tp.tv_nsec);
}

FTBWriter::FTBWriter ()
{
    opt_verbose = 0;
    ioptions    = 0;
    ooptions    = 0;
    opt_verbsec = 0;
    opt_segsize = 0;
    opt_since   = 0;

    freeblocks  = NULL;
    xorblocks   = NULL;
    zisopen     = 0;
    ssbasename  = NULL;
    sssegname   = NULL;
    inodesname  = NULL;
    inodesdevno = 0;
    inodeslist  = NULL;
    ssfd        = -1;
    lastverbsec = 0;
    inodessize  = 0;
    inodesused  = 0;
    lastfileno  = 0;
    lastseqno   = 0;
    lastxorno   = 0;
    thissegno   = 0;
    byteswrittentoseg = 0;
    memset (&zstrm, 0, sizeof zstrm);

    comprqueue = SlotQueue<ComprSlot> ();
    encrqueue  = SlotQueue<Block *> ();
    writequeue = SlotQueue<Block *> ();
}

FTBWriter::~FTBWriter ()
{
    Block *b;
    ComprSlot cs;
    uint32_t i;

    while ((b = freeblocks) != NULL) {
        freeblocks = *(Block **)b;
        free (b);
    }

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

    if (ssfd >= 0) {
        close (ssfd);
    }

    if (zisopen) {
        zstrm.avail_out = 0;
        zstrm.next_out  = NULL;
        deflateEnd (&zstrm);
    }

    while (comprqueue.trydequeue (&cs)) {
        free (cs.buf);
    }

    while (encrqueue.trydequeue (&b)) {
        free (b);
    }

    while (writequeue.trydequeue (&b)) {
        free (b);
    }
}

/**
 * @brief Write saveset.
 * @param ssname = name of saveset (or "-" to write to stdout).
 * @param rootpath = root path of files to back up
 * @returns exit status code
 */
int FTBWriter::write_saveset (char const *ssname, char const *rootpath)
{
    bool ok;
    Header hdr;
    int rc;
    pthread_t compr_thandl, encr_thandl, write_thandl;

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
        if (opt_segsize > 0) {
            ssbasename = ssname;
            sssegname  = (char *) alloca (strlen (ssbasename) + 10);
            ssname     = sssegname;
            sprintf (sssegname, "%s.%.*u", ssbasename, SEGNODECDIGS, ++ thissegno);
        }
        ssfd = open (ssname, O_WRONLY | O_CREAT | O_TRUNC | ooptions, 0666);
        if (ssfd < 0) {
            fprintf (stderr, "ftbackup: creat(%s) saveset error: %s\n", ssname, strerror (errno));
            return EX_SSIO;
        }
        if (isatty (ssfd)) {
            fprintf (stderr, "ftbackup: cannot write saveset to a tty\n");
            close (ssfd);
            ssfd = -1;
            return EX_SSIO;
        }
    }

    /*
     * Create compression, encryption and writing threads.
     */
    rc = pthread_create (&compr_thandl, NULL, compr_thread_wrapper, this);
    if (rc != 0) SYSERR (pthread_create, rc);
    if (cripter != NULL) {
        rc = pthread_create (&encr_thandl, NULL, encr_thread_wrapper, this);
        if (rc != 0) SYSERR (pthread_create, rc);
    }
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

    printthreadcputime ("readfiles");

    /*
     * Wait for threads to finish.
     */
    rc = pthread_join (compr_thandl, NULL);
    if (rc != 0) SYSERR (pthread_join, rc);
    if (cripter != NULL) {
        rc = pthread_join (encr_thandl, NULL);
        if (rc != 0) SYSERR (pthread_join, rc);
    }
    rc = pthread_join (write_thandl, NULL);
    if (rc != 0) SYSERR (pthread_join, rc);

    /*
     * Finished, close saveset file.
     */
    if ((strcmp (ssname, "-") != 0) && (close (ssfd) < 0)) {
        ssfd = -1;
        fprintf (stderr, "ftbackup: close() saveset error: %s\n", strerror (errno));
        return EX_SSIO;
    }
    ssfd = -1;

    return ok ? EX_OK : EX_FILIO;
}

/**
 * @brief Write the given file/directory/whatever to the currently open saveset.
 * @param path = path to file
 * @returns true: no file io errors
 *         false: some io errors
 */
bool FTBWriter::write_file (char const *path, struct stat const *dirstat)
{
    Header *hdr;
    int pathlen;
    struct stat statbuf;

    /*
     * See what type of thing we are dealing with.
     */
    if (lstat (path, &statbuf) < 0) {
        fprintf (stderr, "ftbackup: lstat(%s) error: %s\n", path, strerror (errno));
        return false;
    }

    /*
     * We can't restore sockets so no sense trying to save them.
     */
    if (S_ISSOCK (statbuf.st_mode)) {
        fprintf (stderr, "ftbackup: skipping socket %s\n", path);
        return true;
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
bool FTBWriter::write_regular (Header *hdr, struct stat const *statbuf)
{
    bool ok;
    int fd, rc;
    struct stat statend;
    uint32_t bs, i;
    uint64_t len, ofs;
    uint8_t *buf;

    /*
     * Only back up regular files changed since the -since option value.
     */
    if (hdr->ctimns < opt_since) return true;

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
        write_raw (&fileno, sizeof fileno, false);
        return true;
    }

    /*
     * Make sure we can open the file before writing header.
     */
    fd = open (hdr->name, O_RDONLY | O_NOATIME | ioptions);
    if (fd < 0) fd = open (hdr->name, O_RDONLY | ioptions);
    if (fd < 0) {
        fprintf (stderr, "ftbackup: open(%s) error: %s\n", hdr->name, strerror (errno));
        return true;
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
    ok = true;
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
                ok = false;
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
        ok = false;
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
bool FTBWriter::write_directory (Header *hdr, struct stat const *statbuf)
{
    bool ok;
    char *bbb, *buf, *path;
    char const *name;
    int i, j, len, longest, nents, pathlen;
    struct dirent *de, **names;

    /*
     * Read and sort the directory contents.
     */
    nents = scandir (hdr->name, &names, NULL, alphasort);
    if (nents < 0) {
        fprintf (stderr, "ftbackup: scandir(%s) error: %s\n", hdr->name, strerror (errno));
        return false;
    }

    /*
     * Get length of the longest name including null terminator.
     */
    longest = 0;
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;
        if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
            len = strlen (name) + 1;
            if (longest < len) longest = len;
        }
    }

    /*
     * Set up a buffer that will hold the directory path + the longest filename therein.
     */
    pathlen = strlen (hdr->name);
    if ((pathlen > 0) && (hdr->name[pathlen-1] == '/')) -- pathlen;
    path = (char *) alloca (pathlen + longest + 2);
    memcpy (path, hdr->name, pathlen);
    path[pathlen++] = '/';

    /*
     * Total up length needed for all filenames in the directory.
     */
    path[pathlen] = 0;
    hdr->size = 0;
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;
        if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
            for (j = 0; j < 255; j ++) if (path[pathlen+j] != name[j]) break;
            len = strlen (name + j) + 1;
            hdr->size += len + 1;
            memcpy (path + pathlen + j, name + j, len);
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
         * Each name is written as:
         *   <number-of-beginning-chars-same-as-last><different-chars-on-end><null>
         */
        buf = (char *) malloc (hdr->size);
        if (buf == NULL) NOMEM ();
        bbb = buf;
        path[pathlen] = 0;
        for (i = 0; i < nents; i ++) {
            de = names[i];
            name = de->d_name;
            if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
                for (j = 0; j < 255; j ++) if (path[pathlen+j] != name[j]) break;
                len = strlen (name + j) + 1;
                *(bbb ++) = j;
                memcpy (bbb, name + j, len);
                bbb += len;
                memcpy (path + pathlen + j, name + j, len);
            }
        }
        if ((ulong_t) (bbb - buf) != hdr->size) abort ();
        if (bbb > buf) write_queue (buf, bbb - buf, 1);
    }

    /*
     * Write all the files in the directory out to the saveset.
     */
    ok = true;
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;
        if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
            strcpy (path + pathlen, name);
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
bool FTBWriter::write_mountpoint (Header *hdr)
{
    /*
     * Only back up mountpoints changed since the -since option value.
     */
    if (hdr->ctimns < opt_since) return true;

    /*
     * Write header out with hdr->size = 0 indicating an empty directory.
     */
    hdr->size = 0;
    write_header (hdr);
    return true;
}

/**
 * @brief Write a symlink out to the saveset.
 */
bool FTBWriter::write_symlink (Header *hdr)
{
    char *buf;
    int rc;

    if (hdr->ctimns < opt_since) return true;

    buf = (char *) malloc (hdr->size + 1);
    while (true) {
        if (buf == NULL) NOMEM ();
        rc = readlink (hdr->name, buf, hdr->size + 1);
        if (rc < 0) {
            fprintf (stderr, "ftbackup: readlink(%s) error: %s\n", hdr->name, strerror (errno));
            return false;
        }
        if ((uint32_t) rc <= hdr->size) break;
        hdr->size += hdr->size / 2 + 1;
        buf = (char *) realloc (buf, hdr->size + 1);
    }

    hdr->size = rc;
    write_header (hdr);
    write_queue (buf, rc, 0);
    return true;
}

/**
 * @brief Write a special file (mknod) out to the saveset.
 */
bool FTBWriter::write_special (Header *hdr, dev_t strdev)
{
    if (hdr->ctimns < opt_since) return true;
    hdr->size = sizeof strdev;
    write_header (hdr);
    write_raw (&strdev, sizeof strdev, false);
    return true;
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
            print_header (stderr, hdr, hdr->name);
        }
    }
    write_raw (hdr, (ulong_t)(&hdr->name[hdr->nameln]) - (ulong_t)hdr, true);
}

/**
 * @brief Write some uncompressed bytes to the saveset, could be header or data.
 * @param buf = address of data to write
 * @param len = length of data to write
 * @param hdr = true iff it is a file header; else it is data
 */
void FTBWriter::write_raw (void const *buf, uint32_t len, bool hdr)
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
    ComprSlot slot;

    slot.buf = buf;
    slot.len = len;
    slot.dty = dty;

    comprqueue.enqueue (slot);
}

/**
 * @brief Take data from comprqueue queue, optionally compress,
 *        and put in blocks for writequeue queue.
 */
void *FTBWriter::compr_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->compr_thread ();
}
void *FTBWriter::compr_thread ()
{
    Block *block;
    int dty, rc;
    ComprSlot slot;
    uint32_t bs, len;
    void *buf;

    block = NULL;
    bs    = 1 << l2bs;

    if (xorgc > 0) {
        xorblocks = (Block **) calloc (xorgc, sizeof *xorblocks);
        if (xorblocks == NULL) NOMEM ();
    }

    while (true) {

        /*
         * Get data of arbitrary length from main thread to process.
         */
        slot = comprqueue.dequeue ();
        buf  = slot.buf;
        len  = slot.len;
        dty  = slot.dty;

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
                zisopen = true;
            }
            zstrm.next_in  = (Bytef *) buf;
            zstrm.avail_in = len;
            while (zstrm.avail_in > 0) {
                if (zstrm.avail_out == 0) {
                    block = malloc_block ();
                    zstrm.next_out  = block->data;
                    zstrm.avail_out = (ulong_t)block + bs - (ulong_t)block->data;
                }
                rc = deflate (&zstrm, Z_NO_FLUSH);
                if (rc != Z_OK) INTERR (deflate, rc);
                if (zstrm.avail_out == 0) {
                    queue_data_block (block);
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
                        zstrm.next_out  = block->data;
                        zstrm.avail_out = (ulong_t)block + bs - (ulong_t)block->data;
                    }
                    rc = deflate (&zstrm, Z_FINISH);
                    if (zstrm.avail_out == 0) {
                        queue_data_block (block);
                        block = NULL;
                    }
                } while (rc == Z_OK);
                if (rc != Z_STREAM_END) INTERR (deflate, rc);
                rc = deflateEnd (&zstrm);
                if (rc != Z_OK) INTERR (deflateEnd, rc);
                zisopen = false;
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
                    zstrm.next_out  = block->data;
                    zstrm.avail_out = (ulong_t)block + bs - (ulong_t)block->data;
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
                    queue_data_block (block);
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
        queue_data_block (block);
    }

    /*
     * Flush final XOR blocks and tell writer thread to terminate.
     */
    queue_data_block (NULL);

    if (xorblocks != NULL) {
        free (xorblocks);
        xorblocks = NULL;
    }

    printthreadcputime ("compress");

    return NULL;
}

/**
 * @brief Fill in data block header and queue block to write_thread().
 * @param block = data block to queue
 */
void FTBWriter::queue_data_block (Block *block)
{
    Block *xorblock;
    uint32_t bs, i, oldxorbc;

    if (block != NULL) {
        bs = 1 << l2bs;

        /*
         * Fill in data block header.
         */
        memcpy (block->magic, BLOCK_MAGIC, 8);
        block->seqno  = ++ lastseqno;
        block->l2bs   = l2bs;
        block->xorgc  = xorgc;
        block->xorsc  = xorsc;
        block->chksum = 0;
        block->chksum = - checksumdata (block, bs);

        /*
         * If we are generating XOR blocks, XOR the data block into the XOR block.
         */
        if ((xorgc > 0) && (xorsc > 0)) {

            /*
             * XOR the data block into the XOR block.
             * Malloc one if there isn't one there already.
             */
            i = (lastseqno - 1) % xorgc;
            xorblock = xorblocks[i];
            if (xorblock == NULL) {
                xorblock = malloc_block ();
                memcpy (xorblock, block, bs);
                xorblock->xorbc = 1;
                xorblocks[i] = xorblock;
            } else {
                oldxorbc = xorblock->xorbc;
                xorblockdata (xorblock, block, bs);
                xorblock->xorbc = ++ oldxorbc;
            }

            /*
             * Now that we have XOR'd the data, queue block for writing.
             */
            queue_block (block);

            /*
             * If that was the last data block of the XOR span, queue XOR blocks for writing.
             */
            if (lastseqno % (xorgc * xorsc) == 0) {
                queue_xor_blocks ();
            }
        } else {

            /*
             * No XOR blocks, queue data block for writing.
             */
            queue_block (block);
        }
    } else {

        /*
         * End of data block stream,
         * if doing XORs, flush final XOR blocks first,
         * then post the EOF marker.
         */
        if ((xorgc > 0) && (xorsc > 0)) {
            queue_xor_blocks ();
        }

        queue_block (NULL);
    }
}

/**
 * @brief Queue XOR blocks to be written to saveset file.
 */
void FTBWriter::queue_xor_blocks ()
{
    Block *block;
    uint32_t bs, i;

    bs = 1 << l2bs;
    for (i = 0; i < xorgc; i ++) {
        block = xorblocks[i];
        if (block != NULL) {
            memcpy (block->magic, BLOCK_MAGIC, 8);
            block->xorno  = lastxorno + i + 1;
            block->chksum = 0;
            block->chksum = - checksumdata (block, bs);
            queue_block (block);
            xorblocks[i] = NULL;
        }
    }
    lastxorno += xorgc;
}

/**
 * @brief Queue block with header filled in to write_thread().
 * @param block = block to queue or NULL for end-of-saveset marker
 */
void FTBWriter::queue_block (Block *block)
{
    if (cripter != NULL) {
        encrqueue.enqueue (block);
    } else {
        writequeue.enqueue (block);
    }
}

/**
 * @brief Malloc a block.  They are page aligned so O_DIRECT will work.
 */
Block *FTBWriter::malloc_block ()
{
    Block *block;
    int rc;
    uint32_t bs;
    uint64_t tmp;

    asm volatile (
        "   movq    %1,%0       \n"
        "   .p2align 3          \n"
        "1:                     \n"
        "   testq   %0,%0       \n"
        "   je      2f          \n"
        "   movq    (%0),%2     \n"
        "   lock                \n"
        "   cmpxchgq %2,%1      \n"
        "   jne     1b          \n"
        "2:                     \n"
        : "=a" (block), "+m" (freeblocks), "=r" (tmp)
        : : "cc");

    if (block == NULL) {
        bs = 1 << l2bs;
        rc = posix_memalign ((void **)&block, PAGESIZE, bs);
        if (rc != 0) NOMEM ();
    }
    memset (block, 0, sizeof *block);
    return block;
}

/**
 * @brief Dequeue blocks from compr_thread(), encrypt, then queue to write_thread().
 */
void *FTBWriter::encr_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->encr_thread ();
}
void *FTBWriter::encr_thread ()
{
    Block *block;
    FILE *noncefile;
    uint32_t bsq, cbs, i;
    uint64_t *array;

    cbs = cripter->BlockSize ();

    if (sizeof block->nonce != 16) abort ();
    if (offsetof (Block, nonce) != 16) abort ();

    noncefile = fopen ("/dev/urandom", "r");
    if (noncefile == NULL) {
        fprintf (stderr, "ftbackup: open(/dev/urandom) error: %s\n", strerror (errno));
        abort ();
    }

    /*
     * Get block size in quadwords.
     */
    bsq = 1 << (l2bs - 3);

    /*
     * Get a block to encrypt.
     */
    while ((block = encrqueue.dequeue ()) != NULL) {

        /*
         * Fill in the nonce with a random number to salt the encryption.
         */
        if (fread (&block->nonce[sizeof block->nonce-cbs], cbs, 1, noncefile) != 1) {
            fprintf (stderr, "read(/dev/urandom) error: %s\n", strerror (errno));
            abort ();
        }

        /*
         * Use nonce for init vector and encrypt the rest of the block.
         * Leave magic number and everything else before nonce in plain text.
         */
        // enc[i] = encrypt ( clr[i] ^ enc[i-1] )
        array = (uint64_t *) block;
        switch (cbs) {
            case 8: {
                for (i = 4; i < bsq; i ++) {
                    array[i] ^= array[i-1];
                    cripter->ProcessAndXorBlock ((byte *) &array[i], NULL, (byte *) &array[i]);
                }
                break;
            }
            case 16: {
                for (i = 4; i < bsq; i += 2) {
                    array[i+0] ^= array[i-2];
                    array[i+1] ^= array[i-1];
                    cripter->ProcessAndXorBlock ((byte *) &array[i], NULL, (byte *) &array[i]);
                }
                break;
            }
            default: abort ();
        }

        /*
         * Queue the encrypted block for writing to the saveset.
         */
        writequeue.enqueue (block);
    }

    /*
     * Tell write_thread() it can close the saveset and exit.
     */
    writequeue.enqueue (NULL);

    fclose (noncefile);

    printthreadcputime ("encrypt");

    return NULL;
}

/**
 * @brief Dequeue blocks from compr_thread() or encr_thread() and write to saveset.
 */
void *FTBWriter::write_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->write_thread ();
}
void *FTBWriter::write_thread ()
{
    Block *block;

    while ((block = writequeue.dequeue ()) != NULL) {
        write_ssblock (block);
        free_block (block);
    }

    printthreadcputime ("writesaveset");

    return NULL;
}

/**
 * @brief Write block to saveset file.
 */
void FTBWriter::write_ssblock (Block *block)
{
    int rc;
    uint32_t bs, ofs;

    if ((opt_segsize > 0) && (byteswrittentoseg >= opt_segsize)) {
        if (close (ssfd) < 0) {
            fprintf (stderr, "ftbackup: close(%s) saveset error: %s\n", sssegname, strerror (errno));
            exit (EX_SSIO);
        }
        sprintf (sssegname, "%s.%.*u", ssbasename, SEGNODECDIGS, ++ thissegno);
        ssfd = open (sssegname, O_WRONLY | O_CREAT | O_TRUNC | ooptions, 0666);
        if (ssfd < 0) {
            fprintf (stderr, "ftbackup: creat(%s) saveset error: %s\n", sssegname, strerror (errno));
            exit (EX_SSIO);
        }
        byteswrittentoseg = 0;
    }

    bs = 1 << l2bs;
    for (ofs = 0; ofs < bs; ofs += rc) {
        rc = write (ssfd, ((uint8_t *)block) + ofs, bs - ofs);
        if (rc <= 0) {
            fprintf (stderr, "ftbackup: write() saveset error: %s\n", (rc == 0) ? "end of file" : strerror (errno));
            exit (EX_SSIO);
        }
    }
    byteswrittentoseg += bs;
}

/**
 * @brief Free block buffer for re-allocation via malloc_block().
 */
void FTBWriter::free_block (Block *block)
{
    asm volatile (
        "   movq    %0,%%rax        \n"
        "   .p2align 3              \n"
        "1:                         \n"
        "   movq    %%rax,(%1)      \n"
        "   lock                    \n"
        "   cmpxchgq %1,%0          \n"
        "   jne     1b              \n"
        : "+m" (freeblocks)
        : "r" (block)
        : "rax", "cc", "memory");
}

/**
 * @brief Slotted queue implementation.
 */

template <class T>
SlotQueue<T>::SlotQueue ()
{
    memset (slots, 0, sizeof slots);
    pthread_cond_init  (&cond,  NULL);
    pthread_mutex_init (&mutex, NULL);
    next = 0;
    used = 0;
}

template <class T>
void SlotQueue<T>::enqueue (T slot)
{
    uint32_t i;

    pthread_mutex_lock (&mutex);
    while (used == SQ_NSLOTS) {
        pthread_cond_wait (&cond, &mutex);
    }
    i = (next + used) % SQ_NSLOTS;
    slots[i] = slot;
    if (++ used == 1) {
        pthread_cond_signal (&cond);
    }
    pthread_mutex_unlock (&mutex);
}

template <class T>
bool SlotQueue<T>::trydequeue (T *slot)
{
    bool suc;
    uint32_t i;

    pthread_mutex_lock (&mutex);
    suc = (used != 0);
    if (suc) {
        i = next;
        *slot = slots[i];
        next = (i + 1) % SQ_NSLOTS;
        if (-- used == SQ_NSLOTS - 1) {
            pthread_cond_signal (&cond);
        }
    }
    pthread_mutex_unlock (&mutex);

    return suc;
}

template <class T>
T SlotQueue<T>::dequeue ()
{
    T slot;
    uint32_t i;

    pthread_mutex_lock (&mutex);
    while (used == 0) {
        pthread_cond_wait (&cond, &mutex);
    }
    i = next;
    slot = slots[i];
    next = (i + 1) % SQ_NSLOTS;
    if (-- used == SQ_NSLOTS - 1) {
        pthread_cond_signal (&cond);
    }
    pthread_mutex_unlock (&mutex);

    return slot;
}
