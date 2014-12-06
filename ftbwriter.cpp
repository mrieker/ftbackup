/**
 * @brief Write saveset by reading files from disk.
 */

//  Copyright (C) 2014, Mike Rieker, www.outerworldapps.com
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include "ftbackup.h"
#include "ftbwriter.h"

struct SkipName {
    SkipName *next;     // next line in this ~SKIPNAMES.FTB or next outer ~SKIPNAMES.FTB
    char const *dir;    // directory path the ~SKIPNAMES.FTB file is in (wildcard relative to this dir)
    char wild[1];       // wildcard line from the ~SKIPNAMES.FTB file
};

static uint32_t inspackeduint32 (char *buf, uint32_t idx, uint32_t val);
static bool skipbyname (SkipName *skipname, char const *path);
static uint64_t getruntime ();
static void printthreadcputime (char const *name);
static void printthreadruntime (char const *name, uint64_t runtime);

FTBWriter::FTBWriter ()
{
    opt_verbose    = 0;
    histdbname     = NULL;
    histssname     = NULL;
    ioptions       = 0;
    ooptions       = 0;
    opt_verbsec    = 0;
    opt_segsize    = 0;
    opt_since      = 0;

    xorblocks      = NULL;
    zisopen        = false;
    ssbasename     = NULL;
    sssegname      = NULL;
    inodesdevno    = 0;
    noncefile      = NULL;
    inodeslist     = NULL;
    ssfd           = -1;
    skipnames      = NULL;
    lastverbsec    = 0;
    inodessize     = 0;
    inodesused     = 0;
    lastfileno     = 0;
    lastseqno      = 0;
    lastxorno      = 0;
    thissegno      = 0;
    byteswrittentoseg = 0;
    inodesmtim     = NULL;
    memset (&zstrm, 0, sizeof zstrm);

    frbufqueue = SlotQueue<void *> ();
    comprqueue = SlotQueue<ComprSlot> ();
    frblkqueue = SlotQueue<Block *> ();
    histqueue  = SlotQueue<HistSlot> ();
    writequeue = SlotQueue<Block *> ();
}

FTBWriter::~FTBWriter ()
{
    Block *b;
    ComprSlot cs;
    HistSlot hs;
    uint32_t i;
    void *f;

    if (xorblocks != NULL) {
        for (i = 0; i < xorgc; i ++) {
            free (xorblocks[i]);
        }
        free (xorblocks);
    }

    if (noncefile != NULL) {
        fclose (noncefile);
    }

    if (inodeslist != NULL) {
        free (inodeslist);
    }

    if (ssfd >= 0) {
        close (ssfd);
    }

    if (inodesmtim != NULL) {
        free (inodesmtim);
    }

    if (zisopen) {
        zstrm.avail_out = 0;
        zstrm.next_out  = NULL;
        deflateEnd (&zstrm);
    }

    while (frbufqueue.trydequeue (&f)) {
        free (f);
    }

    while (comprqueue.trydequeue (&cs)) {
        free (cs.buf);
    }

    while (frblkqueue.trydequeue (&b)) {
        free (b);
    }

    while (histqueue.trydequeue (&hs)) {
        if (hs.fname != NULL) free (hs.fname);
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
    Header endhdr;
    int i, rc;
    pthread_t compr_thandl, hist_thandl, write_thandl;
    void *buf;

    maybesetdefaulthasher ();

    /*
     * Create saveset file.
     */
    ssbasename = ssname;
    if (strcmp (ssname, "-") == 0) {
        ssfd = STDOUT_FILENO;
        fflush (stdout);
    } else {
        if (opt_segsize > 0) {
            sssegname = (char *) alloca (strlen (ssbasename) + SEGNODECDIGS + 4);
            ssname    = sssegname;
            sprintf (sssegname, "%s%.*u", ssbasename, SEGNODECDIGS, ++ thissegno);
        }
        ssfd = open (ssname, O_WRONLY | O_CREAT | O_TRUNC | ooptions, 0666);
        if (ssfd < 0) {
            fprintf (stderr, "ftbackup: creat(%s) saveset error: %s\n", ssname, mystrerr (errno));
            return EX_SSIO;
        }
    }

    if (isatty (ssfd)) {
        fprintf (stderr, "ftbackup: cannot write saveset to a tty\n");
        if (strcmp (ssname, "-") != 0) close (ssfd);
        return EX_SSIO;
    }

    /*
     * Create compression, history and writing threads.
     */
    rc = pthread_create (&compr_thandl, NULL, compr_thread_wrapper, this);
    if (rc != 0) SYSERR (pthread_create, rc);
    if (histdbname != NULL) {
        rc = pthread_create (&hist_thandl, NULL, hist_thread_wrapper, this);
        if (rc != 0) SYSERR (pthread_create, rc);
    }
    rc = pthread_create (&write_thandl, NULL, write_thread_wrapper, this);
    if (rc != 0) SYSERR (pthread_create, rc);

    /*
     * Start counting runtime for this thread.
     */
    rft_runtime = - getruntime ();

    /*
     * Malloc some page-aligned buffers for reading from files.
     */
    for (i = 0; i < SQ_NSLOTS; i ++) {
        rc = posix_memalign ((void **)&buf, PAGESIZE, FILEIOSIZE);
        if (rc != 0) NOMEM ();
        frbufqueue.enqueue (buf);
    }

    /*
     * Process root path of files to back up.
     */
    ok = write_file (rootpath, NULL);

    /*
     * Write EOF header so reader knows it got the whole saveset.
     */
    memset (&endhdr, 0, sizeof endhdr);
    write_header (&endhdr);

    /*
     * Tell the other threads to flush and terminate.
     */
    write_queue (NULL, 0, 0);

    /*
     * Stop counting runtime for this thread and print it.
     */
    rft_runtime += getruntime ();
    printthreadruntime ("read", rft_runtime);

    /*
     * Wait for other threads to finish.
     */
    rc = pthread_join (compr_thandl, NULL);
    if (rc != 0) SYSERR (pthread_join, rc);
    if (histdbname != NULL) {
        rc = pthread_join (hist_thandl, NULL);
        if (rc != 0) SYSERR (pthread_join, rc);
    }
    rc = pthread_join (write_thandl, NULL);
    if (rc != 0) SYSERR (pthread_join, rc);

    /*
     * Finished, close saveset file.
     */
    if ((strcmp (ssname, "-") != 0) && (close (ssfd) < 0)) {
        ssfd = -1;
        fprintf (stderr, "ftbackup: close() saveset error: %s\n", mystrerr (errno));
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
    bool ok;
    char *xattrslistbuf;
    Header *hdr;
    int rc;
    struct stat statbuf;
    uint32_t hdrnamealloc, i, j, pathlen, xattrslistlen, xattrsvalslen;

    /*
     * See what type of thing we are dealing with.
     */
    if (tfs->fslstat (path, &statbuf) < 0) {
        fprintf (stderr, "ftbackup: lstat(%s) error: %s\n", path, mystrerr (errno));
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
     * Get extended attributes length, if any.
     */
    rc = tfs->fsllistxattr (path, NULL, 0);
    if (rc < 0) {
        if (errno != ENOTSUP) {
            fprintf (stderr, "ftbackup: llistxattr(%s) error: %s\n", path, strerror (errno));
            return false;
        }
        rc = 0;
    }
    xattrslistlen = rc;
    xattrsvalslen = 0;
    xattrslistbuf = NULL;
    if (xattrslistlen > 0) {
        xattrslistbuf = (char *) alloca (xattrslistlen);
        rc = tfs->fsllistxattr (path, xattrslistbuf, xattrslistlen);
        if (rc < 0) {
            fprintf (stderr, "ftbackup: llistxattr(%s) error: %s\n", path, strerror (errno));
            return false;
        }
        xattrslistlen = rc;
        for (i = 0; i < xattrslistlen; i += ++ j) {
            rc = tfs->fslgetxattr (path, xattrslistbuf + i, NULL, 0);
            if (rc < 0) {
                fprintf (stderr, "ftbackup: lgetxattr(%s,%s) error: %s\n", path, xattrslistbuf + i, strerror (errno));
                return false;
            }
            xattrsvalslen += rc + 5;
            j = strlen (xattrslistbuf + i);
        }
        xattrslistlen += 5;
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
    hdrnamealloc = pathlen + 1 + xattrslistlen + xattrsvalslen;
    hdr = (Header *) malloc (hdrnamealloc + sizeof *hdr);
    if (hdr == NULL) NOMEM ();
    memset (hdr, 0, sizeof *hdr);

    hdr->mtimns = NANOTIME (statbuf.st_mtim);
    hdr->ctimns = NANOTIME (statbuf.st_ctim);
    hdr->atimns = NANOTIME (statbuf.st_atim);
    hdr->size   = statbuf.st_size;
    hdr->stmode = statbuf.st_mode;
    hdr->ownuid = statbuf.st_uid;
    hdr->owngid = statbuf.st_gid;

    memcpy (hdr->name, path, pathlen);
    hdr->name[pathlen++] = 0;

    if (xattrslistlen > 0) {
        hdr->flags = HFL_XATTRS;
        xattrslistlen -= 5;
        pathlen = inspackeduint32 (hdr->name, pathlen, xattrslistlen);
        memcpy (hdr->name + pathlen, xattrslistbuf, xattrslistlen);
        pathlen += xattrslistlen;
        for (i = 0; i < xattrslistlen; i += ++ j) {
            rc = tfs->fslgetxattr (path, xattrslistbuf + i, hdr->name + pathlen + 5, hdrnamealloc - pathlen - 5);
            if (rc < 0) {
                fprintf (stderr, "ftbackup: lgetxattr(%s,%s) error: %s\n", path, xattrslistbuf + i, strerror (errno));
                free (hdr);
                return false;
            }
            j = inspackeduint32 (hdr->name, pathlen, (uint32_t) rc);
            memmove (hdr->name + j, hdr->name + pathlen + 5, rc);
            pathlen = j + rc;
            j = strlen (xattrslistbuf + i);
        }
    }

    hdr->nameln = pathlen;

    /*
     * Mountpoints get an empty directory instead of descending into the filesystem.
     */
    if ((dirstat != NULL) && (statbuf.st_dev != dirstat->st_dev)) {
        fprintf (stderr, "ftbackup: skipping mountpoint %s\n", hdr->name);
        ok = write_mountpoint (hdr);
    }

    /*
     * Otherwise, write it out to saveset based on what its type is.
     */
    else if (S_ISREG (statbuf.st_mode)) ok = write_regular (hdr, &statbuf);
    else if (S_ISDIR (statbuf.st_mode)) ok = write_directory (hdr, &statbuf);
    else if (S_ISLNK (statbuf.st_mode)) ok = write_symlink (hdr);
                                   else ok = write_special (hdr, statbuf.st_rdev);

    free (hdr);
    return ok;
}

static uint32_t inspackeduint32 (char *buf, uint32_t idx, uint32_t val)
{
    while (val > 0x7F) {
        buf[idx++] = val | 0x80;
        val >>= 7;
    }
    buf[idx++] = val;
    return idx;
}

/**
 * @brief Write a regular file out to the saveset.
 */
bool FTBWriter::write_regular (Header *hdr, struct stat const *statbuf)
{
    bool ok;
    int fd, rc;
    struct stat statend;
    uint32_t i, plen;
    uint64_t len, ofs;
    void *buf;

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
    if ((i < inodesused) && (inodesmtim[i] == hdr->mtimns)) {
        uint32_t fileno = i;
        hdr->flags = HFL_HDLINK;
        write_header (hdr);
        write_raw (&fileno, sizeof fileno, false);
        return true;
    }

    /*
     * Make sure we can open the file before writing header.
     */
    fd = tfs->fsopen (hdr->name, O_RDONLY | O_NOATIME | ioptions);
    if (fd < 0) fd = tfs->fsopen (hdr->name, O_RDONLY | ioptions);
    if (fd < 0) {
        fprintf (stderr, "ftbackup: open(%s) error: %s\n", hdr->name, mystrerr (errno));
        return false;
    }

    ofs = 0;
    ok  = true;

    /*
     * Write out header.
     */
    write_header (hdr);

    /*
     * Write file contents to saveset.
     * We always write the exact number of bytes shown in the header.
     */
    for (; ofs < hdr->size; ofs += rc) {
        len  = hdr->size - ofs;                     // number of bytes to end-of-file
        if (len > FILEIOSIZE) len = FILEIOSIZE;     // never more than this at a time
        plen = (len + PAGESIZE - 1) & -PAGESIZE;    // page-aligned length for O_DIRECT
        rft_runtime += getruntime ();
        buf  = frbufqueue.dequeue ();               // page-aligned buffer for O_DIRECT
        rft_runtime -= getruntime ();
        if (ok) {
            rc = tfs->fspread (fd, buf, plen, ofs);
            if (rc < 0) {
                fprintf (stderr, "ftbackup: pread(%s, ..., %llu, %llu) error: %s\n",
                        hdr->name, len, ofs, mystrerr (errno));
                ok = false;
                memset (buf, 0x69, len);
                rc = len;
            } else if ((uint32_t) rc < len) {
                fprintf (stderr, "ftbackup: pread(%s, ..., %llu, %llu) error: only got %d byte%s\n",
                        hdr->name, len, ofs, rc, ((rc == 1) ? "" : "s"));
                ok = false;
                memset ((uint8_t *) buf + rc, 0x69, len - rc);
            } else {
                rc = len;  // might be more if plen > len and file has been extended since hdr->size was set
            }
        } else {
            memset (buf, 0x69, len);
            rc = len;
        }
        write_queue (buf, rc, 2);
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
        inodesmtim = (uint64_t *) realloc (inodesmtim, inodessize * sizeof *inodesmtim);
        if ((inodeslist == NULL) || (inodesmtim == NULL)) NOMEM ();
    }
    if (inodesdevno != statbuf->st_dev) {
        fprintf (stderr, "ftbackup: %s different dev_t %lu than %lu\n", hdr->name, statbuf->st_dev, inodesdevno);
        ok = false;
    } else {
        while (inodesused <= i) {
            inodeslist[inodesused] = 0;
            inodesmtim[inodesused] = 0;
            inodesused ++;
        }
        inodeslist[i] = statbuf->st_ino;
        inodesmtim[i] = hdr->mtimns;
    }

    /*
     * If different mtime than when started, output warning message.
     */
    if (tfs->fsfstat (fd, &statend) < 0) {
        fprintf (stderr, "ftbackup: fstat(%s) at end of backup error: %s\n", hdr->name, mystrerr (errno));
    } else if (NANOTIME (statend.st_mtim) > hdr->mtimns) {
        fprintf (stderr, "ftbackup: file %s modified during processing\n", hdr->name);
    }

    /*
     * All done, good or bad.
     */
    tfs->fsclose (fd);

    return ok;
}

/**
 * @brief Write a directory out to the saveset followed by all the files in the directory.
 */
bool FTBWriter::write_directory (Header *hdr, struct stat const *statbuf)
{
    bool ok;
    char *bbb, *buf, *p, *path, *q, *snbuf, *snname;
    char const *name;
    int i, j, len, longest, nents, pathlen, snfd, snlen;
    SkipName *saveskipnames, *skipname;
    struct dirent *de, **names;
    struct stat statend;

    ok = true;

    /*
     * Read and sort the directory contents.
     */
    nents = tfs->fsscandir (hdr->name, &names, NULL, alphasort);
    if (nents < 0) {
        fprintf (stderr, "ftbackup: scandir(%s) error: %s\n", hdr->name, mystrerr (errno));
        return false;
    }

    /*
     * If directory contains ~SKIPNAMES.FTB, add those names as wildcards
     * to list of files to skip.
     */
    saveskipnames = skipnames;
    snbuf  = NULL;
    snname = (char *) alloca (strlen (hdr->name) + 16);
    sprintf (snname, "%s/~SKIPNAMES.FTB", hdr->name);
    snfd = open (snname, O_RDONLY);
    if ((snfd < 0) && (errno != ENOENT)) {
        fprintf (stderr, "ftbackup: open(%s) error: %s\n", snname, mystrerr (errno));
        ok = false;
    }
    if (snfd >= 0) {
        snlen = lseek (snfd, 0, SEEK_END);
        snbuf = (char *) mmap (NULL, snlen, PROT_READ, MAP_SHARED, snfd, 0);
        if (snbuf == (char *) MAP_FAILED) {
            fprintf (stderr, "ftbackup: mmap(%s) error: %s\n", snname, mystrerr (errno));
            snbuf = NULL;
            ok = false;
        }
        close (snfd);
    }
    if (snbuf != NULL) {
        for (p = snbuf; p < snbuf + snlen; p = ++ q) {
            q = strchr (p, '\n');
            if (q == NULL) q = snbuf + snlen;
            if (q > p) {
                skipname = (SkipName *) alloca (q - p + sizeof *skipname);
                skipname->next = skipnames;
                skipname->dir  = hdr->name;
                memcpy (skipname->wild, p, q - p);
                skipname->wild[q-p] = 0;
                skipnames = skipname;
            }
        }
        munmap (snbuf, snlen);
    }

    /*
     * If directory contains ~SKIPDIR.FTB pretend that is the only file it contains.
     */
    for (i = 0; i < nents; i ++) {
        de = names[i];
        if (strcmp (de->d_name, "~SKIPDIR.FTB") == 0) {
            fprintf (stderr, "ftbackup: skipping directory %s for containing ~SKIPDIR.FTB\n", hdr->name);
            for (i = 0; i < nents; i ++) {
                if (names[i] != de) free (names[i]);
            }
            nents = 1;
            names[0] = de;
            break;
        }
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
        if (hdr->size > 0) {
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
            write_queue (buf, bbb - buf, 1);
        }
    }

    /*
     * Write the files in the directory out to the saveset.
     */
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;
        if ((strcmp (name, ".") != 0) && (strcmp (name, "..") != 0)) {
            strcpy (path + pathlen, name);
            if (!skipbyname (skipnames, path)) {
                ok &= write_file (path, statbuf);
            }
        }
        free (de);
    }
    free (names);
    skipnames = saveskipnames;

    /*
     * If different mtime than when started, output warning message.
     */
    if (tfs->fsstat (hdr->name, &statend) < 0) {
        fprintf (stderr, "ftbackup: stat(%s) at end of backup error: %s\n", hdr->name, mystrerr (errno));
    } else if (NANOTIME (statend.st_mtim) > hdr->mtimns) {
        fprintf (stderr, "ftbackup: directory %s modified during processing\n", hdr->name);
    }

    return ok;
}

/**
 * @brief See if a file is in the current ~SKIPNAMES.FTB list.
 * @param skipname = list of names to skip
 * @param path = full path being tested
 * @returns true: file matched by skip list
 *         false: file not in skip list
 */
static bool skipbyname (SkipName *skipname, char const *path)
{
    for (; skipname != NULL; skipname = skipname->next) {

        // get directory the ~SKIPNAMES.FTB file was in
        // it is the directory its wild applies to
        int sndirlen = strlen (skipname->dir);
        while ((sndirlen > 0) && (skipname->dir[sndirlen-1] == '/')) -- sndirlen;

        // see if the file being tested is in that directory
        if (memcmp (skipname->dir, path, sndirlen) != 0) continue;
        if (path[sndirlen] != '/') continue;
        do sndirlen ++;
        while (path[sndirlen] == '/');

        // if name matches wildcard, skip the file
        char const *wild = skipname->wild;
        if (wildcardmatch (wild, path + sndirlen)) return true;
    }

    return false;
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
        rc = tfs->fsreadlink (hdr->name, buf, hdr->size + 1);
        if (rc < 0) {
            fprintf (stderr, "ftbackup: readlink(%s) error: %s\n", hdr->name, mystrerr (errno));
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
 * @param dty = 1: compress data bytes before writing then call free()
 *              2: compress data bytes before writing then call frbufqueue.enqueue()
 *              0: write data bytes as given without compression
 *             -1: write file header bytes as given without compression
 */
void FTBWriter::write_queue (void *buf, uint32_t len, int dty)
{
    ComprSlot slot;

    slot.buf = buf;
    slot.len = len;
    slot.dty = dty;

    rft_runtime += getruntime ();
    comprqueue.enqueue (slot);
    rft_runtime -= getruntime ();
}

/**
 * @brief Take data from comprqueue queue, optionally compress,
 *        and put in blocks for writequeue queue.
 */
#define CHECKROOM do { \
    if (zstrm.avail_out == 0) {                                         \
        block = frblkqueue.dequeue ();                                  \
        memset (block, 0, sizeof *block);                               \
        block->seqno    = ++ lastseqno;                                 \
        zstrm.next_out  = block->data;                                  \
        zstrm.avail_out = (ulong_t)block + bs - (ulong_t)block->data;   \
    }                                                                   \
} while (false)

#define CHECKFULL do { \
    if (zstrm.avail_out == 0) {                                         \
        writequeue.enqueue (block);                                     \
        block = NULL;                                                   \
    }                                                                   \
} while (false)

void *FTBWriter::compr_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->compr_thread ();
}
void *FTBWriter::compr_thread ()
{
    Block *block;
    HistSlot hs;
    int dty, i, rc;
    ComprSlot slot;
    uint32_t bs, len;
    void *buf;

    /*
     * Get some blocks to write to.
     */
    for (i = 0; i < SQ_NSLOTS; i ++) {
        block = malloc_block ();
        frblkqueue.enqueue (block);
    }

    block = NULL;
    bs    = (1 << l2bs) - hashsize ();

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
                CHECKROOM;
                rc = deflate (&zstrm, Z_NO_FLUSH);
                if (rc != Z_OK) INTERR (deflate, rc);
                CHECKFULL;
            }
        }

        /*
         * Otherwise just copy as is to fixed-size blocks.
         */
        else {
            if (zisopen) {
                do {
                    CHECKROOM;
                    rc = deflate (&zstrm, Z_FINISH);
                    CHECKFULL;
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
                CHECKROOM;

                // see if it is a file header
                if (dty < 0) {

                    // if first header in the block, save its offset for recoveries
                    if (block->hdroffs == 0) {
                        block->hdroffs = (ulong_t)zstrm.next_out - (ulong_t)block;
                    }

                    // if writing history, queue to history writing thread
                    if ((histdbname != NULL) && (((Header *)buf)->nameln > 0)) {
                        hs.fname = strdup (((Header *)buf)->name);
                        hs.seqno = block->seqno;
                        if (hs.fname == NULL) NOMEM ();
                        histqueue.enqueue (hs);
                    }
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
                CHECKFULL;
            } while (zstrm.avail_in > 0);
        }

        /*
         * Either way, all done with input buffer.
         */
        if (dty == 2) frbufqueue.enqueue (buf);
                               else free (buf);
    }

    /*
     * Pad and queue final data block.
     */
    if (zstrm.avail_out != 0) {
        memset (zstrm.next_out, 0xFF, zstrm.avail_out);
        writequeue.enqueue (block);
    }

    /*
     * Tell history thread to close database and exit.
     */
    if (histdbname != NULL) {
        hs.fname = NULL;
        hs.seqno = 0;
        histqueue.enqueue (hs);
    }

    /*
     * Tell writer thread to write final blocks out.
     */
    writequeue.enqueue (NULL);

    printthreadcputime ("compress");

    return NULL;
}

void *FTBWriter::hist_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->hist_thread ();
}
void *FTBWriter::hist_thread ()
{
    char *sqlerr;
    char const *fieldname;
    HistSlot hs;
    int filesel_fileid, rc;
    sqlite3 *histdb;
    sqlite3_int64 fileid, ssid;
    sqlite3_stmt *fileins, *filesel, *inststmt, *savestmt;
    uint64_t flushist, now, wht_runtime;

    wht_runtime = - getruntime ();

    /*
     * Create and/or open database.
     */
    rc = sqlite3_open_v2 (histdbname, &histdb, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_open(%s) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    rc = sqlite3_busy_timeout (histdb, SQL_TIMEOUT_MS);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite_busy_timeout(%s, %d) error: %s\n", histdbname, SQL_TIMEOUT_MS, sqlite3_errmsg (histdb));
    }

    /*
     * If newly created, create tables and triggers.
     */
    rc = sqlite3_exec (histdb,
        "CREATE TABLE IF NOT EXISTS files (fileid INTEGER PRIMARY KEY AUTOINCREMENT, name NOT NULL UNIQUE);"

        "CREATE TABLE IF NOT EXISTS savesets (ssid INTEGER PRIMARY KEY AUTOINCREMENT, name NOT NULL, time NOT NULL);"

        "CREATE TABLE IF NOT EXISTS instances ("
            "fileid NOT NULL, "
            "ssid NOT NULL, "
            "seqno NOT NULL, "
            "PRIMARY KEY (fileid, ssid));"

        "CREATE INDEX IF NOT EXISTS idx_file_name ON files (name);"
        "CREATE INDEX IF NOT EXISTS idx_inst_fileid ON instances (fileid);"
        "CREATE INDEX IF NOT EXISTS idx_inst_ssid ON instances (ssid);"

        // disallow changing inter-table id numbers to maintain references
        "CREATE TRIGGER IF NOT EXISTS upd_fileid_block "
            "BEFORE UPDATE OF fileid ON files "
            "BEGIN SELECT RAISE(ABORT,'files.fileid immutable'); END;"

        "CREATE TRIGGER IF NOT EXISTS upd_ssid_block "
            "BEFORE UPDATE OF ssid ON savesets "
            "BEGIN SELECT RAISE(ABORT,'savesets.ssid immutable'); END;"

        "CREATE TRIGGER IF NOT EXISTS upd_inst_fileid_block "
            "BEFORE UPDATE OF fileid ON instances "
            "BEGIN SELECT RAISE(ABORT,'instances.fileid immutable'); END;"

        "CREATE TRIGGER IF NOT EXISTS upd_inst_ssid_block "
            "BEFORE UPDATE OF ssid ON instances "
            "BEGIN SELECT RAISE(ABORT,'instances.ssid immutable'); END;"

        // delete associated instance records whenever a file record is deleted
        "CREATE TRIGGER IF NOT EXISTS del_file_dangling_insts "
            "AFTER DELETE ON files "
            "BEGIN DELETE FROM instances WHERE instances.fileid=OLD.fileid; END;"

        // delete associated instance records when a saveset record is deleted
        "CREATE TRIGGER IF NOT EXISTS del_save_dangling_insts "
            "AFTER DELETE ON savesets "
            "BEGIN DELETE FROM instances WHERE instances.ssid=OLD.ssid; END;"

        // delete associated unreferenced file record when an instance record is deleted
        "CREATE TRIGGER IF NOT EXISTS del_unrefd_files "
            "AFTER DELETE ON instances "
            "WHEN NOT EXISTS (SELECT * FROM instances WHERE instances.fileid=OLD.fileid) "
            "BEGIN DELETE FROM files WHERE files.fileid=OLD.fileid; END",

        NULL, NULL, &sqlerr);

    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_exec(%s, CREATE TABLE) error: %s\n", histdbname, sqlerr);
        sqlite3_free (sqlerr);
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    /*
     * Write saveset record and get its rowid number.
     */
    rc = sqlite3_prepare_v2 (histdb, 
            "INSERT INTO savesets (name,time) VALUES (?1,CURRENT_TIMESTAMP)", -1, &savestmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }
    rc = sqlite3_bind_text (savestmt, 1, (histssname == NULL) ? ssbasename : histssname, -1, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) INTERR (sqlite3_bind_text, rc);
    rc = sqlite3_step (savestmt);
    if (rc != SQLITE_DONE) {
        fprintf (stderr, "ftbackup: sqlite3_step(%s, INSERT INTO savesets) error: %s\n", histdbname, sqlerr);
        sqlite3_free (sqlerr);
        sqlite3_close (histdb);
        exit (EX_HIST);
    }
    sqlite3_finalize (savestmt);
    ssid = sqlite3_last_insert_rowid (histdb);

    /*
     * Pre-compile SQL statements used in processing loop.
     */
    rc = sqlite3_prepare_v2 (histdb, 
            "SELECT fileid FROM files WHERE name=?1",
            -1, &filesel, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s, SELECT FROM files) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }
    for (filesel_fileid = 0;; filesel_fileid ++) {
        fieldname = sqlite3_column_name (filesel, filesel_fileid);
        if (strcmp (fieldname, "fileid") == 0) break;
    }

    rc = sqlite3_prepare_v2 (histdb, 
            "INSERT INTO files (name) VALUES (?1)",
            -1, &fileins, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s, INSERT INTO files) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    rc = sqlite3_prepare_v2 (histdb, 
            "INSERT OR IGNORE INTO instances (fileid,ssid,seqno) VALUES (?1,?2,?3)",
            -1, &inststmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s, INSERT INTO instances) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    /*
     * Lock database for efficiency.
     */
    rc = sqlite3_exec (histdb, "BEGIN", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_exec(%s, BEGIN) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        exit (EX_HIST);
    }

    /*
     * Keep processing incoming names until we get and end marker.
     */
    flushist = 0;
    while (true) {

        /*
         * Dequeue an entry to process.
         */
        wht_runtime += getruntime ();
        hs = histqueue.dequeue ();
        wht_runtime -= getruntime ();
        if (hs.fname == NULL) break;

        /*
         * See if record already exists with the filename.
         * If so, get the corresponding fileid.
         * If not, insert new record and get corresponding fileid.
         */
        sqlite3_reset (filesel);
        rc = sqlite3_bind_text (filesel, 1, hs.fname, -1, SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) INTERR (sqlite3_bind_text,  rc);
        rc = sqlite3_step (filesel);
        switch (rc) {

            /*
             * Record already exists, get the fileid.
             */
            case SQLITE_ROW: {
                fileid = sqlite3_column_int64 (filesel, filesel_fileid);
                break;
            }

            /*
             * No such record, insert record then get the fileid.
             */
            case SQLITE_DONE: {
                sqlite3_reset (fileins);
                rc = sqlite3_bind_text (fileins, 1, hs.fname, -1, SQLITE_TRANSIENT);
                if (rc != SQLITE_OK) INTERR (sqlite3_bind_text,  rc);
                rc = sqlite3_step (fileins);
                if (rc != SQLITE_DONE) {
                    fprintf (stderr, "ftbackup: sqlite3_step(%s, INSERT INTO files) error: %s\n", histdbname, sqlite3_errmsg (histdb));
                    sqlite3_close (histdb);
                    exit (EX_HIST);
                }
                fileid = sqlite3_last_insert_rowid (histdb);
                break;
            }

            /*
             * Some error.
             */
            default: {
                fprintf (stderr, "ftbackup: sqlite3_step(%s, SELECT FROM files) error: %s\n", histdbname, sqlite3_errmsg (histdb));
                sqlite3_close (histdb);
                exit (EX_HIST);
            }
        }

        /*
         * Insert an instance record saying the file is backed up to the saveset.
         */
        sqlite3_reset (inststmt);

        rc = sqlite3_bind_int64 (inststmt, 1, fileid);
        if (rc != SQLITE_OK) INTERR (sqlite3_bind_int64, rc);
        rc = sqlite3_bind_int64 (inststmt, 2, ssid);
        if (rc != SQLITE_OK) INTERR (sqlite3_bind_int64, rc);
        rc = sqlite3_bind_int64 (inststmt, 3, (uint64_t) hs.seqno);
        if (rc != SQLITE_OK) INTERR (sqlite3_bind_int64, rc);

        rc = sqlite3_step (inststmt);
        if (rc != SQLITE_DONE) {
            fprintf (stderr, "ftbackup: sqlite3_step(%s, INSERT INTO instances) error: %s\n", histdbname, sqlite3_errmsg (histdb));
            sqlite3_close (histdb);
            exit (EX_HIST);
        }

        /*
         * Entry all processed.
         */
        free (hs.fname);

        /*
         * Flush history from time to time so listing will work.
         */
        now = getruntime ();
        if (flushist + (SQL_TIMEOUT_MS * 500000ULL) < now) {
            rc = sqlite3_exec (histdb, "COMMIT", NULL, NULL, NULL);
            if (rc != SQLITE_OK) {
                fprintf (stderr, "ftbackup: sqlite3_exec(%s, COMMIT) error: %s\n", histdbname, sqlite3_errmsg (histdb));
                exit (EX_HIST);
            }
            rc = sqlite3_exec (histdb, "BEGIN", NULL, NULL, NULL);
            if (rc != SQLITE_OK) {
                fprintf (stderr, "ftbackup: sqlite3_exec(%s, BEGIN) error: %s\n", histdbname, sqlite3_errmsg (histdb));
                exit (EX_HIST);
            }
            flushist = now;
        }
    }

    /*
     * All done, flush database changes to file.
     */
    rc = sqlite3_exec (histdb, "COMMIT", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_exec(%s, COMMIT) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        exit (EX_HIST);
    }

    /*
     * Close database.
     */
    sqlite3_finalize (fileins);
    sqlite3_finalize (filesel);
    sqlite3_finalize (inststmt);
    sqlite3_close (histdb);
    wht_runtime += getruntime ();

    printthreadruntime ("history", wht_runtime);

    return NULL;
}

/**
 * @brief Dequeue data blocks from compr_thread(), xor, hash, encrypt, then write to saveset.
 */
void *FTBWriter::write_thread_wrapper (void *ftbw)
{
    return ((FTBWriter *) ftbw)->write_thread ();
}
void *FTBWriter::write_thread ()
{
    Block *block;
    uint32_t i;
    uint64_t wst_runtime;

    wst_runtime = - getruntime ();

    /*
     * Maybe set up array of XOR blocks.
     */
    if (xorgc > 0) {
        xorblocks = (Block **) malloc (xorgc * sizeof *xorblocks);
        if (xorblocks == NULL) NOMEM ();
        for (i = 0; i < xorgc; i ++) {
            xorblocks[i] = block = malloc_block ();
            block->xorbc = 0;
        }
    }

    /*
     * Maybe get source of random numbers for the nonces.
     */
    if (cipher != NULL) {
        noncefile = fopen ("/dev/urandom", "r");
        if (noncefile == NULL) {
            fprintf (stderr, "ftbackup: open(/dev/urandom) error: %s\n", mystrerr (errno));
            abort ();
        }
    }

    /*
     * Process datablocks.
     */
    wst_runtime += getruntime ();
    while ((block = writequeue.dequeue ()) != NULL) {
        wst_runtime -= getruntime ();
        xor_data_block (block);
        frblkqueue.enqueue (block);
        wst_runtime += getruntime ();
    }
    wst_runtime -= getruntime ();

    /*
     * Flush final XOR blocks.
     */
    if (xorgc > 0) {
        hash_xor_blocks ();
        for (i = 0; i < xorgc; i ++) {
            free (xorblocks[i]);
        }
        free (xorblocks);
        xorblocks = NULL;
    }

    if (noncefile != NULL) {
        fclose (noncefile);
        noncefile = NULL;
    }

    wst_runtime += getruntime ();
    printthreadruntime ("write", wst_runtime);

    return NULL;
}

/**
 * @brief Malloc a block.  They are page aligned so O_DIRECT will work.
 */
Block *FTBWriter::malloc_block ()
{
    Block *block;
    int rc;
    uint32_t bs;

    bs = 1 << l2bs;
    rc = posix_memalign ((void **)&block, PAGESIZE, bs);
    if (rc != 0) NOMEM ();
    return block;
}

/**
 * @brief Fill in data block header, XOR it into XOR blocks, hash, encrypt and write to saveset.
 */
void FTBWriter::xor_data_block (Block *block)
{
    Block *xorblock;
    uint32_t bs, dataseqno, i, oldxorbc;

    bs = (1 << l2bs) - hashsize ();

    /*
     * Fill in data block header.
     */
    memcpy (block->magic, BLOCK_MAGIC, 8);
    block->l2bs  = l2bs;
    block->xorgc = xorgc;
    block->xorsc = xorsc;
    dataseqno = block->seqno;

    /*
     * If we are generating XOR blocks, XOR the data block into the XOR block.
     */
    if (xorgc > 0) {

        /*
         * XOR the data block into the XOR block.
         */
        i = (dataseqno - 1) % xorgc;
        xorblock = xorblocks[i];
        oldxorbc = xorblock->xorbc;
        if (oldxorbc == 0) {
            memcpy (xorblock, block, bs);
        } else {
            xorblockdata (xorblock, block, bs);
        }
        xorblock->xorbc = ++ oldxorbc;

        /*
         * Now that we have XOR'd the data, hash block and write to saveset.
         */
        hash_block (block);

        /*
         * If that was the last data block of the XOR span, hash XOR blocks and write to saveset.
         */
        if (dataseqno % (xorgc * xorsc) == 0) {
            hash_xor_blocks ();
        }
    } else {

        /*
         * No XOR blocks, hash data block and write to saveset.
         */
        hash_block (block);
    }
}

/**
 * @brief Hash XOR blocks and write to saveset.
 */
void FTBWriter::hash_xor_blocks ()
{
    Block *block;
    uint32_t i;

    for (i = 0; i < xorgc; i ++) {
        block = xorblocks[i];
        if (block->xorbc != 0) {
            memcpy (block->magic, BLOCK_MAGIC, 8);
            block->xorno = lastxorno + i + 1;
            hash_block (block);
            block->xorbc = 0;
        }
    }
    lastxorno += xorgc;
}

/**
 * @brief Maybe encrypt data, then hash it and write to saveset.
 */
void FTBWriter::hash_block (Block *block)
{
    uint32_t bs, bsq, cbs, i;
    uint64_t *array;

    cbs = 0;
    if (cipher != NULL) {

        /*
         * Fill in the nonce with a random number to salt the encryption.
         */
        cbs = cipher->BlockSize ();
        if (fread (&block->nonce[sizeof block->nonce-cbs], cbs, 1, noncefile) != 1) {
            fprintf (stderr, "read(/dev/urandom) error: %s\n", mystrerr (errno));
            abort ();
        }
    }

    /*
     * Hash the nonce and the data.
     */
    bs = (1U << l2bs) - hashsize ();
    hasher->Update ((uint8_t *)block, bs);
    hasher->Final  ((uint8_t *)block + bs);

    if (cipher != NULL) {

        /*
         * Make sure the 'i = 4' in the for loops below will work.
         *
         *  +-----------------------+
         *  |  16 bytes of magic    |
         *  |  ...and block number  |
         *  +-----------------------+
         *  |  16 bytes of nonce    |
         *  |                       |
         *  +-----------------------+
         *  |  data to encrypt ...  | i = 4: quadword[4]
         */
        if (sizeof block->nonce != 16) abort ();
        if (offsetof (Block, nonce) != 16) abort ();

        /*
         * Use nonce for init vector and encrypt the block, including the hash,
         * Leave magic number and everything else before nonce in plain text.
         */
        // CBC: enc[i] = encrypt ( clr[i] ^ enc[i-1] )
        array = (uint64_t *) block;
        bsq = 1U << (l2bs - 3);
        switch (cbs) {
            case 8: {
                for (i = 4; i < bsq; i ++) {
                    array[i] ^= array[i-1];
                    cipher->ProcessAndXorBlock ((byte *) &array[i], NULL, (byte *) &array[i]);
                }
                break;
            }
            case 16: {
                for (i = 4; i < bsq; i += 2) {
                    array[i+0] ^= array[i-2];
                    array[i+1] ^= array[i-1];
                    cipher->ProcessAndXorBlock ((byte *) &array[i], NULL, (byte *) &array[i]);
                }
                break;
            }
            default: abort ();
        }
    }

    /*
     * Write the possibly encrypted block to the saveset.
     */
    write_ssblock (block);
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
            fprintf (stderr, "ftbackup: close(%s) saveset error: %s\n", sssegname, mystrerr (errno));
            exit (EX_SSIO);
        }
        sprintf (sssegname, "%s%.*u", ssbasename, SEGNODECDIGS, ++ thissegno);
        ssfd = open (sssegname, O_WRONLY | O_CREAT | O_TRUNC | ooptions, 0666);
        if (ssfd < 0) {
            fprintf (stderr, "ftbackup: creat(%s) saveset error: %s\n", sssegname, mystrerr (errno));
            exit (EX_SSIO);
        }
        byteswrittentoseg = 0;
    }

    bs = 1 << l2bs;
    for (ofs = 0; ofs < bs; ofs += rc) {
        rc = write (ssfd, ((uint8_t *)block) + ofs, bs - ofs);
        if (rc <= 0) {
            fprintf (stderr, "ftbackup: write() saveset error: %s\n", (rc == 0) ? "end of file" : mystrerr (errno));
            exit (EX_SSIO);
        }
    }
    byteswrittentoseg += bs;
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

static uint64_t getruntime ()
{
    int rc;
    struct timespec tp;

    rc = clock_gettime (CLOCK_MONOTONIC, &tp);
    if (rc < 0) SYSERRNO (clock_gettime);
    return ((uint64_t) tp.tv_sec * 1000000000ULL) + tp.tv_nsec;
}

static void printthreadcputime (char const *name)
{
    int rc;
    struct timespec tp;

    rc = clock_gettime (CLOCK_THREAD_CPUTIME_ID, &tp);
    if (rc < 0) SYSERRNO (clock_gettime);
    fprintf (stderr, "ftbackup: thread %8s cpu time %6u.%.9u\n",
            name, (uint32_t) tp.tv_sec, (uint32_t) tp.tv_nsec);
}

static void printthreadruntime (char const *name, uint64_t runtime)
{
    int rc;
    struct timespec tp;

    rc = clock_gettime (CLOCK_THREAD_CPUTIME_ID, &tp);
    if (rc < 0) SYSERRNO (clock_gettime);
    fprintf (stderr, "ftbackup: thread %8s run time %6u.%.9u, cpu time %6u.%.9u\n",
            name, 
            (uint32_t) (runtime / 1000000000U), (uint32_t) (runtime % 1000000000U),
            (uint32_t) tp.tv_sec, (uint32_t) tp.tv_nsec);
}
