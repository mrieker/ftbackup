
#include "ftbackup.h"
#include "ftbreader.h"

struct DirTime {
    DirTime *next;
    uint64_t atimns;
    uint64_t mtimns;
    uint16_t nameln;
    char     name[1];
};

struct EndOfSSFile {
};

struct LostSSBlock {
    uint32_t seqno;
    LostSSBlock (uint32_t sn) { seqno = sn; }
};

static uint32_t extpackeduint32 (char const **ptr);
static uint32_t findnextsegno (char const *basename, uint32_t lastsegno);
static void slashtonull (char *buf, int len);
static void nulltoslash (char *buf, int len);
static void updatetimes (IFSAccess *ifsa, char const *name, uint64_t atimns, uint64_t mtimns);
static bool rmdirentry  (IFSAccess *ifsa, char const *dirname, char const *entname);

FTBReader::FTBReader ()
{
    opt_incrmntl  = false;
    opt_overwrite = false;
    opt_simrderrs = 0;

    xorblocks     = NULL;
    wprwrite      = 0;
    zisopen       = 0;
    inodesname    = NULL;
    ssbasename    = NULL;
    sssegname     = NULL;
    wprfile       = NULL;
    ssfd          = -1;
    linkedBlocks  = NULL;
    linkedRBlock  = NULL;
    memset (&ssstat, 0, sizeof ssstat);
    inodessize    = 0;
    inodesused    = 0;
    lastfileno    = 0;
    lastseqno     = 0;
    lastxorno     = 0;
    thissegno     = 0;
    pipepos       = 0;
    readoffset    = 0;
    gotxors       = NULL;
    memset (&zstrm,  0, sizeof zstrm);
}

/**
 * @brief Destructor:  Free off any malloc()d memory & close files.
 */
FTBReader::~FTBReader ()
{
    LinkedBlock *linkedBlock;
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
    if (wprfile != NULL) {
        fclose (wprfile);
    }
    if (ssfd >= 0) {
        close (ssfd);
    }
    while ((linkedBlock = linkedBlocks) != NULL) {
        linkedBlocks = linkedBlock->next;
        free (linkedBlock);
    }
    if (linkedRBlock != NULL) {
        free (linkedRBlock);
    }
    if (gotxors != NULL) {
        free (gotxors);
    }
    if (zisopen) {
        zstrm.avail_out = 0;
        zstrm.next_out  = NULL;
        inflateEnd (&zstrm);
    }
}

/**
 * @brief Read through saveset, listing and/or restoring all files therein.
 * @returns exit code
 */
int FTBReader::read_saveset (char const *ssname, char const *srcprefix, char const *dstprefix)
{
    Block *rblock;
    bool needhdr, ok, setimes, thisok;
    char const *xattrsnameend, *xattrsnameptr, *xattrsvaluptr;
    char *dstname, *dstnamebuf, *p, *srcprefixnul;
    DirTime *dirTime, *dirTimes;
    Header *hdr;
    int cmp, ssnamelen;
    uint32_t dstnameall, dstnamelen, dstprefixlen, hassegno, hdrall, lastfilenofinished;
    uint32_t skipped, srcnamelen, srcprefixlen, xattrslistlen, xattrsvalulen;

    maybesetdefaulthasher ();

    dstnameall = 0;
    dstnamebuf = NULL;
    hdr        = (Header *) malloc (sizeof *hdr);
    hdrall     = sizeof *hdr;

    if (hdr == NULL) NOMEM ();

    dstprefixlen = strlen (dstprefix);
    srcprefixlen = strlen (srcprefix);

    srcprefixnul = (char *) alloca (srcprefixlen);
    memcpy (srcprefixnul, srcprefix, srcprefixlen);
    slashtonull (srcprefixnul, srcprefixlen);

    if (strcmp (ssname, "-") == 0) {
        ssfd = STDIN_FILENO;
    } else {

        // if name ends in .<exactly-SEGNODECDIGS-digits> the digits are the starting segment number
        // and everything before that is the base name
        hassegno  = 0xFFFFFFFFU;
        ssnamelen = strlen (ssname);
        if ((ssnamelen > SEGNODECDIGS) && (ssname[ssnamelen-SEGNODECDIGS-1] == '.')) {
            hassegno = strtoul (ssname + ssnamelen - SEGNODECDIGS, &p, 10);
            if (*p != 0) hassegno = 0xFFFFFFFFU;
        }

        // if it has .<digits>, the user is giving us an explicit segment to start at
        // so we must be able to open that exact file
        if (hassegno != 0xFFFFFFFFU) {

            // the segment number is the <digits>
            thissegno = hassegno;

            // this segment name is whatever the whole string is that we were given
            sssegname = (char *) alloca (ssnamelen + 16);
            strcpy (sssegname, ssname);

            // the base name is everything before the .<digits>
            p = (char *) alloca (ssnamelen - SEGNODECDIGS);
            memcpy (p, ssname, ssnamelen - SEGNODECDIGS - 1);
            p[ssnamelen-SEGNODECDIGS-1] = 0;
            ssbasename = p;

            // we must be able to open that specific file as given by the user
            ssfd = open (ssname, O_RDONLY);
            if (ssfd < 0) {
                fprintf (stderr, "ftbackup: open(%s) error: %s\n", ssname, mystrerr (errno));
                return EX_SSIO;
            }
        } else {

            // if no .<digits>, the user could be saying to open that exact file
            // or for us to find the first segment file starting with that name
            ssfd = open (ssname, O_RDONLY);
            if (ssfd < 0) {

                // if something like permissions error, print error and die
                if (errno != ENOENT) {
                    fprintf (stderr, "ftbackup: open(%s) error: %s\n", ssname, mystrerr (errno));
                    return EX_SSIO;
                }

                // the given name is not found, try opening <givenname>.<lowest-segno>
                thissegno  = findnextsegno (ssname, 0);
                if (thissegno == 0) {
                    fprintf (stderr, "ftbackup: open(%s) error: %s\n", ssname, mystrerr (ENOENT));
                    return EX_SSIO;
                }
                ssbasename = ssname;
                sssegname  = (char *) alloca (ssnamelen + SEGNODECDIGS + 4);
                sprintf (sssegname, "%s%.*u", ssname, SEGNODECDIGS, thissegno);
                ssfd = open (sssegname, O_RDONLY);
                if (ssfd < 0) {
                    fprintf (stderr, "ftbackup: open(%s) error: %s\n", sssegname, mystrerr (errno));
                    return EX_SSIO;
                }
            }
        }
    }

    if (fstat (ssfd, &ssstat) < 0) SYSERRNO (fstat);

    try {

        dirTimes = NULL;
        lastfilenofinished = 0;
        needhdr = true;
        ok = true;
        while (true) {
            try {

                /*
                 * File header should be next in saveset.
                 * It should have correct magic number and be next file in sequence.
                 */
                if (needhdr) {
                    read_raw (hdr, (ulong_t)hdr->name - (ulong_t)hdr, false);
                    if (memcmp (hdr->magic, HEADER_MAGIC, 8) != 0) {
                        fprintf (stderr, "ftbackup: bad header magic number\n");
                        throw new LostSSBlock (0);
                    }
                    if (++ lastfileno != hdr->fileno) {
                        fprintf (stderr, "ftbackup: bad header file number, have %u, expect %u\n", hdr->fileno, lastfileno);
                        ok = false;
                    }
                }
                lastfileno = hdr->fileno;
                needhdr = true;

                /*
                 * Filename length of 0 is the end-of-saveset marker.
                 */
                if (hdr->nameln == 0) break;

                /*
                 * Read the rest of the header in, including the filename and extended attributes.
                 */
                if (hdrall < hdr->nameln + sizeof *hdr) {
                    hdrall = hdr->nameln + sizeof *hdr;
                    hdr = (Header *) realloc (hdr, hdrall);
                    if (hdr == NULL) NOMEM ();
                }
                read_raw (hdr->name, hdr->nameln, false);

                /*
                 * See if file is selected and create destination filename string.
                 */
                srcnamelen = strlen (hdr->name) + 1;
                dstname = NULL;
                slashtonull (hdr->name, srcnamelen - 1);
                cmp = memcmp (hdr->name, srcprefixnul, srcprefixlen);
                nulltoslash (hdr->name, srcnamelen - 1);
                if (cmp >  0) break;
                if (cmp == 0) {
                    dstnamelen = dstprefixlen + srcnamelen - srcprefixlen;
                    if (dstnameall < dstnamelen) {
                        dstnameall = dstnamelen;
                        dstnamebuf = (char *) realloc (dstnamebuf, dstnameall);
                        if (dstnamebuf == NULL) NOMEM ();
                    }
                    memcpy (dstnamebuf, dstprefix, dstprefixlen);
                    memcpy (dstnamebuf + dstprefixlen, hdr->name + srcprefixlen, srcnamelen - srcprefixlen);
                    dstname = dstnamebuf;
                }

                /*
                 * If we won't be restoring anything more to a previous directory,
                 * set its times.
                 */
                while ((dstname != NULL) && ((dirTime = dirTimes) != NULL) &&
                        (memcmp (dstname, dirTime->name, dirTime->nameln) > 0)) {
                    dirTimes = dirTime->next;
                    updatetimes (tfs, dirTime->name, dirTime->atimns, dirTime->mtimns);
                    free (dirTime);
                }

                /*
                 * If file is selected, maybe output listing line.
                 * Then if in listing mode, don't write output file (just skip over the data).
                 */
                if (dstname != NULL) {
                    dstname = maybe_output_listing (dstname, hdr);
                }

                /*
                 * Call the processing funtion, skipping the data if not enabled.
                 */
                setimes = false;
                     if (S_ISREG (hdr->stmode)) thisok = read_regular   (hdr, dstname);
                else if (S_ISDIR (hdr->stmode)) thisok = read_directory (hdr, dstname, &setimes);
                else if (S_ISLNK (hdr->stmode)) thisok = read_symlink   (hdr, dstname);
                                           else thisok = read_special   (hdr, dstname);
                ok &= thisok;
                lastfilenofinished = hdr->fileno;

                /*
                 * If restored successfully, try to set ownership, protection and times.
                 */
                if (thisok && (dstname != NULL)) {
                    tfs->fslchown (dstname, hdr->ownuid, hdr->owngid);
                    if (!S_ISLNK (hdr->stmode)) tfs->fschmod (dstname, hdr->stmode);
                    if (!S_ISDIR (hdr->stmode)) {
                        updatetimes (tfs, dstname, hdr->atimns, hdr->mtimns);
                    } else if (setimes) {
                        cmp = strlen (dstname);
                        dirTime = (DirTime *) malloc (cmp + sizeof *dirTime);
                        if (dirTime == NULL) NOMEM ();
                        dirTime->next   = dirTimes;
                        dirTime->atimns = hdr->atimns;
                        dirTime->mtimns = hdr->mtimns;
                        dirTime->nameln = cmp;
                        strcpy (dirTime->name, dstname);
                        dirTimes = dirTime;
                    }
                    if (hdr->flags & HFL_XATTRS) {
                        xattrsnameptr = hdr->name + srcnamelen;
                        xattrslistlen = extpackeduint32 (&xattrsnameptr);
                        xattrsvaluptr = xattrsnameend = xattrsnameptr + xattrslistlen;
                        while (xattrsnameptr < xattrsnameend) {
                            xattrsvalulen = extpackeduint32 (&xattrsvaluptr);
                            if (tfs->fslsetxattr (dstname, xattrsnameptr, xattrsvaluptr, xattrsvalulen, 0) < 0) {
                                fprintf (stderr, "ftbackup: lsetxattr(%s,%s) error: %s\n",
                                        dstname, xattrsnameptr, strerror (errno));
                                ok = false;
                            }
                            xattrsnameptr += strlen (xattrsnameptr) + 1;
                            xattrsvaluptr += xattrsvalulen;
                        }
                    }
                }
            } catch (LostSSBlock *lssb) {
                delete lssb;

                /*
                 * Some unrecoverable media error, skip to next block with a valid file header.
                 */
                fprintf (stderr, "ftbackup: searching saveset for next file header\n");
                while (true) {
                    try {
                        do {
                            rblock = read_block (true);
                            read_raw (hdr, (ulong_t)hdr->name - (ulong_t)hdr, false);
                        } while (memcmp (hdr->magic, HEADER_MAGIC, 8) != 0);
                        break;
                    } catch (LostSSBlock *lssb) {
                        delete lssb;
                    }
                }
                fprintf (stderr, "ftbackup: found file header in block %u at offset %u\n", rblock->seqno, rblock->hdroffs);
                skipped = hdr->fileno - lastfilenofinished - 1;
                fprintf (stderr, "ftbackup: lost %u file%s due to bad saveset media\n", skipped, ((skipped == 1) ? "" : "s"));
                needhdr = false;
                ok = false;
            }
        }

        /*
         * Reached normal end of saveset, flush all pending directory time updates.
         */
        while ((dirTime = dirTimes) != NULL) {
            dirTimes = dirTime->next;
            updatetimes (tfs, dirTime->name, dirTime->atimns, dirTime->mtimns);
            free (dirTime);
        }

        /*
         * All done.
         */
        if (dstnamebuf != NULL) free (dstnamebuf);
        if (hdr != NULL) free (hdr);
        close (ssfd);
        ssfd = -1;
        return ok ? EX_OK : EX_FILIO;

    } catch (EndOfSSFile *eossf) {
        delete eossf;

        /*
         * Got to end of saveset without seeing end mark.
         */
        return EX_SSIO;
    }
}

static uint32_t extpackeduint32 (char const **ptr)
{
    int shf = 0;
    uint32_t val = 0;
    uint32_t byt;

    do {
        byt  = (uint8_t) *((*ptr) ++);
        val |= (byt & 0x7F) << shf;
        shf += 7;
    } while (byt & 0x80);
    return val;
}

/**
 * @brief Restore a regular file's contents from the saveset.
 * @param hdr = backup header
 * @param dstname = where to restore the regular file to
 * @returns true: success
 *         false: error writing file
 */
bool FTBReader::read_regular (Header *hdr, char const *dstname)
{
    int fd, rc;
    struct stat statbuf;
    uint32_t oldfileno, wofs;
    uint64_t len, rofs;
    uint8_t buf[FILEIOSIZE];

    /*
     * See if it's an hardlink to an earlier file in the saveset.
     */
    if (hdr->flags & HFL_HDLINK) {
        read_raw (&oldfileno, sizeof oldfileno, false);
        if (dstname != NULL) {
            if ((oldfileno >= inodesused) || (inodesname[oldfileno] == NULL)) {
                fprintf (stderr, "ftbackup: hardlink %s missing old file %u\n", hdr->name, oldfileno);
                return false;
            }
            if (opt_overwrite) tfs->fsunlink (dstname);
            if (tfs->fslink (inodesname[oldfileno], dstname) < 0) {
                fprintf (stderr, "ftbackup: link(%s, %s) error: %s\n", inodesname[oldfileno], dstname, mystrerr (errno));
                return false;
            }
        }
        return true;
    }

    /*
     * Not an hardlink, create the file and put in list of hardlinkable files.
     * Use O_EXCL to be consistent with other things like link(),mkdir(),mknod(),symlink().
     */
    if (dstname == NULL) {
        fd = -1;
    } else {
        if (opt_overwrite) tfs->fsunlink (dstname);
        fd = tfs->fsopen (dstname, O_WRONLY | O_CREAT | O_EXCL, hdr->stmode);
        if (fd < 0) {
            fprintf (stderr, "ftbackup: creat(%s) error: %s\n", dstname, mystrerr (errno));
        } else if (tfs->fsftruncate (fd, hdr->size) < 0) {
            fprintf (stderr, "ftbackup: ftruncate(%s, %llu) error: %s\n", dstname, hdr->size, mystrerr (errno));
            tfs->fsclose (fd);
            fd = -1;
        } else if (tfs->fsfstat (fd, &statbuf) < 0) {
            fprintf (stderr, "ftbackup: fstat(%s) error: %s\n", dstname, mystrerr (errno));
            tfs->fsclose (fd);
            fd = -1;
        } else {

            /*
             * Enter in list of restored regular files in case there is
             * a later reference to the file via hardlinked file number.
             */
            if (inodessize <= hdr->fileno) {
                do inodessize += inodessize / 2 + 10;
                while (inodessize <= hdr->fileno);
                inodesname = (char **) realloc (inodesname, inodessize * sizeof *inodesname);
                if (inodesname == NULL) NOMEM ();
            }
            while (inodesused <= hdr->fileno) {
                inodesname[inodesused++] = NULL;
            }
            inodesname[hdr->fileno] = strdup (dstname);
            if (inodesname[hdr->fileno] == NULL) NOMEM ();
        }
    }

    /*
     * Read data from saveset and write to file.
     */
    try {
        for (rofs = 0; rofs < hdr->size; rofs += len) {
            len = hdr->size - rofs;
            if (len > sizeof buf) len = sizeof buf;
            read_raw (buf, len, true);
            if (fd >= 0) {
                for (wofs = 0; wofs < len; wofs += rc) {
                    rc = tfs->fswrite (fd, buf + wofs, len - wofs);
                    if (rc <= 0) {
                        fprintf (stderr, "ftbackup: write(%s) error: %s\n", dstname, ((rc == 0) ? "end of file" : mystrerr (errno)));
                        tfs->fsclose (fd);
                        fd = -1;
                        break;
                    }
                }
            }
        }

    } catch (...) {

        /*
         * Warn that file is corrupted cuz of unrecoverable media error exception.
         */
        if (fd >= 0) {
            fprintf (stderr, "ftbackup: file %s corrupt due to unrecoverable saveset media errors\n", dstname);
            tfs->fsclose (fd);
        }

        throw;
    }

    /*
     * Done writing, close file.
     */
    if ((fd >= 0) && (tfs->fsclose (fd) < 0)) {
        fprintf (stderr, "ftbackup: close(%s) error %s\n", dstname, mystrerr (errno));
        fd = -1;
    }

    return (dstname == NULL) || (fd >= 0);
}

/**
 * @brief Restore a directory's contents from the saveset.
 * @param hdr = backup header
 * @param dstname = where to restore the directory to
 * @returns true: success
 *         false: error creating directory
 *         *setimes = false: do not set atime/mtime
 *                     true: set atime/mtime later
 */
bool FTBReader::read_directory (Header *hdr, char const *dstname, bool *setimes)
{
    bool ok;
    char buf[32768], *nameptr;
    int ient, nents;
    struct dirent **names;
    uint32_t len, namelen, numsame, preserve;
    uint64_t ofs;

    /*
     * Create the directory if it doesn't already exist.
     */
    ok = true;
    if (dstname != NULL) {
        *setimes = tfs->fsmkdir (dstname, hdr->stmode) >= 0;
        if (!*setimes && (errno != EEXIST)) {
            fprintf (stderr, "ftbackup: mkdir(%s) error: %s\n", dstname, mystrerr (errno));
            ok = false;
        }
    }

    /*
     * If doing an incremental restore, read the existing directory contents.
     * It may have stuff in it that we need to delete.
     *
     * For non-incremental, pretend the existing directory is empty
     * so we won't try to delete anything from it below.
     */
    names = NULL;
    nents = 0;
    if (opt_incrmntl && (dstname != NULL)) {
        nents = tfs->fsscandir (dstname, &names, NULL, alphasort);
        if (nents < 0) {
            fprintf (stderr, "ftbackup: scandir(%s) error: %s\n", dstname, mystrerr (errno));
            names = NULL;
            nents = 0;
            ok    = false;
        }

        // and we also want the directory time set back to the
        // saved time cuz it is going to look like it looked back
        // then when the restore is complete
        *setimes = true;
    }

    try {

        /*
         * Read names of files that existed in directory at time of backup.
         *
         * If doing incremental and a file exists in existing directory that
         * isn't in the backed up directory, delete the existing file from
         * the existing directory so the existing directory will end up 
         * matching the backed up directory.
         *
         * For excremental restores, just skip over the backed up contents.
         */
        ient    = 0;
        len     = 0;
        namelen = 0;
        nameptr = buf;
        ofs     = 0;
        while ((ofs < hdr->size) || (nameptr < buf + len)) {

            /*
             * Maybe we need to read more from the saveset.
             * If so, fill buf with as much as it can hold.
             *  hdr->size = total bytes in the directory file
             *  namelen = 0: next byte at ofs is a 'same' byte
             *         else: next byte at ofs is in a string after a 'same' byte
             *               and namelen = number of good bytes at beginning of buf
             *  ofs = byte offset within directory file we will read next
             */
            if (nameptr >= buf + len) {

                // preserve any filename bytes already at beginning of buffer
                preserve = namelen;
                if ((preserve == 0) && (ofs > 0)) {
                    preserve = strnlen (buf + 1, 255) + 1;
                }
                if (preserve >= sizeof buf) abort ();

                // how many bytes to end of buffer
                // but not more than are in directory
                len = sizeof buf - preserve;
                if (len > hdr->size - ofs) len = hdr->size - ofs;

                // read bytes, being sure to preserve what is needed
                read_raw (buf + preserve, len, true);
                ofs += len;       // how many bytes just read in
                len += preserve;  // all bytes in buffer

                // point to 'same' byte at beginning of buf
                // it must always be 0 cuz we don't have any bytes before it to splice
                // eg, if the very first thing in the directory were <2>cdef, there's
                // no way to know what the two characters before cdef are.
                nameptr = buf;
                if (*nameptr != 0) abort ();

                // if we started reading right at a 'same' byte, we preserved up to 255
                // bytes of the previous filename, so just point at the 'same' byte.
                if (namelen == 0) {
                    nameptr += preserve;
                }

                // no matter what, at this point nameptr points to the 'same' byte
                // of the next name to process
            }

            /*
             * There is a name in buf starting at nameptr.
             * It points to a string of the form:
             *  <number-of-beginning-chars-same-as-last><different-chars-on-end><null>
             *
             * Find the end of the name if any.
             * If no end, save the part we have and read next block.
             *
             *  nameptr = where in buf the 'same' byte is
             *  len = total number of bytes in buf
             *  buf[0] = 0
             */
            numsame = (uint8_t) *(nameptr ++);
            namelen = strnlen (nameptr, buf + len - nameptr);
            if (numsame + 1 + namelen > sizeof buf) abort ();
            memmove (buf + numsame + 1, nameptr, namelen);
            if (namelen >= buf + len - nameptr) {
                namelen += numsame + 1;  // point just past last good char at beg of buf
                nameptr  = buf + len;    // cause next loop to read from directory
                continue;                // read more from directory
            }
            buf[numsame+1+namelen] = 0;

            nameptr += namelen + 1;      // point to next 'same' byte
            namelen  = 0;                // in case nameptr is right at eob,
                                         // next thing in file is a 'same' byte

            /*
             * We have a complete null-terminated name starting at buf[1].
             * Delete any entries in the existing directory that are lower than the
             * backed up name.
             */
            while ((ient < nents) && (strcmp (names[ient]->d_name, buf + 1) < 0)) {
                rmdirentry (tfs, dstname, names[ient]->d_name);
                free (names[ient++]);
            }

            /*
             * Skip over the matching name in the existing directory if it is there.
             */
            while ((ient < nents) && (strcmp (names[ient]->d_name, buf + 1) == 0)) {
                free (names[ient++]);
            }
        }

        /*
         * Delete any existing directory entries beyond the end of those in the backup.
         */
        while (ient < nents) {
            rmdirentry (tfs, dstname, names[ient]->d_name);
            free (names[ient++]);
        }
    } catch (...) {

        /*
         * If exception, just free off the rest of the names without deleting anything.
         * Probably got an unrecoverable media error.
         */
        while (ient < nents) {
            free (names[ient++]);
        }
        if (names != NULL) free (names);

        throw;
    }

    return ok;
}

/**
 * @brief Restore a symlink from the saveset.
 * @param hdr = backup header
 * @param dstname = where to restore the symlink to
 * @returns true: success
 *         false: error creating symlink
 */
bool FTBReader::read_symlink (Header *hdr, char const *dstname)
{
    char *link;

    link = (char *) alloca (hdr->size + 1);
    read_raw (link, hdr->size, false);
    link[hdr->size] = 0;

    if (dstname != NULL) {
        if (opt_overwrite) tfs->fsunlink (dstname);
        if (tfs->fssymlink (link, dstname) < 0) {
            fprintf (stderr, "ftbackup: symlink(%s) error: %s\n", dstname, mystrerr (errno));
            return false;
        }
    }
    return true;
}

/**
 * @brief Restore a special file's contents from the saveset.
 * @param hdr = backup header
 * @param dstname = where to restore the special file to
 * @returns true: success
 *         false: error creating node
 */
bool FTBReader::read_special (Header *hdr, char const *dstname)
{
    dev_t rdev;

    read_raw (&rdev, sizeof rdev, false);

    if (dstname != NULL) {
        if (opt_overwrite) tfs->fsunlink (dstname);
        if (tfs->fsmknod (dstname, hdr->stmode, rdev) < 0) {
            fprintf (stderr, "ftbackup: mknod(%s) error: %s\n", dstname, mystrerr (errno));
            return false;
        }
    }
    return true;
}

/**
 * @brief Read raw data from saveset block, unzipping it if necessary.
 * @param buf = where to return the data
 * @param len = number of bytes to return
 * @param zip = true: data in saveset is compressed, unzip it
 *             false: data in saveset is uncompressed, copy it
 */
void FTBReader::read_raw (void *buf, uint32_t len, bool zip)
{
    int rc;

    zstrm.next_out  = (Bytef *) buf;
    zstrm.avail_out = len;
    while (zstrm.avail_out > 0) {

        /*
         * Caller wants more data, so make sure we have some input to work with.
         */
        if (zstrm.avail_in == 0) {
            read_block (false);
        }

        /*
         * See if data in block is zipped.
         */
        if (zip) {

            /*
             * It is zipped, make sure our unzipper is open.
             */
            if (!zisopen) {
                uint32_t ai = zstrm.avail_in;
                uint32_t ao = zstrm.avail_out;
                Bytef   *ni = zstrm.next_in;
                Bytef   *no = zstrm.next_out;
                memset (&zstrm, 0, sizeof zstrm);
                rc = inflateInit (&zstrm);
                if (rc != Z_OK) INTERR (inflateInit, rc);
                zstrm.avail_in  = ai;
                zstrm.avail_out = ao;
                zstrm.next_in   = ni;
                zstrm.next_out  = no;
                zisopen = true;
            }

            /*
             * Unzip some stuff.
             */
            rc = inflate (&zstrm, Z_SYNC_FLUSH);
            if (rc == Z_STREAM_END) {
                rc = inflateEnd (&zstrm);
                if (rc != Z_OK) INTERR (inflateEnd, rc);
                zisopen = false;
                continue;
            }
            if ((rc != Z_OK) && (rc != Z_NEED_DICT)) {
                fprintf (stderr, "ftbackup: inflate() error %d\n", rc);
                inflateEnd (&zstrm);
                zisopen = false;
                zstrm.avail_in = 0;
                throw new LostSSBlock (0);
            }
        } else {

            /*
             * If unzipper open, close it.
             */
            if (zisopen) {
                uint8_t junk[64];
                uint32_t ao = zstrm.avail_out;
                Bytef   *no = zstrm.next_out;

                /*
                 * There might be some junk bytes on the end,
                 * so make sure we consume them all up before
                 * marking the unzipper closed.
                 */
                zstrm.avail_out = sizeof junk;
                zstrm.next_out  = junk;
                rc = inflate (&zstrm, Z_FINISH);
                if ((rc != Z_OK) && (rc != Z_NEED_DICT)) {

                    /*
                     * Free off all unzipper context.
                     */
                    zstrm.avail_out = sizeof junk;
                    zstrm.next_out  = junk;
                    rc = inflateEnd (&zstrm);
                    if (rc != Z_OK) INTERR (inflateEnd, rc);
                    zisopen = false;
                }

                /*
                 * Loop back to make sure we have some input data to process.
                 */
                zstrm.avail_out = ao;
                zstrm.next_out  = no;
                continue;
            }

            /*
             * Do simple memcpy of as much as we can at once.
             */
            len = zstrm.avail_in;
            if (len > zstrm.avail_out) len = zstrm.avail_out;
            memcpy (zstrm.next_out, zstrm.next_in, len);
            zstrm.avail_in  -= len;
            zstrm.avail_out -= len;
            zstrm.next_in   += len;
            zstrm.next_out  += len;
        }
    }
}

/**
 * @brief Read next data block from saveset, performing recovery if needed.
 * @param skipfh = true: read whole block lastseqno+1
 *                false: repeat until block with file header found
 * @returns pointer to block just read
 *          lastseqno incremented
 *          lastxorno possibly incremented
 *          l2bs,xorgc,xorsc filled in if first call
 *          zstrm.avail_in = number of bytes available
 *          zstrm.next_in  = pointer to bytes within block
 */
Block *FTBReader::read_block (bool skipfh)
{
    Block *rblock;
    uint32_t offs;

    /*
     * Discard block from last call.
     */
    if (linkedRBlock != NULL) {
        free (linkedRBlock);
        linkedRBlock = NULL;
    }

    /*
     * If first call, we need some craziness to get block size and XOR parameters.
     */
    if (readoffset == 0) {
        read_first_block ();
    }

    /*
     * Either read it or recover it.
     */
nextblock:
    lastseqno ++;
    zstrm.avail_in = 0;
    zstrm.next_in  = NULL;
    linkedRBlock   = read_or_recover_block ();

    /*
     * Point to block and get offset to start of data in block.
     */
    rblock = &linkedRBlock->block;
    offs   = (ulong_t)rblock->data - (ulong_t)rblock;

    /*
     * If skipping to next file header, get another block
     * if this block doesn't have a file header.
     */
    if (skipfh) {
        offs = rblock->hdroffs;
        if (offs == 0) goto nextblock;
    }

    /*
     * Set up the data descriptor.
     */
    zstrm.avail_in = (1 << l2bs) - offs - hashsize ();
    zstrm.next_in  = (uint8_t *)rblock + offs;
    return rblock;
}

/**
 * @brief Scan the saveset for a valid header and get the blocksize and XOR parameters.
 * @returns with first readable block pushed on linkedBlocks list
 *          and l2bs, xorgc, xorsc, xorblocks, gotxors initialized
 *          and readoffset updated just past whatever was read
 */
void FTBReader::read_first_block ()
{
    Block *miniBlock, *miniEncrp, *tblock;
    int rc;
    LinkedBlock *bigBlock;
    uint32_t bs, bytesCopied, i;
    uint64_t miniOffset;

    /*
     * Step through file by minimum block size until we find a valid block header.
     */
    bigBlock    = NULL;
    bs          = 0;
    bytesCopied = 0;
    miniBlock   = (Block *) alloca (MINBLOCKSIZE);
    miniEncrp   = (Block *) alloca (MINBLOCKSIZE);
    while (true) {

        /*
         * Keep reading until we get something of MINBLOCKSIZE.
         * Any error means get rid of bigBlock that we are trying to build.
         */
        while (true) {
            miniOffset = readoffset;
            rc = wrapped_pread (miniBlock, MINBLOCKSIZE, readoffset);
            readoffset += MINBLOCKSIZE;
            if (rc == MINBLOCKSIZE) break;
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: %s\n", miniOffset,
                    ((rc > 0) ? "partial read" : (rc == 0) ? "end of file" : mystrerr (errno)));
            if (rc == 0) {
                fprintf (stderr, "ftbackup: unable to locate a backup header\n");
                throw new EndOfSSFile ();
            }
            if (bigBlock != NULL) {
                free (bigBlock);
                bigBlock = NULL;
            }
        }

        /*
         * Found something readable, see if it has correct magic number and a valid block size field.
         * If so, it is the beginning of a bigBlock.
         */
        memcpy (miniEncrp, miniBlock, MINBLOCKSIZE);
        if (bigBlock == NULL) {
            decrypt_block (miniBlock, MINBLOCKSIZE);
            if (memcmp (miniBlock->magic, BLOCK_MAGIC, 8) != 0) continue;
            if (miniBlock->xorbc != 0) continue;
            l2bs = miniBlock->l2bs;
            if (l2bs > 31) continue;
            bs   = 1 << l2bs;
            if ((bs < MINBLOCKSIZE) || (bs > MAXBLOCKSIZE)) continue;
            bigBlock      = (LinkedBlock *) malloc (bs + sizeof *bigBlock);
            if (bigBlock == NULL) NOMEM ();
            bytesCopied   = 0;
        }

        /*
         * Copy the (possibly encrypted) mini block data to the bigBlock.
         */
        memcpy ((uint8_t *)&bigBlock->block + bytesCopied, miniEncrp, MINBLOCKSIZE);
        bytesCopied += MINBLOCKSIZE;

        /*
         * Done if the whole big block is valid.
         */
        if (bytesCopied < bs) continue;
        tblock = &bigBlock->block;
        if (!decrypt_block (tblock, bs)) continue;
        xorgc = tblock->xorgc;
        xorsc = tblock->xorsc;
        if (blockisvalid (tblock)) break;
        free (bigBlock);
        bigBlock = NULL;
    }

    /*
     * Push this block as the one-and-only valid block so far.
     */
    bigBlock->next = NULL;
    linkedBlocks = bigBlock;

    /*
     * Calloc XOR blocks and fill in this block as initial XOR.
     */
    if (xorgc > 0) {
        xorblocks = (Block **) malloc (xorgc * sizeof *xorblocks);
        if (xorblocks == NULL) NOMEM ();
        for (i = 0; i < xorgc; i ++) {
            xorblocks[i] = (Block *) calloc (1, bs - hashsize ());
            if (xorblocks[i] == NULL) NOMEM ();
        }
        i = (tblock->seqno - 1) % xorgc;
        memcpy (xorblocks[i], tblock, bs - hashsize ());

        gotxors = (uint8_t *) calloc (xorgc, sizeof *gotxors);
        if (gotxors == NULL) NOMEM ();
        gotxors[i] = 1;
    }
}

/**
 * @brief Scan forward trying to read or recover block lastseqno.
 * @returns pointer to malloc()d block
 *          also, readoffset incremented past what was last read
 *                xorblocks, gotxors updated
 *                possibly intermediate blocks stacked on linkedBlocks
 */
FTBReader::LinkedBlock *FTBReader::read_or_recover_block ()
{
    int rc;
    LinkedBlock *linkedBlock, **lLinkedBlock;
    uint32_t bs, bsnh, i;
    uint64_t lastreadoffs;

    bs = 1 << l2bs;
    if (readoffset % bs != 0) abort ();
    bsnh = bs - hashsize ();

    /*
     * Maybe block has already been read by a previous recovery operation.
     * If found, unlink it from list and return.
     */
    for (lLinkedBlock = &linkedBlocks; (linkedBlock = *lLinkedBlock) != NULL; lLinkedBlock = &linkedBlock->next) {
        if (linkedBlock->block.seqno == lastseqno) {
            *lLinkedBlock = linkedBlock->next;
            return linkedBlock;
        }
    }

    /*
     * If no XOR blocks, just a simple read will hopefully work.
     * If read error, return NULL and hopefully there is stuff later in the saveset we can process.
     * If end of file, print error message and exit cuz there's nothing more that can be handled.
     */
    if (xorgc == 0) {

        // if any stacked blocks, they are all ahead of us, so return NULL for now.
        // all any more reading could do is stack more blocks even farther out.
        if (linkedBlocks != NULL) goto unrecoverable;

        // read from saveset
        linkedBlock = (LinkedBlock *) malloc (bs + sizeof *linkedBlock);
        if (linkedBlock == NULL) NOMEM ();
noxoread:
        lastreadoffs = readoffset;
        rc = wrapped_pread (&linkedBlock->block, bs, readoffset);
        readoffset += bs;

        // if read error, output message and read again.
        // if the lastseqno block is lost, it will be detected on next read.
        if (rc < 0) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: %s\n", lastreadoffs, mystrerr (errno));
            goto noxoread;
        }

        // if short read, means we are at the end of file and there is nothing more we can do.
        if ((uint32_t) rc < bs) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: end of file\n", lastreadoffs);
            throw new EndOfSSFile ();
        }

        // maybe decrypt the block
        if (!decrypt_block (&linkedBlock->block, bs)) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: block digest not valid\n", lastreadoffs);
            goto noxoread;
        }

        // make sure the magic number etc are valid
        // if not, treat it just like a read error
        if (!blockisvalid (&linkedBlock->block)) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: block not valid\n", lastreadoffs);
            goto noxoread;
        }

        // make sure we aren't missing any blocks.
        // if there is a gap, save this block for later
        // and return a NULL cuz we are missing this one.
        if (linkedBlock->block.seqno < lastseqno) goto noxoread;
        if (linkedBlock->block.seqno > lastseqno) {
            linkedBlock->next = NULL;
            linkedBlocks = linkedBlock;
            goto unrecoverable;
        }

        // we got the block asked for
        return linkedBlock;
    }

    /*
     * If we have completed processing all XOR blocks in the span containing lastseqno,
     * it is hopeless to read more as it can't be recovered.
     *
     *   lastxorno / xorgc = number of fully completed spans including all XOR blocks
     *   lastseqno-1 / xorsc / xorgc = zero-based span number for wanted block
     *
     *   span0 (1)(2) span1 (3)(4) span2  ...
     *    lastxorno^  ^lastseqno
     *
     * So if we want a block in span #1 and have completed 1 span, keep reading.
     *
     * Note that lastxorno is always a multiple of xorgc, ie, we have either
     * completed all XOR blocks or we haven't.
     */
    while (lastxorno <= (lastseqno - 1) / xorsc) {

        /*
         * Try to read a whole 'bs' sized block.
         */
        if (linkedBlock == NULL) linkedBlock = (LinkedBlock *) malloc (bs + sizeof *linkedBlock);
        if (linkedBlock == NULL) NOMEM ();
        lastreadoffs = readoffset;
        rc = wrapped_pread (&linkedBlock->block, bs, lastreadoffs);

        /*
         * The next read will be at beginning of next block no matter if this one was an error or not.
         */
        readoffset += bs;

        /*
         * If it failed to read, output message and try to read next.
         */
        if ((rc < 0) || ((uint32_t) rc != bs)) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: %s\n", lastreadoffs,
                    ((rc > 0) ? "partial read" : (rc == 0) ? "end of file" : mystrerr (errno)));

            /*
             * If IO error, try to read more.
             */
            if (rc != 0) continue;

            /*
             * If end of file with no stacked blocks, we are done
             * as there isn't anything more to process.  Otherwise,
             * say the lastseqno is unrecoverable and we will be
             * called back later to process the stacked blocks.
             */
            if (linkedBlocks == NULL) throw new EndOfSSFile ();
            goto unrecoverable;
        }

        /*
         * Maybe decrypt the block.
         */
        if (!decrypt_block (&linkedBlock->block, bs)) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: block digest not valid\n", lastreadoffs);
            continue;
        }

        /*
         * They should all have basic validity.
         */
        if (!blockbaseisvalid (&linkedBlock->block)) {
            fprintf (stderr, "ftbackup: pread(%llu) saveset error: block invalid\n", lastreadoffs);
            continue;
        }

        /*
         * If just read an XOR block, XOR the data in and hopefully recover a missing block.
         */
        if (linkedBlock->block.xorbc > 0) {

            /*
             * Make sure this XOR block is for what we think is the current span.
             * If it is old, it is a duplicate somehow, so just ignore it.
             * If it is newer, then we missed a whole span, so reset all the XOR groups first.
             *
             * lastxorno = number of whole XOR blocks completed before current span,
             *   ie, this one should be at lastxorno+1..lastxorno+xorgc.
             *
             * The normal case:
             *     ... (1)(2) ... (3)(4)
             *    lastxorno^   ^   ^linkedBlock->block.xorno (could be 3 or 4 actually)
             * xorblocks covers^
             *
             * The case where lots of stuff missing:
             *     ... (1)(2) ... (3)(4) ... (5)(6)
             *    lastxorno^   ^         ^ ^  ^linkedBlock->block.xorno (could be 5 or 6 actually)
             * xorblocks covers^         ^-^all of this stuff missing
             *                              so lastxorno never got set to 4
             */
            if (linkedBlock->block.xorno <= lastxorno) continue;
            if (linkedBlock->block.xorno >  lastxorno + xorgc) {
                lastxorno = linkedBlock->block.xorno - xorgc;
                memset (gotxors, 0, xorgc * sizeof *gotxors);
                for (i = 0; i < xorgc; i ++) memset (xorblocks[i], 0, bsnh);
            }

            /*
             * Get which XOR group this XOR block beints to, ie, which of (3) or (4) it is.
             */
            i = (linkedBlock->block.xorno - 1) % xorgc;
            if (gotxors[i] == linkedBlock->block.xorbc - 1) {

                /*
                 * There is exactly one missing data block from the group,
                 * so XORing in this one should recover the missing block.
                 */
                xorblockdata (&linkedBlock->block, xorblocks[i], bsnh);

                /*
                 * The result should be a valid data block.
                 * If not, just go on reading.
                 */
                memcpy (linkedBlock->block.magic, BLOCK_MAGIC, 8);
                linkedBlock->block.xorno = 0;
                linkedBlock->block.xorbc = 0;
                if (!blockisvalid (&linkedBlock->block)) {
                    fprintf (stderr, "ftbackup: recovered block at %llu is not valid\n", lastreadoffs);
                    continue;
                }
                fprintf (stderr, "ftbackup: block %u recovered via XOR\n", linkedBlock->block.seqno);

                /*
                 * If it is the block we are looking for, we are all done.
                 */
                if (linkedBlock->block.seqno == lastseqno) return linkedBlock;

                /*
                 * Not looking for it yet, stack it for later if we will want it later.
                 */
                if (linkedBlock->block.seqno > lastseqno) {
                    linkedBlock->next = linkedBlocks;
                    linkedBlocks = linkedBlock;
                    linkedBlock = NULL;
                }
            }
            continue;
        }

        /*
         * If data block is not valid, treat it like a read error, ie,
         * ignore it and try to read next block.
        */
        if (!blockisvalid (&linkedBlock->block)) continue;

        /*
         * Should never get a duplicate, ignore if so.
         */
        if (linkedBlock->block.seqno < lastseqno) continue;

        /*
         * If first data block in span, clear all XOR groups.
         *
         *   lastxorno / xorgc = previously completed spans
         *   linkedBlock->block.seqno-1 / xorgc / xorsc = which zero-based span we are in
         *
         *   span0 (1)(2) span1 (3)(4) span2 ...
         *
         * So if lastxorno points to (2) and we are in span1, that is ok.
         * But if we are in span2, clear the XOR groups and point lastxorno at (4).
         */
        i = ((linkedBlock->block.seqno - 1) / xorgc / xorsc) * xorgc;
        if (lastxorno < i) {
            lastxorno = i;
            memset (gotxors, 0, xorgc * sizeof *gotxors);
            for (i = 0; i < xorgc; i ++) memset (xorblocks[i], 0, bsnh);
        }

        /*
         * Valid data block, XOR it into group.
         */
        i = (linkedBlock->block.seqno - 1) % xorgc;
        xorblockdata (xorblocks[i], &linkedBlock->block, bsnh);
        gotxors[i] ++;

        /*
         * If it is the block we are looking for, we are all done.
         */
        if (linkedBlock->block.seqno == lastseqno) return linkedBlock;

        /*
         * Not the block we want, stack it for later and read another.
         */
        linkedBlock->next = linkedBlocks;
        linkedBlocks      = linkedBlock;
        linkedBlock       = NULL;
    }
    fprintf (stderr, "ftbackup: reached end of xor span\n");

unrecoverable:
    fprintf (stderr, "ftbackup: block %u unrecoverable\n", lastseqno);
    throw new LostSSBlock (lastseqno);
}

/**
 * @brief Just like pread(), but also handles spilling over segment files.
 *        We also can fake errors at random.
 */
long FTBReader::wrapped_pread (void *buf, long len, uint64_t pos)
{
    long ofs, rc;
    struct timeval nowtv;
    uint64_t rpos;

    /*
     * Simulate errors at random, but replay from /tmp/simrderrs.dat if present.
     */
    if (opt_simrderrs != 0) {
        if (wprfile == NULL) {
            wprfile = fopen ("/tmp/simrderrs.dat", "a+");
            if (wprfile == NULL) {
                fprintf (stderr, "wrapped_pread*: error creating /tmp/simrderrs.dat: %s\n", mystrerr (errno));
                abort ();
            }
            setlinebuf (wprfile);
        }

        if (!wprwrite) {
            rc = fscanf (wprfile, "%llu %lu %lu\n", &rpos, &nowtv.tv_sec, &nowtv.tv_usec);
            if (rc < 3) {
                fprintf (stderr, "wrapped_pread*: writing /tmp/simrderrs.dat\n");
                wprwrite = 1;
            } else if (rpos != pos) {
                fprintf (stderr, "wrapped_pread*: rpos %llu != pos %llu\n", rpos, pos);
                abort ();
            } else if (pos == 0) {
                fprintf (stderr, "wrapped_pread*: reading /tmp/simrderrs.dat\n");
            }
        }

        if (wprwrite) {
            if (gettimeofday (&nowtv, NULL) < 0) SYSERRNO (gettimeofday);
            fprintf (wprfile, "%llu %lu %lu\n", pos, nowtv.tv_sec, nowtv.tv_usec);
        }

        if ((nowtv.tv_usec % opt_simrderrs) == 0) {
            errno = MYESIMRDER;
            return -1;
        }
    }

    /*
     * Block devices use the real pread() directly.
     */
    while (S_ISREG (ssstat.st_mode) || S_ISBLK (ssstat.st_mode)) {

        /*
         * See if position is within this segment file.
         * If so, we should be able to read directly from file.
         * If not doing segments, do the read anyway and let it error out.
         */
        if ((pos - pipepos < (uint64_t) ssstat.st_size) || (thissegno == 0)) {
            return pread (ssfd, buf, len, pos - pipepos);
        }

        /*
         * At the end of that segment file, see if there is another segment file.
         * If not, return end-of-file status just like the pread() above would have.
         */
        thissegno = findnextsegno (ssbasename, thissegno);
        if (thissegno == 0) return 0;

        /*
         * There is a next segment file, so close the last one and try to open next one.
         */
        close (ssfd);
        sprintf (sssegname, "%s%.*u", ssbasename, SEGNODECDIGS, thissegno);
        ssfd = open (sssegname, O_RDONLY);
        if (ssfd < 0) {
            fprintf (stderr, "ftbackup: open(%s) error: %s\n", sssegname, mystrerr (errno));
            exit (EX_SSIO);
        }

        /*
         * We should always be able to get its size and what type of file it is.
         */
        if (!fstat (ssfd, &ssstat) < 0) SYSERRNO (fstat);

        /*
         * Save what position within all segments this segment starts at.
         */
        pipepos = pos;
    }

    /*
     * Pipe, make sure it is at correct position.
     */
    while (pipepos < pos) {
        ofs = pos - pipepos;
        if (ofs > len) ofs = len;
        rc = read (ssfd, buf, ofs);
        if (rc <= 0) return rc;
        pipepos += rc;
    }

    /*
     * Try to read all requested bytes.
     */
    for (ofs = 0; ofs < len; ofs += rc) {
        rc = read (ssfd, (char *) buf + ofs, len - ofs);
        if (rc < 0) return rc;
        if (rc == 0) break;
    }
    return ofs;
}

/**
 * @brief Decrypt the block's contents if -decrypt option was given.
 * @param block = block to decrypt
 * @param bs = block size in bytes, including block header and including hash on the end
 * @returns whether or not the hash validated
 */
bool FTBReader::decrypt_block (Block *block, uint32_t bs)
{
    bool hashok;
    uint32_t i;
    uint64_t *array;

    bs -= hashsize ();

    hasher->Update (hashinibuf, hashinilen);
    hasher->Update ((uint8_t *)block, bs);
    hashok = hasher->Verify ((uint8_t *)block + bs);

    if (cripter != NULL) {
        array = (uint64_t *) block;

        switch (cripter->BlockSize ()) {
            case 8: {
                for (i = bs / 8; -- i >= 4;) {
                    cripter->ProcessAndXorBlock ((byte *) &array[i], NULL, (byte *) &array[i]);
                    array[i] ^= array[i-1];
                }
                break;
            }
            case 16: {
                for (i = bs / 8; (i -= 2) >= 4;) {
                    cripter->ProcessAndXorBlock ((byte *) &array[i], NULL, (byte *) &array[i]);
                    array[i+0] ^= array[i-2];
                    array[i+1] ^= array[i-1];
                }
                break;
            }
            default: abort ();
        }
    }

    return hashok;
}

/**
 * @brief Find next segment file for the given base name in the form <basename>.<segno>
 *        where <segno> is exactly SEGNODECDIGS decimal digits gt 0
 * @param basename = string coming before the .<segno> string on the end
 * @param lastsegno = previous segment number processed
 * @returns 0: no next file found
 *       else: next greater segment number
 */
static uint32_t findnextsegno (char const *basename, uint32_t lastsegno)
{
    char *dirname, *name, *p;
    int i, nents, plen;
    struct dirent *de, **names;
    uint32_t nextsegno, segno;

    /*
     * Get basename's directory name in 'dirname'
     * then point basename to remainder of basename
     */
    dirname = (char *) alloca (strlen (basename) + 2);
    strcpy (dirname, basename);
    p = strrchr (dirname, '/');
    if (p != NULL) {
        *p = 0;
        basename += ++ p - dirname;
    } else {
        strcpy (dirname, ".");
    }
    plen = strlen (basename);

    /*
     * Scan whatever directory the base name file is in.
     */
    nents = scandir (dirname, &names, NULL, alphasort);
    if (nents < 0) {
        fprintf (stderr, "ftbackup: scandir(%s) error: %s\n", dirname, mystrerr (errno));
        exit (EX_SSIO);
    }

    /*
     * Hopefully we can find a higher numbered file in there somewhere.
     */
    nextsegno = 0;
    for (i = 0; i < nents; i ++) {
        de = names[i];
        name = de->d_name;

        /*
         * The name must be exactly <base name> <exactly SEGNODECDIGS decimal digits>.
         */
        if (((int) strlen (name) == plen + SEGNODECDIGS) && (memcmp (name, basename, plen) == 0)) {
            segno = strtoul (name + plen, &p, 10);
            if ((*p == 0) && (segno > lastsegno)) {
                nextsegno = segno;
                break;
            }
        }

        /*
         * No luck, free it off and try next.
         */
        free (de);
    }

    /*
     * Free of remainder of names.
     */
    while (i < nents) free (names[i++]);
    free (names);

    /*
     * Return segment number from the <SEGNODECDIGS decimal digits> or 0 if not found.
     */
    return nextsegno;
}

/**
 * @brief Replace all the slashes in a string with nulls.
 */
static void slashtonull (char *buf, int len)
{
    int i;
    for (i = 0; i < len; i ++) if (buf[i] == '/') buf[i] = 0;
}

/**
 * @brief Replace all the nulls in a string with slashes.
 */
static void nulltoslash (char *buf, int len)
{
    int i;
    for (i = 0; i < len; i ++) if (buf[i] == 0) buf[i] = '/';
}

/**
 * @brief Try to set a file's last access and last modification times.
 */
static void updatetimes (IFSAccess *ifsa, char const *name, uint64_t atimns, uint64_t mtimns)
{
    struct timespec times[2];

    memset (times, 0, sizeof times);
    times[0].tv_sec  = atimns / 1000000000;
    times[0].tv_nsec = atimns % 1000000000;
    times[1].tv_sec  = mtimns / 1000000000;
    times[1].tv_nsec = mtimns % 1000000000;
    ifsa->fslutimes (name, times);
}

/**
 * @brief Remove an entry from a directory, recursively if necessary.
 * @param dirname = name of the directory
 * @param entname = name of entry to remove
 * @returns true iff the entry was removed
 */
static bool rmdirentry (IFSAccess *ifsa, char const *dirname, char const *entname)
{
    bool ndel;
    char name[strlen(dirname)+strlen(entname)+2];
    DIR *dir;
    struct dirent *ent;

    if ((strcmp (entname, ".") == 0) || (strcmp (entname, "..") == 0)) return false;

    sprintf (name, "%s/%s", dirname, entname);

    if (ifsa->fsunlink (name) >= 0) return true;

    do {
        ndel = false;
        dir = ifsa->fsopendir (name);
        if (dir != NULL) {
            while ((ent = ifsa->fsreaddir (dir)) != NULL) {
                ndel |= rmdirentry (ifsa, name, ent->d_name);
            }
            ifsa->fsclosedir (dir);
        }
    } while (ndel);

    return ifsa->fsrmdir (name) >= 0;
}
