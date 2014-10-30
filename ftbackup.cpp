/**
 * @brief Fault-tolerant backup.
 */

#include "ftbackup.h"
#include "ftbreader.h"
#include "ftbwriter.h"

static int cmd_backup (int argc, char **argv);
static bool write_nanos_to_file (uint64_t nanos, char const *name);
static uint64_t read_nanos_from_file (char const *name);

static int cmd_diff (int argc, char **argv);
static bool diff_file (char const *path1, char const *path2);
static bool diff_regular (char const *path1, char const *path2);
static bool diff_directory (char const *path1, char const *path2);
static bool diff_symlink (char const *path1, char const *path2);
static bool diff_special (char const *path1, char const *path2, struct stat *stat1, struct stat *stat2);

static int cmd_list (int argc, char **argv);
static int cmd_restore (int argc, char **argv);
static int cmd_version (int argc, char **argv);
static int cmd_xorvfy (int argc, char **argv);

int main (int argc, char **argv)
{
    setlinebuf (stdout);
    setlinebuf (stderr);

    if (argc >= 2) {
        if (strcasecmp (argv[1], "backup")  == 0) return cmd_backup  (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "diff")    == 0) return cmd_diff    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "list")    == 0) return cmd_list    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "restore") == 0) return cmd_restore (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "version") == 0) return cmd_version (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "xorvfy")  == 0) return cmd_xorvfy  (argc - 1, argv + 1);
        fprintf (stderr, "ftbackup: unknown command %s\n", argv[1]);
    }
    fprintf (stderr, "usage: ftbackup backup ...\n");
    fprintf (stderr, "       ftbackup diff ...\n");
    fprintf (stderr, "       ftbackup list ...\n");
    fprintf (stderr, "       ftbackup restore ...\n");
    fprintf (stderr, "       ftbackup version\n");
    fprintf (stderr, "       ftbackup xorvfy ...\n");
    return EX_CMD;
}

/**
 * @brief Create a saveset.
 */
static int cmd_backup (int argc, char **argv)
{
    char *p, *rootpath, *ssname;
    FTBWriter ftbwriter = FTBWriter ();
    int i, j, k;
    uint32_t blocksize;

    ftbwriter.l2bs  = __builtin_ctz (DEFBLOCKSIZE);
    ftbwriter.xorgc = DEFXORGC;
    ftbwriter.xorsc = DEFXORSC;
    rootpath = NULL;
    ssname   = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-blocksize") == 0) {
                if (++ i >= argc) goto usage;
                blocksize = strtoul (argv[i], &p, 0);
                if ((*p != 0) || (blocksize < MINBLOCKSIZE) || (blocksize > MAXBLOCKSIZE) ||
                        ((blocksize & -blocksize) != blocksize)) {
                    fprintf (stderr, "ftbackup: invalid blocksize %s\n", argv[i]);
                    goto usage;
                }
                ftbwriter.l2bs = __builtin_ctz (blocksize);
                continue;
            }
            if (strcasecmp (argv[i], "-idirect") == 0) {
                ftbwriter.ioptions |= O_DIRECT;
                continue;
            }
            if (strcasecmp (argv[i], "-noxor") == 0) {
                ftbwriter.xorgc = ftbwriter.xorsc = 0;
                continue;
            }
            if (strcasecmp (argv[i], "-odirect") == 0) {
                ftbwriter.ooptions |= O_DIRECT;
                continue;
            }
            if (strcasecmp (argv[i], "-osync") == 0) {
                ftbwriter.ooptions |= O_SYNC;
                continue;
            }
            if (strcasecmp (argv[i], "-record") == 0) {
                if (++ i >= argc) goto usage;
                if (!write_nanos_to_file (0, argv[i])) return EX_CMD;
                continue;
            }
            if (strcasecmp (argv[i], "-since") == 0) {
                if (++ i >= argc) goto usage;
                ftbwriter.opt_since = read_nanos_from_file (argv[i]);
                if (ftbwriter.opt_since == 0) return EX_CMD;
                continue;
            }
            if (strcasecmp (argv[i], "-verbose") == 0) {
                ftbwriter.opt_verbose = true;
                continue;
            }
            if (strcasecmp (argv[i], "-verbsec") == 0) {
                if (++ i >= argc) goto usage;
                ftbwriter.opt_verbsec = strtol (argv[i], &p, 0);
                if ((*p != 0) || (ftbwriter.opt_verbsec <= 0)) {
                    fprintf (stderr, "ftbackup: invalid verbsec %s\n", argv[i]);
                    goto usage;
                }
                continue;
            }
            if (strcasecmp (argv[i], "-xor") == 0) {
                if (++ i >= argc) goto usage;
                j = strtol (argv[i], &p, 0);
                if (*p == ',') {
                    k = strtol (++ p, &p, 0);
                    if ((*p == 0) && (j > 0) && (k > 0) && ((j | k) <= 255)) {
                        ftbwriter.xorsc = j;
                        ftbwriter.xorgc = k;
                        continue;
                    }
                }
                fprintf (stderr, "ftbackup: invalid xorgc,xorsc %s\n", argv[i]);
                goto usage;
            }
            fprintf (stderr, "ftbackup: unknown option %s\n", argv[i]);
            goto usage;
        }
        if (ssname == NULL) {
            ssname = argv[i];
            continue;
        }
        if (rootpath == NULL) {
            rootpath = argv[i];
            continue;
        }
        fprintf (stderr, "ftbackup: unknown argument %s\n", argv[i]);
        goto usage;
    }
    if (ssname == NULL) {
        fprintf (stderr, "ftbackup: missing <saveset>\n");
        goto usage;
    }
    if (rootpath == NULL) {
        fprintf (stderr, "ftbackup: missing <rootpath>\n");
        goto usage;
    }
    return ftbwriter.write_saveset (ssname, rootpath);

usage:
    fprintf (stderr, "usage: ftbackup backup [<options>...] <saveset> <rootpath>\n");
    fprintf (stderr, "    -blocksize <bs>       write <bs> bytes at a time\n");
    fprintf (stderr, "                            powers-of-two, range %u..%u\n", MINBLOCKSIZE, MAXBLOCKSIZE);
    fprintf (stderr, "                            default is %u\n", DEFBLOCKSIZE);
    fprintf (stderr, "    -idirect              use O_DIRECT when reading files\n");
    fprintf (stderr, "    -noxor                don't write any recovery blocks\n");
    fprintf (stderr, "                            default is to write recovery blocks\n");
    fprintf (stderr, "    -odirect              use O_DIRECT when writing saveset\n");
    fprintf (stderr, "    -osync                use O_SYNC when writing saveset\n");
    fprintf (stderr, "    -record <file>        record backup date/time to given file\n");
    fprintf (stderr, "    -since <file>         skip files with ctime earlier than in file\n");
    fprintf (stderr, "                            default is to process all files\n");
    fprintf (stderr, "    -verbose              print name of each file processed\n");
    fprintf (stderr, "    -verbsec <seconds>    print name of file every <seconds> seconds\n");
    fprintf (stderr, "    -xor <xorsc>,<xorgc>  write XOR recovery blocks\n");
    fprintf (stderr, "                            xorsc says write one XOR block per xorsc data blocks\n");
    fprintf (stderr, "                            xorgc says group xorgc XOR blocks together for writing\n");
    fprintf (stderr, "                            each is an integer in range 1..255\n");
    fprintf (stderr, "                            default is %u,%u\n", DEFXORSC, DEFXORGC);
    fprintf (stderr, "  Note: restore must have enough memory for bs*(xorsc*xorsc+1) bytes.\n");
    return EX_CMD;
}

/**
 * @brief Write number of nanoseconds to a file in an human readable format.
 * @param nanos = number of nanoseconds since Jan 1, 1970 0:0:0 UTC (or 0 for current time)
 * @param name = name of file to write to
 */
static bool write_nanos_to_file (uint64_t nanos, char const *name)
{
    FILE *file;
    struct timespec structts;
    struct tm structtm;

    if (nanos == 0) {
        if (clock_gettime (CLOCK_REALTIME, &structts) < 0) abort ();
    } else {
        structts.tv_sec  = nanos / 1000000000;
        structts.tv_nsec = nanos % 1000000000;
    }

    structtm = *gmtime (&structts.tv_sec);

    file = fopen (name, "w");
    if (file == NULL) {
        fprintf (stderr, "ftbackup: error creating %s\n", name);
        return false;
    }

    fprintf (file, "%04d-%02d-%02d %02d:%02d:%02d.%09ld\n",
        structtm.tm_year + 1900, structtm.tm_mon + 1, structtm.tm_mday,
        structtm.tm_hour, structtm.tm_min, structtm.tm_sec,
        structts.tv_nsec);

    if (fclose (file) < 0) {
        fprintf (stderr, "ftbackup: error closing %s\n", name);
        return false;
    }

    return true;
}

/**
 * @brief Read nanosecond time as written by write_nanos_to_file().
 * @param name = name of file to read
 * @returns 0: read/decode error
 *       else: number of nanoseconds since Jan 1, 1970 0:0:0 UTC
 */
static uint64_t read_nanos_from_file (char const *name)
{
    FILE *file;
    struct tm structtm;
    time_t secs;
    uint32_t nanos;

    memset (&structtm, 0, sizeof structtm);
    nanos = 0;

    file = fopen (name, "r");
    if (file == NULL) {
        fprintf (stderr, "ftbackup: error opening %s\n", name);
        return 0;
    }
    if (fscanf (file, "%d-%d-%d %d:%d:%d.%u",
        &structtm.tm_year, &structtm.tm_mon, &structtm.tm_mday,
        &structtm.tm_hour, &structtm.tm_min, &structtm.tm_sec, &nanos) != 7) {
        fprintf (stderr, "ftbackup: error reading %s\n", name);
        fclose (file);
        return 0;
    }
    fclose (file);

    structtm.tm_year -= 1900;
    structtm.tm_mon  --;
    secs = timegm (&structtm);
    return secs * 1000000000ULL + nanos;
}

/**
 * @brief Compare two directories.
 */
static int cmd_diff (int argc, char **argv)
{
    char *dir1, *dir2;
    if (argc != 3) goto usage;
    dir1 = argv[1];
    dir2 = argv[2];
    return diff_file (dir1, dir2) ? EX_SSIO : EX_OK;

usage:
    fprintf (stderr, "usage: ftbackup diff <path1> <path2>\n");
    return EX_CMD;
}

static bool diff_file (char const *path1, char const *path2)
{
    bool err;
    int rc1, rc2;
    struct stat stat1, stat2;

    rc1 = lstat (path1, &stat1);
    if (rc1 < 0) {
        printf ("diff file lstat %s error %d\n", path1, errno);
        return true;
    }
    rc2 = lstat (path2, &stat2);
    if (rc2 < 0) {
        printf ("diff file lstat %s error %d\n", path2, errno);
        return true;
    }

    err = false;
    if (stat1.st_mode != stat2.st_mode) {
        printf ("diff file mode mismatch %s (0%o) vs %s (0%o)\n", path1, stat1.st_mode, path2, stat2.st_mode);
        if ((stat1.st_mode ^ stat2.st_mode) & S_IFMT) return true;
        err = true;
    }

    if ((stat1.st_mtim.tv_sec  != stat2.st_mtim.tv_sec) ||
        (stat1.st_mtim.tv_nsec != stat2.st_mtim.tv_nsec)) {
        printf ("diff file mtime mismatch %s (%ld.%09ld) vs %s (%ld.%09ld)\n",
                path1, stat1.st_mtim.tv_sec, stat1.st_mtim.tv_nsec,
                path2, stat2.st_mtim.tv_sec, stat2.st_mtim.tv_nsec);
        err = true;
    }

    if (S_ISREG (stat1.st_mode)) return err | diff_regular   (path1, path2);
    if (S_ISDIR (stat1.st_mode)) return err | diff_directory (path1, path2);
    if (S_ISLNK (stat1.st_mode)) return err | diff_symlink   (path1, path2);
    return err | diff_special (path1, path2, &stat1, &stat2);
}

static bool diff_regular (char const *path1, char const *path2)
{
    bool err;
    int fd1, fd2, rc1, rc2;
    uint8_t buf1[32768], buf2[32768];

    fd1 = open (path1, O_RDONLY | O_NOATIME);
    if (fd1 < 0) {
        printf ("diff regular open %s error %d\n", path1, errno);
        return true;
    }

    fd2 = open (path2, O_RDONLY | O_NOATIME);
    if (fd2 < 0) {
        printf ("diff regular open %s error %d\n", path2, errno);
        close (fd1);
        return true;
    }

    err = false;
    while (true) {
        rc1 = read (fd1, buf1, sizeof buf1);
        if (rc1 < 0) {
            printf ("diff regular read %s error %d\n", path1, errno);
            err = true;
            break;
        }
        rc2 = read (fd2, buf2, sizeof buf2);
        if (rc2 < 0) {
            printf ("diff regular read %s error %d\n", path2, errno);
            err = true;
            break;
        }
        if (rc1 != rc2) {
            printf ("diff regular length mismatch %s vs %s\n", path1, path2);
            err = true;
            break;
        }
        if (rc1 == 0) break;
        if (memcmp (buf1, buf2, rc1) != 0) {
            printf ("diff regular content mismatch %s vs %s\n", path1, path2);
            err = true;
            break;
        }
    }

    close (fd1);
    close (fd2);
    return err;
}

static bool diff_directory (char const *path1, char const *path2)
{
    bool err;
    char *file1, *file2, *name1, *name2;
    int cmp, i, j, len1, len2, longest, nents1, nents2;
    struct dirent **names1, **names2;

    nents1 = scandir (path1, &names1, NULL, alphasort);
    if (nents1 < 0) {
        printf ("diff directory scandir %s error %d\n", path1, errno);
        return true;
    }

    err = true;

    nents2 = scandir (path2, &names2, NULL, alphasort);
    if (nents2 < 0) {
        printf ("diff directory scandir %s error %d\n", path2, errno);
        names2 = NULL;
        goto done;
    }

    longest = 0;
    for (i = 0; i < nents1; i ++) {
        len1 = strlen (names1[i]->d_name);
        if (longest < len1) longest = len1;
        len2 = strlen (names1[i]->d_name);
        if (longest < len2) longest = len2;
    }

    len1  = strlen (path1);
    len2  = strlen (path2);
    name1 = (char *) alloca (len1 + longest + 2);
    name2 = (char *) alloca (len2 + longest + 2);
    memcpy (name1, path1, len1);
    memcpy (name2, path2, len2);
    name1[len1++] = '/';
    name2[len2++] = '/';

    cmp = 0;
    err = false;
    for (i = j = 0; (i < nents1) || (j < nents2);) {
        file1 = (i < nents1) ? names1[i]->d_name : NULL;
        file2 = (j < nents2) ? names2[j]->d_name : NULL;

        // skip any '.' or '..' entries
        if ((i < nents1) && ((strcmp (file1, ".") == 0) || (strcmp (file1, "..") == 0))) {
            i ++;
            continue;
        }

        if ((j < nents2) && ((strcmp (file2, ".") == 0) || (strcmp (file2, "..") == 0))) {
            j ++;
            continue;
        }

        // do string compare iff there is a name in both arrays
        if ((i < nents1) && (j < nents2)) cmp = strcmp (file1, file2);

        // if second array empty or its name is after first, first array has a unique name
        if ((j >= nents2) || (cmp < 0)) {
            printf ("diff directory only %s contains %s\n", path1, file1);
            i ++;
            err = true;
            continue;
        }

        // if first array empty or its name is after second, second array has a unique name
        if ((i >= nents1) || (cmp > 0)) {
            printf ("diff directory only %s contains %s\n", path2, file2);
            j ++;
            err = true;
            continue;
        }

        // both names match, do nesting compare of files
        strcpy (name1 + len1, file1);
        strcpy (name2 + len2, file2);
        err |= diff_file (name1, name2);
        i ++; j ++;
    }

done:
    for (i = 0; i < nents1; i ++) free (names1[i]);
    for (i = 0; i < nents2; i ++) free (names2[i]);
    if (names1 != NULL) free (names1);
    if (names2 != NULL) free (names2);
    return err;
}

static bool diff_symlink (char const *path1, char const *path2)
{
    char buf1[32768], buf2[32768];
    int rc1, rc2;

    rc1 = readlink (path1, buf1, sizeof buf1);
    if (rc1 < 0) {
        printf ("diff symlink %s read error %d\n", path1, errno);
        return true;
    }
    rc2 = readlink (path2, buf2, sizeof buf2);
    if (rc2 < 0) {
        printf ("diff symlink %s read error %d\n", path2, errno);
        return true;
    }

    if (rc1 != rc2) {
        printf ("diff symlink length mismatch %s (%d) vs %s (%d)\n", path1, rc1, path2, rc2);
        return true;
    }
    if (memcmp (buf1, buf2, rc1) != 0) {
        printf ("diff symlink value mismatch %s (%*.*s) vs %s (%*.*s)\n",
                path1, rc1, rc1, buf1, path2, rc2, rc2, buf2);
        return true;
    }
    return false;
}

static bool diff_special (char const *path1, char const *path2, struct stat *stat1, struct stat *stat2)
{
    if (stat1->st_rdev != stat2->st_rdev) {
        printf ("diff special rdev mismatch %s (0x%lX) vs %s (0x%lX)\n",
                path1, stat1->st_rdev, path2, stat2->st_rdev);
        return true;
    }
    return false;
}

/**
 * @brief List contents of a saveset.
 */
struct FTBLister : FTBReader {
    char *maybe_output_listing (char *dstname, Header *hdr);
};

static int cmd_list (int argc, char **argv)
{
    char *ssname;
    FTBLister ftblister = FTBLister ();
    int i;

    ssname = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-simrderrs") == 0) {
                if (++ i >= argc) goto usage;
                ftblister.opt_simrderrs = atoi (argv[i]);
                continue;
            }
            fprintf (stderr, "ftbackup: unknown option %s\n", argv[i]);
            goto usage;
        }
        if (ssname != NULL) {
            fprintf (stderr, "ftbackup: unknown argument %s\n", argv[i]);
            goto usage;
        }
        ssname = argv[i];
    }
    if (ssname == NULL) {
        fprintf (stderr, "ftbackup: missing <saveset>\n");
        goto usage;
    }
    return ftblister.read_saveset (ssname, "", "");

usage:
    fprintf (stderr, "usage: ftbackup list [-simrderrs <mod>] <saveset>\n");
    return EX_CMD;
}

char *FTBLister::maybe_output_listing (char *dstname, Header *hdr)
{
    print_header (stdout, hdr, hdr->name);
    return NULL;
}

/**
 * @brief Restore from a saveset.
 */
struct FTBRestorer : FTBReader {
    bool opt_verbose;
    int opt_verbsec;
    time_t lastverbsec;

    char *maybe_output_listing (char *dstname, Header *hdr);
};

static int cmd_restore (int argc, char **argv)
{
    char const *dstprefix, *srcprefix;
    char *p, *ssname;
    FTBRestorer ftbrestorer = FTBRestorer ();
    int i;

    dstprefix = NULL;
    srcprefix = NULL;
    ssname    = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-incremental") == 0) {
                ftbrestorer.opt_incrmntl  = true;
                ftbrestorer.opt_overwrite = true;
                continue;
            }
            if (strcasecmp (argv[i], "-overwrite") == 0) {
                ftbrestorer.opt_overwrite = true;
                continue;
            }
            if (strcasecmp (argv[i], "-simrderrs") == 0) {
                if (++ i >= argc) goto usage;
                ftbrestorer.opt_simrderrs = atoi (argv[i]);
                continue;
            }
            if (strcasecmp (argv[i], "-verbose") == 0) {
                ftbrestorer.opt_verbose = true;
                continue;
            }
            if (strcasecmp (argv[i], "-verbsec") == 0) {
                if (++ i >= argc) goto usage;
                ftbrestorer.opt_verbsec = strtol (argv[i], &p, 0);
                if ((*p != 0) || (ftbrestorer.opt_verbsec <= 0)) {
                    fprintf (stderr, "ftbackup: verbsec %s must be integer greater than zero\n", argv[i]);
                    goto usage;
                }
                continue;
            }
            fprintf (stderr, "ftbackup: unknown option %s\n", argv[i]);
            goto usage;
        }
        if (ssname == NULL) {
            ssname = argv[i];
            continue;
        }
        if (srcprefix == NULL) {
            srcprefix = argv[i];
            continue;
        }
        if (dstprefix == NULL) {
            dstprefix = argv[i];
            continue;
        }
        fprintf (stderr, "ftbackup: unknown argument %s\n", argv[i]);
        goto usage;
    }
    if (dstprefix == NULL) {
        fprintf (stderr, "ftbackup: missing required arguments\n");
        goto usage;
    }
    return ftbrestorer.read_saveset (ssname, srcprefix, dstprefix);

usage:
    fprintf (stderr, "usage: ftbackup restore [-incremental] [-overwrite] [-simrderrs <mod>] [-verbose] [-verbsec <seconds>] <saveset> <srcprefix> <dstprefix>\n");
    fprintf (stderr, "        <srcprefix> = restore only files beginning with this prefix\n");
    fprintf (stderr, "                      use '' to restore all files\n");
    fprintf (stderr, "        <dstprefix> = what to replace <srcprefix> part of filename with to construct output filename\n");
    return EX_CMD;
}

char *FTBRestorer::maybe_output_listing (char *dstname, Header *hdr)
{
    if (opt_verbose || ((opt_verbsec > 0) && (time (NULL) >= lastverbsec + opt_verbsec))) {
        lastverbsec = time (NULL);
        print_header (stderr, hdr, dstname);
    }
    return dstname;
}

/**
 * @brief Display version string as given by Makefile.
 */
static int cmd_version (int argc, char **argv)
{
    printf ("%s %s%s\n", GITCOMMITHASH, GITCOMMITDATE, (GITCOMMITCLEAN ? "" : " (dirty)"));
    return 0;
}

/**
 * @brief Verify the XOR blocks of a saveset.
 */
static int cmd_xorvfy (int argc, char **argv)
{
    char const *name;
    Block baseblock, *block, *xorblk, **xorblocks;
    bool ok;
    int fd, rc;
    uint32_t bs, lastseqno, lastxorno, xorgn;
    uint64_t rdpos;
    uint8_t *xorcounts;

    if (argc != 2) {
        fprintf (stderr, "usage: ftbackup xorvfy <saveset>\n");
        return EX_CMD;
    }

    name = argv[1];
    fd = open (name, O_RDONLY);
    if (fd < 0) {
        fprintf (stderr, "open(%s) error: %s\n", name, strerror (errno));
        return EX_SSIO;
    }

    rc = read (fd, &baseblock, sizeof baseblock);
    if (rc != sizeof baseblock) {
        if (rc < 0) {
            fprintf (stderr, "read(%s) error: %s\n", name, strerror (errno));
        } else {
            fprintf (stderr, "read(%s) file too short\n", name);
        }
        close (fd);
        return EX_SSIO;
    }

    bs = 1 << baseblock.l2bs;

    block = (Block *) malloc (bs);
    xorblocks = (Block **) malloc (baseblock.xorgc * sizeof *xorblocks);
    for (xorgn = 0; xorgn < baseblock.xorgc; xorgn ++) xorblocks[xorgn] = (Block *) calloc (1, bs);
    xorcounts = (uint8_t *) calloc (baseblock.xorgc, sizeof *xorcounts);

    lastseqno = 0;
    lastxorno = 0;
    ok = false;
    rdpos = 0;

    while (((rc = pread (fd, block, bs, rdpos)) >= 0) && ((uint32_t) rc == bs)) {
        if (memcmp (block->magic, BLOCK_MAGIC, 8) != 0) {
            fprintf (stderr, "%llu: bad block magic\n", rdpos);
            goto done;
        }
        if (block->xorno == 0) {
            if ((block->l2bs != baseblock.l2bs) || (block->xorgc != baseblock.xorgc) || (block->xorsc != baseblock.xorsc)) {
                fprintf (stderr, "%llu: bad numbers %u,%u,%u, expect %u,%u,%u in seqno %u\n",
                        rdpos, block->l2bs, block->xorgc, block->xorsc,
                        baseblock.l2bs, baseblock.xorgc, baseblock.xorsc, block->seqno);
                goto done;
            }
            if (++ lastseqno != block->seqno) {
                fprintf (stderr, "%llu: bad seqno %u\n", rdpos, block->seqno);
                goto done;
            }
            xorgn  = (block->seqno - 1) % baseblock.xorgc;
            xorblk = xorblocks[xorgn];
            //if (xorgn == 0) fprintf (stderr, "xorvfy*: seqno %6u data %02X xor before %02X\n", block->seqno, block->data[0], xorblk->data[0]);
            FTBackup::xorblockdata (xorblk, block, bs);
            //if (xorgn == 0) fprintf (stderr, "xorvfy*:                      xor  after %02X\n", xorblk->data[0]);
            xorcounts[xorgn] ++;
        } else {
            if (++ lastxorno != block->xorno) {
                fprintf (stderr, "%llu: bad xorno %u\n", rdpos, block->xorno);
                goto done;
            }
            xorgn  = (block->xorno - 1) % baseblock.xorgc;
            xorblk = xorblocks[xorgn];
            if (xorcounts[xorgn] != block->xorbc) {
                fprintf (stderr, "%llu: bad xorbc %u\n", rdpos, block->xorbc);
                goto done;
            }
            //if (xorgn == 0) fprintf (stderr, "xorvfy*: xorno %6u data %02X xor before %02X\n", block->xorno, block->data[0], xorblk->data[0]);
            FTBackup::xorblockdata (xorblk, block, bs);
            //if (xorgn == 0) fprintf (stderr, "xorvfy*:                      xor  after %02X\n", xorblk->data[0]);
            for (rc = 0; rc < (uint8_t *)block + bs - block->data; rc ++) {
                if (xorblk->data[rc] != 0) {
                    fprintf (stderr, "%llu: bad xor data at xorno %u[%d]\n", rdpos, block->xorno, rc);
                    goto done;
                }
            }

            memset (xorblk, 0, bs);
            xorcounts[xorgn] = 0;
        }

        rdpos += bs;
    }

    if (rc < 0) {
        fprintf (stderr, "%llu: pread() error: %s\n", rdpos, strerror (errno));
        goto done;
    }
    if (rc != 0) {
        fprintf (stderr, "%llu: pread() only %d bytes of %u\n", rdpos, rc, bs);
        goto done;
    }

    for (xorgn = 0; xorgn < baseblock.xorgc; xorgn ++) {
        if (xorcounts[xorgn] != 0) {
            fprintf (stderr, "%llu: missing end xor block group %u\n", rdpos, xorgn);
            goto done;
        }
    }

    fprintf (stderr, "success!\n");
    ok = true;

done:
    close (fd);
    for (xorgn = 0; xorgn < baseblock.xorgc; xorgn ++) free (xorblocks[xorgn]);
    free (block);
    free (xorblocks);
    free (xorcounts);

    return ok ? EX_OK : EX_SSIO;
}

/**
 * @brief Print a backup header giving the details of a file in the saveset.
 */
void FTBackup::print_header (FILE *out, Header *hdr, char const *name)
{
    char ftype, prots[10];
    struct tm lcl;
    time_t sec;

    ftype =
        S_ISREG  (hdr->stmode) ? '-' :
        S_ISDIR  (hdr->stmode) ? 'd' :
        S_ISLNK  (hdr->stmode) ? 'l' :
        S_ISFIFO (hdr->stmode) ? 'p' :
        S_ISBLK  (hdr->stmode) ? 'b' :
        S_ISCHR  (hdr->stmode) ? 'c' : '?';

    prots[0] = (hdr->stmode & 0400) ? 'r' : '-';
    prots[1] = (hdr->stmode & 0200) ? 'w' : '-';
    prots[2] = (hdr->stmode & 0100) ? 'x' : '-';
    prots[3] = (hdr->stmode & 0040) ? 'r' : '-';
    prots[4] = (hdr->stmode & 0020) ? 'w' : '-';
    prots[5] = (hdr->stmode & 0010) ? 'x' : '-';
    prots[6] = (hdr->stmode & 0004) ? 'r' : '-';
    prots[7] = (hdr->stmode & 0002) ? 'w' : '-';
    prots[8] = (hdr->stmode & 0001) ? 'x' : '-';
    prots[9] = 0;

    sec = hdr->mtimns / 1000000000;
    lcl = *localtime (&sec);
    fprintf (out, "%c%s  %6u/%6u  %12llu  %04d-%02d-%02d %02d:%02d:%02d.%09lld  %s\n",
        ftype, prots, hdr->ownuid, hdr->owngid, hdr->size,
        lcl.tm_year + 1900, lcl.tm_mon + 1, lcl.tm_mday, lcl.tm_hour, lcl.tm_min, lcl.tm_sec,
        hdr->mtimns % 1000000000, name);
}

/**
 * @brief Check data (not XOR) block's validity.
 */
bool FTBackup::blockisvalid (Block *block)
{
    Header *hdr;
    uint32_t bs, i;

    if (!blockbaseisvalid (block)) return false;

    if ((block->l2bs != l2bs) || (block->xorgc != xorgc) || (block->xorsc != xorsc) || (block->xorbc > xorsc)) {
        fprintf (stderr, "ftbackup: bad block size\n");
        return false;
    }

    if (block->hdroffs != 0) {
        bs = 1 << l2bs;
        if (block->hdroffs < (ulong_t)block->data - (ulong_t)block) goto bbho;
        if (block->hdroffs >= bs) goto bbho;
        hdr = (Header *)((ulong_t)block + block->hdroffs);
        i = (ulong_t)hdr->magic - (ulong_t)block;
        if (i < bs) {
            i = bs - i;
            if (i > 8) i = 8;
            if (memcmp (hdr->magic, HEADER_MAGIC, i) != 0) goto bbho;
        }
    }

    return true;

bbho:
    fprintf (stderr, "ftbackup: bad block hdroffs\n");
    return false;
}

// applies to both data and xor blocks
bool FTBackup::blockbaseisvalid (Block *block)
{
    uint32_t bs, chksum;

    if (memcmp (block->magic, BLOCK_MAGIC, 8) != 0) {
        fprintf (stderr, "ftbackup: bad block magic number\n");
        return false;
    }

    bs = 1 << l2bs;
    chksum = checksumdata (block, bs);
    if (chksum != 0) {
        fprintf (stderr, "ftbackup: bad block checksum\n");
        return false;
    }

    return true;
}

/**
 * @brief XOR the source data into the destination.
 * @param dst = output pointer
 * @param src = input pointer
 * @param nby = number of bytes, assumed to be multiple of 8 ge 16
 */
void FTBackup::xorblockdata (void *dst, void const *src, uint32_t nby)
{
    uint64_t tmp;

    asm volatile (
        "   shrl    $3,%3       \n"
        "   movq    (%1),%0     \n"
        "   decl    %3          \n"
        "   .p2align 3          \n"
        "1:                     \n"
        "   addq    $8,%1       \n"
        "   xorq    %0,(%2)     \n"
        "   addq    $8,%2       \n"
        "   decl    %3          \n"
        "   movq    (%1),%0     \n"
        "   jne     1b          \n"
        "   xorq    %0,(%2)     \n"
            : "=r" (tmp), "+r" (src), "+r" (dst), "+r" (nby)
            : : "cc", "memory");
}

/**
 * @brief Compute 32-bit sum of data.
 * @param src = array of 32-bit values
 * @param nby = number of bytes, assumed to be multiple of 4 ge 8
 * @returns sum of the values
 */
uint32_t FTBackup::checksumdata (void const *src, uint32_t nby)
{
    uint32_t sum, tmp;

    asm (
        "   xorl    %2,%2       \n"
        "   shrl    $2,%3       \n"
        "   movl    (%1),%0     \n"
        "   decl    %3          \n"
        "   .p2align 3          \n"
        "1:                     \n"
        "   addq    $4,%1       \n"
        "   addl    %0,%2       \n"
        "   decl    %3          \n"
        "   movl    (%1),%0     \n"
        "   jne     1b          \n"
        "   addl    %0,%2       \n"
            : "=r" (tmp), "+r" (src), "=r" (sum), "+r" (nby)
            : : "cc", "memory");
    return sum;
}
