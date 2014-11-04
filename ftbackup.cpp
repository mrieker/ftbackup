/**
 * @brief Fault-tolerant backup.
 */

#include "ftbackup.h"
#include "ftbreader.h"
#include "ftbwriter.h"

#include <signal.h>
#include <termios.h>

static char *volatile siginthand_cpoutname;
static FTBWriter *siginthand_ftbwriter;

static int cmd_backup (int argc, char **argv);
static bool write_nanos_to_file (uint64_t nanos, char const *name);
static uint64_t read_nanos_from_file (char const *name);
static void siginthand (int signum);

static int cmd_compare (int argc, char **argv);

static int cmd_diff (int argc, char **argv);
static bool diff_file (char const *path1, char const *path2);
static bool diff_regular (char const *path1, char const *path2);
static bool diff_directory (char const *path1, char const *path2);
static bool issocket (char const *path, char const *file);
static bool ismountpointoremptydir (char const *name);
static bool diff_symlink (char const *path1, char const *path2);
static bool diff_special (char const *path1, char const *path2, struct stat *stat1, struct stat *stat2);

static int cmd_list (int argc, char **argv);
static int cmd_restore (int argc, char **argv);
static int cmd_version (int argc, char **argv);
static int cmd_xorvfy (int argc, char **argv);

static int decodecipherargs (CryptoPP::BlockCipher **cripter, int argc, char **argv, int i, bool enc);
static CryptoPP::BlockCipher *getciphercontext (char const *name, bool enc);
static CryptoPP::HashTransformation *gethashercontext (char const *name);
static void usagecipherargs (char const *decenc);
static bool readpasswd (char *pwbuff, size_t pwsize);

/**
 * @brief A spot for IFSAccess typeinfo and vtable.
 */
IFSAccess::~IFSAccess () { }

/**
 * @brief All accesses to the filesystem are passed through to corresponding system calls.
 */
struct FullFSAccess : IFSAccess {
    FullFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0) { return open (name, flags, mode); }
    virtual int fsclose (int fd) { return close (fd); }
    virtual int fsftruncate (int fd, uint64_t len) { return ftruncate (fd, len); }
    virtual int fsread (int fd, void *buf, int len) { return read (fd, buf, len); }
    virtual int fspread (int fd, void *buf, int len, uint64_t pos) { return pread (fd, buf, len, pos); }
    virtual int fswrite (int fd, void const *buf, int len) { return write (fd, buf, len); }
    virtual int fsfstat (int fd, struct stat *buf) { return fstat (fd, buf); }
    virtual int fsstat (char const *name, struct stat *buf) { return stat (name, buf); }
    virtual int fslstat (char const *name, struct stat *buf) { return lstat (name, buf); }
    virtual int fslutimes (char const *name, struct timespec *times) {
        return utimensat (AT_FDCWD, name, times, AT_SYMLINK_NOFOLLOW);
    }
    virtual int fslchown (char const *name, uid_t uid, gid_t gid) { return lchown (name, uid, gid); }
    virtual int fschmod (char const *name, mode_t mode) { return chmod (name, mode); }
    virtual int fsunlink (char const *name) { return unlink (name); }
    virtual int fsrmdir (char const *name) { return rmdir (name); }
    virtual int fslink (char const *oldname, char const *newname) { return link (oldname, newname); }
    virtual int fssymlink (char const *oldname, char const *newname) { return symlink (oldname, newname); }
    virtual int fsreadlink (char const *name, char *buf, int len) { return readlink (name, buf, len); }
    virtual int fsscandir (char const *dirname, struct dirent ***names, 
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) {
        return scandir (dirname, names, filter, compar);
    }
    virtual int fsmkdir (char const *dirname, mode_t mode) { return mkdir (dirname, mode); }
    virtual int fsmknod (char const *name, mode_t mode, dev_t rdev) { return mknod (name, mode, rdev); }
    virtual DIR *fsopendir (char const *name) { return opendir (name); }
    virtual struct dirent *fsreaddir (DIR *dir) { return readdir (dir); }
    virtual void fsclosedir (DIR *dir) { closedir (dir); }
};

FullFSAccess::FullFSAccess () { }
static FullFSAccess fullFSAccess;

/**
 * @brief All accesses to the filesystem are returned as 'not implemented' errors.
 */
struct NullFSAccess : IFSAccess {
    NullFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0) { errno = ENOSYS; return -1; }
    virtual int fsclose (int fd) { errno = ENOSYS; return -1; }
    virtual int fsftruncate (int fd, uint64_t len) { errno = ENOSYS; return -1; }
    virtual int fsread (int fd, void *buf, int len) { errno = ENOSYS; return -1; }
    virtual int fspread (int fd, void *buf, int len, uint64_t pos) { errno = ENOSYS; return -1; }
    virtual int fswrite (int fd, void const *buf, int len) { errno = ENOSYS; return -1; }
    virtual int fsfstat (int fd, struct stat *buf) { errno = ENOSYS; return -1; }
    virtual int fsstat (char const *name, struct stat *buf) { errno = ENOSYS; return -1; }
    virtual int fslstat (char const *name, struct stat *buf) { errno = ENOSYS; return -1; }
    virtual int fslutimes (char const *name, struct timespec *times) { errno = ENOSYS; return -1; }
    virtual int fslchown (char const *name, uid_t uid, gid_t gid) { errno = ENOSYS; return -1; }
    virtual int fschmod (char const *name, mode_t mode) { errno = ENOSYS; return -1; }
    virtual int fsunlink (char const *name) { errno = ENOSYS; return -1; }
    virtual int fsrmdir (char const *name) { errno = ENOSYS; return -1; }
    virtual int fslink (char const *oldname, char const *newname) { errno = ENOSYS; return -1; }
    virtual int fssymlink (char const *oldname, char const *newname) { errno = ENOSYS; return -1; }
    virtual int fsreadlink (char const *name, char *buf, int len) { errno = ENOSYS; return -1; }
    virtual int fsscandir (char const *dirname, struct dirent ***names, 
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) { errno = ENOSYS; return -1; }
    virtual int fsmkdir (char const *dirname, mode_t mode) { errno = ENOSYS; return -1; }
    virtual int fsmknod (char const *name, mode_t mode, dev_t rdev) { errno = ENOSYS; return -1; }
    virtual DIR *fsopendir (char const *name) { errno = ENOSYS; return NULL; }
    virtual struct dirent *fsreaddir (DIR *dir) { errno = ENOSYS; return NULL; }
    virtual void fsclosedir (DIR *dir) { }
};

NullFSAccess::NullFSAccess () { }
static NullFSAccess nullFSAccess;

int main (int argc, char **argv)
{
    setlinebuf (stdout);
    setlinebuf (stderr);

    if (argc >= 2) {
        if (strcasecmp (argv[1], "backup")  == 0) return cmd_backup  (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "compare") == 0) return cmd_compare (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "diff")    == 0) return cmd_diff    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "list")    == 0) return cmd_list    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "restore") == 0) return cmd_restore (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "version") == 0) return cmd_version (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "xorvfy")  == 0) return cmd_xorvfy  (argc - 1, argv + 1);
        fprintf (stderr, "ftbackup: unknown command %s\n", argv[1]);
    }
    fprintf (stderr, "usage: ftbackup backup ...\n");
    fprintf (stderr, "       ftbackup compare ...\n");
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
    bool restart;
    char *p, *rootpath, *ssname;
    FTBWriter ftbwriter = FTBWriter ();
    int i, j, k;
    uint32_t blocksize;
    uint64_t spansize;

    restart  = false;
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
            if (strcasecmp (argv[i], "-encrypt") == 0) {
                i = decodecipherargs (&ftbwriter.cripter, argc, argv, i, true);
                if (i < 0) goto usage;
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
            if (strcasecmp (argv[i], "-restart") == 0) {
                restart = true;
                continue;
            }
            if (strcasecmp (argv[i], "-segsize") == 0) {
                if (++ i >= argc) goto usage;
                ftbwriter.opt_segsize = strtoull (argv[i], &p, 0);
                if (*p != 0) {
                    fprintf (stderr, "ftbackup: invalid blocksize %s\n", argv[i]);
                    goto usage;
                }
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

    // how many bytes needed for one span of data blocks plus their corresponding XOR blocks
    spansize = 1 << ftbwriter.l2bs;
    if (ftbwriter.xorgc != 0) spansize *= ftbwriter.xorgc * (ftbwriter.xorsc + 1);

    // enforce this constraint so that each segment file starts an XOR span anew thus
    // allowing the reader to assume the XOR values are all zeroes at beginning of each
    // segment file
    if (ftbwriter.opt_segsize % spansize != 0) {
        fprintf (stderr, "ftbackup: segsize %llu=0x%llX not multiple of span+xor size %llu=0x%llX\n",
                ftbwriter.opt_segsize, ftbwriter.opt_segsize, spansize, spansize);
        goto usage;
    }

    // checkpoint if user does control-C
    siginthand_cpoutname = (char *) alloca (strlen (ssname) + 4);
    sprintf (siginthand_cpoutname, "%s.cp", ssname);
    siginthand_ftbwriter = &ftbwriter;
    if (signal (SIGINT, siginthand) == SIG_ERR) {
        fprintf (stderr, "ftbackup: signal(SIGINT) error: %s\n", strerror (errno));
    }

    // if restart mode, tell write_saveset() where to begin
    // otherwise, delete any old restart file laying around cuz it will be no good once we start writing
    if (restart) {
        fprintf (stderr, "ftbackup: attempting restart from %s\n", siginthand_cpoutname);
        ftbwriter.start_cpin (siginthand_cpoutname);
    } else {
        unlink (siginthand_cpoutname);
    }

    // write saveset...
    ftbwriter.tfs = &fullFSAccess;
    return ftbwriter.write_saveset (ssname, rootpath);

usage:
    fprintf (stderr, "usage: ftbackup backup [<options>...] <saveset> <rootpath>\n");
    fprintf (stderr, "    -blocksize <bs>       write <bs> bytes at a time\n");
    fprintf (stderr, "                            powers-of-two, range %u..%u\n", MINBLOCKSIZE, MAXBLOCKSIZE);
    fprintf (stderr, "                            default is %u\n", DEFBLOCKSIZE);
    usagecipherargs ("encrypt");
    fprintf (stderr, "    -idirect              use O_DIRECT when reading files\n");
    fprintf (stderr, "    -noxor                don't write any recovery blocks\n");
    fprintf (stderr, "                            default is to write recovery blocks\n");
    fprintf (stderr, "    -odirect              use O_DIRECT when writing saveset\n");
    fprintf (stderr, "    -osync                use O_SYNC when writing saveset\n");
    fprintf (stderr, "    -record <file>        record backup date/time to given file\n");
    fprintf (stderr, "    -restart              restart a checkpointed backup\n");
    fprintf (stderr, "    -segsize <segsz>      write saveset to multiple files, each of maximum size <segsz>\n");
    fprintf (stderr, "                            default is to write saveset to one file no matter how big\n");
    fprintf (stderr, "                            <segsz> %% (<bs> * <xorgc> * (<xorsc> + 1)) == 0\n");
    fprintf (stderr, "                            ...so each segment file starts a new xor span\n");
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
        if (clock_gettime (CLOCK_REALTIME, &structts) < 0) SYSERRNO (clock_gettime);
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
 * @brief Start checkpointing if user does a control-C.
 */
static void siginthand (int signum)
{
    char *cpoutname = NULL;
    asm ("xchgq %1,%0" : "+m" (siginthand_cpoutname), "+r" (cpoutname) : : "cc");
    if (cpoutname != NULL) {
        signal (signum, SIG_DFL);
        if (!siginthand_ftbwriter->start_cpout (cpoutname)) {
            dprintf (STDERR_FILENO, "\nftbackup: creat(%s) error: %s\n", cpoutname, mystrerr (errno));
            exit (EX_SSIO);
        }
        dprintf (STDERR_FILENO, "\nftbackup: writing checkpoint to %s\n", cpoutname);
    }
}

/**
 * @brief Compare saveset to filesystem.
 */
struct FTBComparer : FTBReader {
    bool opt_verbose;
    int opt_verbsec;
    time_t lastverbsec;

    char *maybe_output_listing (char *dstname, Header *hdr);
};

// when file is written to, compare with actual contents already on disk
// files should never be read, so return error status if attempt to read
struct CompFSAccess : IFSAccess {
    CompFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0);
    virtual int fsclose (int fd) { return close (fd); }
    virtual int fsftruncate (int fd, uint64_t len);
    virtual int fsread (int fd, void *buf, int len) { errno = ENOSYS; return -1; }
    virtual int fspread (int fd, void *buf, int len, uint64_t pos) { errno = ENOSYS; return -1; }
    virtual int fswrite (int fd, void const *buf, int len);
    virtual int fsfstat (int fd, struct stat *buf) { return fstat (fd, buf); }
    virtual int fsstat (char const *name, struct stat *buf) { errno = ENOSYS; return -1; }
    virtual int fslstat (char const *name, struct stat *buf) { errno = ENOSYS; return -1; }
    virtual int fslutimes (char const *name, struct timespec *times);
    virtual int fslchown (char const *name, uid_t uid, gid_t gid);
    virtual int fschmod (char const *name, mode_t mode);
    virtual int fsunlink (char const *name);
    virtual int fsrmdir (char const *name);
    virtual int fslink (char const *oldname, char const *newname);
    virtual int fssymlink (char const *oldname, char const *newname);
    virtual int fsreadlink (char const *name, char *buf, int len) { errno = ENOSYS; return -1; }
    virtual int fsscandir (char const *dirname, struct dirent ***names, 
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) {
        errno = ENOSYS; return -1;
    }
    virtual int fsmkdir (char const *dirname, mode_t mode);
    virtual int fsmknod (char const *name, mode_t mode, dev_t rdev);
    virtual DIR *fsopendir (char const *name) { errno = ENOSYS; return NULL; }
    virtual struct dirent *fsreaddir (DIR *dir) { errno = ENOSYS; return NULL; }
    virtual void fsclosedir (DIR *dir) { }
};

static int cmd_compare (int argc, char **argv)
{
    char const *srcprefix;
    char *p, *ssname;
    CompFSAccess compFSAccess = CompFSAccess ();
    FTBComparer ftbcomparer = FTBComparer ();
    int i;

    srcprefix = NULL;
    ssname    = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = decodecipherargs (&ftbcomparer.cripter, argc, argv, i, false);
                if (i < 0) goto usage;
                continue;
            }
            if (strcasecmp (argv[i], "-incremental") == 0) {
                ftbcomparer.opt_incrmntl  = true;
                ftbcomparer.opt_overwrite = true;
                continue;
            }
            if (strcasecmp (argv[i], "-simrderrs") == 0) {
                if (++ i >= argc) goto usage;
                ftbcomparer.opt_simrderrs = atoi (argv[i]);
                continue;
            }
            if (strcasecmp (argv[i], "-verbose") == 0) {
                ftbcomparer.opt_verbose = true;
                continue;
            }
            if (strcasecmp (argv[i], "-verbsec") == 0) {
                if (++ i >= argc) goto usage;
                ftbcomparer.opt_verbsec = strtol (argv[i], &p, 0);
                if ((*p != 0) || (ftbcomparer.opt_verbsec <= 0)) {
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
        fprintf (stderr, "ftbackup: unknown argument %s\n", argv[i]);
        goto usage;
    }
    if (srcprefix == NULL) {
        fprintf (stderr, "ftbackup: missing required arguments\n");
        goto usage;
    }
    ftbcomparer.tfs = &compFSAccess;
    return ftbcomparer.read_saveset (ssname, srcprefix, srcprefix);

usage:
    fprintf (stderr, "usage: ftbackup compare [-decrypt ...] [-incremental] [-overwrite] [-simrderrs <mod>] [-verbose] [-verbsec <seconds>] <saveset> <srcprefix>\n");
    usagecipherargs ("decrypt");
    fprintf (stderr, "        <srcprefix> = compare only files beginning with this prefix\n");
    fprintf (stderr, "                      use '' to compare all files\n");
    return EX_CMD;
}

char *FTBComparer::maybe_output_listing (char *dstname, Header *hdr)
{
    if (opt_verbose || ((opt_verbsec > 0) && (time (NULL) >= lastverbsec + opt_verbsec))) {
        lastverbsec = time (NULL);
        print_header (stderr, hdr, hdr->name);
    }
    return dstname;
}

CompFSAccess::CompFSAccess () { }

// instead of creating the file, we open it for reading so when file is written, we can compare data.
int CompFSAccess::fsopen (char const *name, int flags, mode_t mode)
{
    int fd = open (name, O_RDONLY | O_NOATIME, mode);
    if (fd < 0) fd = open (name, O_RDONLY, mode);
    return fd;
}

// instead of extending file to the given size, make sure it is exactly that size
// ...as this call is used to pre-extend the file to the exact size as in saveset
int CompFSAccess::fsftruncate (int fd, uint64_t len)
{
    struct stat statbuf;
    int rc = fstat (fd, &statbuf);
    if ((rc >= 0) && (len != (uint64_t) statbuf.st_size)) {
        errno = MYEDATACMP;
        rc = -1;
    }
    return rc;
}

// instead of writing the data to the file, compare it to the file's existing data
int CompFSAccess::fswrite (int fd, void const *buf, int len)
{
    char cmp[len];
    int rc = read (fd, cmp, len);
    if ((rc >= 0) && (memcmp (buf, cmp, rc) != 0)) {
        errno = MYEDATACMP;
        rc = -1;
    }
    return rc;
}

// make sure the file's times are as given
int CompFSAccess::fslutimes (char const *name, struct timespec *times)
{
    struct stat statbuf;
    int rc = lstat (name, &statbuf);
    if (rc >= 0) {
        if ((times[0].tv_sec  != statbuf.st_atim.tv_sec)  ||
            (times[0].tv_nsec != statbuf.st_atim.tv_nsec) ||
            (times[1].tv_sec  != statbuf.st_mtim.tv_sec)  ||
            (times[1].tv_nsec != statbuf.st_mtim.tv_nsec)) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
}

// make sure the file's ownership is as given
int CompFSAccess::fslchown (char const *name, uid_t uid, gid_t gid)
{
    struct stat statbuf;
    int rc = lstat (name, &statbuf);
    if (rc >= 0) {
        if ((uid != statbuf.st_uid) || (gid != statbuf.st_gid)) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
}

// make sure the file's protections and type are as given
int CompFSAccess::fschmod (char const *name, mode_t mode)
{
    struct stat statbuf;
    int rc = stat (name, &statbuf);
    if (rc >= 0) {
        if (mode != statbuf.st_mode) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
}

// instead of deleting the file, make sure the file doesn't exist
int CompFSAccess::fsunlink (char const *name)
{
    struct stat statbuf;
    int rc = stat (name, &statbuf);
    if (rc >= 0) {
        errno = MYEDATACMP;
        rc = -1;
    }
    if ((rc < 0) && (errno == ENOENT)) rc = 0;
    return rc;
}
int CompFSAccess::fsrmdir (char const *name)
{
    return fsunlink (name);
}

// instead of hardlinking to an existing file, make sure the two files are already hardlinked
int CompFSAccess::fslink (char const *oldname, char const *newname)
{
    struct stat oldstatbuf, newstatbuf;
    int rc = lstat (oldname, &oldstatbuf) | lstat (newname, &newstatbuf);
    if (rc >= 0) {
        if ((oldstatbuf.st_dev != newstatbuf.st_dev) ||
            (oldstatbuf.st_ino != newstatbuf.st_ino)) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
}

// instead of writing a symlink, make sure the existing symlink is as given
int CompFSAccess::fssymlink (char const *oldname, char const *newname)
{
    int len = strlen (oldname);
    char cmp[len+1];
    int rc = readlink (newname, cmp, len + 1);
    if (rc >= 0) {
        if ((rc != len) || (memcmp (cmp, oldname, len) != 0)) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
}

// instead of creating a directory, make sure the given directory exists
int CompFSAccess::fsmkdir (char const *dirname, mode_t mode)
{
    struct stat statbuf;
    int rc = lstat (dirname, &statbuf);
    if ((rc >= 0) && !S_ISDIR (statbuf.st_mode)) {
        errno = MYEDATACMP;
        rc = -1;
    }
    return rc;
}

// instead of making a device special file, make sure the one that exists is the same
int CompFSAccess::fsmknod (char const *name, mode_t mode, dev_t rdev)
{
    struct stat statbuf;
    int rc = lstat (name, &statbuf);
    if (rc >= 0) {
        if ((statbuf.st_mode != mode) || (statbuf.st_rdev != rdev)) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
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

    if (S_ISREG  (stat1.st_mode)) return err | diff_regular   (path1, path2);
    if (S_ISDIR  (stat1.st_mode)) return err | diff_directory (path1, path2);
    if (S_ISLNK  (stat1.st_mode)) return err | diff_symlink   (path1, path2);
    return err | diff_special (path1, path2, &stat1, &stat2);
}

static bool diff_regular (char const *path1, char const *path2)
{
    bool err;
    int fd1, fd2, rc1, rc2;
    uint8_t buf1[32768], buf2[32768];

    fd1 = open (path1, O_RDONLY | O_NOATIME);
    if (fd1 < 0) fd1 = open (path1, O_RDONLY);
    if (fd1 < 0) {
        printf ("diff regular open %s error %d\n", path1, errno);
        return true;
    }

    fd2 = open (path2, O_RDONLY | O_NOATIME);
    if (fd2 < 0) fd2 = open (path2, O_RDONLY);
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
    if ((len1 > 0) && (name1[len1-1] != '/')) name1[len1++] = '/';
    if ((len2 > 0) && (name2[len2-1] != '/')) name2[len2++] = '/';

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
            if (!issocket (path1, file1)) {
                printf ("diff directory only %s contains %s\n", path1, file1);
                err = true;
            }
            i ++;
            continue;
        }

        // if first array empty or its name is after second, second array has a unique name
        if ((i >= nents1) || (cmp > 0)) {
            if (!issocket (path2, file2)) {
                printf ("diff directory only %s contains %s\n", path2, file2);
                err = true;
            }
            j ++;
            continue;
        }

        // both names match
        strcpy (name1 + len1, file1);
        strcpy (name2 + len2, file2);

        // if both are either a mount point or an empty directory,
        // don't bother comparing the contents, cuz a mount point
        // gets backed up as an empty directory
        if (!ismountpointoremptydir (name1) || !ismountpointoremptydir (name2)) {

            // otherwise, compare the two entries
            err |= diff_file (name1, name2);
        }

        i ++; j ++;
    }

done:
    for (i = 0; i < nents1; i ++) free (names1[i]);
    for (i = 0; i < nents2; i ++) free (names2[i]);
    if (names1 != NULL) free (names1);
    if (names2 != NULL) free (names2);
    return err;
}

static bool issocket (char const *path, char const *file)
{
    char name[strlen(path)+strlen(file)+2];
    struct stat statbuf;

    sprintf (name, "%s/%s", path, file);
    return (stat (name, &statbuf) >= 0) && S_ISSOCK (statbuf.st_mode);
}

static bool ismountpointoremptydir (char const *name)
{
    char copy[strlen(name)+1], *p;
    DIR *innerdir;
    struct dirent *de;
    struct stat inner, outer;

    /*
     * If supplied file isn't a directory, it can't be
     * either a mount point or an empty directory.
     */
    if (stat (name, &inner) < 0) return false;
    if (!S_ISDIR (inner.st_mode)) return false;

    /*
     * See what device the given directory's directory is on.
     */
    strcpy (copy, name);
    while (true) {
        p = strrchr (copy, '/');
        if (p == NULL) return false;
        if (p[1] != 0) break;
        *p = 0;
    }
    if (p == copy) p ++;
    *p = 0;
    if (stat (copy, &outer) < 0) return false;

    /*
     * If the two directories are on different devices,
     * the given directory is a mount point.
     */
    if (outer.st_dev != inner.st_dev) return true;

    /*
     * Same device, see if the given directory is empty,
     * ie, if it would normally be used as a mount point.
     */
    innerdir = opendir (name);
    if (innerdir == NULL) return false;
    while ((de = readdir (innerdir)) != NULL) {
        if (strcmp (de->d_name, ".") == 0) continue;
        if (strcmp (de->d_name, "..") == 0) continue;
        closedir (innerdir);
        return false;
    }
    closedir (innerdir);
    return true;
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
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = decodecipherargs (&ftblister.cripter, argc, argv, i, false);
                if (i < 0) goto usage;
                continue;
            }
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
    ftblister.tfs = &nullFSAccess;
    return ftblister.read_saveset (ssname, "", "$$ make sure nothing gets written to disk $$");

usage:
    fprintf (stderr, "usage: ftbackup list [-decrypt ... ] [-simrderrs <mod>] <saveset>\n");
    usagecipherargs ("decrypt");
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
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = decodecipherargs (&ftbrestorer.cripter, argc, argv, i, false);
                if (i < 0) goto usage;
                continue;
            }
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
    ftbrestorer.tfs = &fullFSAccess;
    return ftbrestorer.read_saveset (ssname, srcprefix, dstprefix);

usage:
    fprintf (stderr, "usage: ftbackup restore [-decrypt ...] [-incremental] [-overwrite] [-simrderrs <mod>] [-verbose] [-verbsec <seconds>] <saveset> <srcprefix> <dstprefix>\n");
    usagecipherargs ("decrypt");
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
    CryptoPP::BlockCipher *cripter;
    int fd, i, rc;
    uint32_t bs, lastseqno, lastxorno, xorgn;
    uint64_t rdpos;
    uint8_t *xorcounts;

    name = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = decodecipherargs (&cripter, argc, argv, i, false);
                if (i < 0) goto usage;
                continue;
            }
            goto usage;
        }
        if (name != NULL) goto usage;
        name = argv[i];
    }
    if (name == NULL) goto usage;

    fd = open (name, O_RDONLY);
    if (fd < 0) {
        fprintf (stderr, "open(%s) error: %s\n", name, mystrerr (errno));
        return EX_SSIO;
    }

    block = (Block *) alloca (MINBLOCKSIZE);
    rc = read (fd, block, MINBLOCKSIZE);
    if (rc != MINBLOCKSIZE) {
        if (rc < 0) {
            fprintf (stderr, "read(%s) error: %s\n", name, mystrerr (errno));
        } else {
            fprintf (stderr, "read(%s) file too short\n", name);
        }
        close (fd);
        return EX_SSIO;
    }

    FTBReader::decrypt_block (cripter, block, MINBLOCKSIZE);

    baseblock = *block;

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
        FTBReader::decrypt_block (cripter, block, bs);
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
        fprintf (stderr, "%llu: pread() error: %s\n", rdpos, mystrerr (errno));
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

usage:
    fprintf (stderr, "usage: ftbackup xorvfy [-decrypt ...] <saveset>\n");
    usagecipherargs ("decrypt");
    return EX_CMD;
}


FTBackup::FTBackup ()
{
    l2bs    = __builtin_ctz (DEFBLOCKSIZE);
    xorgc   = DEFXORGC;
    xorsc   = DEFXORSC;
    cripter = NULL;
}

FTBackup::~FTBackup ()
{
    if (cripter != NULL) delete cripter;
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
        S_ISREG  (hdr->stmode) ? ((hdr->flags & HFL_HDLINK) ? 'h' : '-') :
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

/**
 * @brief Decode command-line encrypt/decrypt arguments and fill in 'cripter'.
 *          -{de,en}crypt <blockciphername> <passwordhasher> <password>
 */
static int decodecipherargs (CryptoPP::BlockCipher **cripter, int argc, char **argv, int i, bool enc)
{
    byte *digest;
    char keybuff[4096], *keyline, *p;
    CryptoPP::HashTransformation *hasher;
    FILE *keyfile;
    size_t defkeylen;

    /*
     * Get block cipher algorithm.
     */
    if (++ i >= argc) return -1;
    *cripter = getciphercontext (argv[i], enc);
    if (*cripter == NULL) return -1;
    defkeylen = (*cripter)->DefaultKeyLength ();

    /*
     * Get password hasher algorithm.
     */
    if (++ i >= argc) return -1;
    hasher = gethashercontext (argv[i]);
    if (hasher == NULL) return -1;

    /*
     * Get key, either directly from arg list or from a file.
     */
    if (++ i >= argc) return -1;
    keyline = argv[i];
    if (strcmp (keyline, "-") == 0) {
        if (!readpasswd (keybuff, sizeof keybuff)) return -1;
        keyline = keybuff;
    } else if (keyline[0] == '@') {
        keyfile = fopen (++ keyline, "r");
        if (keyfile == NULL) {
            fprintf (stderr, "ftbackup: open(%s) error: %s\n", keyline, mystrerr (errno));
            return -1;
        }
        if (fgets (keybuff, sizeof keybuff, keyfile) == NULL) {
            fprintf (stderr, "ftbackup: read(%s) error: %s\n", keyline, mystrerr (errno));
            fclose (keyfile);
            return -1;
        }
        fclose (keyfile);
        p = strrchr (keybuff, '\n');
        if (p == NULL) {
            fprintf (stderr, "ftbackup: read(%s) buffer overflow\n", keyline);
            return -1;
        }
        *p = 0;
        keyline = keybuff;
    }

    /*
     * Set up hash of the key string as the block cipher key.
     */
    digest = (byte *) alloca (defkeylen);
    hasher->Update ((byte const *) keyline, (size_t) strlen (keyline));
    hasher->TruncatedFinal (digest, defkeylen);
    (*cripter)->SetKey (digest, defkeylen, CryptoPP::g_nullNameValuePairs);

    delete hasher;

    return i;
}

#include "cryptopp562/blowfish.h"
#include "cryptopp562/camellia.h"
#include "cryptopp562/cast.h"
#include "cryptopp562/des.h"
#include "cryptopp562/gost.h"
#include "cryptopp562/idea.h"
#include "cryptopp562/mars.h"
#include "cryptopp562/rc2.h"
#include "cryptopp562/rc5.h"
#include "cryptopp562/rc6.h"
#include "cryptopp562/rijndael.h"
#include "cryptopp562/safer.h"
#include "cryptopp562/seed.h"
#include "cryptopp562/serpent.h"
#include "cryptopp562/shark.h"
#include "cryptopp562/skipjack.h"
#include "cryptopp562/square.h"
#include "cryptopp562/tea.h"
#include "cryptopp562/twofish.h"

#define CIPHLIST \
    _CIPHOP (Blowfish)  \
    _CIPHOP (Camellia)  \
    _CIPHOP (CAST128)   \
    _CIPHOP (CAST256)   \
    _CIPHOP (DES)       \
    _CIPHOP (DES_EDE2)  \
    _CIPHOP (DES_EDE3)  \
    _CIPHOP (DES_XEX3)  \
    _CIPHOP (GOST)      \
    _CIPHOP (IDEA)      \
    _CIPHOP (MARS)      \
    _CIPHOP (RC2)       \
    _CIPHOP (RC5)       \
    _CIPHOP (RC6)       \
    _CIPHOP (Rijndael)  \
    _CIPHOP (SAFER_K)   \
    _CIPHOP (SAFER_SK)  \
    _CIPHOP (SEED)      \
    _CIPHOP (Serpent)   \
    _CIPHOP (SHARK)     \
    _CIPHOP (SKIPJACK)  \
    _CIPHOP (Square)    \
    _CIPHOP (TEA)       \
    _CIPHOP (Twofish)   \
    _CIPHOP (XTEA)

static CryptoPP::BlockCipher *getciphercontext (char const *name, bool enc)
{
#define _CIPHOP(Name) \
    if (strcasecmp (name, #Name) == 0) { \
        return enc ? (CryptoPP::BlockCipher *) new CryptoPP::Name::Encryption () : (CryptoPP::BlockCipher *) new CryptoPP::Name::Decryption (); \
    }
    CIPHLIST
#undef _CIPHOP

    fprintf (stderr, "ftbackup: unknown cipher algorithm %s\n", name);
#define _CIPHOP(Name) \
    fprintf (stderr, "ftbackup:   %s\n", #Name);
    CIPHLIST
#undef _CIPHOP

    return NULL;
}

#include "cryptopp562/ripemd.h"
#include "cryptopp562/sha.h"
#include "cryptopp562/tiger.h"
#include "cryptopp562/whrlpool.h"

#define HASHLIST \
    _HASHOP (RIPEMD160) \
    _HASHOP (RIPEMD320) \
    _HASHOP (RIPEMD128) \
    _HASHOP (RIPEMD256) \
    _HASHOP (SHA1)      \
    _HASHOP (SHA224)    \
    _HASHOP (SHA256)    \
    _HASHOP (SHA384)    \
    _HASHOP (SHA512)    \
    _HASHOP (Tiger)     \
    _HASHOP (Whirlpool)

static CryptoPP::HashTransformation *gethashercontext (char const *name)
{
#define _HASHOP(Name) \
    if (strcasecmp (name, #Name) == 0) { \
        return new CryptoPP::Name (); \
    }
    HASHLIST
#undef _HASHOP

    fprintf (stderr, "ftbackup: unknown hasher algorithm %s\n", name);
#define _HASHOP(Name) \
    fprintf (stderr, "ftbackup:   %s\n", #Name);
    HASHLIST
#undef _HASHOP
    return NULL;
}

static void usagecipherargs (char const *decenc)
{
    fprintf (stderr, "    -%s <cipher> <hasher> <keyspec>\n", decenc);
    fprintf (stderr, "                            cipher = block cipher algorithm (use '?' for list)\n");
    fprintf (stderr, "                            hasher = key hasher algorithm (use '?' for list)\n");
    fprintf (stderr, "                            keyspec = - : prompt at stdin\n");
    fprintf (stderr, "                              @filename : read from first line of file\n");
    fprintf (stderr, "                                   else : literal string\n");
}

static bool readpasswd (char *pwbuff, size_t pwsize)
{
    int rc, ttyfd;
    struct termios nflags, oflags;

    ttyfd = open ("/dev/tty", O_RDWR | O_NOCTTY);
    if (ttyfd < 0) {
        fprintf (stderr, "open(/dev/tty) error: %s\n", mystrerr (errno));
        return false;
    }

    if (tcgetattr (ttyfd, &oflags) < 0) {
        fprintf (stderr, "tcgetattr(/dev/tty) error: %s\n", mystrerr (errno));
        goto err2;
    }

    nflags = oflags;
    nflags.c_lflag &= ~ECHO;
    nflags.c_lflag |= ECHONL;

    if (tcsetattr (ttyfd, TCSANOW, &nflags) < 0) {
        fprintf (stderr, "tcsetattr(/dev/tty) error: %s\n", mystrerr (errno));
        goto err2;
    }

    do {
        rc = write (ttyfd, "password: ", 10);
        if (rc < 10) {
            if (rc >= 0) errno = EPIPE;
            fprintf (stderr, "write(/dev/tty) error: %s\n", mystrerr (errno));
            goto err1;
        }
        rc = read (ttyfd, pwbuff, pwsize);
        if (rc <= 0) {
            if (rc == 0) errno = EPIPE;
            fprintf (stderr, "read(/dev/tty) error: %s\n", mystrerr (errno));
            goto err1;
        }
    } while ((rc == 1) || (pwbuff[rc-1] != '\n'));

    pwbuff[--rc] = 0;

    if (tcsetattr (ttyfd, TCSANOW, &oflags) < 0) {
        fprintf (stderr, "tcsetattr(/dev/tty) error: %s\n", mystrerr (errno));
        goto err2;
    }

    close (ttyfd);
    return true;

err1:
    tcsetattr (ttyfd, TCSANOW, &oflags);
err2:
    close (ttyfd);
    return false;
}

char const *mystrerr (int err)
{
    if (err == MYEDATACMP) return "data compare mismatch";
    if (err == MYESIMRDER) return "simulated read error";
    return strerror (err);
}
