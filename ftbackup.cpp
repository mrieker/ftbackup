/**
 * @brief Fault-tolerant backup.
 */

static char const *gpl = "\
\n\
  Copyright (C) 2014, Mike Rieker, www.outerworldapps.com  \n\
\n\
  This program is free software: you can redistribute it and/or modify  \n\
  it under the terms of the GNU General Public License as published by  \n\
  the Free Software Foundation, either version 3 of the License, or  \n\
  (at your option) any later version.  \n\
\n\
  This program is distributed in the hope that it will be useful,  \n\
  but WITHOUT ANY WARRANTY; without even the implied warranty of  \n\
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the  \n\
  GNU General Public License for more details.  \n\
\n\
  You should have received a copy of the GNU General Public License  \n\
  along with this program.  If not, see <http://www.gnu.org/licenses/>.  \n\
\n\
  THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES,  \n\
  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND  \n\
  FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL OUTER WORLD  \n\
  APPS OR ANY CONTRIBUTORS TO THIS SOFTWARE BE LIABLE FOR ANY DIRECT, INDIRECT,  \n\
  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT  \n\
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,  \n\
  OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF  \n\
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING  \n\
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,  \n\
  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.  \n\
";

#include "ftbackup.h"
#include "ftbreader.h"
#include "ftbwriter.h"

#include <signal.h>
#include <termios.h>

static int cmd_backup (int argc, char **argv);
static bool write_nanos_to_file (uint64_t nanos, char const *name);
static uint64_t read_nanos_from_file (char const *name);

static int cmd_compare (int argc, char **argv);

static int cmd_diff (int argc, char **argv);
static bool diff_file (char const *path1, char const *path2);
static char *formatime (char *buff, time_t time);
static bool readxattrnamelist (char const *path, int *xlp, char **xnp);
static void sortxattrnamelist (char *xn, int xl);
static bool diff_regular (char const *path1, char const *path2);
static bool diff_directory (char const *path1, char const *path2);
static bool containsskipdir (int nents, struct dirent **names);
static bool issocket (char const *path, char const *file);
static bool ismountpointoremptydir (char const *name);
static bool diff_symlink (char const *path1, char const *path2);
static bool diff_special (char const *path1, char const *path2, struct stat *stat1, struct stat *stat2);

static int cmd_help (int argc, char **argv);
static int cmd_history (int argc, char **argv);
static bool sanitizedatestr (char *outstr, char const *instr);
static int cmd_history_delss (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char **wildcards, bool del);
static int cmd_history_list (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char **wildcards);
static int cmd_license (int argc, char **argv);
static int cmd_list (int argc, char **argv);
static int cmd_restore (int argc, char **argv);
static int cmd_version (int argc, char **argv);
static int cmd_xorvfy (int argc, char **argv);

static CryptoPP::BlockCipher *getciphercontext (char const *name, bool enc);
static CryptoPP::HashTransformation *gethashercontext (char const *name);
static void usagecipherargs (char const *decenc);
static bool readpasswd (char const *prompt, char *pwbuff, size_t pwsize);

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
    virtual int fsllistxattr (char const *path, char *list, int size) { return llistxattr (path, list, size); }
    virtual int fslgetxattr (char const *path, char const *name, void *value, int size) { return lgetxattr (path, name, value, size); }
    virtual int fslsetxattr (char const *path, char const *name, void const *value, int size, int flags) { return lsetxattr (path, name, value, size, flags); }
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
    virtual int fsllistxattr (char const *path, char *list, int size) { errno = ENOSYS; return -1; }
    virtual int fslgetxattr (char const *path, char const *name, void *value, int size) { errno = ENOSYS; return -1; }
    virtual int fslsetxattr (char const *path, char const *name, void const *value, int size, int flags) { errno = ENOSYS; return -1; }
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
        if (strcasecmp (argv[1], "help")    == 0) return cmd_help    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "history") == 0) return cmd_history (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "license") == 0) return cmd_license (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "list")    == 0) return cmd_list    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "restore") == 0) return cmd_restore (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "version") == 0) return cmd_version (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "xorvfy")  == 0) return cmd_xorvfy  (argc - 1, argv + 1);
        fprintf (stderr, "ftbackup: unknown command %s\n", argv[1]);
    }
    fprintf (stderr, "usage: ftbackup backup ...\n");
    fprintf (stderr, "       ftbackup compare ...\n");
    fprintf (stderr, "       ftbackup diff ...\n");
    fprintf (stderr, "       ftbackup help\n");
    fprintf (stderr, "       ftbackup history ...\n");
    fprintf (stderr, "       ftbackup license\n");
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
    uint64_t spansize;

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
                i = ftbwriter.decodecipherargs (argc, argv, i, true);
                if (i < 0) goto usage;
                continue;
            }
            if (strcasecmp (argv[i], "-history") == 0) {
                if (++ i >= argc) goto usage;
                if (memcmp (argv[i], "::", 2) == 0) {
                    ftbwriter.histssname = argv[i] + 2;
                    if (++ i >= argc) goto usage;
                }
                if (argv[i][0] == ':') goto usage;
                ftbwriter.histdbname = argv[i];
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

    // write saveset...
    ftbwriter.tfs = &fullFSAccess;
    return ftbwriter.write_saveset (ssname, rootpath);

usage:
    fprintf (stderr, "usage: ftbackup backup [<options>...] <saveset> <rootpath>\n");
    fprintf (stderr, "    -blocksize <bs>       write <bs> bytes at a time\n");
    fprintf (stderr, "                            powers-of-two, range %u..%u\n", MINBLOCKSIZE, MAXBLOCKSIZE);
    fprintf (stderr, "                            default is %u\n", DEFBLOCKSIZE);
    usagecipherargs ("encrypt");
    fprintf (stderr, "    -history [::<histss>] <histdb>\n");
    fprintf (stderr, "                          add filenames saved to SQLite database\n");
    fprintf (stderr, "    -idirect              use O_DIRECT when reading files\n");
    fprintf (stderr, "    -noxor                don't write any recovery blocks\n");
    fprintf (stderr, "                            default is to write recovery blocks\n");
    fprintf (stderr, "    -odirect              use O_DIRECT when writing saveset\n");
    fprintf (stderr, "    -osync                use O_SYNC when writing saveset\n");
    fprintf (stderr, "    -record <file>        record backup date/time to given file\n");
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
    virtual int fsllistxattr (char const *path, char *list, int size) { errno = ENOSYS; return -1; }
    virtual int fslgetxattr (char const *path, char const *name, void *value, int size) { errno = ENOSYS; return -1; }
    virtual int fslsetxattr (char const *path, char const *name, void const *value, int size, int flags);
};

static int cmd_compare (int argc, char **argv)
{
    char const *dstprefix, *srcprefix;
    char *p, *ssname;
    CompFSAccess compFSAccess = CompFSAccess ();
    FTBComparer ftbcomparer = FTBComparer ();
    int i;

    dstprefix = NULL;
    srcprefix = NULL;
    ssname    = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = ftbcomparer.decodecipherargs (argc, argv, i, false);
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
    ftbcomparer.tfs = &compFSAccess;
    return ftbcomparer.read_saveset (ssname, srcprefix, dstprefix);

usage:
    fprintf (stderr, "usage: ftbackup compare [-decrypt ...] [-incremental] [-overwrite] [-simrderrs <mod>] [-verbose] [-verbsec <seconds>] <saveset> <srcprefix> <dstprefix>\n");
    usagecipherargs ("decrypt");
    fprintf (stderr, "        <srcprefix> = compare only files beginning with this prefix\n");
    fprintf (stderr, "                      use '' to compare all files\n");
    fprintf (stderr, "        <dstprefix> = what to replace <srcprefix> part of filename with to construct output filename\n");
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

// instead of writing an extended attribute, make sure it exists and value is the same
int CompFSAccess::fslsetxattr (char const *path, char const *name, void const *value, int size, int flags)
{
    char buf[size];
    int rc = lgetxattr (path, name, buf, size);
    if (rc >= 0) {
        if ((rc != size) || (memcmp (buf, value, size) != 0)) {
            errno = MYEDATACMP;
            rc = -1;
        }
    }
    return rc;
}

/**
 * @brief Compare two directory trees.
 */
static int cmd_diff (int argc, char **argv)
{
    char *dir1, *dir2;
    int rc;

    if (argc != 3) goto usage;
    dir1 = argv[1];
    dir2 = argv[2];
    rc = diff_file (dir1, dir2) ? EX_SSIO : EX_OK;
    if (rc != EX_OK) printf ("\n");
    return rc;

usage:
    fprintf (stderr, "usage: ftbackup diff <path1> <path2>\n");
    return EX_CMD;
}

static bool diff_file (char const *path1, char const *path2)
{
    bool err;
    char time1[24], time2[24], *xb1, *xb2, *xe1, *xe2, *xm1, *xm2, *xn1, *xn2;
    int cmp, len, rc1, rc2, xa1, xa2, xl1, xl2;
    struct stat stat1, stat2;

    /*
     * Get stats of both files.
     */
    rc1 = lstat (path1, &stat1);
    if (rc1 < 0) {
        printf ("\ndiff file lstat %s error: %s\n", path1, mystrerr (errno));
        return true;
    }
    rc2 = lstat (path2, &stat2);
    if (rc2 < 0) {
        printf ("\ndiff file lstat %s error: %s\n", path2, mystrerr (errno));
        return true;
    }

    /*
     * Report differences in mode and/or mtimes.
     */
    err = false;
    len = (strlen (path1) > strlen (path2)) ? strlen (path1) : strlen (path2);

    if (stat1.st_mode != stat2.st_mode) {
        printf ("\ndiff file mode mismatch\n  %*s  0%.6o\n  %*s  0%.6o\n",
                len, path1, stat1.st_mode,
                len, path2, stat2.st_mode);
        if ((stat1.st_mode ^ stat2.st_mode) & S_IFMT) return true;
        err = true;
    }

    if ((stat1.st_mtim.tv_sec  != stat2.st_mtim.tv_sec) ||
        (stat1.st_mtim.tv_nsec != stat2.st_mtim.tv_nsec)) {
        printf ("\ndiff file mtime mismatch\n  %*s  %s.%09ld\n  %*s  %s.%09ld\n",
                len, path1, formatime (time1, stat1.st_mtim.tv_sec), stat1.st_mtim.tv_nsec,
                len, path2, formatime (time2, stat2.st_mtim.tv_sec), stat2.st_mtim.tv_nsec);
        err = true;
    }

    /*
     * Get sorted extended attribute name lists for both files.
     */
    err |= readxattrnamelist (path1, &xl1, &xn1);
    err |= readxattrnamelist (path2, &xl2, &xn2);
    xm1  = xn1;
    xm2  = xn2;

    /*
     * Compare the extended attributes.
     */
    xa1 = xa2 = 0;
    xb1 = xb2 = NULL;
    xe1 = xn1 + xl1;
    xe2 = xn2 + xl2;
    while ((xn1 < xe1) || (xn2 < xe2)) {

        /*
         * Compare next attribute names from path1 and path2.
         *   cmp = -1: xattr name from path1 .lt. xattr name from path2
         *          1: xattr name from path1 .gt. xattr name from path2
         *          0: xattr names are equal
         */
        if ((xn1 < xe1) && (xn2 < xe2)) cmp = strcmp (xn1, xn2);
                    else if (xn1 < xe1) cmp = -1;
                                   else cmp =  1;
        if (cmp < 0) {
            printf ("\ndiff file %s missing xattr %s\n", path2, xn1);
            err = true;
            xn1 += strlen (xn1) + 1;
            continue;
        }
        if (cmp > 0) {
            printf ("\ndiff file %s missing xattr %s\n", path1, xn2);
            err = true;
            xn2 += strlen (xn2) + 1;
            continue;
        }

        /*
         * Name present in both files, compare the xattr values.
         */
        xl1 = lgetxattr (path1, xn1, NULL, 0);
        if (xl1 < 0) {
            printf ("\ndiff file lgetxattr(%s,%s) error: %s\n", path1, xn1, strerror (errno));
            err = true;
            goto nextxattr;
        }
        xl2 = lgetxattr (path2, xn2, NULL, 0);
        if (xl2 < 0) {
            printf ("\ndiff file lgetxattr(%s,%s) error: %s\n", path2, xn2, strerror (errno));
            err = true;
            goto nextxattr;
        }
        if (xa1 < xl1) {
            xa1 = xl1;
            xb1 = (char *) realloc (xb1, xa1);
            if (xb1 == NULL) NOMEM ();
        }
        if (xa2 < xl2) {
            xa2 = xl2;
            xb2 = (char *) realloc (xb2, xa2);
            if (xb2 == NULL) NOMEM ();
        }
        xl1 = lgetxattr (path1, xn1, xb1, xa1);
        if (xl1 < 0) {
            printf ("\ndiff file lgetxattr(%s,%s) error: %s\n", path1, xn1, strerror (errno));
            err = true;
            goto nextxattr;
        }
        xl2 = lgetxattr (path2, xn2, xb2, xa2);
        if (xl2 < 0) {
            printf ("\ndiff file lgetxattr(%s,%s) error: %s\n", path2, xn2, strerror (errno));
            err = true;
            goto nextxattr;
        }
        if ((xl1 != xl2) || (memcmp (xb1, xb2, xl2) != 0)) {
            printf ("\ndiff file xattr %s mismatch\n  %*s  %*.*s\n  %*s  %*.*s\n",
                    xn1, len, path1, xl1, xl1, xb1, len, path2, xl2, xl2, xb2);
            err = true;
        }
    nextxattr:
        xn1 += strlen (xn1) + 1;
        xn2 += strlen (xn2) + 1;
    }

    if (xm1 != NULL) free (xm1);
    if (xm2 != NULL) free (xm2);
    if (xb1 != NULL) free (xb1);
    if (xb2 != NULL) free (xb2);

    /*
     * Compare file contents.
     */
    if (S_ISREG  (stat1.st_mode)) return err | diff_regular   (path1, path2);
    if (S_ISDIR  (stat1.st_mode)) return err | diff_directory (path1, path2);
    if (S_ISLNK  (stat1.st_mode)) return err | diff_symlink   (path1, path2);
    return err | diff_special (path1, path2, &stat1, &stat2);
}

static char *formatime (char *buff, time_t time)
{
    struct tm tm;
    tm = *localtime (&time);
    sprintf (buff, "%04d-%02d-%02d %02d:%02d:%02d",
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    return buff;
}

/**
 * @brief Read a file's extended attribute name list.
 * @param path = file's path name
 * @param xlp  = where to return length of extended attributes name list
 * @param xnp  = where to return malloc'd extended attributes name list
 * @returns true: successful
 *         false: failed, error message already printed
 */
static bool readxattrnamelist (char const *path, int *xlp, char **xnp)
{
    bool err;

    err  = false;
    *xnp = NULL;
    *xlp = llistxattr (path, NULL, 0);
    if (*xlp < 0) {
        if (errno != ENOTSUP) {
            printf ("\ndiff file llistxattr %s error: %s\n", path, mystrerr (errno));
            err = true;
        }
        *xlp = 0;
    } else if (*xlp > 0) {
        *xnp = (char *) malloc (*xlp);
        if (*xnp == NULL) NOMEM ();
        *xlp = llistxattr (path, *xnp, *xlp);
        if (*xlp < 0) {
            printf ("\ndiff file llistxattr %s error: %s\n", path, mystrerr (errno));
            *xlp = 0;
            err  = true;
        } else if (*xlp > 0) {
            if ((*xnp)[*xlp-1] != 0) {
                printf ("\ndiff file llistxattr %s badly formatted names\n", path);
                *xlp = 0;
                err  = true;
            } else {
                sortxattrnamelist (*xnp, *xlp);
            }
        }
    }
    return err;
}

/**
 * @brief Sort extended attribute name list.
 * @param xn = extended attribute name list (series of null terminated strings)
 * @param xl = length of xn (total length of all null terminated strings including the nulls)
 * @returns with strings in xn sorted
 */
static void sortxattrnamelist (char *xn, int xl)
{
    bool swapped;
    char xt[xl];
    int i, j, k;

    do {
        swapped = false;
        i = 0;
        while (i < xl) {
            j = strlen (xn + i) + 1 + i;            // point to beginning of next name in list
            if (j >= xl) break;                     // stop if there isn't a next name
            if (strcmp (xn + i, xn + j) > 0) {      // see if the i'th name is .gt. the j'th name
                k = strlen (xn + j) + 1 + j;        // point past the j'th name
                memcpy (xt, xn + i, j - i);         // copy i'th name to temp
                memmove (xn + i, xn + j, k - j);    // shift the j'th name up over the i'th name
                memcpy (xn + i + k - j, xt, j - i); // copy i'th name just after where j'th is
                i += k - j;                         // point to where the i'th name is now
                swapped = true;                     // remember that we have to scan again
            } else {
                i = j;                              // point to next name in list
            }
        }
    } while (swapped);
}

static bool diff_regular (char const *path1, char const *path2)
{
    bool err;
    int fd1, fd2, len, rc1, rc2;
    uint8_t buf1[FILEIOSIZE], buf2[FILEIOSIZE];
    uint64_t ofs;

    fd1 = open (path1, O_RDONLY | O_NOATIME);
    if (fd1 < 0) fd1 = open (path1, O_RDONLY);
    if (fd1 < 0) {
        printf ("\ndiff regular open %s error: %s\n", path1, mystrerr (errno));
        return true;
    }

    fd2 = open (path2, O_RDONLY | O_NOATIME);
    if (fd2 < 0) fd2 = open (path2, O_RDONLY);
    if (fd2 < 0) {
        printf ("\ndiff regular open %s error: %s\n", path2, mystrerr (errno));
        close (fd1);
        return true;
    }

    err = false;
    len = (strlen (path1) > strlen (path2)) ? strlen (path1) : strlen (path2);

    ofs = 0;
    while (true) {
        rc1 = read (fd1, buf1, sizeof buf1);
        if (rc1 < 0) {
            printf ("\ndiff regular read %s error: %s\n", path1, mystrerr (errno));
            err = true;
            break;
        }
        rc2 = read (fd2, buf2, sizeof buf2);
        if (rc2 < 0) {
            printf ("\ndiff regular read %s error: %s\n", path2, mystrerr (errno));
            err = true;
            break;
        }
        if (rc1 != rc2) {
            printf ("\ndiff regular length mismatch\n  %*s  %12llu\n  %*s  %12llu\n", 
                    len, path1, ofs + rc1,
                    len, path2, ofs + rc2);
            err = true;
            break;
        }
        if (rc1 == 0) break;
        for (rc2 = 0; rc2 < rc1; rc2 ++) {
            if (buf1[rc2] != buf2[rc2]) break;
        }
        if (rc2 < rc1) {
            printf ("\ndiff regular content mismatch\n  %*s  %12llu/%02X\n  %*s  %12llu/%02X\n",
                    len, path1, ofs + rc2, buf1[rc2],
                    len, path2, ofs + rc2, buf2[rc2]);
            err = true;
            break;
        }
        ofs += rc1;
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

    err = false;

    nents1 = scandir (path1, &names1, NULL, alphasort);
    if (nents1 < 0) {
        printf ("\ndiff directory scandir %s error: %s\n", path1, mystrerr (errno));
        return true;
    }

    nents2 = scandir (path2, &names2, NULL, alphasort);
    if (nents2 < 0) {
        printf ("\ndiff directory scandir %s error: %s\n", path2, mystrerr (errno));
        names2 = NULL;
        err    = true;
        goto done;
    }

    if (containsskipdir (nents1, names1) && containsskipdir (nents2, names2)) goto done;

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
    for (i = j = 0; (i < nents1) || (j < nents2);) {
        file1 = (i < nents1) ? names1[i]->d_name : NULL;
        file2 = (j < nents2) ? names2[j]->d_name : NULL;

        // skip any '.' or '..' entries and skip sockets cuz we don't back them up or restore them
        if ((file1 != NULL) && ((strcmp (file1, ".") == 0) || (strcmp (file1, "..") == 0) || issocket (path1, file1))) {
            i ++;
            continue;
        }

        if ((file2 != NULL) && ((strcmp (file2, ".") == 0) || (strcmp (file2, "..") == 0) || issocket (path2, file2))) {
            j ++;
            continue;
        }

        // do string compare iff there is a name in both arrays
             if (file1 == NULL) cmp =  1;
        else if (file2 == NULL) cmp = -1;
                           else cmp = strcmp (file1, file2);

        // if second array empty or its name is after first, first array has a unique name
        if (cmp < 0) {
            printf ("\ndiff directory only %s contains %s\n", path1, file1);
            err = true;
            i ++;
            continue;
        }

        // if first array empty or its name is after second, second array has a unique name
        if (cmp > 0) {
            printf ("\ndiff directory only %s contains %s\n", path2, file2);
            err = true;
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

static bool containsskipdir (int nents, struct dirent **names)
{
    int i;

    for (i = 0; i < nents; i ++) {
        if (strcmp (names[i]->d_name, "~SKIPDIR.FTB") == 0) return true;
    }
    return false;
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
    int len, rc1, rc2;

    rc1 = readlink (path1, buf1, sizeof buf1);
    if (rc1 < 0) {
        printf ("\ndiff symlink %s read error: %s\n", path1, mystrerr (errno));
        return true;
    }
    rc2 = readlink (path2, buf2, sizeof buf2);
    if (rc2 < 0) {
        printf ("\ndiff symlink %s read error: %s\n", path2, mystrerr (errno));
        return true;
    }

    len = (strlen (path1) > strlen (path2)) ? strlen (path1) : strlen (path2);

    if ((rc1 != rc2) || (memcmp (buf1, buf2, rc1) != 0)) {
        printf ("\ndiff symlink value mismatch\n  %*s  <%*.*s>\n  %*s  <%*.*s>\n",
                len, path1, rc1, rc1, buf1,
                len, path2, rc2, rc2, buf2);
        return true;
    }
    return false;
}

static bool diff_special (char const *path1, char const *path2, struct stat *stat1, struct stat *stat2)
{
    int len;

    if (stat1->st_rdev != stat2->st_rdev) {
        len = (strlen (path1) > strlen (path2)) ? strlen (path1) : strlen (path2);
        printf ("\ndiff special rdev mismatch\n  %*s  0x%.8lX\n  %*s  0x%.8lX\n",
                len, path1, stat1->st_rdev,
                len, path2, stat2->st_rdev);
        return true;
    }
    return false;
}

/**
 * @brief Display help screen.
 *        Opens the ftbackup.html file in a web browser.
 */
static int cmd_help (int argc, char **argv)
{
    char buff[4096], name[1024], *p, *q;
    FILE *file;

    // http://standards.freedesktop.org/desktop-entry-spec/desktop-entry-spec-1.1.html

    /*
     * First check for ~/.local/share/applications/preferred-web-browser.desktop
     */
    p = getenv ("HOME");
    if (p != NULL) {
        snprintf (name, sizeof name, "%s/.local/share/applications/preferred-web-browser.desktop", p);
        file = fopen (name, "r");
        if (file != NULL) goto findexec;
    }

    /*
     * Search /usr/share/applications/mimeinfo.cache for a line saying what app reads text/html files.
     */
    strcpy (name, "/usr/share/applications/mimeinfo.cache");
    file = fopen (name, "r");
    if (file == NULL) {
        fprintf (stderr, "ftbackup: fopen(%s) error: %s\n", name, mystrerr (errno));
        goto alt;
    }
    while (true) {
        if (fgets (buff, sizeof buff, file) == NULL) {
            fclose (file);
            fprintf (stderr, "ftbackup: can't find text/html= in %s\n", name);
            goto alt;
        }
        p = strchr (buff, '\n');
        if (p == NULL) continue;
        *p = 0;
        if (strncasecmp (buff, "text/html=", 10) == 0) break;
    }
    fclose (file);

    /*
     * Search the application file for the command line.
     */
    for (p = buff + 10;; p = ++ q) {
        q = strchr (p, ';');
        if (q != NULL) *q = 0;
        snprintf (name, sizeof name, "/usr/share/applications/%s", p);
        file = fopen (name, "r");
        if (file != NULL) break;
        if ((errno != ENOENT) || (q == NULL)) {
            fprintf (stderr, "ftbackup: fopen(%s) error: %s\n", name, mystrerr (errno));
            if (q == NULL) goto alt;
        }
    }

findexec:
    while (true) {
        if (fgets (buff, sizeof buff, file) == NULL) {
            fclose (file);
            fprintf (stderr, "ftbackup: can't find exec= in %s\n", name);
            goto alt;
        }
        p = strchr (buff, '\n');
        if (p == NULL) continue;
        *p = 0;
        if (strncasecmp (buff, "exec=", 5) == 0) break;
    }
    fclose (file);

    /*
     * The line contains %u or %U for place to put a URL.
     * Or it contains %f or %F for place to file a filename.
     */
    p = strstr (buff + 5, "%u");
    if (p == NULL) p = strstr (buff + 5, "%U");
    if (p != NULL) {
        strcpy (p, "file://");
        p += strlen (p);
        q  = argv[-1];
        if (q[0] != '/') {
            UNUSED (getcwd (p, buff + sizeof buff - p));
            p += strlen (p);
            if (p[-1] != '/') *(p ++) = '/';
            while (memcmp (q, "./", 2) == 0) q += 2;
        }
        strcpy (p, q);
        strcat (p, ".html");
        p = buff + 5;
        goto spawn;
    }

    p = strstr (buff + 5, "%f");
    if (p == NULL) p = strstr (buff + 5, "%F");
    if (p != NULL) {
        strcpy (p, argv[-1]);
        strcat (p, ".html");
        p = buff + 5;
        goto spawn;
    }

    fprintf (stderr, "ftbackup: can't find %%f,%%F,%%u,%%U tag in %s of %s\n", buff + 5, name);
alt:
    fprintf (stderr, "ftbackup: open %s.html in web browser\n", argv[-1]);
    return 1;

spawn:
    fprintf (stderr, "ftbackup: spawning %s\n", p);
    file = popen (p, "w");
    if (file == NULL) {
        fprintf (stderr, "ftbackup: popen(%s) error: %s\n", p, mystrerr (errno));
        return 1;
    }
    pclose (file);
    return 0;
}

/**
 * @brief Display history information.
 */
static int cmd_history (int argc, char **argv)
{
    bool delss, listss;
    char const *histdbname;
    char ssbefore[24], sssince[24];
    char **wildcards;
    int i, nwildcards, rc;

    /*
     * Parse command line
     */
    delss      = false;
    listss     = false;
    histdbname = NULL;
    strcpy (ssbefore, "9999-99-99 99:99:99");
    strcpy (sssince,  "0000-00-00 00:00:00");
    wildcards  = (char **) alloca (argc * sizeof *wildcards);
    nwildcards = 0;
    for (i = 0; ++ i < argc;) {
        if (argv[i][0] == '-') {
            if (strcasecmp (argv[i], "-delss") == 0) {
                if (listss) goto usage;
                delss = true;
                continue;
            }
            if (strcasecmp (argv[i], "-listss") == 0) {
                if (delss) goto usage;
                listss = true;
                continue;
            }
            if (strcasecmp (argv[i], "-ssbefore") == 0) {
                if (++ i >= argc) goto usage;
                if (!sanitizedatestr (ssbefore, argv[i])) goto usage;
                continue;
            }
            if (strcasecmp (argv[i], "-sssince") == 0) {
                if (++ i >= argc) goto usage;
                if (!sanitizedatestr (sssince, argv[i])) goto usage;
                continue;
            }
            fprintf (stderr, "ftbackup: unknown option %s\n", argv[i]);
            goto usage;
        }
        if (histdbname == NULL) {
            histdbname = argv[i];
            continue;
        }
        wildcards[nwildcards++] = argv[i];
    }

          if (delss) rc = cmd_history_delss (sssince, ssbefore, histdbname, nwildcards, wildcards, true);
    else if (listss) rc = cmd_history_delss (sssince, ssbefore, histdbname, nwildcards, wildcards, false);
                else rc = cmd_history_list  (sssince, ssbefore, histdbname, nwildcards, wildcards);

    if (rc != EX_CMD) return rc;

usage:
    fprintf (stderr, "usage: ftbackup history [-ssbefore 'yyyy-mm-dd hh:mm:ss'] [-sssince 'yyyy-mm-dd hh:mm:ss'] <histdb> [<filewildcard> ...]\n");
    fprintf (stderr, "       ftbackup history [-ssbefore 'yyyy-mm-dd hh:mm:ss'] [-sssince 'yyyy-mm-dd hh:mm:ss'] <histdb> -delss <sswildcard> ...\n");
    fprintf (stderr, "       ftbackup history [-ssbefore 'yyyy-mm-dd hh:mm:ss'] [-sssince 'yyyy-mm-dd hh:mm:ss'] <histdb> -listss [<sswildcard> ...]\n");
    return EX_CMD;
}

static bool sanitizedatestr (char *outstr, char const *instr)
{
    char buff[12], *p, delim;
    unsigned int len, val;

    len = 4;
    while (true) {
        val = strtoul (instr, &p, 10);

        sprintf (buff, "%u", val);
        if (strlen (buff) > len) return false;

        sprintf (buff, "%.*u", len, val);
        memcpy (outstr, buff, len);
        outstr += len;

        while ((*p > 0) && (*p <= ' ')) p ++;
        if (*p == 0) return true;

        delim = *(outstr ++);
        if ((delim != ' ') && (*(p ++) != delim)) return false;

        len   = 2;
        instr = p;
    }
}

/**
 * @brief Delete savesets from or List savesets in the database.
 */
static int cmd_history_delss (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char **wildcards, bool del)
{
    char const *fieldname, *name, *time, *wildcard;
    char delinsts[60], numstr[12];
    char **queryparams;
    char *wildcardend;
    int i, nqueryparams, rc, savesel_ssid, savesel_name, savesel_time, wildcardlen;
    sqlite3 *histdb;
    sqlite3_int64 ssid;
    sqlite3_stmt *savesel;
    std::string querystr;

    /*
     * Open given database.
     */
    if (histdbname == NULL) return EX_CMD;
    rc = sqlite3_open_v2 (histdbname, &histdb, del ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_open(%s) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    /*
     * Pre-compile select statement that selects the files based on given wildcards.
     */
    querystr     = "SELECT ssid,name,time FROM savesets";
    queryparams  = (char **) alloca (nwildcards * 2 * sizeof *queryparams);
    nqueryparams = 0;

    for (i = 0; i < nwildcards; i ++) {

        /*
         * Get wildcard from command line.
         * If fixed part on the beginning is null, eg, '*.x',
         * then use query to select all files.
         */
        wildcard = wildcards[i];
        wildcardlen = wildcardlength (wildcard);
        if (wildcardlen == 0) {
            querystr = "SELECT ssid,name,time FROM savesets";
            nqueryparams = 0;
            break;
        }

        /*
         * Fixed part non-null, eg, 'a' from 'a*', make it a range to select,
         * eg, 'a*' => BETWEEN 'a' AND 'b'.
         */
        queryparams[nqueryparams] = strdup (wildcard);
        queryparams[nqueryparams][wildcardlen] = 0;
        if (nqueryparams == 0) {
            querystr += " WHERE";
        } else {
            querystr += " OR";
        }

        wildcardend = strdup (wildcard);
        do if (++ wildcardend[wildcardlen-1] != 0) break;
        while (-- wildcardlen > 0);
        wildcardend[wildcardlen] = 0;

        if (wildcardlen == 0) {
            querystr += " name >= ?";
            sprintf (numstr, "%d", ++ nqueryparams);
            querystr += numstr;
        } else {
            querystr += " name BETWEEN ?";
            sprintf (numstr, "%d", ++ nqueryparams);
            querystr += numstr;

            queryparams[nqueryparams] = wildcardend;
            querystr += " AND ?";
            sprintf (numstr, "%d", ++ nqueryparams);
            querystr += numstr;
        }
    }

    querystr += " ORDER BY name";

    rc = sqlite3_prepare_v2 (histdb, querystr.c_str (), -1, &savesel, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s, SELECT FROM savesets) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    for (i = 0; i < nqueryparams; i ++) {
        rc = sqlite3_bind_text (savesel, i + 1, queryparams[i], -1, free);
        if (rc != SQLITE_OK) INTERR (sqlite3_bind_text, rc);
    }

    for (savesel_name = 0;; savesel_name ++) {
        fieldname = sqlite3_column_name (savesel, savesel_name);
        if (strcmp (fieldname, "name") == 0) break;
    }
    for (savesel_ssid = 0;; savesel_ssid ++) {
        fieldname = sqlite3_column_name (savesel, savesel_ssid);
        if (strcmp (fieldname, "ssid") == 0) break;
    }
    for (savesel_time = 0;; savesel_time ++) {
        fieldname = sqlite3_column_name (savesel, savesel_time);
        if (strcmp (fieldname, "time") == 0) break;
    }

    /*
     * Step through list of savesets.
     */
    while ((rc = sqlite3_step (savesel)) == SQLITE_ROW) {
        ssid = sqlite3_column_int64 (savesel, savesel_ssid);
        name = (char const *) sqlite3_column_text (savesel, savesel_name);
        time = (char const *) sqlite3_column_text (savesel, savesel_time);
        if (strcmp (time, sssince)  <  0) continue;
        if (strcmp (time, ssbefore) >= 0) continue;

        /*
         * See if the saveset matches any of the wildcards given on the command line.
         */
        for (i = 0; i < nwildcards; i ++) {
            wildcard = wildcards[i];
            if (wildcardmatch (wildcard, name)) break;
        }
        if (((nwildcards == 0) && !del) || (i < nwildcards)) {
            printf ("  %s  %s\n", time, name);

            /*
             * Maybe delete the saveset record.
             * Triggers in the database will delete all other referenced records.
             */
            if (del) {
                sprintf (delinsts, "DELETE FROM savesets WHERE ssid=%lld", ssid);
                rc = sqlite3_exec (histdb, delinsts, NULL, NULL, NULL);
                if (rc != SQLITE_OK) {
                    fprintf (stderr, "ftbackup: sqlite3_exec(%s, DELETE FROM savesets) error: %s\n", histdbname, sqlite3_errmsg (histdb));
                    sqlite3_close (histdb);
                    return EX_HIST;
                }
            }
        }
    }

    if (rc != SQLITE_DONE) {
        fprintf (stderr, "ftbackup: sqlite3_step(%s, SELECT FROM savesets) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        return EX_HIST;
    }

    sqlite3_finalize (savesel);
    sqlite3_close (histdb);

    return EX_OK;
}

/**
 * @brief List files in the database.
 */
static int cmd_history_list (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char **wildcards)
{
    bool printed;
    char const *fieldname, *name, *savename, *savetime, *wildcard;
    char numstr[12];
    char **queryparams;
    char *wildcardend;
    int filesel_fileid, filesel_name, i, instsel_name, instsel_time;
    int nqueryparams, rc, wildcardlen;
    sqlite3 *histdb;
    sqlite3_int64 fileid;
    sqlite3_stmt *filesel, *instsel;
    std::string querystr;

    /*
     * Open given database.
     */
    if (histdbname == NULL) return EX_CMD;
    rc = sqlite3_open_v2 (histdbname, &histdb, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_open(%s) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    /*
     * Pre-compile select statement that selects the files based on given wildcards.
     */
    querystr     = "SELECT fileid,name FROM files";
    queryparams  = (char **) alloca (nwildcards * 2 * sizeof *queryparams);
    nqueryparams = 0;

    for (i = 0; i < nwildcards; i ++) {

        /*
         * Get wildcard from command line.
         * If fixed part on the beginning is null, eg, '*.x',
         * then use query to select all files.
         */
        wildcard = wildcards[i];
        wildcardlen = wildcardlength (wildcard);
        if (wildcardlen == 0) {
            querystr = "SELECT fileid,name FROM files";
            nqueryparams = 0;
            break;
        }

        /*
         * Fixed part non-null, eg, 'a' from 'a*', make it a range to select,
         * eg, 'a*' => BETWEEN 'a' AND 'b'.
         */
        queryparams[nqueryparams] = strdup (wildcard);
        queryparams[nqueryparams][wildcardlen] = 0;
        if (nqueryparams == 0) {
            querystr += " WHERE";
        } else {
            querystr += " OR";
        }

        wildcardend = strdup (wildcard);
        do if (++ wildcardend[wildcardlen-1] != 0) break;
        while (-- wildcardlen > 0);
        wildcardend[wildcardlen] = 0;

        if (wildcardlen == 0) {
            querystr += " name >= ?";
            sprintf (numstr, "%d", ++ nqueryparams);
            querystr += numstr;
        } else {
            querystr += " name BETWEEN ?";
            sprintf (numstr, "%d", ++ nqueryparams);
            querystr += numstr;

            queryparams[nqueryparams] = wildcardend;
            querystr += " AND ?";
            sprintf (numstr, "%d", ++ nqueryparams);
            querystr += numstr;
        }
    }

    querystr += " ORDER BY name";

    rc = sqlite3_prepare_v2 (histdb, querystr.c_str (), -1, &filesel, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s, SELECT FROM files) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }

    for (i = 0; i < nqueryparams; i ++) {
        rc = sqlite3_bind_text (filesel, i + 1, queryparams[i], -1, free);
        if (rc != SQLITE_OK) INTERR (sqlite3_bind_text, rc);
    }

    for (filesel_fileid = 0;; filesel_fileid ++) {
        fieldname = sqlite3_column_name (filesel, filesel_fileid);
        if (strcmp (fieldname, "fileid") == 0) break;
    }
    for (filesel_name = 0;; filesel_name ++) {
        fieldname = sqlite3_column_name (filesel, filesel_name);
        if (strcmp (fieldname, "name") == 0) break;
    }

    /*
     * Pre-compile select statement that selects savesets for a file.
     */
    rc = sqlite3_prepare_v2 (histdb,
        "SELECT name,time FROM instances,savesets WHERE instances.fileid=?1 AND savesets.ssid=instances.ssid ORDER BY time DESC",
        -1, &instsel, NULL);
    if (rc != SQLITE_OK) {
        fprintf (stderr, "ftbackup: sqlite3_prepare(%s, SELECT FROM instances) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        exit (EX_HIST);
    }
    for (instsel_name = 0;; instsel_name ++) {
        fieldname = sqlite3_column_name (instsel, instsel_name);
        if (strcmp (fieldname, "name") == 0) break;
    }
    for (instsel_time = 0;; instsel_time ++) {
        fieldname = sqlite3_column_name (instsel, instsel_time);
        if (strcmp (fieldname, "time") == 0) break;
    }

    /*
     * Step through list of files.
     */
    while ((rc = sqlite3_step (filesel)) == SQLITE_ROW) {
        fileid = sqlite3_column_int64 (filesel, filesel_fileid);
        name   = (char const *) sqlite3_column_text (filesel, filesel_name);

        /*
         * See if the file matches any of the wildcards given on the command line.
         */
        for (i = 0; i < nwildcards; i ++) {
            wildcard = wildcards[i];
            if (wildcardmatch (wildcard, name)) break;
        }
        if ((nwildcards == 0) || (i < nwildcards)) {

            /*
             * See what savesets the file has been saved to.
             */
            sqlite3_reset (instsel);
            rc = sqlite3_bind_int64 (instsel, 1, fileid);
            if (rc != SQLITE_OK) INTERR (sqlite3_bind_int64, rc);

            /*
             * Print out saveset time and name.
             */
            printed = false;
            while ((rc = sqlite3_step (instsel)) == SQLITE_ROW) {
                savetime = (char const *) sqlite3_column_text  (instsel, instsel_time);
                savename = (char const *) sqlite3_column_text  (instsel, instsel_name);
                if (strcmp (savetime, sssince)  <  0) continue;
                if (strcmp (savetime, ssbefore) >= 0) continue;

                if (!printed) {
                    printf ("\n%s\n", name);
                    printed = true;
                }
                printf ("  %s  %s\n", savetime, savename);
            }

            if (rc != SQLITE_DONE) {
                fprintf (stderr, "ftbackup: sqlite3_step(%s, SELECT FROM instances) error: %s\n", histdbname, sqlite3_errmsg (histdb));
                sqlite3_close (histdb);
                return EX_HIST;
            }
        }
    }

    if (rc != SQLITE_DONE) {
        fprintf (stderr, "ftbackup: sqlite3_step(%s, SELECT FROM files) error: %s\n", histdbname, sqlite3_errmsg (histdb));
        sqlite3_close (histdb);
        return EX_HIST;
    }

    sqlite3_finalize (filesel);
    sqlite3_finalize (instsel);
    sqlite3_close (histdb);
    return EX_OK;
}

/**
 * @brief Display license string.
 */
static int cmd_license (int argc, char **argv)
{
    printf ("%s\n", gpl);
    return 0;
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
                i = ftblister.decodecipherargs (argc, argv, i, false);
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
                i = ftbrestorer.decodecipherargs (argc, argv, i, false);
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
struct FTBXorVfy : FTBReader {
    char *maybe_output_listing (char *dstname, Header *hdr) { return dstname; }
};

static int cmd_xorvfy (int argc, char **argv)
{
    char const *name;
    Block baseblock, *block, *xorblk, **xorblocks;
    bool ok;
    FTBXorVfy ftbxorvfy;
    int fd, i, rc;
    uint32_t bs, bsnh, lastseqno, lastxorno, xorgn;
    uint64_t rdpos;
    uint8_t *xorcounts;

    name      = NULL;
    ftbxorvfy = FTBXorVfy ();
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = ftbxorvfy.decodecipherargs (argc, argv, i, false);
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

    ftbxorvfy.maybesetdefaulthasher ();

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

    ftbxorvfy.decrypt_block (block, MINBLOCKSIZE);

    baseblock = *block;

    bs   = 1 << baseblock.l2bs;
    bsnh = bs - ftbxorvfy.hashsize ();

    block = (Block *) malloc (bs);
    xorblocks = (Block **) malloc (baseblock.xorgc * sizeof *xorblocks);
    for (xorgn = 0; xorgn < baseblock.xorgc; xorgn ++) xorblocks[xorgn] = (Block *) calloc (1, bsnh);
    xorcounts = (uint8_t *) calloc (baseblock.xorgc, sizeof *xorcounts);

    lastseqno = 0;
    lastxorno = 0;
    ok = false;
    rdpos = 0;

    while (((rc = pread (fd, block, bs, rdpos)) >= 0) && ((uint32_t) rc == bs)) {
        if (!ftbxorvfy.decrypt_block (block, bs)) {
            fprintf (stderr, "%llu: block digest not valid\n", rdpos);
            goto done;
        }
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
            FTBackup::xorblockdata (xorblk, block, bsnh);
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
            FTBackup::xorblockdata (xorblk, block, bsnh);
            for (rc = 0; rc < (uint8_t *)block + bsnh - block->data; rc ++) {
                if (xorblk->data[rc] != 0) {
                    fprintf (stderr, "%llu: bad xor data at xorno %u[%d]\n", rdpos, block->xorno, rc);
                    goto done;
                }
            }

            memset (xorblk, 0, bsnh);
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
    l2bs       = __builtin_ctz (DEFBLOCKSIZE);
    xorgc      = DEFXORGC;
    xorsc      = DEFXORSC;
    cipher     = NULL;
    hasher     = NULL;
    hashinibuf = NULL;
    hashinilen = 0;
}

FTBackup::~FTBackup ()
{
    if (cipher     != NULL) delete cipher;
    if (hasher     != NULL) delete hasher;
    if (hashinibuf != NULL) free (hashinibuf);
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
    uint32_t bsnh, i;

    if (!blockbaseisvalid (block)) return false;

    if ((block->l2bs != l2bs) || (block->xorgc != xorgc) || (block->xorsc != xorsc) || (block->xorbc > xorsc)) {
        fprintf (stderr, "ftbackup: bad block size\n");
        return false;
    }

    if (block->hdroffs != 0) {
        bsnh = (1 << l2bs) - hashsize ();
        if (block->hdroffs < (ulong_t)block->data - (ulong_t)block) goto bbho;
        if (block->hdroffs >= bsnh) goto bbho;
        hdr = (Header *)((ulong_t)block + block->hdroffs);
        i = (ulong_t)hdr->magic - (ulong_t)block;
        if (i < bsnh) {
            i = bsnh - i;
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
    if (memcmp (block->magic, BLOCK_MAGIC, 8) != 0) {
        fprintf (stderr, "ftbackup: bad block magic number\n");
        return false;
    }

    return true;
}

/**
 * @brief Compute size of hash at end of data.
 * @return hash size in bytes
 */
uint32_t FTBackup::hashsize ()
{
    return hasher->DigestSize ();
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
 * @brief Decode command-line encrypt/decrypt arguments and fill in cipher, hasher, hashinibuf, hashinilen.
 *          -{de,en}crypt [:<blockciphername>] [:<passwordhasher>] <password>
 */
int FTBackup::decodecipherargs (int argc, char **argv, int i, bool enc)
{
    char const *ciphername, *hashername;
    char keybuff[4096], keybufr[4096], *keyline, *p;
    CryptoPP::BlockCipher *newcipher;
    CryptoPP::HashTransformation *newhasher;
    FILE *keyfile;
    size_t defkeylen;

    ciphername = DEF_CIPHERNAME;
    hashername = DEF_HASHERNAME;
    cipher  = getciphercontext (ciphername, enc);
    hasher  = gethashercontext (hashername);
    keyline = NULL;

    while (++ i < argc) {
        if (argv[i][0] == ':') {
            newcipher = getciphercontext (argv[i] + 1, enc);
            if (newcipher != NULL) {
                if (cipher != NULL) delete cipher;
                ciphername = argv[i] + 1;
                cipher = newcipher;
                continue;
            }
            newhasher = gethashercontext (argv[i] + 1);
            if (newhasher != NULL) {
                if (hasher != NULL) delete hasher;
                hashername = argv[i] + 1;
                hasher = newhasher;
                continue;
            }
            fprintf (stderr, "ftbackup: unknown cipher/hasher %s\n", argv[i] + 1);
            return -1;
        }
        keyline = argv[i];
        break;
    }
    if (keyline == NULL) {
        fprintf (stderr, "ftbackup: missing key string\n");
        return -1;
    }

    defkeylen  = cipher->DefaultKeyLength ();
    hashinilen = hasher->DigestSize ();
    if (hashinilen < defkeylen) {
        fprintf (stderr, "ftbackup: hash %s size %u too small for cipher %s key size %lu\n",
                hashername, hashinilen, ciphername, defkeylen);
        return -1;
    }

    /*
     * Get key, either directly from arg list or from a file.
     */
    if (strcmp (keyline, "-") == 0) {
        while (true) {
            if (!readpasswd ("password: ", keybuff, sizeof keybuff)) return -1;
            if (!enc) break;
            if (!readpasswd ("pw again: ", keybufr, sizeof keybufr)) return -1;
            if (strcmp (keybufr, keybuff) == 0) break;
            fprintf (stderr, "ftbackup: passwords don't match\n");
        }
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
    hashinibuf = (uint8_t *) malloc (hashinilen);
    if (hashinibuf == NULL) NOMEM ();
    hasher->Update ((byte const *) keyline, (size_t) strlen (keyline));
    hasher->Final (hashinibuf);
    cipher->SetKey (hashinibuf, defkeylen, CryptoPP::g_nullNameValuePairs);

    return i;
}

#include "cryptopp562/aes.h"
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
    _CIPHOP (AES)       \
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
    return NULL;
}

/**
 * @brief Get hasher when none was specified on the command line.
 * It is used in the non-encrypted case to hash the data for an integrity check.
 */
#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include "cryptopp562/md5.h"

void FTBackup::maybesetdefaulthasher ()
{
    if (hasher == NULL) {
        hasher = new CryptoPP::Weak::MD5 ();
    }
}

static void usagecipherargs (char const *decenc)
{
    fprintf (stderr, "    -%s [:<cipher>] [:<hasher>] <keyspec>\n", decenc);
    fprintf (stderr, "                            cipher = block cipher algorithm (default %s)\n", DEF_CIPHERNAME);
    fprintf (stderr, "                            hasher = key hasher algorithm (default %s)\n", DEF_HASHERNAME);
    fprintf (stderr, "                           keyspec = - : prompt at stdin\n");
    fprintf (stderr, "                             @filename : read from first line of file\n");
    fprintf (stderr, "                                  else : literal string\n");

    fprintf (stderr, "              cipher algorithms:"
#define _CIPHOP(Name) " " #Name
    CIPHLIST
#undef _CIPHOP
    "\n");

    fprintf (stderr, "              hasher algorithms:"
#define _HASHOP(Name) " " #Name
    HASHLIST
#undef _HASHOP
    "\n");
}

static bool readpasswd (char const *prompt, char *pwbuff, size_t pwsize)
{
    int promptlen, rc, ttyfd;
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

    promptlen = strlen (prompt);
    do {
        rc = write (ttyfd, prompt, promptlen);
        if (rc < promptlen) {
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

/**
 * @brief Find out how many chars at beginning of string are not wildcard.
 * @param wild = wildcard
 * @returns number of chars at beginning of wildcard
 */
int wildcardlength (char const *wild)
{
    int i;
    for (i = 0;; i ++) {
        char wc = wild[i];
        if ((wc == 0) || (wc == '*') || (wc == '?') || (wc == '\\')) break;
    }
    return i;
}

/**
 * @brief Match a filename against a wildcard.
 * @param wild = wildcard to match
 * @param name = filename to match
 * @returns true: filename is matched by wildcard
 *         false: does not match
 */
bool wildcardmatch (char const *wild, char const *name)
{
    int i = 0;
    int j = 0;
    int wcend = strlen (wild);
    int nmend = strlen (name);

    // keep going as long as there are wildcard chars to match
    while (i < wcend) {
        char wc = wild[i];

        // '*' matches any number of (including zero) chars from name
        if (wc == '*') {

            // skip over all the '*'s in a row in wildcard
            // '**' matches chars including '/' from name (supa = true)
            // '*' matches chars excluding '/' from name (supa = false)
            bool supa = false;
            while ((++ i < wcend) && (wild[i] == '*')) {
                supa = true;
            }

            // optimization: if no more '*'s in wildcard, then match the end of both strings.
            if (strchr (wild + i, '*') == NULL) {
                int k = nmend - (wcend - i);
                if (j > k) return false;
                if (!supa && (memchr (name + j, '/', k - j) != NULL)) return false;
                return wildcardmatch (wild + i, name + k);
            }

            // try matching what's left of name with what's left in wildcard.
            // take one char at a time from name and try to match.
            // take '/' from name iff supa mode.
            while (!wildcardmatch (wild + i, name + j)) {
                if (++ j >= nmend) return false;
                if (!supa && (name[j-1] == '/')) return false;
            }
            return true;
        }

        // there's a wildcard char other than '*' to match
        // if nothing left in name, it's no match
        if (j >= nmend) return false;

        // fail match if chars don't match
        if (wc != '?') {
            if (wc == '\\') {
                if (++ i >= wcend) break;
                wc = wild[i];
            }

            if (name[j] != wc) return false;
        }

        // those chars match, on to next chars
        i ++;
        j ++;
    }

    return j >= nmend;
}
