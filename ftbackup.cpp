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
static int cmd_dumprecord (int argc, char **argv);
static int cmd_help (int argc, char **argv);
static char *afgetln (FILE *file);
static int cmd_history (int argc, char **argv);
static bool sanitizedatestr (char *outstr, char const *instr);
static int cmd_history_delss (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char const **wildcards, bool del);
static int cmd_history_list (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char const **wildcards);
static int cmd_license (int argc, char **argv);
static int cmd_list (int argc, char **argv);
static int cmd_restore (int argc, char **argv, IFSAccess *tfs);
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
 * @brief Compare saveset file to disk file:
 *        - when file is written to, compare with actual contents already on disk
 *        - files should never be read, so return error status if attempt to read
 */
struct CompFSAccess : IFSAccess {
    CompFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0) { errno = ENOSYS; return -1; }
    virtual int fsclose (int fd) { return close (fd); }
    virtual int fscreat (char const *name, char const *tmpname, bool overwrite, mode_t mode=0);
    virtual int fsclose (int fd, char const *name, char const *tmpname, bool overwrite) { return close (fd); }
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

CompFSAccess::CompFSAccess () { }
static CompFSAccess compFSAccess;

// instead of creating the file, we open it for reading so when file is written, we can compare data.
int CompFSAccess::fscreat (char const *name, char const *tmpname, bool overwrite, mode_t mode)
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
 * @brief All accesses to the filesystem are passed through to corresponding system calls.
 */
struct FullFSAccess : IFSAccess {
    FullFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0) { return open (name, flags, mode); }
    virtual int fsclose (int fd) { return close (fd); }
    virtual int fscreat (char const *name, char const *tmpname, bool overwrite, mode_t mode=0);
    virtual int fsclose (int fd, char const *name, char const *tmpname, bool overwrite);
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

int FullFSAccess::fscreat (char const *name, char const *tmpname, bool overwrite, mode_t mode)
{
    struct stat statbuf;

    // optimization:  if not overwriting and permanent file already exists, return EEXIST
    if (!overwrite && (stat (name, &statbuf) >= 0)) {
        errno = EEXIST;
        return -1;
    }

    // either overwriting or file doesn't exist, create temporary file
    return open (tmpname, O_WRONLY | O_CREAT, mode);
}

int FullFSAccess::fsclose (int fd, char const *name, char const *tmpname, bool overwrite)
{
    // close temporary file
    if (close (fd) < 0) return -1;

    // if overwriting, rename temp file to perm name, deleting any previous perm file
    if (overwrite) {
        if (rename (tmpname, name) < 0) return -2;
    } else {

        // not overwriting, attempt to create hardlink, failing if perm file already exists
        if (link (tmpname, name) < 0) return -3;

        // remove temp file name
        unlink (tmpname);
    }
    return 0;
}

static FullFSAccess fullFSAccess;

/**
 * @brief All accesses to the filesystem are returned as 'not implemented' errors.
 */
struct NullFSAccess : IFSAccess {
    NullFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0) { errno = ENOSYS; return -1; }
    virtual int fsclose (int fd) { errno = ENOSYS; return -1; }
    virtual int fscreat (char const *name, char const *tmpname, bool overwrite, mode_t mode=0) { errno = ENOSYS; return -1; }
    virtual int fsclose (int fd, char const *name, char const *tmpname, bool overwrite) { errno = ENOSYS; return -1; }
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
        if (strcasecmp (argv[1], "backup")     == 0) return cmd_backup     (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "compare")    == 0) return cmd_restore    (argc - 1, argv + 1, &compFSAccess);
        if (strcasecmp (argv[1], "diff")       == 0) return cmd_diff       (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "dumprecord") == 0) return cmd_dumprecord (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "help")       == 0) return cmd_help       (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "history")    == 0) return cmd_history    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "license")    == 0) return cmd_license    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "list")       == 0) return cmd_list       (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "restore")    == 0) return cmd_restore    (argc - 1, argv + 1, &fullFSAccess);
        if (strcasecmp (argv[1], "version")    == 0) return cmd_version    (argc - 1, argv + 1);
        if (strcasecmp (argv[1], "xorvfy")     == 0) return cmd_xorvfy     (argc - 1, argv + 1);
        fprintf (stderr, "ftbackup: unknown command %s\n", argv[1]);
    }
    fprintf (stderr, "usage: ftbackup backup ...\n");
    fprintf (stderr, "       ftbackup compare ...\n");
    fprintf (stderr, "       ftbackup diff ...\n");
    fprintf (stderr, "       ftbackup dumprecord ...\n");
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
                ftbwriter.opt_record = argv[i];
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
                ftbwriter.opt_since = argv[i];
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
    fprintf (stderr, "                          add filenames saved to database\n");
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

    nents1 = scandir (path1, &names1, NULL, myalphasort);
    if (nents1 < 0) {
        printf ("\ndiff directory scandir %s error: %s\n", path1, mystrerr (errno));
        return true;
    }

    nents2 = scandir (path2, &names2, NULL, myalphasort);
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
 * @brief Dump contents of a backup -record file.
 */
static int cmd_dumprecord (int argc, char **argv)
{
    SinceReader sincrdr;
    struct tm lcl;
    time_t sec;

    if (argc != 2) goto usage;

    if (!sincrdr.open (argv[1])) {
        return EX_SSIO;
    }
    while (sincrdr.read ()) {
        sec = sincrdr.ctime / 1000000000;
        lcl = *localtime (&sec);
        printf ("%04d-%02d-%02d %02d:%02d:%02d.%09lld  %s\n",
                lcl.tm_year + 1900, lcl.tm_mon + 1, lcl.tm_mday, lcl.tm_hour, lcl.tm_min, lcl.tm_sec,
                sincrdr.ctime % 1000000000, sincrdr.fname);
    }
    sincrdr.close ();
    return 0;

usage:
    fprintf (stderr, "usage: ftbackup dumprecord <recordfile>\n");
    return EX_CMD;
}

/**
 * @brief Display help screen.
 *        Opens the ftbackup.html file in a web browser.
 */
static int cmd_help (int argc, char **argv)
{
    char *buff, *html, *name, *p, *q;
    FILE *file;
    int rc;
    uint32_t len;

    buff = NULL;

    /*
     * HTML file is same as exe file with .html on the end.
     */
    for (len = 128;; len += len / 2) {
        html = (char *) alloca (len + 5);
        rc = readlink ("/proc/self/exe", html, len);
        if (rc < 0) {
            fprintf (stderr, "readlink(/proc/self/exe) error: %s\n", mystrerr (errno));
            return EX_CMD;
        }
        if ((uint32_t) rc < len) {
            strcpy (html + rc, ".html");
            break;
        }
    }

    // http://standards.freedesktop.org/desktop-entry-spec/desktop-entry-spec-1.1.html

    /*
     * First check for ~/.local/share/applications/preferred-web-browser.desktop
     */
    p = getenv ("HOME");
    if (p != NULL) {
        name = (char *) alloca (strlen (p) + 60);
        sprintf (name, "%s/.local/share/applications/preferred-web-browser.desktop", p);
        file = fopen (name, "r");
        if (file != NULL) goto findexec;
    }

    /*
     * Search /usr/share/applications/mimeinfo.cache for a line saying what app reads text/html files.
     */
    name = (char *) alloca (40);
    strcpy (name, "/usr/share/applications/mimeinfo.cache");
    file = fopen (name, "r");
    if (file == NULL) {
        fprintf (stderr, "ftbackup: fopen(%s) error: %s\n", name, mystrerr (errno));
        goto alt;
    }
    while (true) {
        buff = afgetln (file);
        if (buff == NULL) {
            fclose (file);
            fprintf (stderr, "ftbackup: can't find text/html= in %s\n", name);
            goto alt;
        }
        if (strncasecmp (buff, "text/html=", 10) == 0) break;
        free (buff);
    }
    fclose (file);

    /*
     * Search the application file for the command line.
     */
    len  = 0;
    name = NULL;
    for (p = buff + 10;; p = ++ q) {
        q = strchr (p, ';');
        if (q != NULL) *q = 0;
        if (len < strlen (p) + 26) {
            len  = strlen (p) + 26;
            name = (char *) alloca (len);
        }
        sprintf (name, "/usr/share/applications/%s", p);
        file = fopen (name, "r");
        if (file != NULL) break;
        if ((errno != ENOENT) || (q == NULL)) {
            fprintf (stderr, "ftbackup: fopen(%s) error: %s\n", name, mystrerr (errno));
            if (q == NULL) goto alt;
        }
    }
    free (buff);

    /*
     * Command line given by 'exec='command.
     */
findexec:
    while (true) {
        buff = afgetln (file);
        if (buff == NULL) {
            fclose (file);
            fprintf (stderr, "ftbackup: can't find exec= in %s\n", name);
            goto alt;
        }
        if (strncasecmp (buff, "exec=", 5) == 0) break;
        free (buff);
    }
    fclose (file);

    /*
     * The line contains %u or %U for place to put a URL.
     * Or it contains %f or %F for place to file a filename.
     */
    buff = (char *) realloc (buff, strlen (buff) + 8 + strlen (html));
    if (buff == NULL) NOMEM ();
    p = strstr (buff + 5, "%u");
    if (p == NULL) p = strstr (buff + 5, "%U");
    if (p != NULL) {
        strcpy (p, "file://");
        strcpy (p + 7, html);
        p = buff + 5;
        goto spawn;
    }

    p = strstr (buff + 5, "%f");
    if (p == NULL) p = strstr (buff + 5, "%F");
    if (p != NULL) {
        strcpy (p, html);
        p = buff + 5;
        goto spawn;
    }

    /*
     * Can't find web browser command.
     */
    fprintf (stderr, "ftbackup: can't find %%f,%%F,%%u,%%U tag in %s of %s\n", buff + 5, name);
alt:
    fprintf (stderr, "ftbackup: open %s.html in web browser\n", argv[-1]);
    free (buff);
    return 1;

    /*
     * Start web browser.
     */
spawn:
    fprintf (stderr, "ftbackup: spawning %s\n", p);
    file = popen (p, "w");
    if (file == NULL) {
        fprintf (stderr, "ftbackup: popen(%s) error: %s\n", p, mystrerr (errno));
        free (buff);
        return 1;
    }
    pclose (file);
    free (buff);
    return 0;
}

/**
 * @brief Read line from file
 * @returns NULL: end of file
 *          else: pointer to malloc()d line buffer, null terminated (no newline)
 */
static char *afgetln (FILE *file)
{
    char *buf, *p;
    uint32_t len, ofs;

    len = 128;
    buf = (char *) malloc (len);
    if (buf == NULL) NOMEM ();
    for (ofs = 0; fgets (buf + ofs, len - ofs, file) != NULL; ofs = strlen (buf)) {
        p = strchr (buf, '\n');
        if (p != NULL) {
            *p = 0;
            return buf;
        }
        len += len / 2;
        buf  = (char *) realloc (buf, len);
        if (buf == NULL) NOMEM ();
    }
    free (buf);
    return NULL;
}

/**
 * @brief Display history information.
 */
static int cmd_history (int argc, char **argv)
{
    bool delss, listss;
    char const *histdbname;
    char ssbefore[24], sssince[24];
    char const **wildcards;
    int i, nwildcards, rc;

    /*
     * Parse command line
     */
    delss      = false;
    listss     = false;
    histdbname = NULL;
    strcpy (ssbefore, "9999-99-99 99:99:99");
    strcpy (sssince,  "0000-00-00 00:00:00");
    wildcards  = (char const **) alloca (argc * sizeof *wildcards);
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
        if (delim == ' ') {
            if ((*p == ' ') || (*p == '@') || (*p == 'T')) p ++;
        } else {
            if (*(p ++) != delim) return false;
        }

        len   = 2;
        instr = p;
    }
}

/**
 * @brief Delete savesets from or List savesets in the database.
 */
static int cmd_history_delss (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char const **wildcards, bool del)
{
    char const *name, *wildcard;
    char time[20];
    HistFileRec filebuf;
    HistSaveRec savebuf;
    int i, j, wildcardlen;
    IX_Rsz filelen, savelen;
    IX_uLong sts;
    time_t timesc;
    uint64_t timens;
    void *rabfiles, *rabsaves;
    struct tm timetm;

    struct SaveKey {
        SaveKey *next;
        uint64_t key;
    };

    SaveKey *key, *keys;

    /*
     * Open given database.
     */
    if (histdbname == NULL) return EX_CMD;
    char *histdbname_files = (char *) alloca (strlen (histdbname) + 7);
    sprintf (histdbname_files, "%s.files", histdbname);
    sts = ix_open_file2 (histdbname_files, !del, 10, &rabfiles, IX_SHARE_W, 0, NULL);
    if (sts != IX_SUCCESS) {
        fprintf (stderr, "ftbackup: ix_open_file(%s) error: %s\n", histdbname_files, ix_errlist (sts));
        exit (EX_HIST);
    }
    char *histdbname_saves = (char *) alloca (strlen (histdbname) + 7);
    sprintf (histdbname_saves, "%s.saves", histdbname);
    sts = ix_open_file2 (histdbname_saves, !del, 10, &rabsaves, IX_SHARE_W, 0, NULL);
    if (sts != IX_SUCCESS) {
        fprintf (stderr, "ftbackup: ix_open_file(%s) error: %s\n", histdbname_saves, ix_errlist (sts));
        exit (EX_HIST);
    }

    /*
     * If no wildcard given, use '**' to get everything.
     * Only for the list function (not delete).
     */
    if (!del && (nwildcards == 0)) {
        wildcards = (char const **) alloca (sizeof *wildcards);
        wildcards[0] = "**";
        nwildcards = 1;
    }

    /*
     * Scan through each given wildcard.
     */
    keys = NULL;
    for (i = 0; i < nwildcards; i ++) {

        /*
         * Get wildcard from command line.
         * Get portion used for start of key.
         */
        wildcard = wildcards[i];
        wildcardlen = wildcardlength (wildcard);

        /*
         * Find first possible match for the wildcard.
         */
        if (wildcardlen > 0) {
            sts = ix_search_key (rabsaves, IX_SEARCH_GEF, 1, wildcardlen, (IX_Rbf const *) wildcard,
                                 sizeof savebuf, (IX_Rbf *) &savebuf, &savelen);
        } else {
            sts = ix_rewind (rabsaves, 1);
            if (sts != IX_SUCCESS) {
                fprintf (stderr, "ftbackup: ix_rewind(%s) error: %s\n", histdbname_saves, ix_errlist (sts));
                exit (EX_HIST);
            }
            sts = ix_search_seq (rabsaves, 1, sizeof savebuf, (IX_Rbf *) &savebuf, &savelen, 0, NULL);
        }

        while (sts == IX_SUCCESS) {
            if ((wildcardlen > 0) && (memcmp (savebuf.path, wildcard, wildcardlen) > 0)) break;

            /*
             * See if it matches time constraint.
             */
            timens = quadswab (savebuf.timens_BE);
            timesc = timens / 1000000000U;
            timetm = *localtime (&timesc);
            sprintf (time, "%.4d-%.2d-%.2d %.2d:%.2d:%.2d",
                        timetm.tm_year + 1900, timetm.tm_mon + 1, timetm.tm_mday,
                        timetm.tm_hour, timetm.tm_min, timetm.tm_sec);

            /*
             * See if the saveset matches the wildcard given on the command line.
             */
            name = savebuf.path;
            wildcard = wildcards[i];
            if (wildcardmatch (wildcards[i], name) && (strcmp (time, sssince) >= 0) && (strcmp (time, ssbefore) < 0)) {

                /*
                 * If so, print.
                 */
                printf ("  %s  %s\n", time, name);

                /*
                 * Maybe delete the saveset record.
                 */
                if (del) {
                    sts = ix_remove_rec (rabsaves);
                    if (sts != IX_SUCCESS) {
                        fprintf (stderr, "ftbackup: ix_remove_rec(%s) error: %s\n", histdbname_saves, ix_errlist (sts));
                        return EX_HIST;
                    }
                    key = (SaveKey *) malloc (sizeof *key);
                    if (key == NULL) NOMEM ();
                    key->next = keys;
                    key->key  = savebuf.timens_BE;
                    keys = key;
                }
            }

            /*
             * Maybe next saveset record matches wildcard.
             */
            sts = ix_search_seq (rabsaves, 1, sizeof savebuf, (IX_Rbf *) &savebuf, &savelen, 0, NULL);
        }

        if ((sts != IX_SUCCESS) && (sts != IX_RECNOTFOUND)) {
            fprintf (stderr, "ftbackup: ix_search(%s) error: %s\n", histdbname_saves, ix_errlist (sts));
            return EX_HIST;
        }
    }

    /*
     * If any savesets were deleted, clean the files records.
     */
    if (keys != NULL) {
        while ((sts = ix_search_seq (rabfiles, 0, sizeof filebuf, (IX_Rbf *) &filebuf, &filelen, 0, NULL)) == IX_SUCCESS) {
            j = 0;
            for (i = 0; (ulong_t)&filebuf.saves[i+1] <= (ulong_t)&filebuf + filelen; i ++) {
                for (key = keys; key != NULL; key = key->next) {
                    if (key->key == filebuf.saves[i]) break;
                }
                if (key == NULL) filebuf.saves[j++] = filebuf.saves[i];
            }
            if (j < i) {
                if (j > 0) {
                    sts = ix_modify_rec (rabfiles, (ulong_t)&filebuf.saves[j] - (ulong_t)&filebuf, (IX_Rbf *)&filebuf);
                    if (sts != IX_SUCCESS) {
                        fprintf (stderr, "ftbackup: ix_modify_rec(%s) error: %s\n", histdbname_files, ix_errlist (sts));
                        return EX_HIST;
                    }
                } else {
                    sts = ix_remove_rec (rabfiles);
                    if (sts != IX_SUCCESS) {
                        fprintf (stderr, "ftbackup: ix_remove_rec(%s) error: %s\n", histdbname_files, ix_errlist (sts));
                        return EX_HIST;
                    }
                }
            }
        }

        if (sts != IX_RECNOTFOUND) {
            fprintf (stderr, "ftbackup: ix_search(%s) error: %s\n", histdbname_files, ix_errlist (sts));
            return EX_HIST;
        }

        do {
            key = keys;
            keys = key->next;
            free (key);
        } while (keys != NULL);
    }

    ix_close_file (rabfiles);
    ix_close_file (rabsaves);

    return EX_OK;
}

/**
 * @brief List files in the database.
 */
static int cmd_history_list (char const *sssince, char const *ssbefore, char const *histdbname, int nwildcards, char const **wildcards)
{
    char const *name, *wildcard;
    HistFileRec filebuf;
    HistSaveRec savebuf;
    int i, j;
    int wildcardlen;
    IX_Rsz filelen, savelen;
    IX_uLong sts;
    struct tm timetm;
    time_t timesc;
    uint64_t timens;
    void *rabfiles, *rabsaves;

    /*
     * Open given database.
     */
    if (histdbname == NULL) return EX_CMD;
    char *histdbname_files = (char *) alloca (strlen (histdbname) + 7);
    sprintf (histdbname_files, "%s.files", histdbname);
    sts = ix_open_file2 (histdbname_files, 1, 10, &rabfiles, IX_SHARE_W, 0, NULL);
    if (sts != IX_SUCCESS) {
        fprintf (stderr, "ftbackup: ix_open(%s) error: %s\n", histdbname_files, ix_errlist (sts));
        exit (EX_HIST);
    }
    char *histdbname_saves = (char *) alloca (strlen (histdbname) + 7);
    sprintf (histdbname_saves, "%s.saves", histdbname);
    sts = ix_open_file2 (histdbname_saves, 1, 10, &rabsaves, IX_SHARE_W, 0, NULL);
    if (sts != IX_SUCCESS) {
        fprintf (stderr, "ftbackup: ix_open(%s) error: %s\n", histdbname_saves, ix_errlist (sts));
        exit (EX_HIST);
    }

    /*
     * If no wildcard given, use '**' to get everything.
     */
    if (nwildcards == 0) {
        wildcards = (char const **) alloca (sizeof *wildcards);
        wildcards[0] = "**";
        nwildcards = 1;
    }

    /*
     * Step through wildcards given on command line.
     */
    for (i = 0; i < nwildcards; i ++) {
        wildcard = wildcards[i];
        wildcardlen = wildcardlength (wildcard);

        sts = ix_search_key (rabfiles, IX_SEARCH_GEF, 0, wildcardlen, (IX_Rbf const *) wildcard, sizeof filebuf, (IX_Rbf *) &filebuf, &filelen);

        while (sts == IX_SUCCESS) {
            name = filebuf.path;
            if ((wildcardlen > 0) && (memcmp (name, wildcard, wildcardlen) > 0)) break;
            if (wildcardmatch (wildcard, name)) {
                printf ("\n%s\n", name);

                /*
                 * Print out savesets' time and name.
                 */
                for (j = 0; (ulong_t)&filebuf.saves[j+1] <= (ulong_t)&filebuf + filelen; j ++) {
                    sts = ix_search_key (rabsaves, IX_SEARCH_EQF, 0, sizeof filebuf.saves[j], (IX_Rbf *)&filebuf.saves[j], sizeof savebuf, (IX_Rbf *)&savebuf, &savelen);
                    if (sts == IX_SUCCESS) {
                        timens = quadswab (savebuf.timens_BE);
                        timesc = timens / 1000000000U;
                        timetm = *localtime (&timesc);
                        printf ("  %.4d-%.2d-%.2d %.2d:%.2d:%.2d  %s\n", 
                                timetm.tm_year + 1900, timetm.tm_mon + 1, timetm.tm_mday,
                                timetm.tm_hour, timetm.tm_min, timetm.tm_sec,
                                savebuf.path);
                    }
                }
            }

            sts = ix_search_seq (rabfiles, 1, sizeof filebuf, (IX_Rbf *)&filebuf, &filelen, 0, NULL);
        }

        if ((sts != IX_RECNOTFOUND) && (sts != IX_SUCCESS)) {
            fprintf (stderr, "ftbackup: ix_search(%s) error: %s\n", histdbname_files, ix_errlist (sts));
            return EX_HIST;
        }
    }

    ix_close_file (rabfiles);
    ix_close_file (rabsaves);

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
    bool opt_atime;
    bool opt_ctime;

    virtual char const *select_file (Header const *hdr);
};

static int cmd_list (int argc, char **argv)
{
    char *ssname;
    FTBLister ftblister = FTBLister ();
    int i;

    ftblister.opt_atime = false;
    ftblister.opt_ctime = false;
    ssname = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-atime") == 0) {
                ftblister.opt_atime = true;
                ftblister.opt_ctime = false;
                continue;
            }
            if (strcasecmp (argv[i], "-ctime") == 0) {
                ftblister.opt_atime = false;
                ftblister.opt_ctime = true;
                continue;
            }
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
    return ftblister.read_saveset (ssname);

usage:
    fprintf (stderr, "usage: ftbackup list [-atime|-ctime] [-decrypt ... ] [-simrderrs <mod>] <saveset>\n");
    usagecipherargs ("decrypt");
    return EX_CMD;
}

char const *FTBLister::select_file (Header const *hdr)
{
    Header alttimehdr;

    alttimehdr = *hdr;
    if (opt_atime) alttimehdr.mtimns = alttimehdr.atimns;
    if (opt_ctime) alttimehdr.mtimns = alttimehdr.ctimns;
    print_header (stdout, &alttimehdr, hdr->name);

    return FTBREADER_SELECT_SKIP;
}

/**
 * @brief Restore from a saveset.
 */
static int cmd_restore (int argc, char **argv, IFSAccess *tfs)
{
    char *p, *ssname;
    char const *savewildcard;
    FTBReadMapper ftbreadmapper = FTBReadMapper ();
    int i;

    savewildcard = NULL;
    ssname = NULL;
    for (i = 0; ++ i < argc;) {
        if ((argv[i][0] == '-') && (argv[i][1] != 0)) {
            if (strcasecmp (argv[i], "-decrypt") == 0) {
                i = ftbreadmapper.decodecipherargs (argc, argv, i, false);
                if (i < 0) goto usage;
                continue;
            }
            if (strcasecmp (argv[i], "-incremental") == 0) {
                ftbreadmapper.opt_incrmntl  = true;
                ftbreadmapper.opt_overwrite = true;
                continue;
            }
            if (strcasecmp (argv[i], "-mkdirs") == 0) {
                ftbreadmapper.opt_mkdirs = true;
                continue;
            }
            if (strcasecmp (argv[i], "-overwrite") == 0) {
                ftbreadmapper.opt_overwrite = true;
                continue;
            }
            if (strcasecmp (argv[i], "-simrderrs") == 0) {
                if (++ i >= argc) goto usage;
                ftbreadmapper.opt_simrderrs = atoi (argv[i]);
                continue;
            }
            if (strcasecmp (argv[i], "-verbose") == 0) {
                ftbreadmapper.opt_verbose = true;
                continue;
            }
            if (strcasecmp (argv[i], "-verbsec") == 0) {
                if (++ i >= argc) goto usage;
                ftbreadmapper.opt_verbsec = strtol (argv[i], &p, 0);
                if ((*p != 0) || (ftbreadmapper.opt_verbsec <= 0)) {
                    fprintf (stderr, "ftbackup: verbsec %s must be integer greater than zero\n", argv[i]);
                    goto usage;
                }
                continue;
            }
            if (strcasecmp (argv[i], "-xverbose") == 0) {
                ftbreadmapper.opt_xverbose = true;
                continue;
            }
            if (strcasecmp (argv[i], "-xverbsec") == 0) {
                if (++ i >= argc) goto usage;
                ftbreadmapper.opt_xverbsec = strtol (argv[i], &p, 0);
                if ((*p != 0) || (ftbreadmapper.opt_xverbsec <= 0)) {
                    fprintf (stderr, "ftbackup: xverbsec %s must be integer greater than zero\n", argv[i]);
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
        savewildcard = argv[i];
        if ((++ i >= argc) || (strcasecmp (argv[i], "-to") != 0)) {
            fprintf (stderr, "ftbackup: missing -to after savewildcard %s\n", savewildcard);
            goto usage;
        }
        if ((++ i >= argc) || (argv[i][0] == '-')) {
            fprintf (stderr, "ftbackup: missing outputmapping after savewildcard %s -to\n", savewildcard);
            goto usage;
        }
        ftbreadmapper.add_mapping (savewildcard, argv[i]);
    }
    if (savewildcard == NULL) {
        fprintf (stderr, "ftbackup: missing required arguments\n");
        goto usage;
    }
    ftbreadmapper.tfs = tfs;
    return ftbreadmapper.read_saveset (ssname);

usage:
    fprintf (stderr, "usage: ftbackup %s [-decrypt ...] [-incremental] [-mkdirs] [-overwrite] [-simrderrs <mod>] [-verbose] [-verbsec <seconds>] [-xverbose] [-xverbsec <seconds>] <saveset> {<savewildcard> -to <outputmapping>} ...\n", argv[0]);
    usagecipherargs ("decrypt");
    fprintf (stderr, "        <savewildcard> = select files from saveset that match this wildcard\n");
    fprintf (stderr, "        <outputmapping> = map the matching filenames to this string\n");
    fprintf (stderr, "        use '**' -to '' to %s all files to same name on disk\n", argv[0]);
    return EX_CMD;
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
    char const *select_file (Header const *hdr) { return FTBREADER_SELECT_SKIP; }
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
    decipher   = NULL;
    encipher   = NULL;
    hasher     = NULL;
    hashinibuf = NULL;
    hashinilen = 0;
}

FTBackup::~FTBackup ()
{
    if (decipher   != NULL) delete decipher;
    if (encipher   != NULL) delete encipher;
    if (hasher     != NULL) delete hasher;
    if (hashinibuf != NULL) free (hashinibuf);
}

/**
 * @brief Print a backup header giving the details of a file in the saveset.
 */
void FTBackup::print_header (FILE *out, Header const *hdr, char const *name)
{
    char ftype, prots[10], xatts;
    mode_t stmode;
    struct tm lcl;
    time_t sec;

    stmode = hdr->stmode;

    ftype =
        S_ISREG  (stmode) ? ((hdr->flags & HFL_HDLINK) ? 'h' : '-') :
        S_ISDIR  (stmode) ? 'd' :
        S_ISLNK  (stmode) ? 'l' :
        S_ISFIFO (stmode) ? 'p' :
        S_ISBLK  (stmode) ? 'b' :
        S_ISCHR  (stmode) ? 'c' : '?';

    prots[0] = (stmode & S_IRUSR) ? 'r' : '-';
    prots[3] = (stmode & S_IRGRP) ? 'r' : '-';
    prots[6] = (stmode & S_IROTH) ? 'r' : '-';

    prots[1] = (stmode & S_IWUSR) ? 'w' : '-';
    prots[4] = (stmode & S_IWGRP) ? 'w' : '-';
    prots[7] = (stmode & S_IWOTH) ? 'w' : '-';

    prots[2] = (stmode & S_IXUSR) ? ((stmode & S_ISUID) ? 's' : 'x') : ((stmode & S_ISUID) ? 'S' : '-');
    prots[5] = (stmode & S_IXGRP) ? ((stmode & S_ISGID) ? 's' : 'x') : ((stmode & S_ISGID) ? 'S' : '-');
    prots[8] = (stmode & S_IXOTH) ? ((stmode & S_ISVTX) ? 't' : 'x') : ((stmode & S_ISVTX) ? 'T' : '-');

    prots[9] = 0;

    xatts = (hdr->flags & HFL_XATTRS) ? '.' : ' ';

    sec = hdr->mtimns / 1000000000;
    lcl = *localtime (&sec);
    fprintf (out, "%c%s%c  %6u/%6u  %12llu  %04d-%02d-%02d %02d:%02d:%02d.%09lld  %s\n",
        ftype, prots, xatts, hdr->ownuid, hdr->owngid, hdr->size,
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
 * @brief Compute size of hash and nonce at end of data.
 * @return hash and nonce size in bytes
 */
uint32_t FTBackup::hashsize ()
{
    uint32_t hs = hasher->DigestSize ();
    if (encipher != NULL) hs += encipher->BlockSize ();
    return hs;
}

/**
 * @brief XOR the source data into the destination.
 * @param dst = output pointer
 * @param src = input pointer
 * @param nby = number of bytes, assumed to be multiple of 4 ge 16
 */
void FTBackup::xorblockdata (void *dst, void const *src, uint32_t nby)
{
#ifdef __amd64__
    uint32_t nqd;
    uint64_t tmp;

    nqd = nby / 8;
    asm volatile (
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
            : "=r" (tmp), "+r" (src), "+r" (dst), "+r" (nqd)
            : : "cc", "memory");

    if (nby & 4) {
        asm volatile (
            "   movl    8(%1),%0    \n"
            "   xorl    %0,8(%2)    \n"
                : "=r" (nqd), "+r" (src), "+r" (dst)
                : : "cc", "memory");
    }
#else
    uint32_t *du32 = (uint32_t *) dst;
    uint32_t const *su32 = (uint32_t const *) src;
    uint32_t nu32 = nby / 4;
    do *(du32 ++) ^= *(su32 ++);
    while (-- nu32 > 0);
#endif
}

/**
 * @brief Decode command-line encrypt/decrypt arguments and fill in decipher, encipher, hasher, hashinibuf, hashinilen.
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
    decipher   = getciphercontext (ciphername, false);
    encipher   = getciphercontext (ciphername, true);
    hasher     = gethashercontext (hashername);
    keyline    = NULL;

    while (++ i < argc) {
        if (argv[i][0] == ':') {
            newcipher = getciphercontext (argv[i] + 1, false);
            if (newcipher != NULL) {
                if (decipher != NULL) delete decipher;
                if (encipher != NULL) delete encipher;
                ciphername = argv[i] + 1;
                decipher   = newcipher;
                encipher   = getciphercontext (ciphername, true);
                continue;
            }
            newhasher = gethashercontext (argv[i] + 1);
            if (newhasher != NULL) {
                if (hasher != NULL) delete hasher;
                hashername = argv[i] + 1;
                hasher    = newhasher;
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

    defkeylen  = encipher->DefaultKeyLength ();
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
    decipher->SetKey (hashinibuf, defkeylen, CryptoPP::g_nullNameValuePairs);
    encipher->SetKey (hashinibuf, defkeylen, CryptoPP::g_nullNameValuePairs);

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
            if (rc >= 0) errno = MYENDOFILE;
            fprintf (stderr, "write(/dev/tty) error: %s\n", mystrerr (errno));
            goto err1;
        }
        rc = read (ttyfd, pwbuff, pwsize);
        if (rc <= 0) {
            if (rc == 0) errno = MYENDOFILE;
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
    if (err == MYENDOFILE) return "end of file";
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
        if ((wc == 0) || (wc == '\\') || wildcardchar (wc)) break;
    }
    return i;
}

bool wildcardchar (char c)
{
    return (c == '*') || (c == '?') || (c == '[');
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
    bool match, notflag, supa;
    char nc, wc, wc2;
    int i, j, k, nmend, wcend;

    i = 0;
    j = 0;
    wcend = strlen (wild);
    nmend = strlen (name);

    // keep going as long as there are wildcard chars to match
    while (i < wcend) {
        wc = wild[i];

        // '*' matches any number of (including zero) chars from name
        if (wc == '*') {

            // skip over all the '*'s in a row in wildcard
            // '**' matches chars including '/' from name (supa = true)
            // '*' matches chars excluding '/' from name (supa = false)
            supa = false;
            while ((++ i < wcend) && (wild[i] == '*')) {
                supa = true;
            }

            // optimization: if no more '*'s or '['s in wildcard, then match the end of both strings.
            if ((strchr (wild + i, '*') == NULL) &&
                (strchr (wild + i, '[') == NULL) &&
                (strchr (wild + i, '\\') == NULL)) {
                k = nmend - (wcend - i);
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
        nc = name[j];

        // for [...], name char must match one of the ... chars
        if (wc == '[') {
            match = false;
            notflag = false;
            if (++ i >= wcend) break;
            wc = wild[i];
            if ((wc == '!') || (wc == '^')) {
                notflag = true;
                if (++ i >= wcend) break;
                wc = wild[i];
            }
            do {
                if (wc == '\\') {
                    if (++ i >= wcend) break;
                    wc = wild[i];
                }
                if ((i + 2 < wcend) && (wild[i+1] == '-')) {
                    i += 2;
                    wc2 = wild[i];
                    if (wc2 == '\\') {
                        if (++ i >= wcend) break;
                        wc2 = wild[i];
                    }
                    match |= (nc >= wc) && (nc <= wc2);
                } else {
                    match |= (nc == wc);
                }
                if (++ i >= wcend) break;
                wc = wild[i];
            } while (wc != ']');
            if (!match ^ notflag) return false;
        }

        // fail match if chars don't match
        else if (wc != '?') {
            if (wc == '\\') {
                if (++ i >= wcend) break;
                wc = wild[i];
            }

            if (nc != wc) return false;
        }

        // those chars match, on to next chars
        i ++;
        j ++;
    }

    return j >= nmend;
}

/**
 * Sort function for scandir().
 * Strict sorting by unsigned char with no locale-dependent comparison.
 * Unsigned compare makes sure abc comes before abc<somethingelse>.
 * Must sort in same order as pathcmp().
 * @returns > 0: a comes after b
 *          < 0: a comes before b
 *          = 0: a same as b
 */
int myalphasort (const struct dirent **a, const struct dirent **b)
{
    char c, d;
    char const *p = (*a)->d_name;
    char const *q = (*b)->d_name;
    while ((c = *p) == (d = *q) && (c != 0)) {
        p ++; q ++;
    }
    return (int) (unsigned int) (unsigned char) c - (int) (unsigned int) (unsigned char) d;
}
