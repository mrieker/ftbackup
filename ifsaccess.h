#ifndef _IFSACCESS_H
#define _IFSACCESS_H

struct IFSAccess {
    virtual ~IFSAccess ();

    virtual int fsopen (char const *name, int flags, int mode=0) =0;
    virtual int fsclose (int fd) =0;
    virtual int fsread (int fd, void *buf, int len) =0;
    virtual int fspread (int fd, void *buf, int len, uint64_t pos) =0;
    virtual int fswrite (int fd, void const *buf, int len) =0;
    virtual int fsfstat (int fd, struct stat *buf) =0;
    virtual int fsstat (char const *name, struct stat *buf) =0;
    virtual int fslstat (char const *name, struct stat *buf) =0;
    virtual int fslutimes (char const *name, struct timespec *times) =0;
    virtual int fslchown (char const *name, int uid, int gid) =0;
    virtual int fschmod (char const *name, mode_t mode) =0;
    virtual int fsunlink (char const *name) =0;
    virtual int fslink (char const *oldname, char const *newname) =0;
    virtual int fssymlink (char const *oldname, char const *newname) =0;
    virtual int fsreadlink (char const *name, char *buf, int len) =0;
    virtual int fsscandir (char const *dirname, struct dirent ***names, 
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) =0;
    virtual int fsmkdir (char const *dirname, mode_t mode) =0;
    virtual int fsmknod (char const *name, mode_t mode, dev_t dev) =0;
    virtual DIR *fsopendir (char const *name) =0;
    virtual struct dirent *fsreaddir (DIR *dir) =0;
    virtual void fsclosedir (DIR *dir) =0;
};

#endif
