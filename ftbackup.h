
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

#ifndef _FTBACKUP_H
#define _FTBACKUP_H

#define _FILE_OFFSET_BITS 64
#define __USE_GNU // fcntl.h: O_DIRECT, O_NOATIME
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include "cryptopp562/cryptlib.h"

#define PAGESIZE (4096U)
#define MINBLOCKSIZE PAGESIZE
#define DEFBLOCKSIZE (32768U)
#define MAXBLOCKSIZE (1024*1024*1024U)
#define SEGNODECDIGS 6
#define FILEIOSIZE (32768U)
#define DEF_CIPHERNAME "AES"
#define DEF_HASHERNAME "SHA1"
#define UNUSED(v) asm volatile ("" : : "g" (v))

#define DEFXORSC 31 // write one XOR block per 31 data blocks
#define DEFXORGC 2  // write XOR blocks in groups of 2, ie, after 62 data blocks

#define EX_OK    0  // everything ran ok
#define EX_CMD   1  // command error
#define EX_SSIO  2  // saveset io error
#define EX_FILIO 3  // file io error
#define EX_HIST  4  // history database error

#define BLOCK_MAGIC  "ftbackup"
#define HEADER_MAGIC "ftbheder"

#define HFL_HDLINK 0x01  // regular file is an hardlink
#define HFL_XATTRS 0x02  // xattrs follow name

#define INTERR(name,err) do { fprintf (stderr, "ftbackup: " #name "() error %d\n", err); abort (); } while (0)
#define NANOTIME(tv) ((tv).tv_sec * 1000000000ULL + (tv).tv_nsec)
#define NOMEM() do { fprintf (stderr, "ftbackup: out of memory\n"); abort (); } while (0)
#define SYSERR(name,err) do { fprintf (stderr, "ftbackup: " #name "() error: %s\n", mystrerr (err)); abort (); } while (0)
#define SYSERRNO(name) SYSERR(name,errno)

typedef unsigned int uint32_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned long long uint64_t;
typedef unsigned long ulong_t;

struct Block {
    char        magic[8];   // magic number BLOCK_MAGIC
    uint32_t    seqno;      // block sequence number
    uint32_t    xorno;      // xor sequence number (0 for data blocks)
    uint8_t     nonce[16];  // encryption nonce (zeroes if not encrypted)
    uint32_t    hdroffs;    // offset of first header in block
    uint8_t     l2bs;       // log2 block size
    uint8_t     xorbc;      // xor block count (0 for data blocks)
    uint8_t     xorgc;      // xor group count
    uint8_t     xorsc;      // xor span count
    uint8_t     data[0];
};

struct Header {
    char        magic[8];   // magic number HEADER_MAGIC
    uint64_t    mtimns;     // data mod time
    uint64_t    ctimns;     // attr mod time
    uint64_t    atimns;     // access time
    uint64_t    size;       // file size
    uint32_t    fileno;     // file number (starts at 1)
    uint32_t    stmode;     // protection and type
    uint32_t    ownuid;     // owner UID
    uint32_t    owngid;     // owner GID
    uint32_t    nameln;     // name and xattr length (incl null)
    uint8_t     flags;      // flag bits
    char        name[0];    // file name (incl null) and xattrs
};

struct FTBackup {
    uint8_t  l2bs;
    uint8_t  xorgc;
    uint8_t  xorsc;

    CryptoPP::BlockCipher *cipher;
    CryptoPP::HashTransformation *hasher;
    uint8_t *hashinibuf;
    uint32_t hashinilen;

    FTBackup ();
    ~FTBackup ();

    static void print_header (FILE *out, Header const *hdr, char const *name);
    bool blockisvalid (Block *block);
    bool blockbaseisvalid (Block *block);
    int decodecipherargs (int argc, char **argv, int i, bool enc);
    void maybesetdefaulthasher ();
    uint32_t hashsize ();
    static void xorblockdata (void *dst, void const *src, uint32_t nby);
};

struct IFSAccess {
    virtual ~IFSAccess ();

    virtual int fsopen (char const *name, int flags, mode_t mode=0) =0;
    virtual int fsclose (int fd) =0;
    virtual int fsftruncate (int fd, uint64_t len) =0;
    virtual int fsread (int fd, void *buf, int len) =0;
    virtual int fspread (int fd, void *buf, int len, uint64_t pos) =0;
    virtual int fswrite (int fd, void const *buf, int len) =0;
    virtual int fsfstat (int fd, struct stat *buf) =0;
    virtual int fsstat (char const *name, struct stat *buf) =0;
    virtual int fslstat (char const *name, struct stat *buf) =0;
    virtual int fslutimes (char const *name, struct timespec *times) =0;
    virtual int fslchown (char const *name, uid_t uid, gid_t gid) =0;
    virtual int fschmod (char const *name, mode_t mode) =0;
    virtual int fsunlink (char const *name) =0;
    virtual int fsrmdir (char const *name) =0;
    virtual int fslink (char const *oldname, char const *newname) =0;
    virtual int fssymlink (char const *oldname, char const *newname) =0;
    virtual int fsreadlink (char const *name, char *buf, int len) =0;
    virtual int fsscandir (char const *dirname, struct dirent ***names, 
            int (*filter)(const struct dirent *),
            int (*compar)(const struct dirent **, const struct dirent **)) =0;
    virtual int fsmkdir (char const *dirname, mode_t mode) =0;
    virtual int fsmknod (char const *name, mode_t mode, dev_t rdev) =0;
    virtual DIR *fsopendir (char const *name) =0;
    virtual struct dirent *fsreaddir (DIR *dir) =0;
    virtual void fsclosedir (DIR *dir) =0;
    virtual int fsllistxattr (char const *path, char *list, int size) =0;
    virtual int fslgetxattr (char const *path, char const *name, void *value, int size) =0;
    virtual int fslsetxattr (char const *path, char const *name, void const *value, int size, int flags) =0;
};

#define MYEDATACMP 632396223
#define MYESIMRDER 632396224
char const *mystrerr (int err);
int wildcardlength (char const *wild);
bool wildcardmatch (char const *wild, char const *name);

#endif
