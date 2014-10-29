
#ifndef _FTBACKUP_H
#define _FTBACKUP_H

#define __USE_GNU // fcntl.h: O_DIRECT, O_NOATIME
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#define MAIN2COMPR_NSLOTS 4
#define COMPR2WRITE_NSLOTS 4
#define PAGESIZE (4096U)
#define MINBLOCKSIZE PAGESIZE
#define DEFBLOCKSIZE (32768U)
#define MAXBLOCKSIZE (1024*1024*1024U)

#define DEFXORSC 31 // write one XOR block per 31 data blocks
#define DEFXORGC 2  // write XOR blocks in groups of 2, ie, after 62 data blocks

#define EX_OK    0  // everything ran ok
#define EX_CMD   1  // command error
#define EX_SSIO  2  // saveset io error
#define EX_FILIO 3  // file io error

#define BLOCK_MAGIC  "ftbackup"
#define HEADER_MAGIC "ftbheder"

#define HFL_HDLINK 0x01  // regular file is an hardlink

#define INTERR(name,err) do { fprintf (stderr, "ftbackup: " #name "() error %d\n", err); abort (); } while (0)
#define NANOTIME(tv) ((tv).tv_sec * 1000000000ULL + (tv).tv_nsec)
#define NOMEM() do { fprintf (stderr, "ftbackup: out of memory\n"); abort (); } while (0)
#define SYSERR(name,err) do { fprintf (stderr, "ftbackup: " #name "() error: %s\n", strerror (errno)); abort (); } while (0)

typedef unsigned int uint32_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned long long uint64_t;
typedef unsigned long ulong_t;

struct Block {
    char        magic[8];   // magic number
    uint32_t    chksum;     // block checksum
    uint32_t    seqno;      // block sequence number
    uint32_t    xorno;      // xor sequence number (0 for data blocks)
    uint32_t    hdroffs;    // offset of first header in block
    uint8_t     l2bs;       // log2 block size
    uint8_t     xorbc;      // xor block count (0 for data blocks)
    uint8_t     xorgc;      // xor group count
    uint8_t     xorsc;      // xor span count
    uint8_t     data[0];
};

struct Header {
    char        magic[8];   // magic number
    uint64_t    mtimns;     // data mod time
    uint64_t    ctimns;     // attr mod time
    uint64_t    atimns;     // access time
    uint64_t    size;       // file size
    uint32_t    fileno;     // file number (starts at 1)
    uint32_t    stmode;     // protection and type
    uint32_t    ownuid;     // owner UID
    uint32_t    owngid;     // owner GID
    uint16_t    nameln;     // name length (incl null)
    uint16_t    flags;      // flag bits
    char        name[0];    // file name (incl null)
};

struct FTBackup {
    uint8_t l2bs;
    uint8_t xorgc;
    uint8_t xorsc;

    static void print_header (FILE *out, Header *hdr, char const *name);
    bool blockisvalid (Block *block);
    bool blockbaseisvalid (Block *block);
    static void xorblockdata (void *dst, void const *src, uint32_t nby);
    static uint32_t checksumdata (void const *src, uint32_t nby);
};

#endif
