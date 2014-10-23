
#ifndef _FTBREADER_H
#define _FTBREADER_H

#include "ftbackup.h"

struct LinkedBlock {
    LinkedBlock *next;
    Block block;
};

struct FTBReader : FTBackup {
    int opt_incrmntl;
    int opt_overwrite;
    uint32_t opt_simrderrs;

    ~FTBReader ();
    int read_saveset (char const *ssname, char const *srcprefix, char const *dstprefix);
    virtual char *maybe_output_listing (char *dstname, Header *hdr) =0;

private:
    Block **xorblocks;
    char **inodesname;
    FILE *wprfile;
    int ssfd;
    int wprwrite;
    int zisopen;
    LinkedBlock *linkedBlocks;
    LinkedBlock *linkedRBlock;
    struct stat ssstat;
    uint32_t inodessize;
    uint32_t inodesused;
    uint32_t lastfileno;
    uint32_t lastseqno;
    uint32_t lastxorno;
    uint64_t pipepos;
    uint64_t readoffset;
    uint8_t *gotxors;
    z_stream zstrm;

    int read_regular (Header *hdr, char const *dstname);
    int read_directory (Header *hdr, char const *dstname, int *setimes);
    int read_symlink (Header *hdr, char const *dstname);
    int read_special (Header *hdr, char const *dstname);
    void read_raw (void *buf, uint32_t len, int zip);
    Block *read_block (int skipfh);
    void read_first_block ();
    LinkedBlock *read_or_recover_block ();
    long wrapped_pread (int fd, void *buf, long len, uint64_t pos);
};

#endif
