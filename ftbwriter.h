
#ifndef _FTBWRITER_H
#define _FTBWRITER_H

#include "ftbackup.h"

struct Main2ComprSlot {
    void    *buf;   // address of data to write
    uint32_t len;   // length of data to write
    int      dty;   // -1: file header to be left uncompressed
                    //  0: data to be left uncompressed
                    //  1: data to be compressed
};

struct FTBWriter : FTBackup {
    int ioptions;
    int ooptions;
    int opt_verbose;
    int opt_verbsec;
    uint64_t opt_since;

    FTBWriter ();
    ~FTBWriter ();
    int write_saveset (char const *ssname, char const *rootpath);

private:
    Block **xorblocks;
    char **inodesname;
    dev_t inodesdevno;
    ino_t *inodeslist;
    int ssfd;
    int zisopen;
    time_t lastverbsec;
    uint32_t inodessize;
    uint32_t inodesused;
    uint32_t lastfileno;
    uint32_t lastseqno;
    uint32_t lastxorno;
    z_stream zstrm;

    Block          *compr2write_slots[COMPR2WRITE_NSLOTS];
    Main2ComprSlot  main2compr_slots[MAIN2COMPR_NSLOTS];
    pthread_cond_t  compr2write_cond;
    pthread_cond_t  main2compr_cond;
    pthread_mutex_t compr2write_mutex;
    pthread_mutex_t main2compr_mutex;
    uint32_t        compr2write_next;
    uint32_t        compr2write_used;
    uint32_t        main2compr_next;
    uint32_t        main2compr_used;

    int write_file (char const *path, struct stat const *dirstat);
    int write_regular (Header *hdr, struct stat const *dirstat);
    int write_directory (Header *hdr, struct stat const *statbuf);
    int write_mountpoint (Header *hdr);
    int write_symlink (Header *hdr);
    int write_special (Header *hdr, dev_t strdev);
    void write_header (Header *hdr);
    void write_raw (void const *buf, uint32_t len, int hdr);
    void write_queue (void *buf, uint32_t len, int dty);
    static void *compr_thread_wrapper (void *ftbw);
    void *compr_thread ();
    Block *malloc_block ();
    void queue_block (Block *block);
    static void *write_thread_wrapper (void *ftbw);
    void *write_thread ();
    void flush_xor_blocks ();
};

#endif
