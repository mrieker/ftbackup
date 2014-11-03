
#ifndef _FTBWRITER_H
#define _FTBWRITER_H

#include "ftbackup.h"

#define SQ_NSLOTS 4

template <class T>
struct SlotQueue {
    SlotQueue ();
    void enqueue (T slot);
    bool trydequeue (T *slot);
    T dequeue ();

private:
    T slots[SQ_NSLOTS];
    pthread_cond_t  cond;
    pthread_mutex_t mutex;
    uint32_t next;
    uint32_t used;
};

struct FTBWriter : FTBackup {
    bool opt_verbose;
    int ioptions;
    int ooptions;
    int opt_verbsec;
    uint64_t opt_segsize;
    uint64_t opt_since;

    IFSAccess *tfs;     // target filesystem, ie, filesystem being backed up

    FTBWriter ();
    ~FTBWriter ();
    int write_saveset (char const *ssname, char const *rootpath);

private:
    struct ComprSlot {
        void    *buf;   // address of data to write
        uint32_t len;   // length of data to write
        int      dty;   // -1: file header to be left uncompressed
                        //  0: data to be left uncompressed
                        //  1: data to be compressed
    };

    Block *volatile freeblocks;
    Block **xorblocks;
    bool zisopen;
    char const *ssbasename;
    char *sssegname;
    dev_t inodesdevno;
    ino_t *inodeslist;
    int ssfd;
    time_t lastverbsec;
    uint32_t inodessize;
    uint32_t inodesused;
    uint32_t lastfileno;
    uint32_t lastseqno;
    uint32_t lastxorno;
    uint32_t thissegno;
    uint64_t byteswrittentoseg;
    uint64_t *inodesmtim;
    z_stream zstrm;

    SlotQueue<ComprSlot> comprqueue;  // variable length data to be compressed and blocked
    SlotQueue<Block *> encrqueue;     // blocks to be encrypted
    SlotQueue<Block *> writequeue;    // blocks to be written to saveset

    bool write_file (char const *path, struct stat const *dirstat);
    bool write_regular (Header *hdr, struct stat const *dirstat);
    bool write_directory (Header *hdr, struct stat const *statbuf);
    bool write_mountpoint (Header *hdr);
    bool write_symlink (Header *hdr);
    bool write_special (Header *hdr, dev_t strdev);
    void write_header (Header *hdr);
    void write_raw (void const *buf, uint32_t len, bool hdr);
    void write_queue (void *buf, uint32_t len, int dty);
    static void *compr_thread_wrapper (void *ftbw);
    void *compr_thread ();
    void queue_data_block (Block *block);
    void queue_xor_blocks ();
    void queue_block (Block *block);
    Block *malloc_block ();
    static void *encr_thread_wrapper (void *ftbw);
    void *encr_thread ();
    static void *write_thread_wrapper (void *ftbw);
    void *write_thread ();
    void write_ssblock (Block *block);
    void free_block (Block *block);
};

#endif
