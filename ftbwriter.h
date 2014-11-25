
//  Copyright (C) 2014, Mike Rieker, Beverly, MA, USA
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

#ifndef _FTBWRITER_H
#define _FTBWRITER_H

#include "ftbackup.h"

#define SQ_NSLOTS 4

struct SkipName;

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
        int      dty;   // -1: file header to be left uncompressed, the free ()
                        //  0: data to be left uncompressed, then free ()
                        //  1: data to be compressed, then free ()
                        //  2: data to be compressed, then frqueue.enqueue ()
    };

    Block **xorblocks;
    bool zisopen;
    char const *ssbasename;
    char *sssegname;
    dev_t inodesdevno;
    FILE *noncefile;
    ino_t *inodeslist;
    int ssfd;
    SkipName *skipnames;
    time_t lastverbsec;
    uint32_t inodessize;
    uint32_t inodesused;
    uint32_t lastfileno;
    uint32_t lastseqno;
    uint32_t lastxorno;
    uint32_t thissegno;
    uint64_t byteswrittentoseg;
    uint64_t *inodesmtim;
    uint64_t rft_runtime;
    z_stream zstrm;

    SlotQueue<void *>    frbufqueue;  // free buffers for reading files
    SlotQueue<ComprSlot> comprqueue;  // variable length data to be compressed and blocked
    SlotQueue<Block *>   frblkqueue;  // free blocks for writing to saveset
    SlotQueue<Block *>   writequeue;  // blocks to be written to saveset

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
    static void *write_thread_wrapper (void *ftbw);
    void *write_thread ();
    Block *malloc_block ();
    void xor_data_block (Block *block);
    void hash_xor_blocks ();
    void hash_block (Block *block);
    void write_ssblock (Block *block);
    void free_block (Block *block);
};

#endif
