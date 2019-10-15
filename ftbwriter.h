
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
    uint32_T next;
    uint32_T used;
};

struct SinceReader {
    uint64_T ctime;
    char *fname;

    SinceReader ();
    ~SinceReader ();

    bool open (char const *name);
    bool read ();
    void close ();

private:
    char const *sincname;
    int sincfd;
    uint32_T sincpaths;
    z_stream zsinc;

    uint8_T sincxbuf[4096];
    uint8_T sinczbuf[4096];

    bool rdraw (void *buf, uint32_T len);
};

struct FTBWriter : FTBackup {
    bool opt_verbose;
    char const *histdbname;
    char const *histssname;
    char const *opt_record;
    char const *opt_since;
    int ioptions;
    int ooptions;
    int opt_verbsec;
    uint64_T opt_segsize;

    IFSAccess *tfs;     // target filesystem, ie, filesystem being backed up

    FTBWriter ();
    ~FTBWriter ();
    int write_saveset (char const *ssname, char const *rootpath);

private:
    struct ComprSlot {
        void    *buf;   // address of data to write
        uint32_T len;   // length of data to write
        int      dty;   // -1: file header to be left uncompressed, the free ()
                        //  0: data to be left uncompressed, then free ()
                        //  1: data to be compressed, then free ()
                        //  2: data to be compressed, then frqueue.enqueue ()
    };

    struct HistSlot {
        char *fname;
        uint32_T seqno;
    };

    Block **xorblocks;
    bool zisopen;
    char const *ssbasename;
    char *sssegname;
    dev_t inodesdevno;
    FILE *noncefile;
    ino_t *inodeslist;
    int recofd;
    int ssfd;
    SinceReader sincrdr;
    SkipName *skipnames;
    time_t lastverbsec;
    uint32_T inodessize;
    uint32_T inodesused;
    uint32_T lastfileno;
    uint32_T lastseqno;
    uint32_T lastxorno;
    uint32_T reconamelen;
    uint32_T thissegno;
    uint64_T byteswrittentoseg;
    uint64_T *inodesmtim;
    uint64_T rft_runtime;
    z_stream zreco;
    z_stream zstrm;

    SlotQueue<void *>    frbufqueue;  // free buffers for reading files
    SlotQueue<ComprSlot> comprqueue;  // variable length data to be compressed and blocked
    SlotQueue<Block *>   frblkqueue;  // free blocks for writing to saveset
    SlotQueue<HistSlot>  histqueue;   // filenames to be written to history
    SlotQueue<Block *>   writequeue;  // blocks to be written to saveset

    uint8_T recozbuf[4096];

    bool write_file (char const *path, struct stat const *dirstat);
    bool write_regular (Header *hdr, struct stat const *dirstat);
    bool write_directory (Header *hdr, struct stat const *statbuf);
    bool write_mountpoint (Header *hdr);
    bool write_symlink (Header *hdr);
    bool write_special (Header *hdr, dev_t strdev);
    void write_header (Header *hdr);
    bool skipbysince (Header const *hdr);
    void maybe_record_file (uint64_T ctime, char const *name);
    void write_reco_data (void const *buf, uint32_T len);
    void write_raw (void const *buf, uint32_T len, bool hdr);
    void write_queue (void *buf, uint32_T len, int dty);
    static void *compr_thread_wrapper (void *ftbw);
    void *compr_thread ();
    static void *hist_thread_wrapper (void *ftbw);
    void *hist_thread ();
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
