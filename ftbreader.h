
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

#ifndef _FTBREADER_H
#define _FTBREADER_H

#include "ftbackup.h"

#define FTBREADER_SELECT_SKIP ((char const *)1)
#define FTBREADER_SELECT_DONE ((char const *)2)

struct FTBReader : FTBackup {
    bool opt_incrmntl;
    bool opt_mkdirs;
    bool opt_overwrite;
    uint32_T opt_simrderrs;

    bool opt_verbose;
    int opt_verbsec;
    bool opt_xverbose;
    int opt_xverbsec;

    IFSAccess *tfs;     // target filesystem, ie, filesystem being restored to

    FTBReader ();
    ~FTBReader ();
    int read_saveset (char const *ssname);
    virtual char const *select_file (Header const *hdr) =0;
    bool decrypt_block (Block *block, uint32_T bs);

protected:
    time_t lastverbsec;
    time_t lastxverbsec;

private:
    struct LinkedBlock {
        LinkedBlock *next;
        Block block;
    };

    Block **xorblocks;
    bool skipall;
    bool wprwrite;
    bool zisopen;
    char **inodesname;
    char const *ssbasename;
    char *sssegname;
    FILE *wprfile;
    int ssfd;
    LinkedBlock *linkedBlocks;
    LinkedBlock *linkedRBlock;
    struct stat ssstat;
    uint32_T inodessize;
    uint32_T inodesused;
    uint32_T lastfileno;
    uint32_T lastseqno;
    uint32_T lastxorno;
    uint32_T thissegno;
    uint64_T pipepos;
    uint64_T readoffset;
    uint8_T *gotxors;
    z_stream zstrm;

    bool read_regular (Header *hdr, char const *dstname);
    bool read_directory (Header *hdr, char const *dstname, bool *setimes);
    bool read_symlink (Header *hdr, char const *dstname);
    bool read_special (Header *hdr, char const *dstname);
    void do_mkdirs (char const *dstname);
    void read_raw (void *buf, uint32_T len, bool zip);
    Block *read_block (bool skipfh);
    void read_first_block ();
    LinkedBlock *read_or_recover_block ();
    long wrapped_pread (void *buf, long len, uint64_T pos);
    long handle_pread_error (void *buf, long len, uint64_T pos);
};

struct FTBReadMapper : FTBReader {
    FTBReadMapper ();
    ~FTBReadMapper ();
    void add_mapping (char const *savwild, char const *outwild);
    virtual char const *select_file (Header const *hdr);

private:
    struct FTBReadMap {
        FTBReadMap *next;
        char const *savewildcard;
        char const *outputmapping;
    };

    char *dstnamebuf;
    FTBReadMap *mappings;
    int dstnameall;
};

#endif
