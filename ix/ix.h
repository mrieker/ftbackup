#ifndef _IX_H
#define _IX_H

#define IX_Long int
#define IX_Word short
#define IX_Byte char
#define IX_uLong unsigned int
#define IX_uWord unsigned short
#define IX_uByte unsigned char

typedef IX_uLong IX_Vbn;			/* virtual block numbers */
typedef IX_uWord IX_Rsz;			/* record sizes */
typedef IX_uByte IX_Rbf;			/* record data contents */
typedef IX_uLong IX_Kat;			/* key attributes */

/* Status codes */

#define IX_BASE 65536
#define IX_SUCCESS 65537
#define IX_NEWKEYVALUE 65538
#define IX_CREATERROR 65539
#define IX_WRITERROR 65540
#define IX_INVKEYSIZE 65541
#define IX_SEEKERROR 65542
#define IX_READERROR 65543
#define IX_INVBKTFMT 65544
#define IX_INVRECSIZE 65545
#define IX_CLOSERROR 65546
#define IX_NOCURREC 65547
#define IX_INVBKTCKSM 65548
#define IX_INVBKTPNTR 65549
#define IX_OPENERROR 65550
#define IX_INVFILHDR 65551
#define IX_TRUNCATED 65552
#define IX_RECNOTFOUND 65553
#define IX_INVKEYREF 65554
#define IX_INVSEARCH 65555
#define IX_IDXTOODEEP 65556
#define IX_BKTINUSE 65557
#define IX_BADALTBKT 65558
#define IX_NOTCHECKEDOUT 65559
#define IX_NOTCHECKEDIN 65560
#define IX_INVFHDCKSM 65561
#define IX_CACHENTNTFND 65562
#define IX_NOSUCHFILE 65563
#define IX_INVBKTVBN 65564
#define IX_NOMEMORY 65565
#define IX_RECTOOSHORT 65566
#define IX_FILELOCKED 65567
#define IX_RECORDLOCKED 65568
#define IX_LOCKERROR 65569
#define IX_READONLY 65570
#define IX_RECDELETED 65571
#define IX_CACHEINVAL 65572

/* Key search function codes */

#define IX_SEARCH_EQB 0
#define IX_SEARCH_EQF 1
#define IX_SEARCH_LEB 2
#define IX_SEARCH_GEF 3
#define IX_SEARCH_LTB 4
#define IX_SEARCH_GTF 5

/* File sharing codes */

#define IX_SHARE_N (0)
#define IX_SHARE_R (1)
#define IX_SHARE_W (-1)

/* Record locking codes */

#define IX_LOCK_R 0
#define IX_LOCK_W 1

/*  Macros for inserting and extracting binary values from string keys	*/
/*									*/
/*    String fields should be declared as 				*/
/*									*/
/*	IX_uByte field[sizeof value];					*/
/*									*/
/*    To insert a value into the field, use IX_ins_xx:			*/
/*									*/
/*	IX_ins_xx (value, field)					*/
/*									*/
/*    To extract a value from the field, use IX_ext_xx:			*/
/*									*/
/*	value = IX_ext_xx (field)					*/
/*									*/
/*    For xx use:							*/
/*									*/
/*	ul = unsigned longwords						*/
/*	uw = unsigned words						*/
/*	ub = unsigned bytes						*/
/*	sl = signed longwords						*/
/*	sw = signed words						*/
/*	sb = signed bytes						*/

#define IX_ins_ul(value,field) { \
	((IX_uByte *) (field))[0] = (IX_uByte)((value) >> 24); \
	((IX_uByte *) (field))[1] = (IX_uByte)((value) >> 16); \
	((IX_uByte *) (field))[2] = (IX_uByte)((value) >> 8); \
	((IX_uByte *) (field))[3] = (IX_uByte)(value); }
#define IX_ins_uw(value,field) { \
	((IX_uByte *) (field))[0] = (IX_uByte)((value) >> 8); \
	((IX_uByte *) (field))[1] = (IX_uByte)(value); }
#define IX_ins_ub(value,field) { \
	((IX_uByte *) (field))[0] = (value); }

#define IX_ins_sl(value,field) { \
	((IX_uByte *) (field))[0] = (IX_uByte)(((value) >> 24) ^ 0x80); \
	((IX_uByte *) (field))[1] = (IX_uByte)((value) >> 16); \
	((IX_uByte *) (field))[2] = (IX_uByte)((value) >> 8); \
	((IX_uByte *) (field))[3] = (IX_uByte)(value); }
#define IX_ins_sw(value,field) { \
	((IX_uByte *) (field))[0] = (IX_uByte)(((value) >> 8) ^ 0x80); \
	((IX_uByte *) (field))[1] = (IX_uByte)(value); }
#define IX_ins_sb(value,field) { \
	((IX_uByte *) (field))[0] = (value) ^ 0x80; }

#define IX_ext_ul(field) \
	 ((((IX_uByte *) (field))[0] << 24) \
        | (((IX_uByte *) (field))[1] << 16) \
        | (((IX_uByte *) (field))[2] << 8) \
        | (((IX_uByte *) (field))[3]))
#define IX_ext_uw(field) \
	 ((((IX_uByte *) (field))[0] << 8) \
        | (((IX_uByte *) (field))[1]))
#define IX_ext_ub(field) (((IX_uByte *) (field))[0])

#define IX_ext_sl(field) \
	(((((IX_uByte *) (field))[0] ^ 0x80) << 24) \
        | (((IX_uByte *) (field))[1] << 16) \
        | (((IX_uByte *) (field))[2] << 8) \
        | (((IX_uByte *) (field))[3]))
#define IX_ext_sw(field) \
	(((((IX_uByte *) (field))[0] ^ 0x80) << 8) \
        | (((IX_uByte *) (field))[1]))
#define IX_ext_sb(field) (((IX_uByte *) (field))[0] ^ 0x80)

/* Key attributes */

#define IX_KAT_CASE (0x00000001)	/* case insensitive */
#define IX_KAT_REV  (0x00000002)	/* reverse direction */

/* User settable global data */

/* - diskio.c */

#if !defined (VMS) && !defined (WIN32)
extern int ix_maxopen;
#endif

/* User callable global routine entrypoints */

/* - batch.c */

IX_uLong ix_setbatch (void *rabv, int newlevel);
void ix_inqbatch (void *rabv, int *curlevel, int *writes);

/* - build_key.c */

IX_uLong ix_build_key (void *rabv, 
                       IX_Rsz krf, 
                       IX_Rsz ksz, 
                       IX_Rsz kof, 
                       IX_Rsz newmrs);

/* - close_file.c */

IX_uLong ix_close_file (void *rabv);

/* - compress_file.c */

IX_uLong ix_compress_file (const IX_Byte *ifspec, 
                           const IX_Byte *ofspec, 
                           IX_Rsz newbks, 
                           int fillpct);

/* - create_file.c */

IX_uLong ix_create_file (const IX_Byte *fspec,
                         IX_Rsz nky,
                         const IX_Rsz *ksz, 
                         const IX_Rsz *kof, 
                         IX_Rsz bks,
                         IX_Rsz mrs,
                         int buffc, 
                         void **rabv);

IX_uLong ix_create_file2 (const IX_Byte *fspec,
                          IX_Rsz nky,
                          const IX_Rsz *ksz, 
                          const IX_Rsz *kof, 
                          IX_Rsz bks,
                          IX_Rsz mrs,
                          int buffc, 
                          void **rabv, 
                          int share, 
                          IX_Rsz logsiz, 
                          IX_Byte *logbuf);

IX_uLong ix_create_file3 (const IX_Byte *fspec,
                          IX_Rsz nky,
                          const IX_Rsz *ksz, 
                          const IX_Rsz *kof, 
                          const IX_Kat *kat, 
                          IX_Rsz bks,
                          IX_Rsz mrs,
                          int buffc, 
                          void **rabv, 
                          int share, 
                          IX_Rsz logsiz, 
                          IX_Byte *logbuf);

/* - errlist.c */

IX_Byte *ix_errlist (IX_uLong status);

/* - fix_file.c */

IX_uLong ix_fix_file (const IX_Byte *ifspec, 
                      const IX_Byte *ofspec);

/* - inquire_file.c */

void ix_inquire_file (void *rabv, 
                      IX_Rsz *nky, 
                      IX_Rsz *ksz, 
                      IX_Rsz *kof, 
                      IX_Rsz *bks,
                      IX_Rsz *mrs);

void ix_inquire_file2 (void *rabv, 
                       IX_Rsz *nky, 
                       IX_Rsz *ksz, 
                       IX_Rsz *kof, 
                       IX_Kat *kat, 
                       IX_Rsz *bks,
                       IX_Rsz *mrs);

/* - insert_rec.c */

IX_uLong ix_insert_rec (void *rabv, IX_Rsz rsz, const IX_Rbf *rbf);

/* - open_file.c */

IX_uLong ix_open_file (const IX_Byte *fspec, 
                       int rdonly, 
                       int buffc, 
                       void **rabv);

IX_uLong ix_open_file2 (const IX_Byte *fspec, 
                        int rdonly, 
                        int buffc, 
                        void **rabv, 
                        int share, 
                        IX_Rsz logsiz, 
                        IX_Byte *logbuf);

/* - modify_rec.c */

IX_uLong ix_modify_rec (void *rabv, IX_Rsz rsz, const IX_Rbf *rbf);

/* - remove_rec.c */

IX_uLong ix_remove_rec (void *rabv);

/* - search_rec.c */

IX_uLong ix_search_key (void *rabv, int how, 
                        IX_Rsz krf, IX_Rsz ksz, const IX_Rbf *kbf, 
                        IX_Rsz rsz, IX_Rbf *rbf, IX_Rsz *rrsz);
IX_uLong ix_search_key2 (void *rabv, int how, 
                         IX_Rsz krf, IX_Rsz ksz, const IX_Rbf *kbf, 
                         IX_Rsz rsz, IX_Rbf *rbf, IX_Rsz *rrsz, 
                         int lockw);

IX_uLong ix_search_seq (void *rabv, int fwd, 
                        IX_Rsz rsz, IX_Rbf *rbf, IX_Rsz *rrsz, 
                        IX_Rsz ksz, const IX_Rbf *kbf);
IX_uLong ix_search_seq2 (void *rabv, int fwd, 
                         IX_Rsz rsz, IX_Rbf *rbf, IX_Rsz *rrsz, 
                         IX_Rsz ksz, const IX_Rbf *kbf, int lockw);

IX_uLong ix_rewind (void *rabv, IX_Rsz krf);

/* - validate_file.c */

IX_uLong ix_validate_file (void *rabv, IX_Vbn *count, IX_Vbn *depth);

#endif
