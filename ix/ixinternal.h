#include "ix.h"

#if defined (VMS)
#include rms
#define NULL ((void *)0)
#elif defined (WIN32)
#include <windows.h>
#define globaldef
#else
#include <string.h>
#define globaldef
#endif

/* Re-define some types without the IX_ prefix */

#define Long IX_Long
#define Word IX_Word
#define Byte IX_Byte
#define uLong IX_uLong
#define uWord IX_uWord
#define uByte IX_uByte

#define Vbn IX_Vbn
#define Rsz IX_Rsz
#define Rbf IX_Rbf
#define Kat IX_Kat

/* Define internally used constants and macros */

#define max_depth 256
#define round_up(value,denom) ((((value) + (denom) - 1) / (denom)) * (denom))

/* Index values */

typedef uLong Idx;

/* Record's file address - actually composed of the vbn of the original */
/*             bucket the record was created in, and a sequence number. */

typedef struct { Vbn vbn;			/* record file address */
                 Vbn seq;
               } Rfa;

/* Key header - There is an array of these at the */
/*   beginning of the file.  One element per key. */

						/* - v1 and v2: */
typedef struct { Vbn vbn;			/* vbn of root bucket */
                 Rsz ksz;			/* key size (bytes) */
                 Rsz kof;			/* key offset (bytes) */
               } Khdv1;

						/* - the ix_open_file routine */
						/*   will allow read-only     */
						/*   access to v1/v2 files    */
						/*   and will convert the     */
						/*   internal khd array to v3 */

						/* - v3 and up: */
typedef struct { Vbn vbn;			/* vbn of root bucket */
                 Rsz ksz;			/* key size (bytes) */
                 Rsz kof;			/* key offset (bytes) */
                 Kat kat;			/* key attributes (IX_KAT_*) */
                 uLong pad1;			/* pad to 16-byte boundary */
               } Khd;

/* File header - There is one of these at the beginning of the */
/*    file.  They are followed by the Khd's defining the keys. */

typedef struct { uLong cksm;			/* file header checksum */
                 Rsz ver;			/* file format version = 3 */
                 Rsz mrs;			/* maximum record size */
                 Rsz nky;			/* number of keys in file */
                 Rsz bls;			/* block size (for vbn) */
                 Rsz bks;			/* bucket size */
                 uWord pad1;			/* pad to longword boundary */
                 Vbn free;			/* listhead of free buckets */
						/* uses bkt.rec[0].left for */
						/* link in free buckets */
                 uLong eof;			/* number of occupied blocks */
                 Rfa rfa;			/* last assigned rfa */
                 Khd khd[1];			/* first key header element */
               } Fhd;

/* These fill the remainder of the bucket up to the data.  The last one has */
/* a rec_size of zero, and the rec_offset has the offset of the lowest data */
/* in the bucket.  Its rec_left pointer points to the next higher bucket.   */

typedef struct { Rsz size;			/* size of this record */
                 Rsz offset;			/* offset of record in bucket */
                 Vbn left;			/* left rrn pointer (vbn) */
                 Rfa rfa;			/* record's rfa */
                 Rsz keysize;			/* compressed key size */
                 uWord pad1;			/* pad to longword size */
                 uLong pad2[3];			/* pad to 16byte size */
               } Rec;

/* Bucket - there is one of these at the beginning of each bucket */

typedef struct { uLong cksm;			/* bucket checksum */
                 uLong vbn;			/* vbn of bucket */
                 uLong pad1;			/* pad to fill old rfa.seq */
                 Rsz krf;			/* krf + 1 of bucket */
						/*  0 = unknown */
						/* -1 = free */
                 uWord pad2;			/* pad to 16byte size */
                 Rec rec[1];			/* record descriptors */
               } Bkt;

/* Current record pointer */

typedef struct { Bkt *bkt;			/* bucket record was found in */
                 Idx idx;			/* index of the record */
                 Idx depth;			/* depth of the bucket */
						/* (depth = 0 : root bucket) */
                 Vbn parvbn[max_depth];		/* parent bucket vbn's */
                 Idx paridx[max_depth];		/* parent record indicies */
                 Rsz krf;			/* associated key-of-ref */
               } Crp;

/* In-memory bucket cache */

typedef struct Cache { struct Cache *next;
                       Khd *khd;
                       Vbn vbn;
                       Bkt *bkt;
                       int write;
                     } Cache;

/* In-memory record access block */

typedef struct Rab { Fhd *fhd;			/* file header pointer */
                     Rsz fhdrsz;		/* size of actual fhd data */
                     Rsz fhdbsz;		/* fhd size allocated on disk */
                     struct Cache *cache;	/* cache table pointer */
                     int writefhd;		/* fhd needs to be written */
                     int maxcache;		/* max cache buffers allowed */
                     Crp crp;			/* sequential search context */
                     int nosqf;			/* if set, next search_seq */
						/* forward will retrieve */
						/* current record */
                     int rdonly;		/* set if read-only */
                     int lockw;			/* 0 : rec locked for read */
						/* 1 : rec locked for write */
                     int batchlvl;		/* batch level */
                     uLong logsiz;		/* log message buffer size */
                     Byte *logbuf;		/* log message buffer pointer */
#if defined (VMS)
                     struct FAB rmsfab;		/* file access block */
                     uLong crp_lkid;		/* record lock id */
                     uLong cache_lkid;		/* cache lock id */
                     uLong cache_lksq;		/*            sequence */
                     uLong cache_lkpi;		/*            process-id */
                     uLong cache_lkch;		/*            I/O channel */
                     uLong cache_lkup;		/*            update flag */
#elif defined (WIN32)
                     HANDLE fh;			/* file handle */
                     HANDLE crp_semhandle;	/* record lock sema4 handle */
                     HANDLE cache_mtxhandle;	/* cache lock mutex handle */
                     Byte cache_mtxname[32];	/*            mutex name */
                     HANDLE cache_semhandle;	/*            sema4 handle */
                     Byte cache_semname[32];	/*            sema4 name */
                     Long cache_semcount;	/*            serial number */
                     int cache_updated;		/*            update flag */
#else
                     int fd;			/* file descriptor */
                     struct Rab *next;		/* next in rabs list */
                     uLong cache_lksq;		/* cache lock sequence */
                     uLong cache_lkpi;		/*            process-id */
                     uLong cache_lkch;		/*            I/O channel */
                     uLong cache_lkup;		/*            update flag */
#endif
                     Byte fspec[1];		/* file spec - must be last */
                   } Rab;

/* Internal global routine entrypoints */

/* - compare_keys.c */

int ix_compare_keys (Rsz ksz1, const Rbf *kbf1, Rfa *rfa1, 
                     Rsz ksz2, const Rbf *kbf2, Rfa *rfa2, 
                     Kat kat);

/* - diskio.c */

uLong ix_lockfile (Rab *rab, int rw);
void ix_unlockfile (Rab *rab);
uLong ix_allocbkt (Rab *rab, Khd *khd, Bkt **rbkt);
uLong ix_checkbkt (Rab *rab, Khd *khd, Bkt *bkt, int fix);
uLong ix_checkfhd (Rab *rab);
uLong ix_flush (Rab *rab, uLong status, int update);
uLong ix_flushwrites (Rab *rab, int writefhd);
uLong ix_wipecache (Rab *rab);
uLong ix_freebkt (Rab *rab, Khd *khd, Bkt *bkt);
uLong ix_readbkt (Rab *rab, Khd *khd, Vbn vbn, Bkt **bktr);
uLong ix_readbktfix (Rab *rab, Khd *khd, Vbn vbn, Bkt **bktr);
uLong ix_readfhd (Rab *rab);
uLong ix_relbkt (Rab *rab, Khd *khd, Bkt *bkt);
uLong ix_writebkt (Rab *rab, Khd *khd, Bkt *bkt);
uLong ix_writebktnr (Rab *rab, Khd *khd, Bkt *bkt);
uLong ix_writefhd (Rab *rab);
int ix_countwrites (Rab *rab);

/* - errlist.c */

#ifndef WIN32
Byte *ix_unixerr ();
#endif
#ifdef VMS
Byte *ix_vmserr (uLong sts);
#endif
#ifdef WIN32
Byte *ix_win32err (uLong sts);
#endif

/* - errorlog.c */

void ix_errorlog (Rab *rab, Byte *ctl, ...);
void ix_messlog (Rab *rab, Byte *ctl, ...);

/* - insert_rec.c */

uLong ix_insert_key (Rab *rab, 
                     Khd *khd, 
                     Rsz ksz, 
                     Rsz kof, 
                     Rsz rsz1, 
                     const Rbf *rbf1, 
                     Rsz rsz2, 
                     const Rbf *rbf2, 
                     Rfa *rfa);

uLong ix_find_ins_spot (Rab *rab, 
                        Khd *khd, 
                        Rsz ksz, 
                        const Rbf *kbf, 
                        Rsz kof, 
                        Crp *crp, 
                        Rfa *rfa);

uLong ix_insert_into_bkt (Rab *rab, 
                          Khd *khd, 
                          Rsz rsz1, 
                          const Rbf *rbf1, 
                          Rsz rsz2, 
                          const Rbf *rbf2, 
                          Rsz ksz, 
                          Rfa *rfa, 
                          Vbn right, 
                          Crp *crp);

uLong ix_split_bkt (Rab *rab, 
                    Khd *khd, 
                    Crp *crp, 
                    int modify);

/* - memory_vms.c, etc. */

/* - - for vms, define prototypes for memory_vms.c           */
/*     for Storage Center, define macros to use its routines */
/*     otherwise, use the memory_unix.c stuff                */

#if defined (SPSC)
void *mallo_malloc (int size, char *file, int line);
void *mallo_calloc (int nele, int size, char *file, int line);
void  mallo_free (void *datav, char *file, int line);
#define ix_memen(write)
#define ix_malloc(size) mallo_malloc (size, __FILE__, __LINE__)
#define ix_calloc(size) mallo_calloc (1, size, __FILE__, __LINE__)
#define ix_free(where) mallo_free (where, __FILE__, __LINE__)
#elif defined (VMS)
void  ix_memen (int write);
void *ix_malloc (uLong size);
void *ix_calloc (uLong size);
void  ix_free (void *where);
#else
void  ix_mallo_memen  (int write, char *file, int line);
void *ix_mallo_calloc (int nele, int size, char *file, int line);
void *ix_mallo_malloc (int size, char *file, int line);
void  ix_mallo_free   (void *datav, char *file, int line);
#define ix_memen(write) ix_mallo_memen (write, __FILE__, __LINE__)
#define ix_malloc(size) ix_mallo_malloc (size, __FILE__, __LINE__)
#define ix_calloc(size) ix_mallo_calloc (1, size, __FILE__, __LINE__)
#define ix_free(where) ix_mallo_free (where, __FILE__, __LINE__)
#endif

/* - os.c */

void ix_os_clofil (Rab *rab);
uLong ix_os_crefil (Rab *rab, Rsz *bls, int share);
void ix_os_crefilerr (Rab *rab);
void ix_os_crefilsuc (Rab *rab);
uLong ix_os_fluwri (Rab *rab, Cache **writes, uLong nwrites, int writefhd);
uLong ix_os_opefil (Rab *rab, int rdonly, int share);
void ix_os_opefilerr (Rab *rab);
void ix_os_opefilsuc (Rab *rab);
uLong ix_os_readit (Rab *rab, Vbn vbn, Rsz rsz, Rbf *rbf);
uLong ix_os_lckfil (Rab *rab, int rw);
void ix_os_ulkfil (Rab *rab);
uLong ix_os_lockcrp (Rab *rab, int rw);
void ix_os_unlockcrp (Rab *rab);

/* - remove_rec.c */

uLong ix_pritoaltkey (Rab *rab, Khd *akhd, Bkt *pbkt, Idx pidx, Crp *acrp);
uLong ix_remove_from_bkt (Rab *rab, Khd *khd, Crp *crp);

/* - search_rec.c */

uLong ix_search_kyf (Rab *rab, Khd *khd, Crp *crp, Vbn vbn, Rfa *rfa, 
                     Rsz ksz, const Rbf *kbf, Rsz kof, int gtr, int *eql);
uLong ix_search_sqf (Rab *rab, Khd *khd, Crp *crp, int incidx);
void ix_relcrp (Rab *rab, Crp *crp);
void ix_relcrpnv (Rab *rab, Crp *crp);
uLong ix_alttoprikey (Rab *rab, Khd *akhd, Khd *pkhd, 
                      Bkt *abkt, Idx aidx, Crp *pcrp);

/* - validate_file.c */

uLong ix_validkey (Rab *rab, Rsz krf, Vbn *counter, Vbn *depth);
