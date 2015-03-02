/************************************************************************/
/*									*/
/*  This module contains the OS dependent files				*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#if defined (VMS)

#include atrdef
#include dvidef
#include fibdef
#include iodef
#include lckdef
#include ssdef

#define fld$uw(base,ofst) *((uWord *)((Byte *)(base) + (ofst)))

#ifndef __DECC
#define FIB$W_EXCTL fib$r_exctl_overlay.fib$w_exctl
#else
#define FIB$W_EXCTL fib$w_exctl
#endif

typedef struct { uLong size;
                 uByte *adrs;
               } Desc;

typedef struct { uWord sts, len;
                 uLong val;
               } Iosb;

typedef struct Iosbb { uWord sts, len;
                       uLong val;
                       struct Iosbb *next;
                     } Iosbb;

typedef struct { uWord size, item;
                 uByte *buff;
               } Itmlst2;

typedef struct { uWord size, item;
                 Byte *adrs;
                 uWord *rlen;
               } Itmlst3;

typedef struct { uWord sts;
                 uWord dummy;
                 uLong lkid;
                 uLong val_seq;
                 uLong val_pid;
                 uLong val_chn;
                 uLong val_spare;
               } Lksb;

globalvalue fat$w_rsize  =  2;
globalvalue fat$w_hiblkh =  4;
globalvalue fat$w_hiblkl =  6;
globalvalue fat$w_efblkh =  8;
globalvalue fat$w_efblkl = 10;

static Long efn = -1;
static uLong cache_lksq = 0;

#define nsp 18
static uLong sp[nsp] = {  2,  3,  5,  7, 11, 13, 17, 19, 23,
                         29, 31, 37, 41, 43, 47, 53, 59, 61 };

#elif defined (WIN32)

#include <stdio.h>

#else

#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

int ix_maxopen   = 255;
static Rab *rabs = NULL;

#endif

static uLong writeit (Rab *rab, Vbn vbn, Rsz rsz, Rbf *rbf);
#if defined (VMS)
static uLong crecachelck (Rab *rab, struct NAM *rmsnam);
#elif defined (WIN32)
static uLong crecachelck (Rab *rab, int share);
#endif
#if !defined (VMS) && !defined (WIN32)
static uLong reopen (Rab *rab);
static void newopen (Rab *rab);
static void oldclose (Rab *rab);
#endif

/************************************************************************/
/*									*/
/*  Close file								*/
/*									*/
/************************************************************************/

void ix_os_clofil (Rab *rab)

{
#if defined (VMS)
  sys$dassgn (rab -> rmsfab.fab$l_stv);
  sys$deq (rab -> cache_lkid, 0, 0, 0);
#elif defined (WIN32)
  CloseHandle (rab -> fh);
  CloseHandle (rab -> cache_mtxhandle);
  CloseHandle (rab -> cache_semhandle);
#else
  oldclose (rab);
  close (rab -> fd);
#endif
}

/************************************************************************/
/*									*/
/*  Create file								*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	rab -> fspec = filespec to be created				*/
/*	share = 0 : don't allow any access by others			*/
/*	        1 : allow only other read accesses			*/
/*	       -1 : allow other read and write accesses			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_os_crefil = IX_SUCCESS : success				*/
/*	                     else : error status			*/
/*	*bls = block size of device					*/
/*									*/
/************************************************************************/

uLong ix_os_crefil (Rab *rab, Rsz *bls, int share)

{
#if defined (VMS)
  Byte rmsesa[256];
  Desc devdes;
  int i;
  Iosb dviios;
  uLong dviclf, sts;
  Itmlst3 dviitm[] = { sizeof dviclf, DVI$_CLUSTER, &dviclf, 0, 
                                   0, 0, 0, 0 };
  struct NAM rmsnam;
#elif defined (WIN32)
  int sharemode;
  uLong sts;
#else
  struct stat statbuf;
#endif

#if defined (VMS)
  if (efn < 0) {
    sts = lib$get_ef (&efn);
    if (!(sts & 1)) lib$stop (sts);
  }

  rab -> cache_lkid = 0;
  rab -> rmsfab = cc$rms_fab;
  rab -> rmsfab.fab$b_fac = FAB$M_GET | FAB$M_PUT | FAB$M_UPD | FAB$M_BIO;
  if (share > 0) rab -> rmsfab.fab$b_shr = FAB$M_SHRGET | FAB$M_UPI;
  if (share < 0) rab -> rmsfab.fab$b_shr = FAB$M_SHRGET | FAB$M_SHRPUT 
                                         | FAB$M_SHRUPD | FAB$M_UPI;
  rab -> rmsfab.fab$b_fns = strlen (rab -> fspec);
  rab -> rmsfab.fab$l_fna = rab -> fspec;
  rab -> rmsfab.fab$l_fop = FAB$M_UFO;
  rab -> rmsfab.fab$b_rfm = FAB$C_FIX;
  rab -> rmsfab.fab$l_nam = &rmsnam;

  rmsnam = cc$rms_nam;
  rmsnam.nam$l_esa = rmsesa;
  rmsnam.nam$b_ess = sizeof rmsesa - 1;

  sts = sys$parse (&(rab -> rmsfab));
  if (sts & 1) {
    devdes.size = rmsnam.nam$t_dvi[0];
    devdes.adrs = rmsnam.nam$t_dvi + 1;
    sts = sys$getdviw (efn, 0, &devdes, dviitm, &dviios, 0, 0, 0);
    if (sts & 1) sts = dviios.sts;
  }
  if (sts & 1) {
    while (dviclf > 32767 / 512) {
      for (i = 0; i < nsp; i ++) if (dviclf % sp[i] == 0) break;
      if (i < nsp) dviclf /= sp[i];
      else dviclf = 1;
    }
    rab -> rmsfab.fab$w_mrs = 512 * dviclf;
  }
  if (sts & 1) sts = sys$create (&(rab -> rmsfab));
  rab -> rmsfab.fab$l_nam = 0;
  if (!(sts & 1)) {
    ix_errorlog (rab, "error creating - %s", ix_vmserr (sts));
    return (IX_CREATERROR);
  }

  /* Create the cache lock and set to NLMODE */

  sts = crecachelck (rab, &rmsnam);
  if (sts != IX_SUCCESS) return (sts);

#elif defined (WIN32)

  sharemode = 0;
  if (share > 0) sharemode = FILE_SHARE_READ;
  if (share < 0) sharemode = FILE_SHARE_READ | FILE_SHARE_WRITE;

  rab -> fh = CreateFile (rab -> fspec, GENERIC_READ | GENERIC_WRITE, 
                          sharemode, NULL, CREATE_NEW, 
                          FILE_FLAG_WRITE_THROUGH | FILE_FLAG_RANDOM_ACCESS, 
                          NULL);
  if (rab -> fh == INVALID_HANDLE_VALUE) {
    sts = GetLastError ();
    if (sts == ERROR_SHARING_VIOLATION) return (IX_FILELOCKED);
    ix_errorlog (rab, "error creating - %s", ix_win32err (sts));
    return (IX_CREATERROR);
  }

  sts = crecachelck (rab, share);
  if (sts != IX_SUCCESS) return (sts);

#else

  rab -> fd = open (rab -> fspec, O_RDWR | O_CREAT | O_TRUNC, 
                    S_IROTH | S_IWOTH | S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR);
  if (rab -> fd < 0) {
    ix_errorlog (rab, "error creating - %s", ix_unixerr ());
    return (IX_CREATERROR);
  }

  if (flock (rab -> fd, LOCK_EX) < 0) {
    ix_errorlog (rab, "error locking - %s", ix_unixerr ());
    unlink (rab -> fspec);
    close (rab -> fd);
    return (IX_LOCKERROR);
  }

#endif

  /* Get disk block size */

#if defined (VMS)
  *bls = rab -> rmsfab.fab$w_mrs;
#elif defined (WIN32)
  /**
  fsp = NULL;
  for (i = 0; rab -> fspec[i] != 0; i ++) {
    fs[i] = rab -> fspec[i];
    if (rab -> fspec[i] == ':') {
	  fsp = fs;
	  fs[i+1] = 0;
      break;
    }
  }
  if (!GetDiskFreeSpace (fsp, &sectperclus, &bytepersect, NULL, NULL)) {
    ix_errorlog (rab, "error getting cluster size - %s", 
                                                 ix_win32err (GetLastError ()));
    CloseHandle (rab -> fh);
    return (IX_CREATERROR);
  }
  if (sectperclus * bytepersect > 32256) *bls = 32256;
  else *bls = (uWord) (sectperclus * bytepersect);
  **/
  *bls = 512;
#elif defined (SCO)
  *bls = 512;
#else
  if (fstat (rab -> fd, &statbuf) < 0) {
    ix_errorlog (rab, "error sensing status - %s", ix_unixerr ());
    close (rab -> fd);
    return (IX_CREATERROR);
  }
  *bls = statbuf.st_blksize;
#endif

  return (IX_SUCCESS);
}

/* This routine is called if there is an error creating the file */

void ix_os_crefilerr (Rab *rab)

{
#if defined (VMS)
  sys$dassgn (rab -> rmsfab.fab$l_stv);
#elif defined (WIN32)
  CloseHandle (rab -> fh);
#else
  close (rab -> fd);
#endif
}

/* This routine is called if the creation succeeds */

void ix_os_crefilsuc (Rab *rab)

{
#if !defined (VMS) && !defined (WIN32)
  newopen (rab);
#endif
}

/************************************************************************/
/*									*/
/*  This routine flushes the writes to the disk				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab      = pointer to rab					*/
/*	writes   = array of Cache entries to write to disk		*/
/*	nwrites  = number of elements in writes				*/
/*	writefhd = 0 : don't write file header				*/
/*	           1 : do write file header				*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_os_fluwri = status						*/
/*	cache write flags cleared					*/
/*	disk file written						*/
/*									*/
/************************************************************************/

uLong ix_os_fluwri (Rab *rab, Cache **writes, uLong nwrites, int writefhd)

{
  Bkt *bkt;
  Cache *cp;
  uLong i, status, sts;

#if defined (VMS)
  uByte extrat[ATR$S_RECATTR];
  Iosbb extios, *iosbb;
  uLong eof;
  struct fibdef extfib;

  Desc extfibdes = { sizeof extfib, &extfib };
  Itmlst2 extatr[] = { ATR$S_RECATTR, ATR$C_RECATTR, extrat,
                       0, 0, 0 };
#elif defined (WIN32)
  uLong neweof, oldeof;
#else
  uLong j;
  struct stat statbuf;
#endif

  status = IX_SUCCESS;

#if defined (VMS)

  /* Extend file if necessary to accomodate any new buckets */

  eof = (rab -> fhd -> bls / 512) * rab -> fhd -> eof;
  if (rab -> rmsfab.fab$l_alq < eof) {
    sts = sys$qiow (efn, rab -> rmsfab.fab$l_stv, IO$_ACCESS, &extios, 0, 0, 
                    0, 0, 0, 0, extatr, 0);
    if (sts & 1) sts = extios.sts;
    if (sts & 1) {
      memset (&extfib, 0, sizeof extfib);
      i = fld$uw (extrat, fat$w_hiblkl) 
        + (fld$uw (extrat, fat$w_hiblkh) << 16);
      if (i < eof) {
        extfib.FIB$W_EXCTL = FIB$M_EXTEND | FIB$M_ALDEF;
        extfib.fib$l_exsz  = eof - i;
      }
      fld$uw (extrat, fat$w_efblkl) = (eof + 1);
      fld$uw (extrat, fat$w_efblkh) = (eof + 1) >> 16;
      sts = sys$qiow (efn, rab -> rmsfab.fab$l_stv, IO$_MODIFY, &extios, 0, 0, 
                      &extfibdes, 0, 0, 0, extatr, 0);
      if (sts & 1) sts = extios.sts;
      if (sts & 1) rab -> rmsfab.fab$l_alq = eof;
    }
    if (!(sts & 1)) {
      ix_errorlog (rab, "error extending - %s", ix_vmserr (sts));
      return (IX_WRITERROR);
    }
  }

  /* Start writing the new file header with the new roots and eof position */

  rab -> rmsfab.fab$l_ctx = 0;			/* no writes in progress yet */

  if (writefhd && rab -> writefhd) {
    sts = ix_checkfhd (rab);			/* make sure it's still valid */
    if (sts == IX_SUCCESS) sts = writeit (rab, 0, /* start writing */
                                          rab -> fhdbsz, (Rbf *) rab -> fhd);
    if (sts == IX_SUCCESS) rab -> writefhd = 0;
    else status = sts;
  }

  /* Start writing the buckets to disk */

  if (status == IX_SUCCESS) for (i = 0; i < nwrites; i ++) {
    cp = writes[i];				/* point to cache entry */
    bkt = cp -> bkt;				/* point to bucket */
    sts = ix_checkbkt (rab, cp -> khd, bkt, 0);	/* make sure it's still valid */
    if (sts == IX_SUCCESS) sts = writeit (rab, cp -> vbn, /* start writing */
                                          rab -> fhd -> bks, (Rbf *) (bkt));
    if (sts == IX_SUCCESS) cp -> write = 0;
    else {
      status = sts;
      break;
    }
  }

  /* Wait for all the writes to complete */

  sts = 1;					/* assume successful */
  while ((iosbb = rab -> rmsfab.fab$l_ctx) != 0) { /* repeat while still more */
    while (iosbb -> sts == 0) {			/* repeat while in progress */
      sys$waitfr (efn);				/* wait for one */
      sys$clref (efn);				/* clear event flag */
    }
    if (sts & 1) sts = iosbb -> sts;		/* it's done, save error sts */
    rab -> rmsfab.fab$l_ctx = iosbb -> next;	/* unlink the iosbb */
    ix_free (iosbb);				/* ... and free it off */
  }
  if (!(sts & 1)) {				/* maybe display error */
    ix_errorlog (rab, "error writing - %s", ix_vmserr (sts));
    if (status == IX_SUCCESS) status = IX_WRITERROR;
  }

#elif defined (WIN32)

  /* Extend file if necessary to accomodate any new buckets */

  neweof = rab -> fhd -> bls * rab -> fhd -> eof;
  oldeof = GetFileSize (rab -> fh, NULL);
  if (neweof > oldeof) {
    if ((SetFilePointer (rab -> fh, neweof, NULL, FILE_BEGIN) == 0xffffffff) 
     || !SetEndOfFile (rab -> fh)) {
      ix_errorlog (rab, "error extending - %s", ix_win32err (GetLastError ()));
      return (IX_WRITERROR);
    }
  }

  /* Now write the buckets to disk */

  for (i = 0; i < nwrites; i ++) {
    cp = writes[i];
    if (cp -> write) {
      bkt = cp -> bkt;
      sts = ix_checkbkt (rab, cp -> khd, bkt, 0);
      if (sts == IX_SUCCESS) sts = writeit (rab, cp -> vbn, 
                                            rab -> fhd -> bks, (Rbf *) (bkt));
      if (sts == IX_SUCCESS) cp -> write = 0;
      else status = sts;
    }
  }

  /* Finally write the new file header with the new roots and eof position */

  if (writefhd && rab -> writefhd) {
    sts = ix_checkfhd (rab);
    if (sts == IX_SUCCESS) 
                      sts = writeit (rab, 0, rab -> fhdbsz, (Rbf *) rab -> fhd);
    if (sts == IX_SUCCESS) rab -> writefhd = 0;
    else status = sts;
  }

#else

  /* Make sure file is open */

  sts = reopen (rab);			/* make sure it's open */
  if (sts != IX_SUCCESS) return (sts);

  /* Write buckets starting just beyond file's actual current eof up to */
  /* the new eof before updating any others.  This way, if the disk is  */
  /* too full to accomodate the new blocks, we will get an error before */
  /* updating any old pointers that point into these new blocks.        */

  if (fstat (rab -> fd, &statbuf) < 0) {	/* get current file size */
    ix_errorlog (rab, "error determining current size - %s", ix_unixerr ());
    return (IX_WRITERROR);
  }

  j = statbuf.st_size / rab -> fhd -> bls;	/* divide by block size */

  for (i = 0; i < nwrites; i ++) {		/* scan through sorted table */
    cp = writes[i];				/* point to cache entry */
    bkt = cp -> bkt;				/* get bucket pointer */
    if (bkt -> vbn >= j) {			/* see if beyond old eof */
      sts = ix_checkbkt (rab, cp -> khd, bkt, 0); /* it is, validate it */
      if (sts == IX_SUCCESS) sts = writeit (rab, cp -> vbn, /* write it */
                                              rab -> fhd -> bks, (Rbf *) (bkt));
      if (sts == IX_SUCCESS) cp -> write = 0;	/* if ok, don't write again */
      else return (sts);
    }
  }

  /* Now write the rest of the buckets to disk */

  for (i = 0; i < nwrites; i ++) {
    cp = writes[i];
    if (cp -> write) {
      bkt = cp -> bkt;
      sts = ix_checkbkt (rab, cp -> khd, bkt, 0);
      if (sts == IX_SUCCESS) sts = writeit (rab, cp -> vbn, 
                                            rab -> fhd -> bks, (Rbf *) (bkt));
      if (sts == IX_SUCCESS) cp -> write = 0;
      else status = sts;
    }
  }

  /* Finally write the new file header with the new roots and eof position */

  if (writefhd && rab -> writefhd) {
    sts = ix_checkfhd (rab);
    if (sts == IX_SUCCESS) 
                      sts = writeit (rab, 0, rab -> fhdbsz, (Rbf *) rab -> fhd);
    if (sts == IX_SUCCESS) rab -> writefhd = 0;
    else status = sts;
  }

#endif

  return (status);
}

/************************************************************************/
/*									*/
/*  Write blocks to file						*/
/*									*/
/*  This routine should only be called from routine flush.  That way, 	*/
/*  if the high level routine gets any errors, the disk file will not 	*/
/*  be updated with possibly erroneous data.				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	vbn = block number to start writing at				*/
/*	      0 writes at beginning of file				*/
/*	      block size is in rab -> fhd -> bls			*/
/*	rsz = number of bytes (exact multiple of rab -> fhd -> bls)	*/
/*	rbf = address of buffer to write to file			*/
/*									*/
/*    Output (Unix):							*/
/*									*/
/*	writeit = IX_SUCCESS : successfully written			*/
/*	                else : error writing				*/
/*									*/
/*    Output (VMS):							*/
/*									*/
/*	writeit = IX_SUCCESS : write successfully started		*/
/*	                       iosbb chained to rmsfab.rab$l_ctx	*/
/*	                else : error starting write			*/
/*									*/
/************************************************************************/

static uLong writeit (Rab *rab, Vbn vbn, Rsz rsz, Rbf *rbf)

{
#if defined (VMS)

  Iosbb *iosbb;
  uLong sts;
  uWord chan;

  rab -> cache_lkup = 1;

  iosbb = ix_malloc (sizeof *iosbb);

  while ((sts = sys$qio (efn, rab -> rmsfab.fab$l_stv, /* start writing */
             IO$_WRITEVBLK, iosbb, 0, 0, rbf, rsz, /* (set efn when done) */
              (vbn * (rab -> fhd -> bls / 512)) + 1, 0, 0, 0)) == SS$_EXQUOTA) {
    sys$waitfr (efn);				/* wait if exquota */
  }
  if (!(sts & 1)) {
    ix_errorlog (rab, "error writing %u bytes at %u - %s", 
                                                     rsz, vbn, ix_vmserr (sts));
    ix_free (iosbb);
    return (IX_WRITERROR);			/* error, return error status */
  }
  iosbb -> next = rab -> rmsfab.fab$l_ctx;	/* success, link up iosbb */
  rab -> rmsfab.fab$l_ctx = iosbb;			

#elif defined (WIN32)

  uLong nwritten, position, sts;

  rab -> cache_updated = 1;

  position = vbn * rab -> fhd -> bls;
  sts = SetFilePointer (rab -> fh, position, NULL, FILE_BEGIN);
  if (sts == 0xffffffff) {
    ix_errorlog (rab, "error positioning to %u for write - %s", 
	             position, ix_win32err (GetLastError ()));
    return (IX_WRITERROR);
  }
  sts = WriteFile (rab -> fh, rbf, rsz, &nwritten, NULL);
  if (!sts) {
    ix_errorlog (rab, "error writing %u bytes at %u - %s",
                                       rsz, vbn, ix_win32err (GetLastError ()));
    return (IX_WRITERROR);
  }
  if (nwritten != rsz) {
    ix_errorlog (rab, "only wrote %u out of %u at %u", nwritten, rsz, vbn);
    return (IX_WRITERROR);
  }

#else

  int sts;
  uLong status;

  status = reopen (rab);			/* make sure it's open */
  if (status != IX_SUCCESS) return (status);

  sts = lseek (rab -> fd, vbn * rab -> fhd -> bls, SEEK_SET);
  if (sts < 0) {
    ix_errorlog (rab, "error positioning to block %u - %s", vbn, ix_unixerr ());
    return (IX_SEEKERROR);
  }
  sts = write (rab -> fd, rbf, rsz);
  if (sts < (int) rsz) {
    if (sts < 0) ix_errorlog (rab, "error writing %u bytes at %u - %s", 
                                                       rsz, vbn, ix_unixerr ());
    else ix_errorlog (rab, "error writing %u bytes at %u - end of file", 
                                                                      rsz, vbn);
    return (IX_WRITERROR);
  }

#endif

  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  Open file								*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	rab -> fspec = filespec to be opened				*/
/*	rdonly = 0 : open it read/write					*/
/*	         1 : open it read-only					*/
/*	share = 0 : don't allow any access by others			*/
/*	        1 : allow only other read accesses			*/
/*	       -1 : allow other read and write accesses			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_os_opefil = IX_SUCCESS : success				*/
/*	                     else : error status			*/
/*									*/
/************************************************************************/

uLong ix_os_opefil (Rab *rab, int rdonly, int share)

{
#if defined (VMS)

  uLong sts;
  struct NAM rmsnam;
  struct XABFHC rmsfhc;

  if (efn < 0) {
    sts = lib$get_ef (&efn);
    if (!(sts & 1)) lib$stop (sts);
  }

  /* Open the file - if read-only, open it with r-o access */

  rab -> cache_lkid = 0;
  rab -> rmsfab = cc$rms_fab;
  rmsfhc = cc$rms_xabfhc;
  rmsnam = cc$rms_nam;
  rab -> rmsfab.fab$b_fac = FAB$M_GET | FAB$M_PUT | FAB$M_UPD | FAB$M_BIO;
  if (rdonly) rab -> rmsfab.fab$b_fac = FAB$M_GET | FAB$M_BIO;
  else if (share > 0) rab -> rmsfab.fab$b_shr = FAB$M_SHRGET | FAB$M_UPI;
  if (share < 0) rab -> rmsfab.fab$b_shr = FAB$M_SHRGET | FAB$M_SHRPUT 
                                         | FAB$M_SHRUPD | FAB$M_UPI;
  rab -> rmsfab.fab$l_fna = rab -> fspec;
  rab -> rmsfab.fab$b_fns = strlen (rab -> fspec);
  rab -> rmsfab.fab$l_fop = FAB$M_UFO;
  rab -> rmsfab.fab$l_nam = &rmsnam;
  rab -> rmsfab.fab$l_xab = &rmsfhc;
  sts = sys$open (&(rab -> rmsfab));
  rab -> rmsfab.fab$l_nam = 0;
  rab -> rmsfab.fab$l_xab = 0;
  if (!(sts & 1)) {
    if (sts == RMS$_FNF) return (IX_NOSUCHFILE);
    if (sts == RMS$_FLK) return (IX_FILELOCKED);
    ix_errorlog (rab, "error opening - %s", ix_vmserr (sts));
    return (IX_OPENERROR);
  }
  rab -> rmsfab.fab$l_alq = rmsfhc.xab$l_ebk - 1;

  /* Create the cache lock and set to NLMODE */

  return (crecachelck (rab, &rmsnam));

#elif defined (WIN32)

  int accessmode, sharemode;
  uLong sts;

  accessmode = GENERIC_READ | GENERIC_WRITE;
  if (rdonly) accessmode = GENERIC_READ;
  sharemode = 0;
  if (rdonly || (share > 0)) sharemode = FILE_SHARE_READ;
  else if (share < 0) sharemode = FILE_SHARE_READ | FILE_SHARE_WRITE;

  rab -> fh = CreateFile (rab -> fspec, accessmode, 
                          sharemode, NULL, OPEN_EXISTING, 
                          FILE_FLAG_WRITE_THROUGH | FILE_FLAG_RANDOM_ACCESS, 
                          NULL);
  if (rab -> fh == INVALID_HANDLE_VALUE) {
    sts = GetLastError ();
    if (sts == ERROR_FILE_NOT_FOUND) return (IX_NOSUCHFILE);
    if (sts == ERROR_SHARING_VIOLATION) return (IX_FILELOCKED);
    ix_errorlog (rab, "error opening - %s", ix_win32err (sts));
    return (IX_OPENERROR);
  } 

  /* Create the cache lock and leave it unlocked */

  return (crecachelck (rab, share));

#else

  int flags;

  flags = O_RDWR;
  if (rdonly) flags = O_RDONLY;
  rab -> fd = open (rab -> fspec, flags, 0);
  if (rab -> fd < 0) {
    if (errno == ENOENT) return (IX_NOSUCHFILE);
    ix_errorlog (rab, "error opening - %s", ix_unixerr ());
    return (IX_OPENERROR);
  }

  flags = LOCK_EX;
  if (rdonly) flags = LOCK_SH;
  if (flock (rab -> fd, flags) < 0) {
    ix_errorlog (rab, "error locking - %s", ix_unixerr ());
    close (rab -> fd);
    return (IX_LOCKERROR);
  }

#endif

  return (IX_SUCCESS);
}

/* This routine is called if the open failed */

void ix_os_opefilerr (Rab *rab)

{
#if defined (VMS)
  sys$dassgn (rab -> rmsfab.fab$l_stv);
  sys$deq (rab -> cache_lkid, 0, 0, 0);
#elif defined (WIN32)
  CloseHandle (rab -> fh);
  CloseHandle (rab -> cache_mtxhandle);
  CloseHandle (rab -> cache_semhandle);
#else
  close (rab -> fd);
#endif
}

/* This routine is called if the open was successful */

void ix_os_opefilsuc (Rab *rab)

{
#if !defined (VMS) && !defined (WIN32)
  newopen (rab);
#endif
}

#if defined (VMS)

/* This routine creates the cache lock */

static uLong crecachelck (Rab *rab, struct NAM *rmsnam)

{
  Byte lcknambuf[32] = { 'I', 'X', '_', 'a' };
  Desc lcknamdes = { 0, lcknambuf };
  Iosb iosb;
  Lksb lksb;
  uLong sts;

  Itmlst3 dviitm[] = { 21, DVI$_ALLDEVNAM, lcknambuf + 10, &lcknamdes.size, 
                        0, 0, 0, 0 };

  /* If no sharing, don't bother creating any lock for efficiency */

  rab -> cache_lkid = 0;
  if (!(rab -> rmsfab.fab$b_shr & FAB$M_UPI)) return (IX_SUCCESS);

  /* Create lock with name IX_a(fid)(adv) */

  memcpy (lcknambuf + 4, rmsnam -> nam$w_fid, 6);
  sts = sys$getdviw (efn, rab -> rmsfab.fab$l_stv, 0, dviitm, &iosb, 0, 0, 0);
  if (sts & 1) sts = iosb.sts;
  if (!(sts & 1)) lib$stop (sts);
  lcknamdes.size += 10;
  sts = sys$enqw (efn, LCK$K_NLMODE, &lksb, 
                  LCK$M_NOQUEUE | LCK$M_SYSTEM | LCK$M_EXPEDITE, 
                  &lcknamdes, 0, 0, 0, 0, 0, 0, 0);
  if (sts & 1) sts = lksb.sts;
  if (!(sts & 1)) {
    sys$dassgn (rab -> rmsfab.fab$l_stv);
    ix_errorlog (rab, "error creating lock - %s", ix_vmserr (sts));
    return (IX_LOCKERROR);
  }

  /* Save its lock-id and return success status */

  rab -> cache_lkid = lksb.lkid;
  return (IX_SUCCESS);
}

#endif

#if defined (WIN32)

/* This routine creates the cache lock */

static uLong crecachelck (Rab *rab, int share)

{
  BY_HANDLE_FILE_INFORMATION fileinfo;
  uLong sts;

  /* If not sharing, don't bother with mutex stuff */

  rab -> cache_mtxhandle = NULL;
  rab -> cache_semhandle = NULL;
  if (share == 0) return (IX_SUCCESS);

  /* Get unique identifier for file to use for names */

  if (!GetFileInformationByHandle (rab -> fh, &fileinfo)) {
    ix_errorlog (rab, "error getting file info - %s", 
                                                 ix_win32err (GetLastError ()));
    return (IX_LOCKERROR);
  }

  /* Create a mutex for the file or access already existing one */

  sprintf (rab -> cache_mtxname, "IX_am%8.8x%8.8x%8.8x", 
                                                 fileinfo.dwVolumeSerialNumber, 
                                                 fileinfo.nFileIndexHigh, 
                                                 fileinfo.nFileIndexLow);
  rab -> cache_mtxhandle = CreateMutex (NULL, 0, rab -> cache_mtxname);
  if (rab -> cache_mtxhandle == NULL) {
    sts = GetLastError ();
    ix_errorlog (rab, "error creating mutex - %s", ix_win32err (sts));
    return (IX_LOCKERROR);
  }

  /* Likewise for a semaphore.  The semaphore will be used to hold   */
  /* the serial number for the cache.  It is never decremented, just */
  /* incremented each time a write operation has been performed.     */

  sprintf (rab -> cache_semname, "IX_as%8.8x%8.8x%8.8x", 
                                                 fileinfo.dwVolumeSerialNumber, 
                                                 fileinfo.nFileIndexHigh, 
                                                 fileinfo.nFileIndexLow);
  rab -> cache_semhandle = CreateSemaphore (NULL, 1, 0x7fffffff, 
                                            rab -> cache_semname);
  if (rab -> cache_semhandle == NULL) {
    sts = GetLastError ();
    CloseHandle (rab -> cache_mtxhandle);
    rab -> cache_mtxhandle = NULL;
    ix_errorlog (rab, "error creating semaphore - %s", ix_win32err (sts));
    return (IX_LOCKERROR);
  }

  return (IX_SUCCESS);
}

#endif

/************************************************************************/
/*									*/
/*  Read blocks from file						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	vbn = block number to start reading at				*/
/*	      0 reads at beginning of file				*/
/*	      block size is in rab -> fhd -> bls			*/
/*	rsz = number of bytes (exact multiple of rab -> fhd -> bls)	*/
/*	rbf = address of buffer to read file into			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_os_readit = IX_SUCCESS : successfully written		*/
/*	                     else : error writing			*/
/*	*rbf = filled in with data read from file			*/
/*									*/
/************************************************************************/

uLong ix_os_readit (Rab *rab, Vbn vbn, Rsz rsz, Rbf *rbf)

{
#if defined (VMS)

  Iosbb iosbb;
  uLong sts, vvbn;

  if (efn < 0) {
    sts = lib$get_ef (&efn);
    if (!(sts & 1)) lib$stop (sts);
  }

  vvbn = vbn;
  if (vvbn != 0) vvbn *= rab -> fhd -> bls / 512;

  sts = sys$qiow (efn, rab -> rmsfab.fab$l_stv, IO$_READVBLK, &iosbb, 0, 0, 
                  rbf, rsz, vvbn + 1, 0, 0, 0);
  if (sts & 1) sts = iosbb.sts;
  if (!(sts & 1)) {
    ix_errorlog (rab, "error reading block %u - %s", vbn, ix_vmserr (sts));
    return (IX_READERROR);
  }

#elif defined (WIN32)

  uLong nread, position, sts;

  position = vbn;
  if (position != 0) position *= rab -> fhd -> bls;
  sts = SetFilePointer (rab -> fh, position, NULL, FILE_BEGIN);
  if (sts == 0xffffffff) {
    ix_errorlog (rab, "error positioning to %u for read - %s", 
	             position, ix_win32err (GetLastError ()));
    return (IX_READERROR);
  }
  sts = ReadFile (rab -> fh, rbf, rsz, &nread, NULL);
  if (!sts) {
    ix_errorlog (rab, "error reading %u bytes at %u - %s",
                                       rsz, vbn, ix_win32err (GetLastError ()));
    return (IX_READERROR);
  }
  if (nread != rsz) {
    ix_errorlog (rab, "only read %u out of %u at %u", nread, rsz, vbn);
    return (IX_READERROR);
  }

#else

  int sts;
  unsigned int offset, status;

  status = reopen (rab);			/* make sure it's open */
  if (status != IX_SUCCESS) return (status);

  offset = vbn;
  if (offset != 0) offset *= rab -> fhd -> bls;
  sts = lseek (rab -> fd, offset, SEEK_SET);
  if (sts < 0) {
    ix_errorlog (rab, "error positioning to block %u - %s", vbn, ix_unixerr ());
    return (IX_SEEKERROR);
  }
  sts = read (rab -> fd, rbf, rsz);
  if (sts < (int) rsz) {
    if (sts < 0) ix_errorlog (rab, "error reading %u bytes at %u - %s", 
                                                       rsz, vbn, ix_unixerr ());
    else ix_errorlog (rab, "error reading %u bytes at %u - end of file", 
                                                                      rsz, vbn);
    return (IX_READERROR);
  }

#endif

  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  Re-open a Unix file							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	rab -> fd >= 0 : file is open, no need to re-open		*/
/*	            -1 : file is closed, need to re-open		*/
/*	            -2 : previous close error, can't re-open		*/
/*									*/
/*    Output:								*/
/*									*/
/*	reopen = IX_SUCCESS : file is (now) open			*/
/*	               else : error status				*/
/*									*/
/************************************************************************/

#if !defined (VMS) && !defined (WIN32)

static uLong reopen (Rab *rab)

{
  int flags;
  Rab **lrab, *xrab;

  if (rab -> fd < 0) {

    /* If maximum number already open, close oldest one */

    if (ix_maxopen == 0) {
      for (xrab = rabs; xrab != NULL; xrab = xrab -> next) 
                                                     if (xrab -> fd >= 0) break;
      ix_maxopen ++;
      if (close (xrab -> fd) < 0) {
        xrab -> fd = -2;
        ix_errorlog (xrab, "error closing %s - %s", 
                                                  xrab -> fspec, ix_unixerr ());
      }
      else xrab -> fd = -1;
    }

    /* Re-open the file */

    if (rab -> fd == -2) return (IX_CLOSERROR);
    flags = O_RDWR;
    if (rab -> rdonly) flags = O_RDONLY;
    rab -> fd = open (rab -> fspec, flags, 0);
    if (rab -> fd < 0) {
      ix_errorlog (rab, "error re-opening - %s", ix_unixerr ());
      return (IX_OPENERROR);
    }
    ix_maxopen --;
  }

  /* Make sure this file is the last one in the list to be closed */

  if (rab -> next != NULL) {
    for (lrab = &rabs; (xrab = *lrab) != NULL;) {
      if (xrab == rab) *lrab = xrab -> next;
      else lrab = &(xrab -> next);
    }
    rab -> next = NULL;
    *lrab = rab;
  }

  return (IX_SUCCESS);
}

/* Add new rab to rabs list */

static void newopen (Rab *rab)

{
  Rab **lrab, *xrab;

  /* If maximum number already open, close oldest one */

  if (ix_maxopen == 0) {
    for (xrab = rabs; xrab != NULL; xrab = xrab -> next) 
                                                     if (xrab -> fd >= 0) break;
    ix_maxopen ++;
    if (close (xrab -> fd) < 0) {
      xrab -> fd = -2;
      ix_errorlog (rab, "error closing %s - %s", xrab -> fspec, ix_unixerr ());
    }
    else xrab -> fd = -1;
  }

  /* Decrement number of files allowed to open */

  ix_maxopen --;

  /* Put this file on the end of the list to be closed */

  for (lrab = &rabs; (xrab = *lrab) != NULL;) {
    if (xrab == rab) *lrab = xrab -> next;
    else lrab = &(xrab -> next);
  }
  rab -> next = NULL;
  *lrab = rab;
}

/* Remove old rab from rabs list */

static void oldclose (Rab *rab)

{
  Rab **lrab, *xrab;

  /* Remove rab from list */

  for (lrab = &rabs; (xrab = *lrab) != NULL;) {
    if (xrab == rab) *lrab = xrab -> next;
    else lrab = &(xrab -> next);
  }

  /* Increment number of files allowed to open */

  ix_maxopen ++;
}

#endif

/************************************************************************/
/*									*/
/*  Lock file access during search or modification			*/
/*  Invalidate cache if another thread modified the file		*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab pointer						*/
/*	rw  = 0 : read-only access desired, 				*/
/*	          no modifications will be performed			*/
/*	      1 : read/write access desired, 				*/
/*	          modification might be performed			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_os_lckfil = IX_SUCCESS : successful completion		*/
/*	            IX_CACHEINVAL : locked, but modifications were 	*/
/*	                            made while unlocked			*/
/*	                     else : error status			*/
/*									*/
/************************************************************************/

uLong ix_os_lckfil (Rab *rab, int rw)

{
#if defined (VMS)

  Lksb lksb;
  uLong lkmode, sts;

  /* Say no updates have been made since lock was set */

  rab -> cache_lkup = 0;

  /* Set the lock for the entyre fyle and read the value block */

  lksb.lkid = rab -> cache_lkid;
  if (lksb.lkid == 0) return (IX_SUCCESS);
  lkmode = LCK$K_PRMODE;
  if (rw) lkmode = LCK$K_EXMODE;
  sts = sys$enqw (efn, lkmode, &lksb, LCK$M_VALBLK | LCK$M_CONVERT, 
                  0, 0, 0, 0, 0, 0, 0, 0);
  if (sts & 1) sts = lksb.sts;

  /* If the lock value block is invalid, set up a new value  */
  /* block to something only this process would generate and */
  /* something that will fail the cache currency test below  */

  /* But it we have it in PRMODE, convert to EXMODE so we can write */
  /* the value block.  Convert to EXMODE via NLMODE so we don't     */
  /* deadlock with others trying to do the same thing, then re-     */
  /* check for SS$_VALNOTVALID in case someone else fixed it.       */

  if ((sts == SS$_VALNOTVALID) && (lkmode != LCK$K_EXMODE)) {
    sts = sys$enqw (efn, LCK$K_NLMODE, &lksb, LCK$M_CONVERT,
                    0, 0, 0, 0, 0, 0, 0, 0);
    if (sts & 1) sts = lksb.sts;
    if (!(sts & 1)) lib$stop (sts);
    sts = sys$enqw (efn, LCK$K_EXMODE, &lksb, LCK$M_VALBLK | LCK$M_CONVERT,
                    0, 0, 0, 0, 0, 0, 0, 0);
    if (sts & 1) sts = lksb.sts;
  }

  if (sts == SS$_VALNOTVALID) {
    rab -> cache_lksq = ++ cache_lksq;
    rab -> cache_lkpi = getpid ();
    rab -> cache_lkch = rab -> rmsfab.fab$l_stv;
    lksb.val_seq = rab -> cache_lksq + 1;
    lksb.val_pid = rab -> cache_lkpi;
    lksb.val_chn = rab -> cache_lkch;
  }

  /* If unsuccessful, return error status */

  else if (!(sts & 1)) {
    ix_errorlog (rab, "error locking file - %s", ix_vmserr (sts));
    return (IX_LOCKERROR);
  }

  /* If cache contents are no longer current, return an error status */

  if ((lksb.val_seq != rab -> cache_lksq) 
   || (lksb.val_pid != rab -> cache_lkpi) 
   || (lksb.val_chn != rab -> cache_lkch)) {
    rab -> cache_lksq = lksb.val_seq;
    rab -> cache_lkpi = lksb.val_pid;
    rab -> cache_lkch = lksb.val_chn;
    return (IX_CACHEINVAL);
  }

#elif defined (WIN32)

  Long semcount, sts;

  /* Say no updates have been made since lock was set */

  rab -> cache_updated = 0;

  /* Set the lock for the entyre fyle */

  if (rab -> cache_mtxhandle == NULL) return (IX_SUCCESS);

  sts = WaitForSingleObject (rab -> cache_mtxhandle, INFINITE);
  if ((sts != WAIT_ABANDONED) && (sts != WAIT_OBJECT_0)) {
    ix_errorlog (rab, "error locking file - %s", ix_win32err (GetLastError ()));
    return (IX_LOCKERROR);
  }

  /* Read cache serial number into semcount */

  if (!ReleaseSemaphore (rab -> cache_semhandle, 1, &semcount)) {
    ix_errorlog (rab, "error reading cache semaphore - %s", 
                                                 ix_win32err (GetLastError ()));
    ReleaseMutex (rab -> cache_mtxhandle);
    return (IX_LOCKERROR);
  }
  sts = WaitForSingleObject (rab -> cache_semhandle, 0);
  if (sts != WAIT_OBJECT_0) {
    ix_errorlog (rab, "error restoring cache semaphore - %s", 
	                                         ix_win32err (GetLastError ()));
	ReleaseMutex (rab -> cache_mtxhandle);
	return (IX_LOCKERROR);
  } 

  /* If cache contents are no longer current, return an error status */

  if (semcount != rab -> cache_semcount) {
    rab -> cache_semcount = semcount;
    return (IX_CACHEINVAL);
  }

#else
  
#endif

  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  Release lock set by ix_os_lckfil					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*									*/
/************************************************************************/

void ix_os_ulkfil (Rab *rab)

{
#if defined (VMS)

  Lksb lksb;
  uLong flags, sts;

  lksb.lkid = rab -> cache_lkid;
  if (lksb.lkid == 0) return;
  flags = LCK$M_CONVERT | LCK$M_NOQUEUE;

  /* If modification was made, set up a new lock value block   */
  /* to something only this process would generate.  Note that */
  /* this assumes that ix_os_lckfil was called with rw=1.      */

  if (rab -> cache_lkup) {
    rab -> cache_lksq = ++ cache_lksq;
    rab -> cache_lkpi = getpid ();
    rab -> cache_lkch = rab -> rmsfab.fab$l_stv;
    flags = LCK$M_CONVERT | LCK$M_NOQUEUE | LCK$M_VALBLK;
    lksb.val_seq = rab -> cache_lksq;
    lksb.val_pid = rab -> cache_lkpi;
    lksb.val_chn = rab -> cache_lkch;
  }

  /* Release the lock */

  sts = sys$enqw (efn, LCK$K_NLMODE, &lksb, flags, 0, 0, 0, 0, 0, 0, 0);
  if (sts & 1) sts = lksb.sts;
  if (!(sts & 1)) lib$stop (sts);

#elif defined (WIN32)

  if (rab -> cache_mtxhandle == NULL) return;

  /* If modification was made, increment semaphore */
  /* count to invalidate all other caches          */

  if (rab -> cache_updated) {
    rab -> cache_semcount ++;
    if (!ReleaseSemaphore (rab -> cache_semhandle, 1, NULL)) 
                   ix_errorlog (rab, "error incrementing semaphore count - %s", 
                                                 ix_win32err (GetLastError ()));
  }

  /* Release the lock */

  if (!ReleaseMutex (rab -> cache_mtxhandle)) 
                                ix_errorlog (rab, "error releasing mutex - %s", 
                                                 ix_win32err (GetLastError ()));

#else

#endif
}

/************************************************************************/
/*									*/
/*  Lock a record - return error status if already locked		*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab with crp set up to record to be locked	*/
/*	rw  = 0 : lock record for read-only access			*/
/*	      1 : lock record for read/write access			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_os_lockcrp = IX_SUCCESS : record successfully locked		*/
/*	           IX_RECORDLOCKED : record is locked by someone else	*/
/*	                      else : error status			*/
/*									*/
/************************************************************************/

uLong ix_os_lockcrp (Rab *rab, int rw)

{
#if defined (VMS)

  Desc lcknamdes;
  Lksb lksb;
  uLong lkmode, sts;

  /* If no shared access, just return success */

  if (rab -> cache_lkid == 0) return (IX_SUCCESS);

  /* Make up lock name = rfa of record to be locked */

  lcknamdes.size = sizeof rab -> crp.bkt -> rec[rab->crp.idx].rfa;
  lcknamdes.adrs = &(rab -> crp.bkt -> rec[rab->crp.idx].rfa);

  /* Set up mode, PR if read-only, EX if read/write */

  lkmode = LCK$K_PRMODE;
  if (rw) lkmode = LCK$K_EXMODE;

  /* Set the lock but don't wait if we can't get it.  Not waiting avoids the */
  /* deadlock situation because sometimes we lock the record when we have    */
  /* the file lock, sometimes we lock the file when we have the record lock. */

  sts = sys$enqw (efn, lkmode, &lksb, LCK$M_NOQUEUE | LCK$M_SYSTEM, 
                  &lcknamdes, rab -> cache_lkid, 0, 0, 0, 0, 0, 0);
  if (sts & 1) sts = lksb.sts;
  if (sts & 1) {
    rab -> crp_lkid = lksb.lkid;
    return (IX_SUCCESS);
  }
  if (sts == SS$_NOTQUEUED) return (IX_RECORDLOCKED);
  ix_errorlog (rab, "error setting record lock - %s", ix_vmserr (sts));
  return (IX_LOCKERROR);

#elif defined (WIN32)

  Byte lockname[48];

  /* If no shared access, just return success */

  if (rab -> cache_mtxhandle == NULL) return (IX_SUCCESS);

  /* Make up lock name = cache_mtxname + rfa */

  sprintf (lockname, "%s%8.8x%8.8x", rab -> cache_mtxname, 
                                   rab -> crp.bkt -> rec[rab->crp.idx].rfa.vbn, 
                                   rab -> crp.bkt -> rec[rab->crp.idx].rfa.seq);

  /* Set the lock but don't wait if we can't get it.  Not waiting avoids the */
  /* deadlock situation because sometimes we lock the record when we have    */
  /* the file lock, sometimes we lock the file when we have the record lock. */

  rab -> crp_semhandle = CreateSemaphore (NULL, 1, 1, lockname);
  if (rab -> crp_semhandle == NULL) {
    ix_errorlog (rab, "error locking record - %s", 
                                                 ix_win32err (GetLastError ()));
    return (IX_LOCKERROR);
  }
  if (GetLastError () == 0) return (IX_SUCCESS);
  CloseHandle (rab -> crp_semhandle);
  rab -> crp_semhandle = NULL;
  return (IX_RECORDLOCKED);

#else

  return (IX_SUCCESS);

#endif
}

/************************************************************************/
/*									*/
/*  Release a record's lock						*/
/*									*/
/************************************************************************/

void ix_os_unlockcrp (Rab *rab)

{
#if defined (VMS)

  if (rab -> crp_lkid != 0) {
    sys$deq (rab -> crp_lkid, 0, 0, 0);
    rab -> crp_lkid = 0;
  }

#elif defined (WIN32)

  if (rab -> crp_semhandle != NULL) {
    CloseHandle (rab -> crp_semhandle);
    rab -> crp_semhandle = NULL;
  }

#else

#endif
}
