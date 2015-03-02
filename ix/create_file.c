/************************************************************************/
/*									*/
/*  This routine is called to create a new indexed file			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

/************************************************************************/
/*									*/
/*  Create file								*/
/*									*/
/*    Input:								*/
/*									*/
/*	fspec = filespec						*/
/*	nky   = number of keys						*/
/*	ksz   = array of key sizes					*/
/*	kof   = array of key offsets					*/
/*	kat   = key attributs						*/
/*	bks   = bucket size						*/
/*	mrs   = maximum record size					*/
/*	buffc = buffer count for cache					*/
/*	rabv  = where to return rab pointer				*/
/*	share = IX_SHARE_N : no access by others allowed		*/
/*	        IX_SHARE_R : read access by others allowed		*/
/*	        IX_SHARE_W : read & write access by others allowed	*/
/*	logsiz = size of log message buffer (or 0 if not wanted)	*/
/*	logbuf = pointer to log message buffer (or NULL if not wanted)	*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_create_file = status						*/
/*	*rabv = rab pointer						*/
/*									*/
/************************************************************************/

uLong ix_create_file (const Byte *fspec, 
                      Rsz nky, 
                      const Rsz *ksz, 
                      const Rsz *kof, 
                      Rsz bks, 
                      Rsz mrs, 
                      int buffc, 
                      void **rabv)

{
  return (ix_create_file3 (fspec, nky, ksz, kof, NULL, bks, mrs, buffc, 
                           rabv, IX_SHARE_N, 0, NULL));
}

uLong ix_create_file2 (const Byte *fspec, 
                       Rsz nky, 
                       const Rsz *ksz, 
                       const Rsz *kof, 
                       Rsz bks, 
                       Rsz mrs, 
                       int buffc, 
                       void **rabv, 
                       int share, 
                       Rsz logsiz, 
                       Byte *logbuf)

{
  return (ix_create_file3 (fspec, nky, ksz, kof, NULL, bks, mrs, buffc, 
                           rabv, share, logsiz, logbuf));
}

uLong ix_create_file3 (const Byte *fspec, 
                       Rsz nky, 
                       const Rsz *ksz, 
                       const Rsz *kof, 
                       const Kat *kat, 
                       Rsz bks, 
                       Rsz mrs, 
                       int buffc, 
                       void **rabv, 
                       int share, 
                       Rsz logsiz, 
                       Byte *logbuf)

{
  Fhd *fhd;
  uLong sts;
  Rab *rab;
  Rsz bls, fhdbsz, fhdrsz, i;

  if (logsiz == 0) logbuf = NULL;
  else if (logbuf != NULL) logbuf[0] = 0;

  /* Make sure there is at least one key */

  if (nky == 0) return (IX_INVKEYREF);

  /* Make sure key sizes are non-zero and keys don't go off end of record */

  for (i = 0; i < nky; i ++) 
           if ((ksz[i] == 0) || (ksz[i] + kof[i] > mrs)) return (IX_INVKEYSIZE);

  /* Create the file */

  ix_memen (1);
  rab = ix_calloc (sizeof *rab + strlen (fspec));
  strcpy (rab -> fspec, fspec);
  rab -> logsiz = logsiz;
  rab -> logbuf = logbuf;
  sts = ix_os_crefil (rab, &bls, share);
  if (sts != IX_SUCCESS) {
    ix_free (rab);
    ix_memen (0);
    return (sts);
  }

  /* Lock write access to it */

  sts = ix_os_lckfil (rab, 1);
  if ((sts != IX_SUCCESS) && (sts != IX_CACHEINVAL)) {
    ix_os_crefilerr (rab);
    ix_free (rab);
    ix_memen (0);
    return (sts);
  }

  /* Round up bucket size to minimum of record */
  /* size and even multiple of block size      */

  if (bks < mrs + sizeof (Bkt) + 2 * sizeof (Rec)) 
                                    bks = mrs + sizeof (Bkt) + 2 * sizeof (Rec);
  bks = round_up (bks, bls);

  /* Build file header */

  fhd = NULL;
  fhdrsz = (Rbf *) (fhd -> khd + nky) - (Rbf *) fhd; /* khd array + header */
  fhdbsz = round_up (fhdrsz, bls);		/* round up to block size */
  fhd = (Fhd *) ix_calloc (fhdbsz);		/* allocate memory & clear it */

  fhd -> ver = 3;				/* file version number */
  fhd -> mrs = mrs;				/* maximum record size */
  fhd -> nky = nky;				/* number of keys */
  fhd -> bls = bls;				/* block size (in bytes) */
  fhd -> bks = bks;				/* bucket size (in bytes) */
  fhd -> eof = fhdbsz / bls;			/* file is empty */
  fhd -> rfa.vbn = fhd -> eof + 1;		/* init rfa.vbn */

  for (i = 0; i < nky; i ++) {
    fhd -> khd[i].vbn = 0;			/* clear root pointer */
    fhd -> khd[i].ksz = ksz[i];			/* get key size */
    fhd -> khd[i].kof = kof[i];			/* get key offset in record */
    if (kat != NULL) fhd -> khd[i].kat = kat[i]; /* get key attributes */
  }

  /* Create the rab */

  rab -> fhd    = fhd;				/* save fhd pointer */
  rab -> fhdrsz = fhdrsz;			/* save actual size */
  rab -> fhdbsz = fhdbsz;			/* save rounded-up size */
  rab -> maxcache = buffc;			/* max buffers in cache */

  /* Write fhd to beginning of file */

  sts = ix_writefhd (rab);			/* write to file */
  if (sts != IX_SUCCESS) {			/* check for success */
    ix_os_crefilerr (rab);			/* failed, close file */
    ix_free (fhd);				/* free off data structures */
    ix_free (rab);
    ix_memen (0);
    return (sts);				/* return error status */
  }

  /* For Unix, put it on the open files list, possibly closing another file */

  ix_os_crefilsuc (rab);

  /* Return address of rab */

  *rabv = rab;					/* success, return rab adrs */
  return (ix_flush (rab, IX_SUCCESS, 1));	/* return success status */
}
