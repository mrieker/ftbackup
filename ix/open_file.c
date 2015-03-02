/************************************************************************/
/*									*/
/*  This routine is called to open an existing file			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

/************************************************************************/
/*									*/
/*  Open file								*/
/*									*/
/*    Input:								*/
/*									*/
/*	fspec  = filespec						*/
/*	rdonly = 0 : read/write						*/
/*	         1 : read-only						*/
/*	buffc  = buffer count for cache					*/
/*	rabv   = where to return rab pointer				*/
/*	share  = IX_SHARE_N : no access by others allowed		*/
/*	         IX_SHARE_R : read access by others allowed		*/
/*	         IX_SHARE_W : read & write access by others allowed	*/
/*	logsiz = size of log message buffer (or 0 if not wanted)	*/
/*	logbuf = pointer to log message buffer (or NULL if not wanted)	*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_open_file = IX_SUCCESS : file opened				*/
/*	               IX_NOSUCHFILE : file does not exist		*/
/*	               IX_FILELOCKED : opened for write by another job	*/
/*	               else : error status				*/
/*	*rabv = rab pointer						*/
/*									*/
/************************************************************************/

uLong ix_open_file (const Byte *fspec,
                    int rdonly,
                    int buffc,
                    void **rabv)

{
  return (ix_open_file2 (fspec, rdonly, buffc, rabv, 
                         IX_SHARE_N, 0, NULL));
}

uLong ix_open_file2 (const Byte *fspec, 
                     int rdonly, 
                     int buffc, 
                     void **rabv, 
                     int share, 
                     Rsz logsiz, 
                     Byte *logbuf)

{
  Fhd *fhd, *fhdv3, fhdbuf;
  uLong sts;
  Rab *rab;
  Rsz fhdbsz, fhdrsz, khdsiz, krf;

  if (logsiz == 0) logbuf = NULL;
  else if (logbuf != NULL) logbuf[0] = 0;

  /* Open the file */

  ix_memen (1);
  rab = ix_calloc (sizeof *rab + strlen (fspec));
  strcpy (rab -> fspec, fspec);
  rab -> logsiz = logsiz;
  rab -> logbuf = logbuf;

  sts = ix_os_opefil (rab, rdonly, share);
  if (sts != IX_SUCCESS) {
    ix_free (rab);
    ix_memen (0);
    return (sts);
  }

  /* Lock read access to it */

  sts = ix_os_lckfil (rab, 0);
  if ((sts != IX_SUCCESS) && (sts != IX_CACHEINVAL)) {
    ix_os_opefilerr (rab);
    ix_free (rab);
    ix_memen (0);
    return (sts);
  }

  /* Read fixed part of header */

  sts = ix_os_readit (rab, 0, sizeof fhdbuf, (Rbf *) &fhdbuf);
  if (sts != IX_SUCCESS) {
    ix_os_opefilerr (rab);
    ix_free (rab);
    ix_memen (0);
    return (sts);
  }

  /* Make sure it is a version 3 file              */
  /* Also allow readonly on v1 and v2 for fix_file */

  if (fhdbuf.ver != 3) {
    ix_errorlog (rab, "file is version %u", fhdbuf.ver);
    if (!rdonly || ((fhdbuf.ver != 1) && (fhdbuf.ver != 2))) {
      ix_os_opefilerr (rab);
      ix_free (rab);
      ix_memen (0);
      return (IX_INVFILHDR);
    }
  }

  /* Create the rab */

  rab -> maxcache = buffc;			/* save buffer count */
  rab -> rdonly   = rdonly;

  /* Read the whole fhd */

  fhd = NULL;
  if (fhdbuf.ver < 3) {
    khdsiz = (Rbf *) ((Khdv1 *)(fhd -> khd) + fhdbuf.nky) /* size of khd array */
           - (Rbf *) (fhd -> khd);
    fhdrsz = (Rbf *) ((Khdv1 *)(fhd -> khd) + fhdbuf.nky) - (Rbf *) fhd; /* array + header */
  } else {
    khdsiz = (Rbf *) (fhd -> khd + fhdbuf.nky) 	/* size of khd array */
           - (Rbf *) (fhd -> khd);
    fhdrsz = (Rbf *) (fhd -> khd + fhdbuf.nky) - (Rbf *) fhd; /* array + header */
  }
  fhdbsz = round_up (khdsiz, fhdbuf.bls);	/* round up to block size */
  fhd = ix_calloc (fhdbsz);			/* allocate memory & clear it */

  rab -> fhd = fhd;				/* save fhd pointer */
  rab -> fhdrsz = fhdrsz;			/* save actual size */
  rab -> fhdbsz = fhdbsz;			/* save rounded-up size */

  sts = ix_readfhd (rab);			/* read the whole fhd */
  if (sts != IX_SUCCESS) {
    ix_os_opefilerr (rab);
    ix_free (rab);
    ix_memen (0);
    return (sts);
  }

  /* For Unix, put it on the open files list, possibly closing another file */

  ix_os_opefilsuc (rab);

  /* If < v3, convert khd array to v3 */

  if (fhdbuf.ver < 3) {
    khdsiz = (Rbf *) (fhd -> khd + fhdbuf.nky) /* size of khd array */
           - (Rbf *) (fhd -> khd);
    fhdrsz = (Rbf *) (fhd -> khd + fhdbuf.nky) - (Rbf *) fhd; /* array + header */
    fhdbsz = round_up (khdsiz, fhdbuf.bls);	/* round up to block size */
    fhdv3  = ix_calloc (fhdbsz);		/* allocate memory & clear it */
    memcpy (fhdv3, fhd, (Rbf *)(fhd -> khd) - (Rbf *)fhd);
    for (krf = 0; krf < fhdbuf.nky; krf ++) {	/* copy and convert keys */
      fhdv3 -> khd[krf].vbn = ((Khdv1 *)(fhd -> khd))[krf].vbn;
      fhdv3 -> khd[krf].ksz = ((Khdv1 *)(fhd -> khd))[krf].ksz;
      fhdv3 -> khd[krf].kof = ((Khdv1 *)(fhd -> khd))[krf].kof;
    }
    ix_free (fhd);				/* get rid of old header */
    rab -> fhd = fhdv3;				/* save new fhd pointer */
						/* leave old fhdbsz as it was */
						/* same with fhdrsz */
						/* - this assumes rdonly is set */
  }

  /* Return address of rab */

  *rabv = rab;					/* success, return rab adrs */
  return (ix_flush (rab, IX_SUCCESS, 1));	/* return success status */
}
