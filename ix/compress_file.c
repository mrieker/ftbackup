/************************************************************************/
/*									*/
/*  This routine compresses unused space from a file			*/
/*									*/
/*  It does this by creating a new output file then neatly copying the 	*/
/*  records from the old input file to the new output file, filling 	*/
/*  the buckets to the requested fill percentage.			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <stdio.h>
#include <stdlib.h>

#ifdef WIN32
#include <search.h>
#include <string.h>
#endif

/************************************************************************/
/*									*/
/*  Compress file							*/
/*									*/
/*    Input:								*/
/*									*/
/*	ifspec  = filespec of input file				*/
/*	ofspec  = filespec of output file				*/
/*	newbks  = new bucket size (0 for same as input)			*/
/*	fillpct = fill percentage of output buckets			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_compress_file = status					*/
/*									*/
/************************************************************************/

uLong ix_compress_file (const Byte *ifspec, 
                        const Byte *ofspec, 
                        Rsz newbks, 
                        int fillpct)

{
  Bkt *ibkt, *nbkt, *obkt;
  int iidx, level, levels, nflush, oidx;
  Kat *kat;
  Khd *okhd;
  uLong occupied, sts;
  Rab *irab, *orab;
  Rfa irfa;
  Rsz irsz, *kof, krf, *ksz, nky, obks;
  Vbn nbkts, nrecs, ovbn, parvbn[max_depth];

  /* Open input file */

  sts = ix_open_file (ifspec, 1, 10, (void **)&irab);
  if (sts != IX_SUCCESS) return (sts);

  /* Create output file with same characteristics */

  nky = irab -> fhd -> nky;

  ksz = ix_malloc (nky * sizeof *ksz);
  kof = ix_malloc (nky * sizeof *kof);
  kat = ix_malloc (nky * sizeof *kat);

  for (krf = 0; krf < nky; krf ++) {
    ksz[krf] = irab -> fhd -> khd[krf].ksz;
    kof[krf] = irab -> fhd -> khd[krf].kof;
    kat[krf] = irab -> fhd -> khd[krf].kat;
  }

  obks = newbks;
  if (obks == 0) obks = irab -> fhd -> bks;
  sts = ix_create_file3 (ofspec, 
                         nky, 
                         ksz, 
                         kof, 
                         kat, 
                         obks, 
                         irab -> fhd -> mrs, 
                         10, 
                         (void **)&orab, 
                         IX_SHARE_N, 
                         0, 
                         NULL);

  ix_free (ksz);
  ix_free (kof);
  ix_free (kat);

  if (sts != IX_SUCCESS) {
    ix_close_file (irab);
    return (sts);
  }

  obks = orab -> fhd -> bks;

  ix_messlog (orab, "bucket size %u, block size %u", obks, orab -> fhd -> bls);

  /* Copy buckets neatly for each key of reference */

  for (krf = 0; krf < nky; krf ++) {

    okhd = orab -> fhd -> khd + krf;		/* point to key header */
    levels = 0;					/* doesn't have any levels */
    level  = 0;
    nflush = 0;
    obkt   = NULL;
    nbkts  = 0;
    nrecs  = 0;

    sts = ix_rewind (irab, krf);		/* rewind input file */
    if (sts != IX_SUCCESS) {
      ix_errorlog (irab, "error rewinding key %u - %s", krf, ix_errlist (sts));
      break;
    }

    /* Allocate a root bucket for the key of reference */

    sts = ix_allocbkt (orab, okhd, &obkt);	/* allocate a bucket */
    if (sts != IX_SUCCESS) {
      ix_errorlog (orab, "error allocating bucket key %u - %s", 
                                                         krf, ix_errlist (sts));
      break;
    }
    nbkts ++;					/* one more bucket in use */
    okhd -> vbn = obkt -> vbn;			/* set up pointer in header */
    sts = ix_writefhd (orab);			/* write header */
    if (sts != IX_SUCCESS) {
      ix_errorlog (orab, "error allocating file header - %s", ix_errlist (sts));
      break;
    }
    oidx = 0;					/* start at beg of bucket */

    while ((sts = ix_search_seq (irab, 1, 0, 	/* step input file forward */
                                 NULL, NULL, 0, NULL)) == IX_SUCCESS) {

      ibkt = irab -> crp.bkt;			/* point to the bucket */
      iidx = irab -> crp.idx;			/* point to record in bucket */
      irsz = (ibkt -> rec[iidx].size + 3) & -4;	/* get compr & rounded size */
      irfa = ibkt -> rec[iidx].rfa;		/* get input record's rfa */

      /* If we're in an upper level bucket and there is         */
      /* something in this bucket, but the left-of-end pointer  */
      /* is null, allocate a lower level bucket and write to it */

check_level:
      if ((level != levels) && (oidx != 0) && (obkt -> rec[oidx].left == 0)) {
        sts = ix_allocbkt (orab, okhd, &nbkt);	/* allocate new child bucket */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error allocating bucket key %u - %s", 
                                                         krf, ix_errlist (sts));
          break;
        }
        nbkts ++;				/* one more bucket in use */
        obkt -> rec[oidx].left = nbkt -> vbn;	/* make it left of end of parent */
        parvbn[level++] = obkt -> vbn;		/* save parent bucket vbn */
        sts = ix_writebkt (orab, okhd, obkt);	/* write parent bucket */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error writing bucket %u - %s", 
                                             parvbn[--level], ix_errlist (sts));
          break;
        }
        nflush ++;
        obkt = nbkt;				/* point to new bucket */
        oidx = 0;				/* it is empty */
      }

      /* If there isn't room in current bucket, */
      /* maybe put record in parent bucket      */

      occupied  = (Rbf *) (obkt -> rec + oidx + 2) - (Rbf *) obkt;
      occupied += ((uLong) obks) - obkt -> rec[oidx].offset + irsz;
      if ((oidx != 0) && (occupied * 100 > ((uLong) obks) * fillpct)) {

        /* No room, write current bucket out to file */

        ovbn = obkt -> vbn;			/* save current vbn */
        sts = ix_writebkt (orab, okhd, obkt);	/* write bucket out */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error writing bucket %u - %s", 
                                                        ovbn, ix_errlist (sts));
          break;
        }
        nflush ++;

        /* If it wasn't the root bucket, go check out the parent */

        if (level != 0) {			/* see if it was root bucket */
          sts = ix_readbkt (orab, okhd, 	/* if not, read parent bucket */
                            parvbn[--level], &obkt);
          if (sts != IX_SUCCESS) {
            ix_errorlog (orab, "error reading bucket %u - %s", 
                                               parvbn[level], ix_errlist (sts));
            break;
          }
          for (oidx = 0; obkt -> rec[oidx].size != 0; oidx ++) {}
          if (obkt -> rec[oidx].left != ovbn) {	/* validate child pointer */
            ix_errorlog (orab, "bucket %u says %u.%u is its parent", 
                                                     ovbn, parvbn[level], oidx);
            sts = IX_INVBKTPNTR;
            break;
          }
          goto check_level;			/* go check it out */
        }

        /* It was the root bucket, go allocate a new root bucket above it */

        sts = ix_allocbkt (orab, okhd, &obkt);	/* allocate new root bucket */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error allocating bucket for key %u - %s", 
                                                         krf, ix_errlist (sts));
          break;
        }
        nbkts ++;				/* one more bucket in use */
        okhd -> vbn = obkt -> vbn;		/* set up new root vbn */
        sts = ix_writefhd (orab);
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error allocating file header - %s", 
                                                              ix_errlist (sts));
          break;
        }
        obkt -> rec[0].left = ovbn;		/* set up left pointer */
        oidx = 0;				/* bucket is empty */
        levels ++;				/* one more total level */
      }

      /* There is plenty of room for record in bucket */

      /* If alternate key, translate rfa to new value                   */
      /* Otherwise, if more than one key, save rfa translation in table */

      if ((orab -> fhd -> rfa.vbn < irfa.vbn) || 
          ((orab -> fhd -> rfa.vbn == irfa.vbn) && (orab -> fhd -> rfa.seq < irfa.seq))) {
        orab -> fhd -> rfa = irfa;
        while (++ (orab -> fhd -> rfa.seq) == 0) ++ (orab -> fhd -> rfa.vbn);
        sts = ix_writefhd (orab);
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error allocating new rfa - %s", ix_errlist (sts));
          break;
        }
      }

      /* Now copy record to bucket */

      obkt -> rec[oidx+1] = obkt -> rec[oidx];	/* move end marker up one */

      obkt -> rec[oidx].size    = ibkt -> rec[iidx].size; /* copy recsize */
      obkt -> rec[oidx].offset -= irsz;		/* decrement offset to data */
      obkt -> rec[oidx].keysize = ibkt -> rec[iidx].keysize; /* copy keysize */

      obkt -> rec[oidx].rfa = irfa;		/* same RFA */

      memcpy (((Rbf *) obkt) + obkt -> rec[oidx].offset, /* copy input data */
              ((Rbf *) ibkt) + ibkt -> rec[iidx].offset, irsz);

      oidx ++;					/* increment record index */
      obkt -> rec[oidx].offset -= irsz;		/* decrement end offset */
      obkt -> rec[oidx].left    = 0;		/* it has no left pointer */

      nrecs ++;					/* one more record written */

      /* Flush file writes if enough waiting */

      if (nflush > 9) {
        ovbn = obkt -> vbn;			/* save current bucket's vbn */
        sts = ix_writebkt (orab, okhd, obkt);	/* write current vbn out */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error writing bucket %u - %s", 
                                                        ovbn, ix_errlist (sts));
          break;
        }
        sts = ix_flush (orab, sts, 1);		/* flush write buffers */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error flushing writes - %s", ix_errlist (sts));
          break;
        }
        sts = ix_readbkt (orab, okhd, ovbn, &obkt); /* read current bucket */
        if (sts != IX_SUCCESS) {
          ix_errorlog (orab, "error reading bucket %u - %s", 
                                                        ovbn, ix_errlist (sts));
          break;
        }
        nflush = 0;				/* reset flush counter */
      }
    }

    /* Check for expected termination on input file for this key of reference */

    if (sts == IX_RECNOTFOUND) sts = IX_SUCCESS;
    else {
      ix_errorlog (irab, "error reading key %u - %s", krf, ix_errlist (sts));
      break;
    }

    /* Write final bucket out for the key of reference */

    ovbn = obkt -> vbn;
    sts = ix_writebkt (orab, okhd, obkt);
    if (sts != IX_SUCCESS) {
      ix_errorlog (orab, "error writing bucket %u - %s", 
                                                        ovbn, ix_errlist (sts));
      break;
    }
    sts = ix_flush (orab, sts, 1);
    if (sts != IX_SUCCESS) {
      ix_errorlog (orab, "error flushing writes - %s", ix_errlist (sts));
      break;
    }

    ix_messlog (irab, "key %u has %u buckets, %u records, %u levels", 
                                                  krf, nbkts, nrecs, ++ levels);
  }

  /* Close files and return */

  ix_close_file (irab);
  ix_close_file (orab);
  return (sts);
}
