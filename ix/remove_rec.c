/************************************************************************/
/*									*/
/*  This routine removes a record from the file				*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <string.h>

static void zap_it_out (Bkt *bkt, Idx idx);

/************************************************************************/
/*									*/
/*  Remove record from file						*/
/*									*/
/*  Input:								*/
/*									*/
/*	rabv = address of record access block				*/
/*	*rab sequential search context = record to be removed		*/
/*									*/
/*  Output:								*/
/*									*/
/*	remove_rec = IX_SUCCESS : record removed			*/
/*	             IX_NOCURREC : no current record context		*/
/*	             else : error					*/
/*	*rab sequential search context = points to next record in 	*/
/*	                                 bucket				*/
/*									*/
/************************************************************************/

uLong ix_remove_rec (void *rabv)

{
  Bkt *pbkt;
  Idx pidx;
  Khd *akhd, *pkhd;
  uLong sts;
  Rab *rab;
  Rsz krf;
  Crp acrp, pcrp, *crpp;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  /* Make sure there is a current record selected */

  if ((rab -> crp.bkt == NULL) || (rab -> nosqf)) return (IX_NOCURREC);

  /* Make sure record is write accessed */

  if (!(rab -> lockw)) return (IX_READONLY);

  /* Lock file for write, preserve crp */

  ix_memen (1);
  sts = ix_lockfile (rab, 3);
  if (sts != IX_SUCCESS) {
    ix_memen (0);
    return (sts);
  }

  /* Get crp stuff */

  pbkt = rab -> crp.bkt;
  pidx = rab -> crp.idx;
  pkhd = rab -> fhd -> khd;

  /* If it's an alternate key, get the primary bucket */

  if (rab -> crp.krf != 0) {
    sts = ix_alttoprikey (rab, 			/* convert alt to pri key */
                   rab -> fhd -> khd + rab -> crp.krf, pkhd, pbkt, pidx, &pcrp);
    if (sts != IX_SUCCESS) goto ret;
    pbkt = pcrp.bkt;				/* get primary key bucket */
    pidx = pcrp.idx;				/* ... and index */
  }

  /* pbkt = primary bucket        */
  /* pidx = primary index         */
  /* pkhd = primary key header    */ 
  /* primary key locked for write */

  /* if rab -> crp.krf != 0,               */
  /*   rab -> crp = alt key search context */
  /*   pcrp = pri key search context       */
  /* else,                                 */
  /*   rab -> crp = pri key search context */

  /* Remove record from each alternate key, one at a time */

  for (krf = 1; krf < rab -> fhd -> nky; krf ++) {
    akhd = rab -> fhd -> khd + krf;		/* point to alt key header */
    if (krf == rab -> crp.krf) {		/* see if doing one in rab */
      sts = ix_remove_from_bkt (rab, 		/* if so, just remove it */
                                akhd, &(rab -> crp));
    } else {
      acrp.krf = krf;				/* if not, find it */
      sts = ix_pritoaltkey (rab, akhd, pbkt, pidx, &acrp);
      if (sts == IX_RECNOTFOUND) sts = IX_SUCCESS;
      else if (sts == IX_SUCCESS) {		/* found, remove from bucket */
        sts = ix_remove_from_bkt (rab, akhd, &acrp);
        ix_relcrp (rab, &acrp);
      }
    }
    if (sts != IX_SUCCESS) break;		/* stop if error */
  }

  /* Remove from primary bucket then release pri record pointer */
  /* if error or if rab points to an alternate key bucket       */

  if (rab -> crp.krf != 0) crpp = &pcrp;	/* get pri crp pointer */
  else crpp = &(rab -> crp);
  if (sts == IX_SUCCESS) sts = ix_remove_from_bkt (rab, pkhd, crpp);
  if ((sts != IX_SUCCESS) || (rab -> crp.krf != 0)) ix_relcrp (rab, crpp);

  /* Flush cache, only write disk if completely successful */

ret:
  return (ix_flush (rab, sts, sts == IX_SUCCESS));
}

/************************************************************************/
/*									*/
/*  Remove record from bucket						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	khd = key's header address					*/
/*	crp = points to record to be removed				*/
/*									*/
/*	key header locked for write					*/
/*									*/
/*    Output:								*/
/*									*/
/*	remove_from_bkt = IX_SUCCESS : successfully removed		*/
/*	                  else : error status				*/
/*	*crp = next record in bucket					*/
/*	if rab -> krf eq khd's krf					*/
/*		rab -> nosqf = 1 : crp now points to previous record	*/
/*		               2 : crp now points to next record	*/
/*		               3 : crp now points to next record in 	*/
/*		                   same bucket, but .left will have 	*/
/*		                   to be scanned			*/
/*									*/
/************************************************************************/

uLong ix_remove_from_bkt (Rab *rab, Khd *khd, Crp *crp)

{
  Bkt *bkt, *bkt2, *bkt3;
  Idx idx, idx2, idx3;
  int nosqf;
  uLong sts;
  Rec *rec2;
  Vbn vbn, vbn2, vbn3, vbn4;

  idx3 = 0;

  bkt = crp -> bkt;				/* point to bucket */
  idx = crp -> idx;				/* get index in bucket */

  /* See if there are both left and right children */

check_links:
  if ((bkt -> rec[idx].left != 0) && (bkt -> rec[idx+1].left != 0)) {

    /* There are both, so find rightmost record of left children */

    vbn2 = bkt -> rec[idx].left;		/* point to left child */
    vbn3 = 0;					/* parent is original bucket */
    while (1) {
      sts = ix_readbkt (rab, khd, vbn2, &bkt2);	/* read it in */
      if (sts != IX_SUCCESS) {
        ix_relcrp (rab, crp);
        return (sts);
      }
      for (idx2 = 0; bkt2 -> rec[idx2].size != 0; idx2 ++) {} /* find end */
      if (bkt2 -> rec[idx2].left == 0) break;	/* see if anything to right */
      vbn3 = vbn2;				/* if so, save parent vbn */
      idx3 = idx2;				/* ... and index */
      vbn2 = bkt2 -> rec[idx2].left;		/* check it out */
      ix_relbkt (rab, khd, bkt2);
    }

    /* If that chase lead to an empty or near empty */
    /* bucket at the bottom, clear the links to it  */

    if (idx2 <= 1) {				/* check for (near) empty */
      if (vbn3 == 0) bkt -> rec[idx].left = 	/* if direct child of bkt */
                           bkt2 -> rec[0].left;	/* ... just unlink it */
      else {
        sts = ix_readbkt (rab, khd, vbn3, &bkt3); /* if not, re-read parent */
        if (sts != IX_SUCCESS) {
          ix_relcrp (rab, crp);
          ix_relbkt (rab, khd, bkt2);
          return (sts);
        }
        bkt3 -> rec[idx3].left = bkt2 -> rec[0].left; /* unlink empty bucket */
        sts = ix_writebkt (rab, khd, bkt3);	/* write parent back to disk */
        if (sts != IX_SUCCESS) {
          ix_relcrp (rab, crp);
          ix_relbkt (rab, khd, bkt2);
          return (sts);
        }
      }

      /* If bucket is already empty, free it off and start scan all over */

      if (idx2 == 0) {
        ix_freebkt (rab, khd, bkt2);		/* free the empty bucket */
        goto check_links;			/* check from top again */
      }
    }

    /* Take the last record from the bottom bucket    */
    /* and put it in place of the one we are removing */

    vbn4 = bkt -> rec[idx+1].left;		/* save right link of one being removed */
    bkt -> rec[idx+1].left = 0;			/* set it to zero for now */
    zap_it_out (bkt, idx);			/* remove entry */

    rec2 = bkt2 -> rec + -- idx2;		/* insert bottom rec in bkt */
    sts = ix_insert_into_bkt (rab, khd, rec2 -> size, 
                              ((Rbf *) bkt2) + rec2 -> offset, 
                              0, NULL, rec2 -> keysize, 
                              &(rec2 -> rfa), vbn4, crp);
    if (sts != IX_SUCCESS) {
      ix_relbkt (rab, khd, bkt2);
      return (sts);
    }

    /* If bottom bucket has other stuff in it, remove */
    /* last entry and write it back out to disk       */

    if (idx2 > 0) {
      zap_it_out (bkt2, idx2);			/* remove entry from bkt2 */
      sts = ix_writebkt (rab, khd, bkt2);	/* write back to disk */
    }

    /* That bottom record only had the one entry, free it off */

    else ix_freebkt (rab, khd, bkt2);

    /* The same spot in the bucket now holds the previous record in */
    /* the file.  So if this is the rab -> krf key, set nosqf = 1.  */

    nosqf = 1;

  } else {

    /* There aren't both left and right children, it is easy to do.      */
    /* Just zap it out of the bucket.  Put its left link into its right. */

    zap_it_out (bkt, idx);

    /* If the bucket is empty, free it up for re-use later. */

    vbn = bkt -> vbn;				/* get bucket's vbn */
    if (bkt -> rec[0].size == 0) {		/* see if bucket is empty */
      if (crp -> depth == 0) {			/* ok, see if root bucket */
        if (khd -> vbn != vbn) {
          ix_errorlog (rab, "bucket %u says it is root but %u is", 
                                                               vbn, khd -> vbn);
          ix_relcrp (rab, crp);
          return (IX_INVBKTPNTR);
        }
        khd -> vbn = bkt -> rec[0].left;	/* root, make left new root */
        sts = ix_writefhd (rab);		/* write file header out */
        ix_freebkt (rab, khd, bkt);		/* free off this bucket */
        ix_relcrpnv (rab, crp);			/* we're at eof now */
      } else {
        vbn3 = crp -> parvbn[crp->depth-1];	/* else, read parent bucket */
        idx3 = crp -> paridx[crp->depth-1];
        sts = ix_readbkt (rab, khd, vbn3, &bkt3);
        if (sts != IX_SUCCESS) {
          ix_relcrp (rab, crp);
          return (sts);
        }
        if (bkt3 -> rec[idx3].left != vbn) {
          ix_errorlog (rab, "bucket %u says %u[%u] is parent", vbn, vbn3, idx3);
          ix_relcrp (rab, crp);
          return (IX_INVBKTPNTR);
        }
        bkt3 -> rec[idx3].left = bkt -> rec[0].left; /* unlink this one */
        sts = ix_writebktnr (rab, khd, bkt3);	/* write parent bucket out */
        crp -> bkt = bkt3;			/* parent is now current */
        crp -> idx = idx3;
        crp -> depth --;			/* not as deep in tree now */
        ix_freebkt (rab, khd, bkt);		/* free off empty bucket */
      }
      if (sts != IX_SUCCESS) {
        ix_relcrp (rab, crp);
        return (sts);
      }
      nosqf = 3;				/* pointing to next rec in */
						/* bucket, but it may have */
						/* children */
    }

    /* Bucket still has stuff in it, just write it back out.  Crp */
    /* points to next record in bucket, but to find the very next */
    /* sequential record, its left link will have to be examined. */

    else {
      sts = ix_writebktnr (rab, khd, bkt);	/* bucket not empty, write it */
      nosqf = 3;				/* pointing to next in bkt */
    }
  }

  /* If rab has this key of reference, set its nosqf flag */

  if ((sts == IX_SUCCESS) 
   && (rab -> crp.krf == khd - rab -> fhd -> khd)) rab -> nosqf = nosqf;

  return (sts);					/* all done */
}

/************************************************************************/
/*									*/
/*  This routine removes an entry from the bucket.  It assumes that at 	*/
/*  least one of the left and right links are zero.			*/
/*									*/
/*    Input:								*/
/*									*/
/*	bkt = pointer to bucket						*/
/*	idx = index in bucket of record to be removed			*/
/*									*/
/*    Output:								*/
/*									*/
/*	*bkt = updated							*/
/*									*/
/************************************************************************/

static void zap_it_out (Bkt *bkt, Idx idx)

{
  Idx n;
  Rsz lowest_offset, record_offset, record_size;

  record_offset = bkt -> rec[idx].offset;	/* record being removed */
  record_size = (bkt -> rec[idx].size + 3) & -4;

  n = idx + 1;					/* fix and count descriptors */
  do bkt -> rec[n].offset += record_size;
  while (bkt -> rec[n++].size != 0);

  lowest_offset = bkt -> rec[--n].offset 	/* get last rec offset */
                - record_size;			/* ... before the move */

  bkt -> rec[idx+1].left |= bkt -> rec[idx].left; /* put links to right */

  memmove ((Rbf *) (bkt -> rec + idx), 	/* remove descriptor */
           (Rbf *) (bkt -> rec + idx + 1), 
           (Rbf *) (bkt -> rec + n) - (Rbf *) (bkt -> rec + idx));

  memmove (((Rbf *) bkt) + lowest_offset + record_size, /* remove data */
           ((Rbf *) bkt) + lowest_offset, 
           record_offset - lowest_offset);
}

/************************************************************************/
/*									*/
/*  This routine finds the alternate key corresponding to a given 	*/
/*  primary key								*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	akhd = alternate key header					*/
/*	pbkt = primary bucket						*/
/*	pidx = primary index						*/
/*	acrp = where to return pointer to alternate record		*/
/*									*/
/*	alternate key header locked for read				*/
/*									*/
/*    Output:								*/
/*									*/
/*	pritoaltkey = IX_SUCCESS : alternate record found		*/
/*	              IX_RECNOTFOUND : alternate record not found	*/
/*	              else : error status				*/
/*	acrp -> bkt = alternate key bucket				*/
/*	        idx = index in acrp -> bkt of record			*/
/*									*/
/*	alternate key header still locked for read			*/
/*									*/
/************************************************************************/

uLong ix_pritoaltkey (Rab *rab, Khd *akhd, Bkt *pbkt, Idx pidx, Crp *acrp)

{
  int eql, pmalloc;
  uLong sts;
  Khd *pkhd;
  Rbf *pbas, *prbf;
  Rec *prec;
  Rsz akof, aksz, pkeysize, pkof, pksz, prsz;

  prsz = 0;

  /* Clear out return record pointer */

  acrp -> bkt = NULL;
  acrp -> depth = 0;

  /* Get alternate key's value from primary bucket */

  aksz = akhd -> ksz;				/* get key size */
  akof = akhd -> kof;				/* get key offset in record */
  pkhd = rab -> fhd -> khd;			/* get primary key hdr adrs */
  pksz = pkhd -> ksz;				/* get pri key size */
  pkof = pkhd -> kof;				/* get pri key offset */
  prec = pbkt -> rec + pidx;			/* point to primary rec desc */
  pbas = ((Rbf *) pbkt) + prec -> offset;	/* base of primary record */
  pkeysize = prec -> keysize;			/* compressed pri key size */
  pmalloc = 0;					/* assume wont have to malloc */

  /* - If alternate key ends after end of     */
  /*   uncompressed record, it's not in index */

  if (akof + aksz > prec -> size - pkeysize + pksz) return (IX_RECNOTFOUND);

  /* - If alternate key comes completely after primary */
  /*   key, point to alternate key's value in bucket   */

  if (akof >= pkof + pksz) {
    prsz = aksz;
    prbf = pbas;	/* work around bug in HP compiler ... */
    prbf += pkeysize;
    prbf += akof;
    prbf -= pksz;	/* ... ignores pksz if this is all one line */
  }

  /* - If alternate key ends at or before end of primary key, same thing */
  /*   only shorten the alternate key if the primary key is compressed   */

  else if (aksz + akof <= pksz + pkof) {
    prsz = aksz;
    if (akof + aksz > pkof + pkeysize) prsz = pkof + pkeysize - akof;
    prbf = pbas + akof;
  }

  /* - Otherwise, the alternate key is split where the primary key may be */
  /*   compressed.  In this case, malloc a buffer and put alt key there.  */

  else {
    pmalloc = 1;
    prbf = (Rbf *) ix_malloc (aksz);
    if (akof >= pkof + pkeysize) memset (prbf, 0, pkof + pksz - akof);
    else {
      memcpy (prbf, pbas + akof, pkof + pkeysize - akof);
      memset (prbf + pkof + pkeysize - akof, 0, pksz - pkeysize);
    }
    memcpy (prbf + pkof + pksz - akof, pbas + pkof + pkeysize, 
            akof + aksz - pkof - pksz);
  }

  /* prsz = size of alternate key value in primary bucket    */
  /* prbf = address of alternate key value in primary bucket */

  /* Now search alternate index for the entry whose key  */
  /* matches prsz/prbf and whose rfa matches prec -> rfa */

  acrp -> depth = 0;				/* start at root */
  acrp -> bkt = NULL;
  sts = ix_search_kyf (rab, akhd, acrp, akhd -> vbn, /* search forward (.ge.) */
                       &(prec -> rfa), prsz, prbf, 0, 0, &eql);
  if ((sts == IX_SUCCESS) && !eql) {
    ix_errorlog (rab, "can't find alt key %u rfa %u.%u", 
                                 akhd - pkhd, prec -> rfa.vbn, prec -> rfa.seq);
    sts = IX_BADALTBKT;				/* they don't */
  }
  if (pmalloc) ix_free (prbf);			/* free temp buffer */
  if (sts != IX_SUCCESS) ix_relcrp (rab, acrp);	/* failed, release search ctx */
  return (sts);					/* return status */
}
