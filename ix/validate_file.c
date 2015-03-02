#include "ixinternal.h"

#include <stdio.h>

static void validate_bkt (Rab *rab, Rsz krf, Vbn par, Vbn vbn, Crp *pcrp, 
                          Rsz ksz1, Rbf *kbf1, Rfa *rfa1, 
                          Rsz ksz2, Rbf *kbf2, Rfa *rfa2, 
                          uLong *status, Vbn *count, Vbn *mdepth, Vbn *xdepth);

/************************************************************************/
/*									*/
/*  This routine validates a file					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	count = address of array to return record counts in		*/
/*	        one element per defined key				*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_validate_file = status					*/
/*									*/
/************************************************************************/

uLong ix_validate_file (void *rabv, Vbn *count, Vbn *depth)

{
  uLong sts, sts2;
  Rab *rab;
  Rsz kend0, kend1, krf;
  Vbn counter, mdepth;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  ix_memen (1);

  /* Lock file for reading, unlock crp, if any */

  sts = ix_lockfile (rab, 0);
  if (sts != IX_SUCCESS) {
    ix_memen (0);
    return (sts);
  }

  /* Validate the free bucket list */

  ix_messlog (rab, "validating free list");
  sts = ix_validkey (rab, 0xffff, &counter, &mdepth);

  /* Validate each key of reference */

  for (krf = 0; krf < rab -> fhd -> nky; krf ++) {
    ix_messlog (rab, "validating key %d", krf);
    sts2 = ix_validkey (rab, krf, &counter, &mdepth);
    if (sts2 != IX_SUCCESS) sts = sts2;
    if (count != NULL) count[krf] = counter;
    if (depth != NULL) depth[krf] = mdepth;
  }

  /* Compare record counts of each key */

  if (count != NULL) for (krf = 1; krf < rab -> fhd -> nky; krf ++) {
    kend0 = rab -> fhd -> khd[krf-1].kof + rab -> fhd -> khd[krf-1].ksz;
    kend1 = rab -> fhd -> khd[krf].kof   + rab -> fhd -> khd[krf].ksz;
    if (((kend0 <= kend1) && (count[krf-1] < count[krf])) 
     || ((kend0 >= kend1) && (count[krf-1] > count[krf]))) {
      ix_errorlog (rab, "key %u has %u records, but key %u has %u records", 
                                        krf - 1, count[krf-1], krf, count[krf]);
      sts = IX_INVBKTPNTR;
    }
  }

  return (ix_flush (rab, sts, sts == IX_SUCCESS));
}

/************************************************************************/
/*									*/
/*  Validate a key tree							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	krf = key of reference (-1 = check free list)			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_validate = IX_SUCCESS : everything is ok			*/
/*	                    else : error in tree			*/
/*	*counter = number of records in tree				*/
/*	*mdepth  = maximum depth of tree				*/
/*									*/
/************************************************************************/

uLong ix_validkey (Rab *rab, Rsz krf, Vbn *counter, Vbn *mdepth)

{
  Bkt *bkt;
  Crp pcrp;
  uLong sts;
  Vbn vbn, xdepth;

  sts = IX_SUCCESS;

  *counter = 0;					/* reset record counter */
  *mdepth  = 0; 				/* reset max depth counter */
  xdepth   = 0;					/* reset current depth counter */

  /* Key -1 means check out free list */

  if (krf == (Rsz) -1) {
    for (vbn = rab -> fhd -> free; vbn != 0;) {
      sts = ix_readbkt (rab, NULL, vbn, &bkt);	/* read free bucket */
      if (sts != IX_SUCCESS) {
        ix_errorlog (rab, "error %u reading free bucket %u", sts, vbn);
        break;
      }
      if (bkt -> vbn != vbn) {
        ix_errorlog (rab, "free bucket %u has bad vbn %u", vbn, bkt -> vbn);
        sts = IX_INVBKTPNTR;
      }
      if (bkt -> rec[0].size != 0) {
        ix_errorlog (rab, "free bucket %u has non-zero size %u", 
                                                       vbn, bkt -> rec[0].size);
        sts = IX_INVBKTFMT;
      }
      if (bkt -> rec[0].offset != rab -> fhd -> bks) {
        ix_errorlog (rab, "free bucket %u has offset %u", 
                                                     vbn, bkt -> rec[0].offset);
        sts = IX_INVBKTFMT;
      }
      if (bkt -> rec[0].keysize != 0) {
        ix_errorlog (rab, "free bucket %u has non-zero keysize %u", 
                                                    vbn, bkt -> rec[0].keysize);
        sts = IX_INVBKTFMT;
      }
      vbn = bkt -> rec[0].left;
      ix_relbkt (rab, NULL, bkt);
    }
  }

  /* Otherwise, check out that specific key */

  else {
    vbn = rab -> fhd -> khd[krf].vbn;		/* get root bucket vbn */
    validate_bkt (rab, krf, 0, vbn, &pcrp, 	/* validate it */
                  0, NULL, NULL, 0, NULL, NULL, 
                  &sts, counter, mdepth, &xdepth);
  }

  return (sts);
}

/************************************************************************/
/*									*/
/*  This routine validates a bucket and its children			*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab    = rab pointer						*/
/*	krf    = key of reference					*/
/*	par    = parent vbn of bucket to be validated			*/
/*	vbn    = vbn of bucket to be validated				*/
/*	pcrp   = address of temp crp to use				*/
/*	ksz1   = lower limit key size					*/
/*	kbf1   = lower limit key address (or NULL if no lower limit)	*/
/*	rfa1   = rfa of lower limit key					*/
/*	ksz2   = upper limit key size					*/
/*	kbf2   = upper limit key address (or NULL if no upper limit)	*/
/*	rfa2   = rfa of upper limit key					*/
/*	status = address of status variable				*/
/*	count  = address of record count variable			*/
/*	mdepth = address of maximum depth variable			*/
/*	xdepth = address of current depth variable			*/
/*									*/
/************************************************************************/

static void validate_bkt (Rab *rab, Rsz krf, Vbn par, Vbn vbn, Crp *pcrp, 
                          Rsz ksz1, Rbf *kbf1, Rfa *rfa1, 
                          Rsz ksz2, Rbf *kbf2, Rfa *rfa2, 
                          uLong *status, Vbn *count, Vbn *mdepth, Vbn *xdepth)

{
  Bkt *bkt;
  Khd *khd;
  uLong sts;
  Rbf *bbkt;
  Rec *reci;
  Rsz i, kof, n;

  khd = rab -> fhd -> khd + krf;		/* point to key header */
  kof = khd -> kof;				/* normal for primary key */
  if (krf != 0) kof = 0;			/* zero for alternate key */

  if (vbn != 0) {

    /* Increment depth counter */

    (*xdepth) ++;
    if (*xdepth > *mdepth) *mdepth = *xdepth;

    /* Read the bucket.  Note that readbkt checks the integrity of the bucket */
    /* as a unit (ie, keys in order, pointers in range, checksum ok, etc).    */

    bkt = rab -> crp.bkt;
    if ((bkt == NULL) || (bkt -> vbn != vbn)) {
      sts = ix_readbkt (rab, khd, vbn, &bkt);
      if (sts != IX_SUCCESS) {
        *status = sts;
        return;
      }
    }
    bbkt = (Rbf *) bkt;

    /* See how many records are in this bucket */

    for (n = 0; bkt -> rec[n].size != 0; n ++) {}
    if (n == 0) ix_errorlog (rab, "bucket %u contains no records", vbn);
    else {
      *count += n;

      /* My first record's key must be .gt. ksz1/kbf1 */

      if (kbf1 != NULL) {
        if (ix_compare_keys (bkt -> rec[0].keysize, 
                             bbkt + bkt -> rec[0].offset + kof, 
                             &(bkt -> rec[0].rfa), 
                             ksz1, kbf1, rfa1, khd -> kat) <= 0) {
          ix_errorlog (rab, "bucket %u first key is lower than parent %u", 
                                                                      vbn, par);
          *status = IX_INVBKTPNTR;
        }
      }

      /* My last record's key must be .lt. ksz2/kbf2 */

      if (kbf2 != NULL) {
        if (ix_compare_keys (bkt -> rec[n-1].keysize, 
                             bbkt + bkt -> rec[n-1].offset + kof, 
                             &(bkt -> rec[n-1].rfa), 
                             ksz2, kbf2, rfa2, khd -> kat) >= 0) {
          ix_errorlog (rab, "bucket %u last key is higher than parent %u", 
                                                                      vbn, par);
          *status = IX_INVBKTPNTR;
        }
      }

      /* If this is an alternate key, make sure  */
      /* all references to primary keys are good */

      if (krf != 0) for (i = 0; i < n; i ++) {
        sts = ix_alttoprikey (rab, khd, rab -> fhd -> khd, bkt, i, pcrp);
        if (sts == IX_SUCCESS) ix_relcrp (rab, pcrp);
        else *status = sts;
      }
    }

    /* Validate all children */

    for (i = 0; i < n; i ++) {
      reci = bkt -> rec + i;
      validate_bkt (rab, krf, vbn, reci -> left, pcrp, 
                    ksz1, kbf1, rfa1, 
                    reci -> keysize, 
                    bbkt + reci -> offset + kof, 
                    &(reci -> rfa), 
                    status, count, mdepth, xdepth);
      ksz1 = reci -> keysize;
      kbf1 = bbkt + reci -> offset + kof;
      rfa1 = &(reci -> rfa);
    }

    validate_bkt (rab, krf, vbn, bkt -> rec[n].left, pcrp, 
                  ksz1, kbf1, rfa1, ksz2, kbf2, rfa2, 
                  status, count, mdepth, xdepth);

    /* Release the bucket */

    if (bkt != rab -> crp.bkt) ix_relbkt (rab, khd, bkt);

    /* Decrement depth counter */

    (*xdepth) --;
  }
}
