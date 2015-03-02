/************************************************************************/
/*									*/
/*  This routine modifies a record in the file				*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <string.h>

static uLong modify_in_bkt (Rab *rab, Khd *khd, Crp *crp, Rsz ksz, Rsz kof, 
                            Rsz rsz1, const Rbf *rbf1, Rsz rsz2, const Rbf *rbf2);

/************************************************************************/
/*									*/
/*  Modify record in file						*/
/*									*/
/*  Input:								*/
/*									*/
/*	rabv = address of record access block				*/
/*	*rab sequential search context = record to be modified		*/
/*	rsz = size of new record's contents				*/
/*	rbf = address of new record's contents				*/
/*									*/
/*  Output:								*/
/*									*/
/*	modify_rec = IX_SUCCESS : record modified			*/
/*	             IX_NOCURREC : no current record context		*/
/*	             else : error					*/
/*	*rab sequential search context = points to "new" record		*/
/*									*/
/************************************************************************/

uLong ix_modify_rec (void *rabv, Rsz rsz, const Rbf *rbf)

{
  Bkt *pbkt;
  Idx pidx;
  int pchg;
  Khd *akhd, *pkhd;
  uLong sts;
  Rab *rab;
  Rec *arec, *prec;
  Rsz acks, akof, aksz, krf, pcks, pkof, pksz;
  Crp acrp, pcrp, *crpp;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  /* Make sure new record is not too big */

  if (rsz > rab -> fhd -> mrs) return (IX_INVRECSIZE);

  /* Make sure there is a current record selected */

  if ((rab -> crp.bkt == NULL) || (rab -> nosqf)) return (IX_NOCURREC);

  /* Make sure record is write accessed */

  if (!(rab -> lockw)) return (IX_READONLY);

  /* Lock file for write, preserve crp */

  ix_memen (1);
  sts = ix_lockfile (rab, 3);
  if (sts != IX_SUCCESS) goto ret;

  /* Get crp stuff */

  pbkt = rab -> crp.bkt;
  pidx = rab -> crp.idx;
  pkhd = rab -> fhd -> khd;			/* get primary key header */

  /* If it's an alternate key, get the primary bucket */

  if (rab -> crp.krf != 0) {
    sts = ix_alttoprikey (rab, 			/* convert alt to pri key */
                  rab -> fhd -> khd + rab -> crp.krf, pkhd, pbkt, pidx, &pcrp); 
    if (sts != IX_SUCCESS) goto ret;
    pbkt = pcrp.bkt;				/* get primary key bucket */
    pidx = pcrp.idx;				/* ... and index */
  }

  /* Make sure new record is big enough to hold entire primary key, check */
  /* for primary key change and get new primary compressed key size.      */

  pksz = pkhd -> ksz;				/* get expanded pri key size */
  pkof = pkhd -> kof;				/* get pri key record offset */
  if (rsz < pkof + pksz) {
    sts = IX_INVRECSIZE;
    goto ret;
  }
  prec = pbkt -> rec + pidx;			/* point to pri rec descr */
  pchg = ix_compare_keys (prec -> keysize, 	/* compare keys */
                          ((Rbf *) pbkt) + prec -> offset + pkof, 
                          NULL, 
                          pksz, 
                          rbf + pkof, 
                          NULL, 
                          pkhd -> kat);
  for (pcks = pksz; pcks > 1; pcks --) if (rbf[pkof+pcks-1] != 0) break;

  /* pbkt = primary bucket        */
  /* pchg = nz : pri key change   */
  /* pcks = new pri compr key siz */
  /* pidx = primary index         */
  /* pkhd = primary key header    */
  /* prec = primary rec in pbkt   */
  /* primary key locked for write */

  /* if rab -> crp.krf != 0,               */
  /*   rab -> crp = alt key search context */
  /*   pcrp = pri key search context       */
  /* else,                                 */
  /*   rab -> crp = pri key search context */

  /* Modify record in each alternate key bucket, as need be, one at a time */

  for (krf = 1; krf < rab -> fhd -> nky; krf ++) {

    /* Get alternate key record search context */

    sts = IX_SUCCESS;
    akhd = rab -> fhd -> khd + krf;		/* point to alt key header */
    aksz = akhd -> ksz;				/* get alt key size */
    akof = akhd -> kof;				/* get alt key buffer offset */
    crpp = &(rab -> crp);			/* assume rab has key of ref */
    if (krf != rab -> crp.krf) {
      crpp = &acrp;				/* if not, lookup the alt key */
      acrp.krf = krf;
      sts = ix_pritoaltkey (rab, akhd, pbkt, pidx, &acrp);
    }

    if ((sts == IX_SUCCESS) || (sts == IX_RECNOTFOUND)) {

      /* If new record does not contain the key, remove it if it was there */

      if (rsz < akof + aksz) {			/* see if new rec has key */
        if (sts == IX_SUCCESS) sts = ix_remove_from_bkt (rab, akhd, crpp);
        else sts = IX_SUCCESS;
      }

      /* If new rec contains key, modify it if   */
      /* it was there, or insert it if it wasn't */

      else {

        /* First, see if key changed in the broadest sense, ie,    */
        /* either primary key value changed, or the record did not */
        /* used to contain this key, or the key value changed.     */

        arec = (sts == IX_SUCCESS) ? crpp -> bkt -> rec + crpp -> idx : NULL;
        if (pchg || (sts == IX_RECNOTFOUND) 	/* if so, see if change */
                 || ix_compare_keys (arec -> keysize, 
                                     ((Rbf *) crpp -> bkt) + arec -> offset, 
                                     NULL, 
                                     aksz, 
                                     rbf + akof, 
                                     NULL, 
                                     akhd -> kat)) {

          /* Change, compress new alternate key value, ie, trim nulls off end */

          for (acks = aksz; acks > 0; acks --) if (rbf[akof+acks-1] != 0) break;

          /* Now either modify the existing entry or insert a new one */

          if (sts == IX_SUCCESS) sts = modify_in_bkt (rab, akhd, crpp, acks, 0, 
                                                      acks, rbf + akof, pcks, 
                                                      rbf + pkof);
          else {
            sts = ix_find_ins_spot (rab, akhd, acks, rbf + akof, 0, crpp, 
                                    &(prec -> rfa));
            if (sts == IX_SUCCESS) sts = ix_insert_into_bkt (rab, akhd, 
                                            acks, rbf + akof, pcks, rbf + pkof, 
                                                 acks, &(prec -> rfa), 0, crpp);
          }
        }
      }
      if (rab -> crp.krf != krf) ix_relcrp (rab, &acrp);
    }
    if (sts != IX_SUCCESS) break;		/* stop if error */
  }

  /* Modify primary bucket's contents */

  crpp = &(rab -> crp);				/* assume rab has primary key */
  if (rab -> crp.krf != 0) crpp = &pcrp;	/* if not, use this context */
  if (sts == IX_SUCCESS) sts = modify_in_bkt (rab, pkhd, crpp, /* modify it */
                                              pcks, pkof, (Rsz) (pkof + pcks), 
                                              rbf, (Rsz) (rsz - pkof - pksz), 
                                              rbf + pkof + pksz);
  if ((rab -> crp.krf != 0) || (sts != IX_SUCCESS)) ix_relcrp (rab, crpp);

  /* Flush cache, only write to disk if completely successful */

ret:
  return (ix_flush (rab, sts, sts == IX_SUCCESS));
}

/************************************************************************/
/*									*/
/*  Modify record in bucket						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	khd = key's header address					*/
/*	crp = points to record to be modified				*/
/*	ksz = compressed key size					*/
/*	kof = offset of key in record					*/
/*	      (for primary key, this is same as key's definition)	*/
/*	      (for alt keys, it is zero, because the "record" consists 	*/
/*	       of just the alt key value followed by the pri key value)	*/
/*	rsz1 = compressed record size					*/
/*	rbf1 = compressed record buffer address				*/
/*	rsz2 = compressed record size					*/
/*	rbf2 = compressed record buffer address				*/
/*									*/
/*	key must be contained entirely in rsz1/rbf1			*/
/*	key header locked for write					*/
/*									*/
/*    Output:								*/
/*									*/
/*	remove_from_bkt = IX_SUCCESS : successfully removed		*/
/*	                  else : error status				*/
/*	*crp = points to where new record actually exists		*/
/*									*/
/*	key header still locked for write				*/
/*									*/
/************************************************************************/

static uLong modify_in_bkt (Rab *rab, Khd *khd, Crp *crp, Rsz ksz, Rsz kof, 
                            Rsz rsz1, const Rbf *rbf1, Rsz rsz2, const Rbf *rbf2)

{
  Bkt *bkt;
  Idx idx, n;
  Long displacement;
  uLong sts;
  Rec *rec;
  Rfa saverfa;
  Rsz empty_offset, lowest_offset, record_size;

  bkt = crp -> bkt;				/* point to bucket */
  idx = crp -> idx;				/* get index in bucket */
  rec = bkt -> rec + idx;			/* point to record descriptor */
  record_size = round_up (rsz1 + rsz2, 4);	/* size of new record */

  /* Check for key change */

  if (ix_compare_keys (rec -> keysize, 
                       ((Rbf *) bkt) + rec -> offset + kof, 
                       NULL, 
                       ksz, 
                       rbf1 + kof, 
                       NULL, 
                       khd -> kat) == 0) {

    /* No key change, see if record fits in bucket as is */

    while (1) {
      for (n = 0; bkt -> rec[n].size != 0; n ++) {} /* count recs in bucket */
      lowest_offset = bkt -> rec[n].offset;	/* get offset to lowest data */
      empty_offset = ((Rbf *) (bkt -> rec + n + 1)) /* get offset to end of */
                   - ((Rbf *) bkt);		/* record descriptors */
      if (lowest_offset - empty_offset + round_up (rec -> size, 4) 
                                                          >= record_size) break;

      /* New data won't fit, split the bucket */

      saverfa = rec -> rfa;			/* save rfa before split */
      sts = ix_split_bkt (rab, khd, crp, 1);	/* split bucket, modify crp */
      if (sts != IX_SUCCESS) return (sts);	/* return if split failed */
      bkt = crp -> bkt;				/* point to bucket */
      idx = crp -> idx;				/* get index in bucket */
      rec = bkt -> rec + idx;			/* point to record descriptor */
      if ((saverfa.vbn != rec -> rfa.vbn) || (saverfa.seq != rec -> rfa.seq)) {
        ix_errorlog (rab, "modify rfa before split %u.%u, after split %u.%u", 
                      saverfa.vbn, saverfa.seq, rec -> rfa.vbn, rec -> rfa.seq);
        ix_relcrp (rab, crp);			/* release old bucket */
        return (IX_INVBKTPNTR);
      }
    }

    /* It will fit, so just replace the old data with the new data */

    displacement = round_up (rec -> size, 4) - record_size;
    if (displacement != 0) {
      memmove (((Rbf *) bkt) + lowest_offset + displacement, /* move data */
               ((Rbf *) bkt) + lowest_offset, 	/* ... to make room */
               rec -> offset - lowest_offset);
      for (; idx <= n; idx ++) 			/* fix descriptors */
                              bkt -> rec[idx].offset += (uWord) displacement;
    }
    rec -> size = rsz1 + rsz2;			/* set up new record size */
    memcpy (((Rbf *) bkt) + rec -> offset, rbf1, rsz1); /* copy in data */
    memcpy (((Rbf *) bkt) + rec -> offset + rsz1, rbf2, rsz2);
    sts = ix_writebktnr (rab, khd, bkt);	/* write bucket, dont release */
    return (sts);				/* that's all for simple case */
  }

  /* Key changed, remove old record, insert new one, keep same rfa */

  saverfa = rec -> rfa;				/* save the rfa */
  sts = ix_remove_from_bkt (rab, khd, crp);	/* remove old record */
  if (sts != IX_SUCCESS) return (sts);
  ix_relcrp (rab, crp);				/* release old bucket */
  sts = ix_find_ins_spot (rab, khd, ksz, 	/* find new spot */
                          rbf1 + kof, kof, crp, &saverfa);
  if (sts != IX_SUCCESS) return (sts);
  sts = ix_insert_into_bkt (rab, khd, rsz1, rbf1, /* insert new rec */
                            rsz2, rbf2, ksz, &saverfa, 0, crp);
  return (sts);
}                         
