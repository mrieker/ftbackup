/************************************************************************/
/*									*/
/*  This routine inserts a record into a file				*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <string.h>

/************************************************************************/
/*									*/
/*  Insert record into file						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = address of record access block				*/
/*	rsz = size of record buffer to be inserted			*/
/*	rbf = address of record buffer to be inserted			*/
/*									*/
/*    Output:								*/
/*									*/
/*	insert_rec = IX_SUCCESS : record inserted			*/
/*	             else : error					*/
/*	rab -> crp = points to record just inserted			*/
/*	             on key of reference previously established		*/
/*	             cleared if record does not use that key		*/
/*									*/
/************************************************************************/

uLong ix_insert_rec (void *rabv, Rsz rsz, const Rbf *rbf)

{
  Idx i;
  uLong sts;
  Rab *rab;
  Rfa rfa;
  Rbf const *xrbf;
  Rsz pkof, pksz, xksz, xrsz;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  /* Make sure file is not read-only */

  if (rab -> rdonly) return (IX_READONLY);

  /* Make sure new record is not too big */

  if (rsz > rab -> fhd -> mrs) return (IX_INVRECSIZE);

  /* Maybe we can compress the primary key */

  pksz = rab -> fhd -> khd[0].ksz;		/* get primary key size */
  pkof = rab -> fhd -> khd[0].kof;		/* get primary key offset */
  if (rsz < pkof + pksz) return (IX_INVRECSIZE); /* rec doesn't contain */
						/* the whole primary key */
  xrbf = rbf + pkof + pksz;			/* get end of primary key */
  for (xksz = pksz; xksz > 1; xksz --) 		/* find last non-null ... */
                    if (*(-- xrbf) != 0) break;	/* ... but don't let it go 0 */

  /* Get write access to file and release crp, if any */

  ix_memen (1);
  sts = ix_lockfile (rab, 1);
  if (sts != IX_SUCCESS) return (sts);

  /* Insert the record, leaving out the nulls compressed from the primary key */

  while (++ (rab -> fhd -> rfa.seq) == 0) ++ (rab -> fhd -> rfa.vbn);
  sts = ix_writefhd (rab);			/* assign it a new rfa */
  if (sts == IX_SUCCESS) {
    rfa = rab -> fhd -> rfa;			/* insert it */
    sts = ix_insert_key (rab, rab -> fhd -> khd + 0, 
                         xksz, pkof, (Rsz) (pkof + xksz), rbf, 
                         (Rsz) (rsz - pksz - pkof), rbf + pkof + pksz, 
                         &rfa);
  }

  /* Insert alternate key records = just the compressed alternate key value */
  /*                                   + the compressed primary key value */

  if (sts == IX_SUCCESS) {
    for (i = 1; i < rab -> fhd -> nky; i ++) {	/* do each alternate key */
      xrsz = rab -> fhd -> khd[i].ksz;		/* alt keys use just the key */
      xrbf = rbf + rab -> fhd -> khd[i].kof;	/* ... for their "record" */
      if (xrbf + xrsz <= rbf + rsz) {		/* if record contains the key */
        while ((xrsz > 0) && (xrbf[xrsz-1] == 0)) xrsz --; /* compress it */
        sts = ix_insert_key (rab, rab -> fhd -> khd + i, xrsz, /* insert it */
                             0, xrsz, xrbf, xksz, rbf + pkof, &rfa);
        if (sts != IX_SUCCESS) break;		/* stop if error */
      }
    }
  }

  /* Lock the record - should always succeed as this is a new record */

  if (sts == IX_SUCCESS) sts = ix_os_lockcrp (rab, 1);

  /* Flush cache and return status.  Only    */
  /* write to disk if completely successful. */

  return (ix_flush (rab, sts, sts == IX_SUCCESS));
}

/************************************************************************/
/*									*/
/*  Insert record's key into file					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of record access block				*/
/*	khd = address of header for the corresponding key		*/
/*	ksz = compressed key size					*/
/*	kof = key offset in record					*/
/*	      (for primary key, same as key definition)			*/
/*	      (for alternate keys, 0, because alt key records consist 	*/
/*	       of just alternate key value and primary key value)	*/
/*	rsz1 = size of first part of record buffer to be inserted	*/
/*	rbf1 = address of first part of record buffer to be inserted	*/
/*	rsz2 = size of second part of record buffer to be inserted	*/
/*	rbf2 = address of second part of record buffer to be inserted	*/
/*	*rfa = rfa to assign to record					*/
/*	       for primary key, set rfa -> vbn = 0			*/
/*	       for alt keys, set rfa = rfa of primary record		*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_insert_key = IX_SUCCESS : inserted				*/
/*	                else : read / write error			*/
/*	*rfa = assigned rfa (for primary keys)				*/
/*	rab -> crp = points to inserted record if rab -> crp.krf = krf 	*/
/*	             of inserted record					*/
/*									*/
/*  Notes:								*/
/*									*/
/*	For the primary key, the record is the actual record's data. 	*/
/*	For alternate keys, the record is the alternate key value plus 	*/
/*	the primary key value and the rfa is the rfa of the primary 	*/
/*	record.								*/
/*									*/
/*	The key must be contained entirely within the first part of 	*/
/*	the record.							*/
/*									*/
/************************************************************************/

uLong ix_insert_key (Rab *rab, 
                     Khd *khd, 
                     Rsz ksz, 
                     Rsz kof, 
                     Rsz rsz1, 
                     const Rbf *rbf1, 
                     Rsz rsz2, 
                     const Rbf *rbf2, 
                     Rfa *rfa)

{
  uLong sts;
  Crp tcrp, *crpp;

  /* Make sure the record size isn't too big to fit in a bucket all by itself */

  if (((rsz1 + rsz2 + 3) & -4) + sizeof (Bkt) + sizeof (Rec) * 2 
                                    > rab -> fhd -> bks) return (IX_INVRECSIZE);

  /* Make sure zhe first part of zhe record contains zhe key */

  if (ksz > khd -> ksz) return (IX_INVKEYSIZE);
  if (rsz1 < ksz + kof) return (IX_INVRECSIZE);

  /* If inserting on rab's crp key, use rab's crp block, else use a temp */

  crpp = &(rab -> crp);
  if (khd - rab -> fhd -> khd != rab -> crp.krf) crpp = &tcrp;

  /* Find spot to insert record at */

  sts = ix_find_ins_spot (rab, khd, ksz, rbf1 + kof, kof, crpp, rfa);

  /* Insert record into bucket at that spot */

  if (sts == IX_SUCCESS) 
                    sts = ix_insert_into_bkt (rab, khd, rsz1, rbf1, rsz2, rbf2, 
                                              ksz, rfa, 0, crpp);

  /* If using temp crp, release temp crp */

  if ((sts == IX_SUCCESS) && (crpp == &tcrp)) ix_relcrp (rab, &tcrp);

  /* Return final status */

  return (sts);
}

/************************************************************************/
/*									*/
/*  This routine search a key's tree to find the right spot to insert 	*/
/*  a new record							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	khd = khd address for the key to be searched			*/
/*	ksz = key buffer size						*/
/*	kbf = key buffer address					*/
/*	kof = offset of key value in record buffer			*/
/*	      (for primary key, same as key definition)			*/
/*	      (for alternate keys, 0, because alt key records consist 	*/
/*	       of just alternate key value and primary key value)	*/
/*	crp = address of sequential search context			*/
/*	rfa = address of rfa						*/
/*									*/
/*    Output:								*/
/*									*/
/*	find_ins_spot = IX_SUCCESS : location found			*/
/*	                      else : error status			*/
/*	*crp = points to record that is to immediately follow the one 	*/
/*	       being inserted						*/
/*	       if tree is empty, the crp has nulls, but a success 	*/
/*	       status is returned					*/
/*									*/
/*    Note that this function is very similar to searching forward on 	*/
/*    a "kgt" function.  However this routine will return an crp at 	*/
/*    the bottom of the B-tree, whereas search_kyf would return one 	*/
/*    near the top.							*/
/*									*/
/************************************************************************/

uLong ix_find_ins_spot (Rab *rab, 
                        Khd *khd, 
                        Rsz ksz, 
                        const Rbf *kbf, 
                        Rsz kof, 
                        Crp *crp, 
                        Rfa *rfa)

{
  Bkt *bkt;
  Idx depth, idx;
  uLong sts;
  Vbn vbn, vbn2;

  /* Init search context */

  crp -> bkt = NULL;
  crp -> idx = 0;
  crp -> depth = 0;
  crp -> krf = khd - rab -> fhd -> khd;

  /* If header is zero, return nulls in the crp along with success status */

  vbn = khd -> vbn;
  if (vbn == 0) return (IX_SUCCESS);

  /* Not so lucky, search for spot to insert record */

  for (depth = 0; depth < max_depth; depth ++) {
    sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read the bucket */
    if (sts != IX_SUCCESS) return (sts);	/* return if read error */
    for (idx = 0; bkt -> rec[idx].size != 0; idx ++) /* scan descriptors for */
             if (ix_compare_keys (ksz, kbf, rfa, /* ... key .gt. mine */
                                  bkt -> rec[idx].keysize, 
                                  ((Rbf *) bkt) + bkt -> rec[idx].offset + kof, 
                                  &(bkt -> rec[idx].rfa), 
                                  khd -> kat) < 0) break;
    vbn2 = bkt -> rec[idx].left;		/* see if it has lower child */
    if (vbn2 == 0) {				/* if it has no lower child, */
      crp -> depth = depth;			/* this is the place for it */
      crp -> bkt = bkt;
      crp -> idx = idx;
      return (IX_SUCCESS);
    }
    ix_relbkt (rab, khd, bkt);			/* else, check out child */
    crp -> parvbn[depth] = vbn;
    crp -> paridx[depth] = idx;
    vbn = vbn2;
  }
  return (IX_IDXTOODEEP);			/* index goes too deep */
}

/************************************************************************/
/*									*/
/*  This routine inserts a record into a bucket, splitting it if 	*/
/*  necessary								*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	khd = address of khd						*/
/*	rsz1 = size of first part of record to be inserted		*/
/*	rbf1 = address of first part of record to be inserted		*/
/*	rsz2 = size of second part of record to be inserted		*/
/*	rbf2 = address of second part of record to be inserted		*/
/*	ksz = compressed key size					*/
/*	*rfa = record's rfa						*/
/*	right = vbn of child to right of record being inserted		*/
/*	crp = points to record that is to immediately follow one being 	*/
/*	      inserted							*/
/*									*/
/*	khd assumed to be locked for write				*/
/*									*/
/*    Output:								*/
/*									*/
/*	insert_into_bkt = IX_SUCCESS : successful completion		*/
/*	                  else : I/O error				*/
/*	*crp = modified to point to record, in case a split was 	*/
/*	       necessary						*/
/*									*/
/*	khd still locked for write					*/
/*									*/
/************************************************************************/

uLong ix_insert_into_bkt (Rab *rab, 
                          Khd *khd, 
                          Rsz rsz1, 
                          const Rbf *rbf1, 
                          Rsz rsz2, 
                          const Rbf *rbf2, 
                          Rsz ksz, 
                          Rfa *rfa, 
                          Vbn right, 
                          Crp *crp)

{
  Bkt *bkt;
  uLong sts;
  Rbf *bbkt;
  Idx idx, k, n;
  Rsz empty_offset, lowest_offset, new_low_offset, record_offset, record_size;
  Vbn vbn;

  /* Check for special case of creating a new root */

  if (khd -> vbn == 0) {
    sts = ix_allocbkt (rab, khd, &bkt);		/* allocate a new bucket */
    if (sts != IX_SUCCESS) return (sts);
    vbn = bkt -> vbn;				/* get its vbn */
    bkt -> rec[0].size = rsz1 + rsz2;		/* set up first record descr */
    bkt -> rec[0].offset = rab -> fhd -> bks - ((rsz1 + rsz2 + 3) & -4);
    bkt -> rec[0].left = 0;
    bkt -> rec[0].rfa = *rfa;			/* anyway, store rfa in descr */
    bkt -> rec[0].keysize = ksz;		/* save compressed key size */
    bkt -> rec[1].size = 0;			/* set up end record descr */
    bkt -> rec[1].offset = bkt -> rec[0].offset;
    bkt -> rec[1].left = 0;
    bkt -> rec[1].rfa.vbn = 0;
    bkt -> rec[1].rfa.seq = 0;
    bkt -> rec[1].keysize = 0;
    bbkt = ((Rbf *) bkt) + bkt -> rec[0].offset;
    memcpy (bbkt, rbf1, rsz1);			/* copy data */
    memcpy (bbkt + rsz1, rbf2, rsz2);
    sts = ix_writebktnr (rab, khd, bkt);	/* write bucket to disk */
						/* but don't release it */
    if (sts == IX_SUCCESS) {
      crp -> bkt = bkt;				/* save search context */
      crp -> idx = 0;
      crp -> depth = 0;
      khd -> vbn = vbn;				/* update header */
      sts = ix_writefhd (rab);
    }
    if (sts != IX_SUCCESS) ix_relcrp (rab, crp); /* failed, release bucket */
    return (sts);
  }

  /* Tree not empty, see if there is enough room for the record in the bucket */

  record_size = (rsz1 + rsz2 + 3) & -4;		/* get new record size */
						/* rounded to longword */
  do {
    bkt = crp -> bkt;				/* get bucket and index */
    idx = crp -> idx;				/* from crp */
    vbn = bkt -> vbn;				/* get bucket's vbn */
    bbkt = (Rbf *) bkt;				/* get byte-sized bkt ptr */
    for (n = idx; bkt -> rec[n].size != 0; n ++) {} /* find last rec */
    lowest_offset = bkt -> rec[n].offset;	/* get its offset */
    empty_offset = (Rbf *) (bkt -> rec + n + 2) - bbkt;

    if (empty_offset + record_size <= lowest_offset) {

      /* There is enough room - insert the record and record descriptor */

      record_offset = ((bkt -> rec[idx].size + 3) & -4) /* offset to end of */
                      + bkt -> rec[idx].offset;	/* data to be moved = */
						/* offset to end of new */
						/* record's data */
      new_low_offset = lowest_offset - record_size; /* offset to beg of */
						/* where new data goes */
						/* don't combine with next */
						/* statement because HP */
						/* compiler will screw it up */

      memmove (bbkt + new_low_offset, 		/* move the old data down */
               bbkt + lowest_offset, 
               record_offset - lowest_offset);

      for (k = idx; k <= n; k ++) 		/* fix old descr's offsets */
           bkt -> rec[k].offset -= record_size;	/* to point to moved location */

      memmove ((Rbf *) (bkt -> rec + idx + 1), 	/* move the old descr's up */
               (Rbf *) (bkt -> rec + idx), 
               ((Rbf *) (bkt -> rec + n + 1)) - ((Rbf *) (bkt -> rec + idx)));

      record_offset -= record_size;		/* get offset to start of new */
						/* record's data */
      memcpy (bbkt + record_offset, rbf1, rsz1); /* copy in new record there */
      memcpy (bbkt + record_offset + rsz1, rbf2, rsz2);

      bkt -> rec[idx].size = rsz1 + rsz2;	/* make new rec's descriptor */
      bkt -> rec[idx].offset = record_offset;	/* leave old left as is */
      bkt -> rec[idx].rfa = *rfa;		/* store rfa in rec descr */
      bkt -> rec[idx].keysize = ksz;		/* save compressed key size */

      bkt -> rec[idx+1].left = right;		/* store right in next's left */

      sts = ix_writebktnr (rab, khd, bkt);	/* write bucket, don't rel */
      if (sts != IX_SUCCESS) ix_relcrp (rab, crp); /* release if failure, tho */
      return (sts);				/* all done */
    }

    /* Not enough room - make like a bananna and split */

    sts = ix_split_bkt (rab, khd, crp, 0);
  } while (sts == IX_SUCCESS);
  return (sts);					/* split failed, return error */
}

/************************************************************************/
/*									*/
/*  This routine splits a bucket					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	khd = address of key header					*/
/*	crp = address of search context					*/
/*	      the bucket that the search context points to is split	*/
/*	      the record that the search context points at within that 	*/
/*	      bucket may or may not be moved by the split		*/
/*	modify = 0 : point crp at bottom of tree 			*/
/*	             (for insertion of new record)			*/
/*	         1 : point crp at record (for modification of record)	*/
/*									*/
/*    Output:								*/
/*									*/
/*	split_bkt = IX_SUCCESS : bucket successfully split		*/
/*	            else : error status					*/
/*	*crp = possibly modified to point to where same record is now 	*/
/*	       located							*/
/*	crp -> bkt needs to be written back out to disk			*/
/*									*/
/************************************************************************/

uLong ix_split_bkt (Rab *rab, Khd *khd, Crp *crp, int modify)

{
  Bkt *bkt, *bkt2, *bkt3;
  Idx idx, k, m, n;
  uLong sts;
  Rbf *bbkt, *bbkt2;
  Rec *recm;
  Rsz record_offset;
  Vbn vbn, vbn2, vbn3;

  sts = ix_allocbkt (rab, khd, &bkt2);		/* allocate a new bucket */
  if (sts != IX_SUCCESS) goto ret1;		/* return if failure */
						/* probably disk is full */
  bbkt2 = (Rbf *) bkt2;				/* set up a byte pointer */
  vbn2 = bkt2 -> vbn;				/* get its vbn */

  bkt = crp -> bkt;				/* get original bucket */
  idx = crp -> idx;				/* ... and index */
  bbkt = (Rbf *) bkt;				/* make a byte pointer */
  vbn = bkt -> vbn;				/* get its vbn */
  for (n = 0; crp -> bkt -> rec[n].size != 0; n ++) {} /* count recs in it */
  m = n / 2;					/* split old bucket in middle */

  /* idx = index of insert point in original bucket      */
  /* m = index of record to be promoted to parent bucket */
  /* n = index of original bucket's end mark             */

  /* Copy data and descriptors after split to new bucket */

  memcpy ((Rbf *) (bkt2 -> rec), 		/* copy record descriptors */
          (Rbf *) (bkt -> rec + m + 1), 
          (Rbf *) (bkt -> rec + n) - (Rbf *) (bkt -> rec + m));

  record_offset = rab -> fhd -> bks;		/* fix record descriptors */
  for (k = 0; k < n - m; k ++) {
    record_offset -= (bkt2 -> rec[k].size + 3) & -4;
    bkt2 -> rec[k].offset = record_offset;
  }

  memcpy (bbkt2 + record_offset, 		/* copy record data */
         bbkt + bkt -> rec[n].offset, 
         rab -> fhd -> bks - record_offset);

  /* bkt = bbkt = points to original bucket                       */
  /* vbn = original bucket's vbn                                  */
  /* bkt2 = bbkt2 = points to new bucket                          */
  /* vbn2 = new bucket's vbn                                      */
  /* idx = index in original bucket for insertion                 */
  /* m = index in original bucket of record to be moved to parent */
  /* n = number of records in original bucket                     */

  /* If old bucket has a parent, insert the "m" record into the parent */

  if (crp -> depth > 0) {
    vbn3 = crp -> parvbn[--(crp->depth)];	/* read parent bucket */
    sts = ix_readbkt (rab, khd, vbn3, &bkt3);
    if (sts != IX_SUCCESS) goto ret2;
    k = crp -> paridx[crp->depth];		/* get parent rec's index */
    if (bkt3 -> rec[k].left != vbn) {		/* make sure it points to bkt */
      ix_errorlog (rab, "bucket %u says %u.%u is its parent", vbn, vbn3, k);
      sts = IX_INVBKTPNTR;
      goto ret2;
    }
    crp -> bkt = bkt3;				/* ok, make it current record */
    crp -> idx = k;
    sts = ix_insert_into_bkt (rab, khd, 	/* insert new top in parent */
                           bkt -> rec[m].size, bbkt + bkt -> rec[m].offset, 
                           0, NULL, bkt -> rec[m].keysize, 
                           &(bkt -> rec[m].rfa), vbn2, crp);
    if (sts != IX_SUCCESS) goto ret3;
    vbn3 = crp -> bkt -> vbn;			/* save parent's actual vbn */
    sts = ix_writebkt (rab, khd, crp -> bkt);	/* write out parent */
    crp -> bkt = NULL;
    if (sts != IX_SUCCESS) goto ret3;
    sts = IX_IDXTOODEEP;			/* push parent location back */
    if (crp -> depth == max_depth) goto ret2;
    crp -> parvbn[crp->depth] = vbn3;
    crp -> paridx[(crp->depth)++] = crp -> idx;
  }

  /* Otherwise, create a new parent and link it at root of tree */

  else {
    sts = ix_allocbkt (rab, khd, &bkt3);	/* allocate a new bucket */
    if (sts != IX_SUCCESS) goto ret2;
    vbn3 = bkt3 -> vbn;
    bkt3 -> rec[0].size    = bkt -> rec[m].size; /* put new top in it */
    bkt3 -> rec[0].offset  = rab -> fhd -> bks 
                           - ((bkt -> rec[m].size + 3) & -4);
    bkt3 -> rec[0].left    = vbn;
    bkt3 -> rec[0].rfa     = bkt -> rec[m].rfa;
    bkt3 -> rec[0].keysize = bkt -> rec[m].keysize;
    bkt3 -> rec[1].size    = 0;			/* put an end in it */
    bkt3 -> rec[1].offset  = bkt3 -> rec[0].offset;
    bkt3 -> rec[1].left    = vbn2;
    bkt3 -> rec[1].rfa.vbn = 0;
    bkt3 -> rec[1].rfa.seq = 0;
    bkt3 -> rec[1].keysize = 0;
    memcpy (((Rbf *) bkt3) + bkt3 -> rec[0].offset, /* copy in the data */
            bbkt + bkt -> rec[m].offset, 
            bkt -> rec[m].size);
    sts = ix_writebkt (rab, khd, bkt3);		/* write bucket to disk */
    if (sts != IX_SUCCESS) goto ret2;
    khd -> vbn = vbn3;				/* update header */
    sts = ix_writefhd (rab);
    if (sts != IX_SUCCESS) goto ret2;
    crp -> parvbn[0] = vbn3;			/* save it as a parent of bkt */
    crp -> paridx[0] = 0;
    crp -> depth = 1;
  }

  /* Put new end mark in original bucket where split happened */

  recm = bkt -> rec + m;
  recm -> offset += (recm -> size + 3) & -4;
  recm -> size    = 0;
  recm -> rfa.vbn = 0;
  recm -> rfa.seq = 0;
  recm -> keysize = 0;

  /* If original insertion is after that point, set up to   */
  /* insert new record in the new split bucket.  Otherwise, */
  /* insert new record in original spot in original bucket. */

  if (idx > m) {
    sts = ix_writebkt (rab, khd, bkt);		/* write original bucket */
    idx -= m + 1;				/* calculate new index */
    bkt = bkt2;					/* ... in split bucket */
    crp -> paridx[crp->depth-1] ++;		/* parent is one notch over */
    crp -> bkt = bkt;				/* return position */
    crp -> idx = idx;				/* of original record */
  } else if ((idx < m) || !modify) {
    sts = ix_writebkt (rab, khd, bkt2);		/* write new bucket */
    crp -> bkt = bkt;				/* return position */
    crp -> idx = idx;				/* of original record */
						/* if split on the original */
						/* record, this will point to */
						/* the new end-of-bucket mark */
						/* in the original record */
  } else {
						/* split right on a record */
						/* caller is modifying, so */
						/* point to where that record */
						/* got put in the parent */
    sts = ix_writebkt (rab, khd, bkt);		/* write original bucket */
    if (sts != IX_SUCCESS) goto ret2;
    sts = ix_writebkt (rab, khd, bkt2);		/* write new bucket */
    if (sts != IX_SUCCESS) goto ret1;
    vbn3 = crp -> parvbn[--(crp->depth)];	/* read parent bucket */
    idx  = crp -> paridx[crp->depth];
    sts = ix_readbkt (rab, khd, vbn3, &bkt3);
    crp -> bkt = bkt3;				/* point to original record */
    crp -> idx = idx;				/* ... in parent bucket */
  }
  goto ret1;

ret3:
  ix_relbkt (rab, khd, bkt);
ret2:
  ix_relbkt (rab, khd, bkt2);
ret1:
  if (sts != IX_SUCCESS) ix_relcrp (rab, crp);	/* release if error */
  return (sts);					/* return status */
}
