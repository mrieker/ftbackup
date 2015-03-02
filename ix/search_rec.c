/************************************************************************/
/*									*/
/*  These routines are called to search the file for a desired record	*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

static uLong search_rtn (uLong sts, 
                         Rab *rab, 
                         Rsz rsz, 
                         Rbf *rbf, 
                         Rsz *rrsz, 
                         int lockw);

static uLong search_kyb (Rab *rab, Khd *khd, Crp *crp, Vbn vbn, 
                         Rsz ksz, const Rbf *kbf, Rsz kof, int leq, int *eql);
static uLong search_sqb (Rab *rab, Khd *khd, Crp *crp);

/************************************************************************/
/*									*/
/*  Search for record by key						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = address of record access block				*/
/*	how = type of search:						*/
/*									*/
/*		IX_SEARCH_EQF = forward, first equal			*/
/*		IX_SEARCH_EQB = backward, last equal			*/
/*		IX_SEARCH_GEF = forward, greater than or equal		*/
/*		IX_SEARCH_GTF = forward, greater than			*/
/*		IX_SEARCH_LEB = backward, less than or equal		*/
/*		IX_SEARCH_LTB = backward, less than			*/
/*									*/
/*	krf = key of reference number					*/
/*	ksz = size of key buffer					*/
/*	kbf = key buffer address					*/
/*	rsz = size of record buffer					*/
/*	rbf = address of record buffer					*/
/*	rrsz = where to return actual record size to			*/
/*	lockw = IX_LOCK_R : lock for read-only access			*/
/*	        IX_LOCK_W : lock for write access			*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_key = IX_SUCCESS : record found				*/
/*	           IX_TRUNCATED : found, but truncated to fit buffer	*/
/*	         IX_RECNOTFOUND : record not found			*/
/*	         IX_RECTOOSHORT : found, but padded to fit buffer	*/
/*	        IX_RECORDLOCKED : found, but locked by another stream	*/
/*	                          a subsequent search_seq will fetch 	*/
/*	                          the next record, but may return a 	*/
/*	                          IX_RECDELETED error status if the 	*/
/*	                          locked record is deleted		*/
/*	                   else : error					*/
/*	*rrsz = size of found record (may be > rsz)			*/
/*	*rabv = sequential search context				*/
/*									*/
/************************************************************************/

uLong ix_search_key (void *rabv, int how, 
                     Rsz krf, Rsz ksz, const Rbf *kbf, 
                     Rsz rsz, Rbf *rbf, Rsz *rrsz)

{
  return (ix_search_key2 (rabv, how, krf, ksz, kbf, rsz, rbf, rrsz, IX_LOCK_W));
}

uLong ix_search_key2 (void *rabv, int how, 
                      Rsz krf, Rsz ksz, const Rbf *kbf, 
                      Rsz rsz, Rbf *rbf, Rsz *rrsz, 
                      int lockw)

{
  int eql;
  Khd *khd;
  uLong sts;
  Rab *rab;
  Rsz kof;
  Vbn vbn;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  /* Validate key of reference */

  if (krf >= rab -> fhd -> nky) return (IX_INVKEYREF);

  /* Get read-only access to file, and release any previous crp */

  ix_memen (1);
  sts = ix_lockfile (rab, 0);
  if (sts != IX_SUCCESS) {
    ix_memen (0);
    return (sts);
  }

  /* Clear nosqf so we really point at the record we end up with */

  rab -> nosqf = 0;

  /* Get root vbn */

  khd = rab -> fhd -> khd + krf;
  vbn = khd -> vbn;
  rab -> crp.krf = krf;

  /* Set up offset to key value in bucket records.  For primary  */
  /* keys, this is the same as the normal offset.  For alternate */
  /* keys, it is always zero, because records in the alternate   */
  /* key buckets consist of just the compressed alternate key    */
  /* value followed by the compressed primary key value.         */

  kof = 0;
  if (krf == 0) kof = khd -> kof;

  /* Dispatch to search routine */

  switch (how) {

    /* Forward, greater than or equal */

    case IX_SEARCH_GEF: {
      sts = ix_search_kyf (rab, khd, &(rab -> crp), vbn, NULL, 
                           ksz, kbf, kof, 0, NULL);
      break;
    }

    /* Forward, equal */

    case IX_SEARCH_EQF: {
      sts = ix_search_kyf (rab, khd, &(rab -> crp), vbn, NULL, 
                           ksz, kbf, kof, 0, &eql);
      if ((sts == IX_SUCCESS) && !eql) {
        ix_relcrp (rab, &(rab -> crp));
        sts = IX_RECNOTFOUND;
      }
      break;
    }

    /* Forward, greater than */

    case IX_SEARCH_GTF: {
      sts = ix_search_kyf (rab, khd, &(rab -> crp), vbn, NULL, 
                           ksz, kbf, kof, 1, NULL);
      break;
    }

    /* Backward, less than or equal */

    case IX_SEARCH_LEB: {
      sts = search_kyb (rab, khd, &(rab -> crp), vbn, ksz, kbf, kof, 1, NULL);
      break;
    }

    /* Backward, equal */

    case IX_SEARCH_EQB: {
      sts = search_kyb (rab, khd, &(rab -> crp), vbn, ksz, kbf, kof, 1, &eql);
      if ((sts == IX_SUCCESS) && !eql) {
        ix_relcrp (rab, &(rab -> crp));
        sts = IX_RECNOTFOUND;
      }
      break;
    }

    /* Backward, less than */

    case IX_SEARCH_LTB: {
      sts = search_kyb (rab, khd, &(rab -> crp), vbn, ksz, kbf, kof, 0, NULL);
      break;
    }

    default: {
      sts = IX_INVSEARCH;
    }
  }

  /* Return data to user's buffer and status */

  return (search_rtn (sts, rab, rsz, rbf, rrsz, lockw));
}

/************************************************************************/
/*									*/
/*  Search sequentially for next record					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	fwd = 0 : backward						*/
/*	      1 : forward						*/
/*	     -1 : re-read current record				*/
/*	rsz = buffer size						*/
/*	rbf = buffer address						*/
/*	rrsz = where to return actual record size to			*/
/*	ksz = size of key buffer (or 0)					*/
/*	kbf = address of key buffer (or NULL)				*/
/*	lockw = IX_LOCK_R : lock for read-only access			*/
/*	        IX_LOCK_W : lock for write access			*/
/*									*/
/*	rab -> crp = points to last record retrieved			*/
/*	             or NULL if at bof/eof				*/
/*	rab -> nosqf = 0 : crp points to last record retrieved		*/
/*	               1 : crp points to record just previous to last 	*/
/*	                   record retrieved				*/
/*	               2 : crp points to very next record to last 	*/
/*	                   record retrieved				*/
/*	               3 : next in bucket to last record retrieved	*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_seq = IX_SUCCESS : record found				*/
/*	         IX_NEWKEYVALUE : found, but key value is different 	*/
/*	                          than that given by ksz/kbf		*/
/*	           IX_TRUNCATED : found, but truncated to fit buffer	*/
/*	         IX_RECNOTFOUND : end of file reached			*/
/*	         IX_RECTOOSHORT : found, but padded to fit buffer	*/
/*	        IX_RECORDLOCKED : found, but locked by another stream	*/
/*	                          a subsequent search_seq will fetch 	*/
/*	                          the next record, but may return a 	*/
/*	                          IX_RECDELETED error status if the 	*/
/*	                          locked record is deleted		*/
/*	          IX_RECDELETED : a previous status of IX_RECORDLOCKED 	*/
/*	                          was returned, but when this routine 	*/
/*	                          was called again, the locked record 	*/
/*	                          could no longer found and the 	*/
/*	                          sequential search context has been 	*/
/*	                          lost					*/
/*	                   else : error					*/
/*	*rrsz = size of found record (may be > rsz)			*/
/*	rab -> crp = points to retrieved record				*/
/*									*/
/************************************************************************/

/********************************************************************************************************************************/
/*																*/
/* The following matricies describe what will be returned based on fwd, crp and nosqf						*/
/*																*/
/*    If crp != NULL, ie, not at bof/eof:											*/
/*																*/
/*      \ nosqf      0                           1                           2                          3			*/
/*  fwd  \        current                    previous                    very next               next in bucket			*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*   < 0  | use current record as is | IX_NOCURREC              |  IX_NOCURREC             | IX_NOCURREC              |		*/
/*  same  |                          |                          |                          |                          |		*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*   = 0  | use search_sqb to find   | use current record as is | use search_sqb to find   | use search_sqb to find   |		*/
/*  prev  | previous record in file  |                          | previous record in file  | previous record in file  |		*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*   > 0  | use search_sqf(1) to     | use search_sqf(1) to     | use current record as is | use search_sqf(0) to     |		*/
/*  next  | find next record in file | find next record in file |                          | find next record in file |		*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/* 																*/
/*    If crp == NULL, ie, at bof/eof:												*/
/* 																*/
/*      \ nosqf      0                           1                           2                          3			*/
/*  fwd  \        current                    previous                    very next               next in bucket			*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*   < 0  | IX_RECNOTFOUND           | IX_NOCURREC              |  IX_NOCURREC             | not possible             |		*/
/*  same  |                          |                          |                          |                          |		*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*   = 0  | use search_key(LEB) to   | IX_RECNOTFOUND           | use search_key(LEB) to   | not possible             |		*/
/*  prev  | find last record in file |                          | find last record in file |                          |		*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*   > 0  | use search_key(GEF) to   | use search_key(GEF) to   | IX_RECNOTFOUND           | not possible             |		*/
/*  next  | find 1st record in file  | find 1st record in file  |                          |                          |		*/
/*        +--------------------------+--------------------------+--------------------------+--------------------------+		*/
/*																*/
/********************************************************************************************************************************/

uLong ix_search_seq (void *rabv, int fwd, Rsz rsz, Rbf *rbf, Rsz *rrsz, 
                     Rsz ksz, const Rbf *kbf)

{
  return (ix_search_seq2 (rabv, fwd, rsz, rbf, rrsz, ksz, kbf, IX_LOCK_W));
}

uLong ix_search_seq2 (void *rabv, int fwd, Rsz rsz, Rbf *rbf, Rsz *rrsz, 
                      Rsz ksz, const Rbf *kbf, int lockw)

{
  Bkt *bkt;
  int how, nosqf;
  Khd *khd;
  uLong sts;
  Rab *rab;
  Rec *rec;
  Rsz kof;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  /* If re-reading current record, and nosqf is set, return no current record */

  nosqf = rab -> nosqf;
  if ((fwd < 0) && (nosqf != 0)) return (IX_NOCURREC);

  /* This routine always leaves the pointer on the current record */

  rab -> nosqf = 0;

  /* Get key header for the key of reference */

  khd = rab -> fhd -> khd + rab -> crp.krf;

  /* Get read-only access to file, preserve crp, if any */

  ix_memen (1);
  sts = ix_lockfile (rab, 2);
  if (sts != IX_SUCCESS) {
    ix_memen (0);
    return (sts);
  }

  /* Release lock on old record, if any, but keep crp intact */

  ix_os_unlockcrp (rab);

  /* See if already pointing at desired    */
  /* record.  Use it if so (might be eof). */

  if (((fwd <  0) && (nosqf == 0)) 
   || ((fwd == 0) && (nosqf == 1)) 
   || ((fwd >  0) && (nosqf == 2))) {
    if (rab -> crp.bkt == NULL) sts = IX_RECNOTFOUND;
    goto useasis;
  }

  /* If at bof/eof, do a key lookup to get first record. */

  if (rab -> crp.bkt == NULL) {
    ix_unlockfile (rab);
    ix_memen (0);
    how = IX_SEARCH_LEB;
    if (fwd > 0) how = IX_SEARCH_GEF;
    return (ix_search_key2 (rab, how, rab -> crp.krf, 
                            ksz, kbf, rsz, rbf, rrsz, lockw));
  }

  /* Find next or previous record.  If nosqf is 3 and going forward, the */
  /* crp has already been incremented to the next record in the bucket.  */

  if (fwd == 0) sts = search_sqb (rab, khd, &(rab -> crp));
  else sts = ix_search_sqf (rab, khd, &(rab -> crp), nosqf != 3);

  /* If successful and key specified, compare with the key. */
  /* If not equal to the key, return an alternate status.   */

useasis:
  if ((sts == IX_SUCCESS) && (kbf != NULL)) {
    bkt = rab -> crp.bkt;
    rec = bkt -> rec + rab -> crp.idx;
    kof = 0;
    if (rab -> crp.krf == 0) kof = khd -> kof;
    if (ix_compare_keys (rec -> keysize, 
                         ((Rbf *) bkt) + rec -> offset + kof, 
                         NULL, 
                         ksz, 
                         kbf, 
                         NULL, 
                         khd -> kat) != 0) sts = IX_NEWKEYVALUE;
  }

  /* Return status and data to user's buffer */

  return (search_rtn (sts, rab, rsz, rbf, rrsz, lockw));
}

/************************************************************************/
/*									*/
/*  This routine resets the sequential search context to the beginning 	*/
/*  of the file.							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = rab address						*/
/*	krf = key to do sequential searches with			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_rewind = IX_SUCCESS : success				*/
/*	            IX_INVKEYREF : invalid key of reference		*/
/*	*rab crp = cleared						*/
/*									*/
/*    Note:								*/
/*									*/
/*	any record lock currently held is released			*/
/*									*/
/************************************************************************/

uLong ix_rewind (void *rabv, Rsz krf)

{
  uLong sts;
  Rab *rab;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  if (krf >= rab -> fhd -> nky) return (IX_INVKEYREF);

  /* Lock file for reading, release old record, if any */

  ix_memen (1);
  sts = ix_lockfile (rab, 0);
  if (sts != IX_SUCCESS) {
    ix_memen (0);
    return (sts);
  }

  /* Set up new key of reference */

  rab -> crp.krf = krf;

  return (ix_flush (rab, IX_SUCCESS, 1));
}

/************************************************************************/
/*									*/
/*  This routine sets up the return values from a search operation	*/
/*									*/
/*    Input:								*/
/*									*/
/*	status = search status						*/
/*	rab = rab address						*/
/*	rab -> crp = points to record just found			*/
/*	rsz = user's record buffer size					*/
/*	rbf = user's record buffer address				*/
/*	rrsz = where to return actual record size to			*/
/*	lockw = IX_LOCK_R : lock for read-only access			*/
/*	        IX_LOCK_W : lock for write access			*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_rtn = possibly updated search status			*/
/*	*rbf = filled in with record's contents				*/
/*	*rrsz = filled in with record's actual size			*/
/*									*/
/************************************************************************/

static uLong search_rtn (uLong status, 
                         Rab *rab, 
                         Rsz rsz, 
                         Rbf *rbf, 
                         Rsz *rrsz, 
                         int lockw)

{
  Bkt *bkt;
  Idx idx;
  Khd *pkhd;
  uLong sts;
  Rbf *bas, *rbf1, *rbf2, *rbf3;
  Rec *rec;
  Rsz rsz1, rsz2, rsz3;
  Crp pcrp;

  /* Try to lock the record if successful */

  if ((status == IX_SUCCESS) 
   || (status == IX_NEWKEYVALUE)) {
    rab -> lockw = IX_LOCK_R;
    if (rab -> rdonly) lockw = IX_LOCK_R;
    sts = ix_os_lockcrp (rab, lockw);
    if (sts == IX_SUCCESS) rab -> lockw = lockw;
    else status = sts;
  }

  /* If error, just return that to caller.  Preserve cache */
  /* contents if simply record not found or record locked. */

  if ((status != IX_SUCCESS) && (status != IX_NEWKEYVALUE)) 
                   return (ix_flush (rab, status, (status == IX_RECNOTFOUND) 
                                               || (status == IX_RECORDLOCKED)));

  /* Success, if no return values wanted, all done */

  if ((rbf == NULL) && (rrsz == NULL)) goto ret;

  /* Return data */

  pkhd = rab -> fhd -> khd;			/* get primary key header */
  bkt = rab -> crp.bkt;				/* get bucket pointer */
  idx = rab -> crp.idx;				/* get bucket index */
  rec = bkt -> rec + idx;			/* get record pointer */

  /* If search by alternate key, the record in the bucket consists of the */
  /* alternate key value followed by the primary key value.  Therefore,   */
  /* we must do a lookup by primary key using the value in the alternate  */
  /* key records.  Furthermore, the rfa's of the two must match, as the   */
  /* primary key may have duplicates.                                     */

  if (rab -> crp.krf != 0) {			/* see if alternate key */
    sts = ix_alttoprikey (rab, rab -> fhd -> khd + rab -> crp.krf, 
                          pkhd, bkt, idx, &pcrp); /* convert alt to pri key */
    if (sts != IX_SUCCESS) {
      ix_relcrp (rab, &(rab -> crp));		/* couldn't, rel rec pointer */
      goto ret;					/* return error status */
    }
    bkt = pcrp.bkt;				/* get data from primary bkt */
    idx = pcrp.idx;
    rec = bkt -> rec + idx;
  }
  bas = ((Rbf *) bkt) + rec -> offset;		/* start of record */

  /* bkt,idx = primary key's bucket and index     */
  /*           from which data is to be retrieved */
  /* rec,bas = primary record                     */

  rsz1 = pkhd -> kof + rec -> keysize;		/* first part is everthing */
  rbf1 = rbf;					/* up to primary key, */
						/* including compressed key */

  rsz2 = pkhd -> ksz - rec -> keysize;		/* second part is what was */
  rbf2 = rbf1 + rsz1;				/* compressed out of pri key */

  rsz3 = rec -> size - rsz1;			/* third part is the rest */
  rbf3 = rbf2 + rsz2;				/* of the record after key */

  if (rrsz != NULL) *rrsz = rsz1 + rsz2 + rsz3; /* return total record size */

  if (rbf != NULL) {				/* see if caller wants data */
    if (rsz1 + rsz2 + rsz3 > rsz) {		/* see if too big for buf */
      if (rsz1 > rsz) {
        rsz1 = rsz;				/* if so, truncate it */
        rsz2 = 0;
        rsz3 = 0;
      } else if (rsz2 > rsz - rsz1) {
        rsz2 = rsz - rsz1;
        rsz3 = 0;
      } else if (rsz3 > rsz - rsz2 - rsz1) rsz3 = rsz - rsz2 - rsz1;
      status = IX_TRUNCATED;
    }

    else if ((rrsz == NULL) 			/* maybe need to pad output */
          && (rsz1 + rsz2 + rsz3 < rsz)) {
      memset (rbf3 + rsz3, 0, rsz - rsz1 - rsz2 - rsz3); /* ok, pad it */
      status = IX_RECTOOSHORT;			/* return alternate status */
    }

    memcpy (rbf1, bas, rsz1);			/* copy first part */
    memset (rbf2, 0, rsz2);			/* zero second part */
    memcpy (rbf3, bas + rsz1, rsz3); 		/* copy third part */
  }

  if (rab -> crp.krf != 0) ix_relcrp (rab, &pcrp); /* if it was alt key, */
						/* release primary rec pntr */

  /* Flush updates to cache only if an expected status is returned */

ret:
  return (ix_flush (rab, status, (status == IX_SUCCESS) 
                              || (status == IX_RECTOOSHORT) 
                              || (status == IX_NEWKEYVALUE) 
                              || (status == IX_TRUNCATED)));
}

/************************************************************************/
/*									*/
/*  This routine searches forward to find a greater than or equal key, 	*/
/*  linking to children and parents as necessary			*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = khd address						*/
/*	crp = record pointer						*/
/*	      crp -> depth = depth of search so far			*/
/*	                   = 0 if starting in root bucket		*/
/*	vbn = vbn to start search in					*/
/*	rfa = address of rfa to serch for (or NULL if none)		*/
/*	ksz = key buffer size						*/
/*	kbf = key buffer address					*/
/*	kof = key value offset in bucket's record			*/
/*	gtr = 0 : search matches on record .ge. key			*/
/*	      1 : search matches on record .gt. key			*/
/*	eql = where to return eql flag to				*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_kyf = IX_SUCCESS : record found				*/
/*	         IX_RECNOTFOUND : record not found			*/
/*	                   else : read error				*/
/*	*eql = 0 : record .gt. key					*/
/*	       1 : record .eq. key					*/
/*	crp -> bkt = bucket pointer of found record			*/
/*	crp -> idx = bucket index of found record			*/
/*	crp -> depth = depth of found record (0 = in root bucket)	*/
/*	crp -> parvbn[] = vbn's of parent buckets			*/
/*	crp -> paridx[] = idx's in parent buckets			*/
/*									*/
/************************************************************************/

uLong ix_search_kyf (Rab *rab, Khd *khd, Crp *crp, Vbn vbn, Rfa *rfa, 
                     Rsz ksz, const Rbf *kbf, Rsz kof, int gtr, int *eql)

{
  Bkt *bkt;
  int c;
  uLong sts;
  Idx idx;

  c = 0;

  if (vbn == 0) return (IX_RECNOTFOUND);	/* not here! */
  if (crp -> depth == max_depth) return (IX_IDXTOODEEP);
  sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read the bucket */
  if (sts != IX_SUCCESS) return (sts);		/* return if read error */
  for (idx = 0; bkt -> rec[idx].size != 0; idx ++) { /* scan bucket */
    c = ix_compare_keys (bkt -> rec[idx].keysize, /* compare rec : key */
                         ((Rbf *) bkt) + bkt -> rec[idx].offset + kof, 
                         &(bkt -> rec[idx].rfa), 
                         ksz, kbf, rfa, khd -> kat);
    if (c >= gtr) break;			/* gtr == 0 : if rec >= key, stop scan */
						/* gtr == 1 : if rec > key, stop scan */
  }
  crp -> parvbn[crp->depth] = vbn;		/* search left for first one */
  crp -> paridx[(crp->depth)++] = idx;
  sts = ix_search_kyf (rab, khd, crp, bkt -> rec[idx].left, rfa, 
                       ksz, kbf, kof, gtr, eql);
  if (sts != IX_RECNOTFOUND) ix_relbkt (rab, khd, bkt); /* if it matched, use it */
  else {
    (crp -> depth) --;	
    if (bkt -> rec[idx].size == 0) {		/* if at end of our bucket */
      ix_relbkt (rab, khd, bkt);		/* ... use our parent record */
    } else {
      crp -> bkt = bkt;				/* else, use our record */
      crp -> idx = idx;
      if (eql != NULL) *eql = (c == 0);
      sts = IX_SUCCESS;
    }
  }

  return (sts);
}

/************************************************************************/
/*									*/
/*  This routine searches backward to find a less than or equal key, 	*/
/*  linking to children and parents as necessary			*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = khd address						*/
/*	crp = record pointer						*/
/*	      crp -> depth = depth of search so far			*/
/*	                   = 0 if starting in root bucket		*/
/*	vbn = vbn to start search in					*/
/*	ksz = key buffer size						*/
/*	kbf = key buffer address					*/
/*	kof = key value offset in bucket's record			*/
/*	leq = 0 : search matches on record .lt. key			*/
/*	      1 : search matches on record .le. key			*/
/*	eql = where to return eql flag to				*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_kyb = IX_SUCCESS : record found				*/
/*	             IX_RECNOTFOUND : record not found			*/
/*	             else : read error					*/
/*	*eql = 0 : record .lt. key					*/
/*	       1 : record .eq. key					*/
/*	crp -> bkt = bucket pointer of found record			*/
/*	crp -> idx = bucket index of found record			*/
/*	crp -> depth = depth of found record (0 = in root bucket)	*/
/*	crp -> parvbn[] = vbn's of parent buckets			*/
/*	crp -> paridx[] = idx's in parent buckets			*/
/*									*/
/************************************************************************/

static uLong search_kyb (Rab *rab, Khd *khd, Crp *crp, Vbn vbn, 
                         Rsz ksz, const Rbf *kbf, Rsz kof, int leq, int *eql)

{
  Bkt *bkt;
  int c;
  uLong sts;
  Idx idx;

  c = 0;

  if (vbn == 0) return (IX_RECNOTFOUND);	/* not here! */
  if (crp -> depth == max_depth) return (IX_IDXTOODEEP);
  sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read the bucket */
  if (sts != IX_SUCCESS) return (sts);		/* return if read error */
  for (idx = 0; bkt -> rec[idx].size != 0; idx ++) {} /* get to end of bucket */
  for (; idx > 0; -- idx) {			/* scan bucket in reverse */
    if (ksz == 0) break;			/* null key is bigger */
						/* than anything */
    c = ix_compare_keys (bkt -> rec[idx-1].keysize, /* compare rec : key */
                         ((Rbf *) bkt) + bkt -> rec[idx-1].offset + kof, 
                         NULL, ksz, kbf, NULL, khd -> kat);
    if (c < leq) break;				/* leq == 0 : if rec < key, stop scan */
						/* leq == 1 : if rec <= key, stop scan */
  }
  crp -> parvbn[crp->depth] = vbn;		/* search right for first one */
  crp -> paridx[(crp->depth)++] = idx;
  sts = search_kyb (rab, khd, crp, bkt -> rec[idx].left, 
                    ksz, kbf, kof, leq, eql);
  if (sts != IX_RECNOTFOUND) ix_relbkt (rab, khd, bkt); /* if it matched, use it */
  else {
    (crp -> depth) --;
    if (idx == 0) {				/* if at beg of our bucket */
      ix_relbkt (rab, khd, bkt);		/* ... use our parent record */
    } else {
      crp -> bkt = bkt;				/* else, use our record */
      crp -> idx = idx - 1;
      if (eql != NULL) *eql = (c == 0);
      sts = IX_SUCCESS;
    }
  }

  return (sts);
}

/************************************************************************/
/*									*/
/*  This routine searches sequentially forward through a bucket, 	*/
/*  linking to parents or children as necessary				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = khd address						*/
/*	vbn = vbn to search in						*/
/*	crp = record pointer						*/
/*	      crp -> depth = depth of search so far			*/
/*	                   = 0 if starting in root bucket		*/
/*	incidx = 0 : don't increment index (it was just incremented)	*/
/*	         1 : increment index					*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_sqf = IX_SUCCESS : record found				*/
/*	             IX_RECNOTFOUND : end of file			*/
/*	             else : read error					*/
/*	crp -> bkt = bucket pointer of found record			*/
/*	crp -> idx = bucket index of found record			*/
/*	crp -> depth = depth of found record (0 = in root bucket)	*/
/*	crp -> parvbn[] = vbn's of parent buckets			*/
/*	crp -> paridx[] = idx's in parent buckets			*/
/*									*/
/************************************************************************/

uLong ix_search_sqf (Rab *rab, Khd *khd, Crp *crp, int incidx)

{
  Bkt *bkt;
  Idx depth, idx;
  uLong sts;
  Vbn vbn;

  bkt = crp -> bkt;				/* get starting point */
  idx = crp -> idx + incidx;			/* ... and increment index to */
						/* next record in same bucket */
  depth = crp -> depth;

  crp -> bkt = NULL;				/* clear them out temporarily */
  crp -> idx = 0;				/* in case we get an error */
  crp -> depth = 0;

  /* Get lowest thing off this record's left link */

  while ((vbn = bkt -> rec[idx].left) != 0) {	/* see if it has a left link */
    if (depth == max_depth) {
      ix_relbkt (rab, khd, bkt);
      return (IX_IDXTOODEEP);
    }
    crp -> parvbn[depth] = bkt -> vbn;		/* if so, save its vbn */
    crp -> paridx[depth++] = idx;		/* ... and index */
    ix_relbkt (rab, khd, bkt);			/* release the old bucket */
    sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read the left child bkt */
    if (sts != IX_SUCCESS) return (sts);	/* return if read error */
    idx = 0;					/* check out the first record */
  }

  /* Now if we're at the end of a bucket, go back up to parent   */
  /* and use the record that got me down here in the first place */

  while (bkt -> rec[idx].size == 0) {		/* see if at end of bucket */
    ix_relbkt (rab, khd, bkt);			/* if so, release bucket */
    if (depth == 0) return (IX_RECNOTFOUND);	/* eof if no parent */
    vbn = crp -> parvbn[--depth];		/* ok, get parent bkt vbn */
    idx = crp -> paridx[depth];			/* ... and record index */
    sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read parent bucket */
    if (sts != IX_SUCCESS) return (sts);	/* abort if read error */
  }

  crp -> bkt = bkt;				/* return found bucket */
  crp -> idx = idx;				/* and the record's index */
  crp -> depth = depth;				/* and the bucket's depth */
  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  This routine searches sequentially backward through a bucket, 	*/
/*  linking to parents or children as necessary				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = khd address						*/
/*	vbn = vbn to search in						*/
/*	crp = record pointer						*/
/*	      crp -> depth = depth of search so far			*/
/*	                   = 0 if starting in root bucket		*/
/*									*/
/*    Output:								*/
/*									*/
/*	search_sqb = IX_SUCCESS : record found				*/
/*	             IX_RECNOTFOUND : end of file			*/
/*	             else : read error					*/
/*	crp -> bkt = bucket pointer of found record			*/
/*	crp -> idx = bucket index of found record			*/
/*	crp -> depth = depth of found record (0 = in root bucket)	*/
/*	crp -> parvbn[] = vbn's of parent buckets			*/
/*	crp -> paridx[] = idx's in parent buckets			*/
/*									*/
/************************************************************************/

static uLong search_sqb (Rab *rab, Khd *khd, Crp *crp)

{
  Bkt *bkt;
  Idx depth, idx;
  uLong sts;
  Vbn vbn;

  bkt = crp -> bkt;				/* get starting point */
  idx = crp -> idx;
  depth = crp -> depth;

  crp -> bkt = NULL;				/* clear them out temporarily */
  crp -> idx = 0;				/* in case we get an error */
  crp -> depth = 0;

  /* Find the highest thing on my left link */

  while ((vbn = bkt -> rec[idx].left) != 0) {	/* see if it has a left link */
    if (depth == max_depth) {
      ix_relbkt (rab, khd, bkt);
      return (IX_IDXTOODEEP);
    }
    crp -> parvbn[depth] = bkt -> vbn;		/* if so, save its vbn */
    crp -> paridx[depth++] = idx;		/* ... and index */
    ix_relbkt (rab, khd, bkt);			/* release the old bucket */
    sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read the left child bkt */
    if (sts != IX_SUCCESS) return (sts);	/* return if read error */
    for (idx = 0; bkt -> rec[idx].size != 0; idx ++) {} /* get end of bucket */
  }

  /* If at beginning of bucket, pop to parent and check it out */

  while (idx == 0) {				/* see if at beg of bucket */
    ix_relbkt (rab, khd, bkt);			/* if so, release bucket */
    if (depth == 0) return (IX_RECNOTFOUND);	/* eof if no parent */
    vbn = crp -> parvbn[--depth];		/* ok, get parent bkt vbn */
    idx = crp -> paridx[depth];			/* ... and record index */
    sts = ix_readbkt (rab, khd, vbn, &bkt);	/* read parent bucket */
    if (sts != IX_SUCCESS) return (sts);	/* abort if read error */
  }

  crp -> bkt = bkt;				/* return found bucket */
  crp -> idx = idx - 1;				/* and the record's index */
  crp -> depth = depth;				/* and the bucket's depth */
  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  Release record pointer						*/
/*									*/
/************************************************************************/

void ix_relcrp (Rab *rab, Crp *crp)

{
  if (crp -> bkt != NULL) 
                    ix_relbkt (rab, rab -> fhd -> khd + crp -> krf, crp -> bkt);
  ix_relcrpnv (rab, crp);
}

/* Call this entrypoint if the bkt pointer is known to be invalid */

void ix_relcrpnv (Rab *rab, Crp *crp)

{
  crp -> depth = 0;
  crp -> bkt = NULL;
}

/************************************************************************/
/*									*/
/*  This routine converts a bkt/idx of an alternate key to the bkt/idx 	*/
/*  of the corresponding primary key.					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab  = address of corresponding rab				*/
/*	akhd = alternate key header pointer				*/
/*	pkhd = primary key header pointer				*/
/*	bkt  = alternate key bucket					*/
/*	idx  = index in bkt of alternate key entry			*/
/*	pcrp = where to return pointer to primary record		*/
/*									*/
/*	primary key header locked for read				*/
/*									*/
/*    Output:								*/
/*									*/
/*	alttoprikey = IX_SUCCESS : success				*/
/*	              else : error status				*/
/*	pcrp -> bkt = primary key bucket				*/
/*	        idx = index in pcrp -> bkt of record			*/
/*									*/
/*	primary key header still locked for read			*/
/*									*/
/************************************************************************/

uLong ix_alttoprikey (Rab *rab, Khd *akhd, Khd *pkhd, 
                      Bkt *abkt, Idx aidx, Crp *pcrp)

{
  Bkt *pbkt;
  int eql;
  uLong sts;
  Rbf *abuf, *adat, *arbf1, *arbf3, *pdat, *pkbf;
  Rec *arec, *prec;
  Rsz arsz1, arsz2, arsz3, pksz;

  arbf1 = NULL;

  /* Search for primary key record using the     */
  /* value and rfa from the alternate key record */

  arec = abkt -> rec + aidx;			/* point to alt rec descr */
  adat = ((Rbf *) abkt) + arec -> offset;	/* point to alt rec data */
  pksz = arec -> size - arec -> keysize;	/* get pri key size in alt rec */
  pkbf = adat + arec -> keysize;		/* point to pri key value in */
						/* ... alternate record */

  pcrp -> depth = 0;				/* search primary from root */
  pcrp -> krf = 0;
  sts = ix_search_kyf (rab, pkhd, pcrp, pkhd -> vbn, &(arec -> rfa), 
                       pksz, pkbf, pkhd -> kof, 0, &eql);
  if (sts != IX_SUCCESS) return (sts);
  if (!eql) {
    ix_relcrp (rab, pcrp);			/* key not eq, not found */
    return (IX_RECNOTFOUND);
  }
  pbkt = pcrp -> bkt;				/* point to primary bucket */
  prec = pbkt -> rec + pcrp -> idx;		/* point to primary rec descr */
  pdat = ((Rbf *) pbkt) + prec -> offset;	/* point to primary rec data */

  /* Now make sure the alternate key value in the primary bucket */
  /* matches the alternate key value in the alternate key bucket */

  if ((akhd -> kof + akhd -> ksz <= pkhd -> kof + prec -> keysize)
   || (prec -> keysize == pkhd -> ksz)) {
    eql = ix_compare_keys (arec -> keysize, 
                           adat, 
                           &(arec -> rfa), 
                           akhd -> ksz, 
                           pdat + akhd -> kof, 
                           &(prec -> rfa), 
                           akhd -> kat);
  } else if (akhd -> kof >= pkhd -> kof + pkhd -> ksz) {
    abuf  = pdat;				/* stupid HP compiler */
    abuf += akhd -> kof;
    abuf -= pkhd -> ksz;
    abuf += prec -> keysize;
    eql = ix_compare_keys (arec -> keysize, adat, &(arec -> rfa), 
                           akhd -> ksz, abuf, &(prec -> rfa), akhd -> kat);
  } else {
    arsz1 = 0;
    if (akhd -> kof < pkhd -> kof + prec -> keysize) {
      arsz1 = pkhd -> kof + prec -> keysize - akhd -> kof;
      arbf1 = pdat + akhd -> kof;
    }
    arsz2 = prec -> keysize - pkhd -> ksz;
    if (arsz1 + arsz2 > akhd -> ksz) arsz2 = akhd -> ksz - arsz1;
    arsz3 = akhd -> ksz - arsz2 - arsz1;
    arbf3 = pdat + pkhd -> kof + prec -> keysize;
    abuf  = ix_malloc (akhd -> ksz);
    memcpy (abuf, arbf1, arsz1);
    memset (abuf + arsz1, 0, arsz2);
    memcpy (abuf + arsz1 + arsz2, arbf3, arsz3);
    eql = ix_compare_keys (arec -> keysize, adat, &(arec -> rfa), 
                           akhd -> ksz, abuf, &(prec -> rfa), akhd -> kat);
    ix_free (abuf);
  }

  if (eql == 0) return (IX_SUCCESS);

  ix_errorlog (rab, "alt key %u rfa %u.%u points to wrong pri rec", 
                    akhd - rab -> fhd -> khd, arec -> rfa.vbn, arec -> rfa.seq);
  return (IX_BADALTBKT);
}
