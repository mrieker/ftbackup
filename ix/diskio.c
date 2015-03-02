/************************************************************************/
/*									*/
/*  These routines perform all I/O to the disk				*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

static uLong checkbktvbn (Rab *rab, Vbn vbn);
static uLong makecache (Rab *rab);
static uLong findcache (Rab *rab, Vbn vbn, Cache **cacher);

/************************************************************************/
/*									*/
/*  Lock a file and flush cache if invalid				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	rw & 1 = 0 : lock it for read-only access			*/
/*	             no modifications will be performed			*/
/*	         1 : lock it for read/write access			*/
/*	             modifications might be performed			*/
/*	rw & 2 = 0 : release any crp					*/
/*	         2 : preserve crp					*/
/*	             it will be referenced after lock			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_lockfile = IX_SUCCESS : successful completion		*/
/*	           IX_RECDELETED : record pointed to by crp could not 	*/
/*	                           be found again			*/
/*	                    else : error status				*/
/*									*/
/************************************************************************/

uLong ix_lockfile (Rab *rab, int rw)

{
  Bkt *bkt;
  int eql;
  uLong sts;
  Rbf keybuff[65536];
  Rec *rec;
  Rfa rfa;
  Rsz keyoffs, keysize;

  /* Set os dependent lock */

  sts = ix_os_lckfil (rab, rw & 1);

  /* Maybe release current record pointer */

  if (!(rw & 2) && (rab -> crp.bkt != NULL) && ((sts == IX_SUCCESS) || (sts == IX_CACHEINVAL))) {
    ix_relcrp (rab, &(rab -> crp));
    ix_os_unlockcrp (rab);
    rab -> nosqf = 0;
  }

  /* If the cache is invalid, flush it and maybe re-establish the crp */

  if (sts == IX_CACHEINVAL) {

    /* If there is a current record pointer, save the rfa and key */

    bkt = rab -> crp.bkt;
    if (bkt != NULL) {
      rec = bkt -> rec + rab -> crp.idx;
      rfa = rec -> rfa;
      keysize = rec -> keysize;
      keyoffs = 0;
      if (rab -> crp.krf == 0) keyoffs = rab -> fhd -> khd[0].kof;
      memcpy (keybuff, ((Rbf *)bkt) + rec -> offset + keyoffs, keysize);
      ix_relcrp (rab, &(rab -> crp));
    }

    /* Wipe out cache */

    ix_wipecache (rab);

    /* Maybe rebuild the crp by searching for the old record */

    if (bkt != NULL) {
      sts = ix_search_kyf (rab, 
                           rab -> fhd -> khd + rab -> crp.krf, 
                           &(rab -> crp), 
                           rab -> fhd -> khd[rab->crp.krf].vbn, 
                           &rfa, 
                           keysize, 
                           keybuff, 
                           keyoffs, 
                           0, 
                           &eql);
      if ((sts == IX_SUCCESS) && !eql) {
        ix_relcrp (rab, &(rab -> crp));
        sts = IX_RECDELETED;
      }
      if (sts != IX_SUCCESS) {
        ix_os_ulkfil (rab);
        return (sts);
      }
    }

    sts = IX_SUCCESS;
  }

  return (sts);
}

/************************************************************************/
/*									*/
/*  Release lock set by ix_lockfile					*/
/*									*/
/************************************************************************/

void ix_unlockfile (Rab *rab)

{
  ix_os_ulkfil (rab);
}

/************************************************************************/
/*									*/
/*  Find an unused bucket and allocate it				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = file's rab						*/
/*	khd = key header for bucket					*/
/*	bktr = where to return pointer to bucket			*/
/*									*/
/*    Output:								*/
/*									*/
/*	allocbkt = IX_SUCCESS : successfully allocated			*/
/*	           else : I/O error					*/
/*	*bktr = pointer to bucket node					*/
/*									*/
/************************************************************************/

uLong ix_allocbkt (Rab *rab, Khd *khd, Bkt **rbkt)

{
  Bkt *bkt;
  Cache *cache;
  uLong sts;
  Vbn vbn;

  /* See if there are any on the free list and use one if so */

  if ((vbn = rab -> fhd -> free) != 0) {

    /* Read the bucket */

    sts = ix_readbkt (rab, NULL, vbn, &bkt);
    if (sts != IX_SUCCESS) return (sts);

    /* Make sure it looks like a free bucket */

    if ((bkt -> rec[0].size != 0) 
     || (bkt -> rec[0].offset != rab -> fhd -> bks) 
     || (bkt -> rec[0].keysize != 0) 
     || (bkt -> vbn != vbn) 
     || ((bkt -> krf != 0) && (bkt -> krf != (uWord) (-1)))) {
      ix_errorlog (rab, "bucket %u on freelist, but does not look free", vbn);
      ix_relbkt (rab, khd, bkt);
      return (IX_INVBKTPNTR);
    }

    /* Ok, update file header to point to next free one on list */

    rab -> fhd -> free = bkt -> rec[0].left;
  }

  /* Otherwise, the file will have to be extended */

  else {

    /* Make sure eof is on a correct bucket boundary */

    sts = checkbktvbn (rab, rab -> fhd -> eof);
    if (sts != IX_SUCCESS) return (sts);

    /* Make new cache entry for the bucket */

    sts = makecache (rab);			/* get new cache & bkt */
    if (sts != IX_SUCCESS) return (sts);
    cache = rab -> cache;			/* point to new cache entry */
    cache -> khd = khd;				/* save bucket's khd */
    bkt = cache -> bkt;				/* get bkt pointer */
    cache -> bkt = NULL;			/* mark it "checked out" */

    /* Clear the header portion, including one record descriptor */

    memset (bkt, 0, sizeof *bkt);

    /* Make the record's vbn = current eof mark */

    cache -> vbn = bkt -> vbn = rab -> fhd -> eof;

    /* Increment eof position by number of blocks in bucket */

    rab -> fhd -> eof += rab -> fhd -> bks / rab -> fhd -> bls;
  }

  /* Anyway, set up initial offset to end of bucket */

  bkt -> rec[0].offset = rab -> fhd -> bks;

  /* Mark bucket as belonging to new key */

  bkt -> krf = khd - rab -> fhd -> khd + 1;

  /* Write file header back out to disk with either */
  /* new free list pointer or new eof position      */

  sts = ix_writefhd (rab);

  /* Return bucket pointer or error status */

  if (sts != IX_SUCCESS) ix_relbkt (rab, khd, bkt);
  else *rbkt = bkt;
  return (sts);
}

/************************************************************************/
/*									*/
/*  Read bucket in from disk						*/
/*									*/
/*  Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = corresponding khd address					*/
/*	      (NULL if being called from allocbkt)			*/
/*	vbn = vbn of bucket to be read					*/
/*	bktr = where to return bucket pointer				*/
/*									*/
/*  Output:								*/
/*									*/
/*	readbkt = IX_SUCCESS : success					*/
/*	          else : error						*/
/*	*bktr = bucket buffer address					*/
/*									*/
/************************************************************************/

uLong ix_readbkt (Rab *rab, Khd *khd, Vbn vbn, Bkt **bktr)

{
  Bkt *bkt;
  Cache *cache;
  Khd *khd0;
  uLong sts;
  Rsz bktkrf;

  *bktr = NULL;					/* assume failure */

  khd0 = rab -> fhd -> khd;			/* point to primary key hdr */
  if (khd == NULL) bktkrf = (Rsz) (-1);			/* get bkt.krf value */
  else bktkrf = khd - khd0 + 1;

  /* Get cache entry for it */

  sts = findcache (rab, vbn, &cache);		/* scan the cache */
  if (sts == IX_SUCCESS) {			/* if entry found */
    bkt = cache -> bkt;				/* get bucket pointer */
    if (bkt == NULL) {				/* see if already checked out */
      ix_errorlog (rab, "bucket %u already in use", vbn);
      return (IX_BKTINUSE);			/* if so, return error status */
    }
    if (cache -> khd != khd) {			/* see if key ok */
      if (cache -> khd == NULL) 
            ix_errorlog (rab, "bucket %u is free, not key %u", vbn, khd - khd0);
      else if (khd == NULL) 
                     ix_errorlog (rab, "bucket %u belongs to key %u, not free", 
                                                      vbn, cache -> khd - khd0);
      else ix_errorlog (rab, "bucket %u belongs to key %u, not %u", 
                                          vbn, cache -> khd - khd0, khd - khd0);
      return (IX_INVBKTPNTR);
    }
    if ((bkt -> krf != 0) && (bkt -> krf != bktkrf)) {
      ix_errorlog (rab, "bucket %u is marked key %u, not %u", 
                                               vbn, bkt -> krf - 1, bktkrf - 1);
      return (IX_INVBKTPNTR);
    }
    cache -> bkt = NULL;			/* ok, check it out */
    *bktr = bkt;				/* return bucket pointer */
    return (IX_SUCCESS);			/* return success */
  }
  if (sts != IX_CACHENTNTFND) return (sts);

  /* Not in cache, make a new cache entry */

  sts = makecache (rab);
  if (sts != IX_SUCCESS) return (sts);

  cache = rab -> cache;
  cache -> khd = khd;
  cache -> vbn = vbn;
  bkt = cache -> bkt;
  cache -> bkt = NULL;

  /* Read bucket from disk */

  sts = ix_os_readit (rab, vbn, rab -> fhd -> bks, (Rbf *) bkt);

  /* Validate the bucket */

  if (sts == IX_SUCCESS) sts = ix_checkbkt (rab, khd, bkt, 0);
  if ((sts == IX_SUCCESS) && (bkt -> vbn != vbn)) {
    ix_errorlog (rab, "bucket at vbn %u has vbn %u", vbn, bkt -> vbn);
    sts = IX_INVBKTVBN;
  }

  /* If failure, free bucket and cache entry */

  if (sts != IX_SUCCESS) {
    rab -> maxcache ++;
    rab -> cache = cache -> next;
    ix_free (bkt);
    ix_free (cache);
    bkt = NULL;
  }

  /* Return bucket pointer and status */

  *bktr = bkt;
  return (sts);
}

/************************************************************************/
/*									*/
/*  Read bucket in from disk, but don't cache and don't check certain 	*/
/*  stuff								*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = corresponding khd address					*/
/*	vbn = vbn of bucket to be read					*/
/*	bktr = where to return bucket pointer				*/
/*									*/
/*    Output:								*/
/*									*/
/*	readbkt = IX_SUCCESS : success					*/
/*	          else : error						*/
/*	*bktr = bucket buffer address					*/
/*									*/
/************************************************************************/

uLong ix_readbktfix (Rab *rab, Khd *khd, Vbn vbn, Bkt **bktr)

{
  Bkt *bkt;
  uLong sts;

  *bktr = NULL;					/* assume failure */

  /* Read bucket from disk */

  bkt = ix_malloc (rab -> fhd -> bks);		/* alloc bucket mem */
  sts = ix_os_readit (rab, vbn, rab -> fhd -> bks, (Rbf *) bkt);

  /* Validate the bucket */

  if (sts == IX_SUCCESS) sts = ix_checkbkt (rab, khd, bkt, 1);

  /* If failure, free bucket */

  if (sts != IX_SUCCESS) {
    ix_free (bkt);
    bkt = NULL;
  }

  /* Return bucket pointer and status */

  *bktr = bkt;
  return (sts);
}

/************************************************************************/
/*									*/
/*  Free a bucket for re-use later					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = file's rab						*/
/*	khd = key header for bucket					*/
/*	bkt = pointer to bucket						*/
/*									*/
/*    Output:								*/
/*									*/
/*	freebkt = IX_SUCCESS : successfully freed			*/
/*	          else : I/O error					*/
/*	bkt is no longer usable						*/
/*	corresponding bucket is no longer usable			*/
/*									*/
/************************************************************************/

uLong ix_freebkt (Rab *rab, Khd *khd, Bkt *bkt)

{
  uLong sts;

  bkt -> rec[0].size = 0;			/* bucket contains no records */
  bkt -> rec[0].offset = rab -> fhd -> bks;	/* set up offset for checkbkt */
  bkt -> rec[0].left = rab -> fhd -> free;	/* link to other free buckets */
  bkt -> rec[0].keysize = 0;			/* no key size */
  bkt -> krf = (Rsz) (-1);				/* mark it free */
  sts = ix_writebkt (rab, NULL, bkt);		/* try to write bucket */
  if (sts == IX_SUCCESS) {
    rab -> fhd -> free = bkt -> vbn;		/* save the vbn on top */
    sts = ix_writefhd (rab);			/* try to write header */
  }
  return (sts);
}

/************************************************************************/
/*									*/
/*  Flag bucket for write back out to disk				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab address						*/
/*	khd = corresponding khd address					*/
/*	      (NULL if being called from freebkt)			*/
/*	bkt = bucket buffer address					*/
/*									*/
/*    Output:								*/
/*									*/
/*	writebkt = IX_SUCCESS : success					*/
/*	           else : error						*/
/*									*/
/*	bkt no longer valid						*/
/*									*/
/************************************************************************/

uLong ix_writebkt (Rab *rab, Khd *khd, Bkt *bkt)

{
  uLong sts;

  /* Flag it for write to disk, don't release bucket */

  sts = ix_writebktnr (rab, khd, bkt);

  /* Free bucket buffer */

  ix_relbkt (rab, khd, bkt);

  /* Return status */

  return (sts);
}

uLong ix_writebktnr (Rab *rab, Khd *khd, Bkt *bkt)

{
  Cache *cache;
  Khd *khd0;
  uLong *ckpt, cksm, sts;
  Rsz i;

  khd0 = rab -> fhd -> khd;			/* point to primary key hdr */

  /* Find cache entry */

  sts = findcache (rab, bkt -> vbn, &cache);
  if ((sts == IX_SUCCESS) && (cache -> bkt != NULL)) sts = IX_NOTCHECKEDOUT;
  if (sts != IX_SUCCESS) {
    ix_errorlog (rab, "error retrieving %u from cache - %s", 
                                                  bkt -> vbn, ix_errlist (sts));
    return (sts);
  }
  if ((khd != NULL) && (cache -> khd != NULL) && (cache -> khd != khd)) {
    ix_errorlog (rab, "bucket %u belongs to key %u, not %u", 
                                   bkt -> vbn, cache -> khd - khd0, khd - khd0);
    return (IX_INVBKTPNTR);
  }
  cache -> khd = khd;

  /* If bkt -> krf is zero (old format), put in new format value */

  if (bkt -> krf == 0) {
    if (khd == NULL) bkt -> krf = (Rsz) (-1);
    else bkt -> krf = khd - khd0 + 1;
  }

  /* Generate new checksum */

  bkt -> cksm = cksm = 0;
  ckpt = (uLong *) bkt;
  for (i = 0; i < rab -> fhd -> bks; i += sizeof cksm) cksm += *(ckpt ++);
  bkt -> cksm = cksm;

  /* Check bucket to see if it's valid before we write it out */

  sts = ix_checkbkt (rab, khd, bkt, 0);

  /* Flag it for write to disk */

  if (sts == IX_SUCCESS) cache -> write = 1;

  /* Return status */

  return (sts);
}

/************************************************************************/
/*									*/
/*  Release bucket without flagging it for write			*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	khd = address of corresponding key's header			*/
/*	bkt = address of bucket being freed				*/
/*									*/
/*    Output:								*/
/*									*/
/*	bkt is no longer valid						*/
/*									*/
/************************************************************************/

uLong ix_relbkt (Rab *rab, Khd *khd, Bkt *bkt)

{
  Cache *cache;
  uLong sts;

  sts = ix_checkbkt (rab, khd, bkt, 0);
  if (sts == IX_SUCCESS) sts = findcache (rab, bkt -> vbn, &cache);
  if ((sts == IX_SUCCESS) && (cache -> bkt != NULL)) sts = IX_NOTCHECKEDOUT;
  if (sts == IX_SUCCESS) cache -> bkt = bkt;
  else ix_errorlog (rab, "error releasing %u to cache - %s", 
                                                  bkt -> vbn, ix_errlist (sts));
  return (sts);
}

/************************************************************************/
/*									*/
/*  Read file header from disk						*/
/*									*/
/*  Input:								*/
/*									*/
/*	rab = rab address						*/
/*									*/
/*  Output:								*/
/*									*/
/*	readfhd = IX_SUCCESS : success					*/
/*	          else : error						*/
/*									*/
/************************************************************************/

uLong ix_readfhd (Rab *rab)

{
  uLong sts;

  /* Say it doesn't have to be written to disk */

  rab -> writefhd = 0;

  /* Read it from disk into rab */

  sts = ix_os_readit (rab, 0, rab -> fhdbsz, (Rbf *) (rab -> fhd));

  /* Check it out */

  if (sts == IX_SUCCESS) sts = ix_checkfhd (rab);

  return (sts);
}

/************************************************************************/
/*									*/
/*  Flag file header for write back out to disk				*/
/*									*/
/*  Input:								*/
/*									*/
/*	rab = rab address						*/
/*									*/
/*  Output:								*/
/*									*/
/*	writefhd = IX_SUCCESS : success					*/
/*	           else : error						*/
/*									*/
/************************************************************************/

uLong ix_writefhd (Rab *rab)

{
  Fhd *fhd;
  uLong *ckpt, cksm, sts;
  Rsz i;

  fhd = rab -> fhd;

  /* Generate new checksum */

  fhd -> cksm = cksm = 0;
  ckpt = (uLong *) fhd;
  for (i = 0; i < rab -> fhdrsz; i += sizeof cksm) cksm += *(ckpt ++);
  fhd -> cksm = cksm;

  /* Check it out */

  sts = ix_checkfhd (rab);

  /* If ok, flag it for write to disk later */

  if (sts == IX_SUCCESS) rab -> writefhd = 1;

  return (sts);
}

/************************************************************************/
/*									*/
/*  This routine flushes the cache back out to the file.		*/
/*  It MUST be called at the end of every high-level routine if any 	*/
/*  file contents have been accessed (even if read-only).		*/
/*  This routine also unlocks the file and disables memory access.	*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab pointer						*/
/*	status = status to be returned to caller			*/
/*	update = 0 : high level error, wipe cache, restore file header	*/
/*	         1 : high level success, perform updates, if any	*/
/*									*/
/*    Output:								*/
/*									*/
/*	flush = usually same as status, or modified with any errors 	*/
/*	        detected by this routine				*/
/*									*/
/************************************************************************/

uLong ix_flush (Rab *rab, uLong status, int update)

{
  Cache *cp, *rabcp;
  uLong sts;

  /* Make sure all cache entries are checked back in.  The  */
  /* only one that may be left is pointed to by rab -> crp. */

  rabcp = NULL;
  for (cp = rab -> cache; cp != NULL; cp = cp -> next) {				/* loop through all cache entries */
    if (cp -> bkt != NULL) continue;							/* if already checked back in, skip it */
    if ((rab -> crp.bkt != NULL) && (rab -> crp.bkt -> vbn == cp -> vbn)) rabcp = cp;	/* if it's the rab crp bucket, it's ok for it to still be checked out */
    else {
      ix_errorlog (rab, "bucket %u not checked back in", cp -> vbn);			/* otherwise, output error message */
      if (update) status = IX_NOTCHECKEDIN;						/* error only if we are going to write disk */
    }
  }
  if (status == IX_NOTCHECKEDIN) {
    ix_memen (0);
    return (IX_NOTCHECKEDIN);
  }

  /* If update is clear, cache contents are trash, so */
  /* wipe it.  Also restore file header from disk.    */

  if (!update) {
    ix_relcrp (rab, &(rab -> crp));
    ix_os_unlockcrp (rab);
    sts = ix_wipecache (rab);
    if (sts != IX_SUCCESS) status = sts;
  }

  /* If update is set, write cache contents back  */
  /* to disk, including the rab -> crp.bkt bucket */

  else if (rab -> batchlvl == 0) {
    if (rabcp != NULL) rabcp -> bkt = rab -> crp.bkt;
    sts = ix_flushwrites (rab, 1);
    if (rabcp != NULL) rabcp -> bkt = NULL;
    if (sts != IX_SUCCESS) status = sts;
  }

  /* Unlock file access */

  ix_unlockfile (rab);

  /* Return composite status */

  ix_memen (0);
  return (status);
}

/************************************************************************/
/*									*/
/*  Flush any pending writes to disk					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	writefhd = 0 : do not write file header out to disk		*/
/*	           1 : write file header out to disk 			*/
/*	               (if it was modified)				*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_flushwrites = IX_SUCCESS : successful			*/
/*	                       else : error status			*/
/*									*/
/************************************************************************/

uLong ix_flushwrites (Rab *rab, int writefhd)

{
  Cache *cp, **writes;
  int update;
  uLong i, j, nwrites, status, sts;

  /* If there are any blocks to be written, sort by ascending vbn */

  update  = 1;
  status  = IX_SUCCESS;
  nwrites = 0;
  writes  = NULL;

  for (cp = rab -> cache; cp != NULL; cp = cp -> next) 
                           if ((cp -> bkt != NULL) && (cp -> write)) nwrites ++;

  if (nwrites != 0) {
    writes = ix_malloc (sizeof *writes * nwrites); /* alloc array */
    i = 0;					/* fill it in */
    for (cp = rab -> cache; cp != NULL; cp = cp -> next) 
                     if ((cp -> bkt != NULL) && (cp -> write)) writes[i++] = cp;
    for (j = nwrites - 1; j > 0; j --) {	/* do a single bubble sort */
      sts = 1;					/* (nothing switched yet) */
      for (i = 0; i < j; i ++) {		/* check each entry */
        if (writes[i] -> vbn > writes[i+1] -> vbn) { /* see if in order */
          cp = writes[i];			/* if not, switch them */
          writes[i] = writes[i+1];
          writes[i+1] = cp;
          sts = 0;				/* remember something switched */
        }
      }
      if (sts) break;				/* done if nothing switched */
    }
  }

  /* Write updates to file and free list array */

  if (update) status = ix_os_fluwri (rab, writes, nwrites, writefhd);
  if (writes != NULL) ix_free (writes);

  /* Return status */

  return (status);
}

/************************************************************************/
/*									*/
/*  Wipe out the cache blocks						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = rab pointer						*/
/*									*/
/*    Output:								*/
/*									*/
/*	cache blocks freed off without being written to disk		*/
/*	file header re-read from disk					*/
/*									*/
/************************************************************************/

uLong ix_wipecache (Rab *rab)

{
  Cache *cp;
  uLong sts;

  while ((cp = rab -> cache) != NULL) {		/* while there are entries */
    rab -> cache = cp -> next;			/* unlink top one */
    if (cp -> bkt != NULL) ix_free (cp -> bkt); /* free the bucket */
    ix_free (cp);				/* free the cache entry */
    rab -> maxcache ++;				/* one less cache entry used */
  }

  sts = ix_readfhd (rab);			/* read file header */
  return (sts);
}

/************************************************************************/
/*									*/
/*  This routine checks a bucket for many things to make sure it is ok	*/
/*									*/
/*  Input:								*/
/*									*/
/*	rab = pointer to corresponding rab				*/
/*	khd = pointer to corresponding key header			*/
/*	      (NULL if free bucket)					*/
/*	bkt = pointer to bucket node to be checked			*/
/*									*/
/*  Output:								*/
/*									*/
/*	ix_checkbkt = IX_SUCCESS : bucket is ok				*/
/*	                    else : bucket is bad			*/
/*									*/
/************************************************************************/

uLong ix_checkbkt (Rab *rab, Khd *khd, Bkt *bkt, int fix)

{
  Byte *msg;
  uLong *ckpt, cksm, sts;
  Rbf *bbkt;
  Rec *rec;
  Rsz bktkrf, cksz, i, kof, ksz, ofs, rsz;

  /* Check the bucket's checksum */

  cksm = 0 - (bkt -> cksm);
  ckpt = (uLong *) bkt;
  for (i = 0; i < rab -> fhd -> bks; i += sizeof cksm) cksm += *(ckpt ++);
  if (cksm != bkt -> cksm) goto ivcksm;

  /* Check the bucket's vbn */

  sts = checkbktvbn (rab, bkt -> vbn);
  if (sts != IX_SUCCESS) return (sts);

  /* Check the bucket's krf */

  if (khd == NULL) bktkrf = (Rsz) (-1);			/* get bkt.krf */
  else bktkrf = khd - rab -> fhd -> khd + 1;
  if ((bkt -> krf != 0) && (bkt -> krf != bktkrf)) { /* validate krf */
    ix_errorlog (rab, "bucket %u marked key %u, not %u", 
                      bkt -> vbn, bkt -> krf - 1, bktkrf - 1);
    return (IX_INVBKTPNTR);
  }

  /* Check its offsets and sizes */

  if (khd == NULL) return (IX_SUCCESS);

  bbkt = (Rbf *) bkt;				/* get a byte-cast pointer */
  ofs = rab -> fhd -> bks;			/* recs start at end of bkt */
  ksz = khd -> ksz;				/* get key size */
  kof = khd -> kof;				/* get key offset */
  if (khd != rab -> fhd -> khd) kof = 0;	/* alt keys use zero */
  for (i = 0;; i ++) {				/* step through rec's */
    rec = bkt -> rec + i;
    msg = "compressed key size too big";
    cksz = rec -> keysize;			/* get compressed key size */
    if (cksz > ksz) goto ivfmt;			/* keysize must be .le. max */
    msg = "record size too big for bucket";
    rsz = (rec -> size + 3) & -4;		/* get compressed record size */
						/* ... rounded to longword */
    if (rsz > ofs) goto ivfmt;			/* don't have room for it */
    msg = "data offset out of order";
    ofs -= rsz;					/* dec offset by rec size */
    if (rec -> offset != ofs) goto ivfmt;	/* invalid if not same as */
						/* ... descriptor's offset */
    if (rsz == 0) break;			/* stop if hit end mark */
    msg = "key goes off end of record";
    if (cksz + kof > rsz) goto ivfmt;		/* key must not go off end */
    msg = "missing rfa";			/* must have an rfa */
    if (!fix && (rec -> rfa.vbn == 0)) goto ivfmt;
    msg = "key value out of order";
    if (!fix && (i > 0) 			/* see if key values in order */
     && (ix_compare_keys (bkt -> rec[i-1].keysize, 
                          bbkt + ofs + rsz + kof, 
                          &(bkt -> rec[i-1].rfa), 
                          cksz, 
                          bbkt + ofs + kof, 
                          &(rec -> rfa), 
                          khd -> kat) > 0)) goto ivfmt;
  }
  msg = "data overlaps descriptors";
  if (ofs < (Rbf *) (bkt -> rec + i + 1) 	/* invalid if offset overlaps */
                            - bbkt) goto ivfmt;	/* ... on top of descriptors */

  return (IX_SUCCESS);

  /* Error returns */

ivcksm:
  ix_errorlog (rab, "bucket %u has invalid checksum", bkt -> vbn);
  return (IX_INVBKTCKSM);

ivfmt:
  ix_errorlog (rab, "bucket %u rec %u invalid - %s", bkt -> vbn, i, msg);
  return (IX_INVBKTFMT);
}

/************************************************************************/
/*									*/
/*  Make sure a bucket's vbn is on a correct boundary			*/
/*									*/
/************************************************************************/

static uLong checkbktvbn (Rab *rab, Vbn vbn)

{
  if ((vbn - (rab -> fhdbsz / rab -> fhd -> bls)) 
                               % (rab -> fhd -> bks / rab -> fhd -> bls) != 0) {
    ix_errorlog (rab, "bucket vbn %u is invalid", vbn);
    return (IX_INVBKTVBN);
  }
  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  This routine checks the file header to make sure it is ok		*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to corresponding rab				*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_checkfhd = IX_SUCCESS : bucket is ok				*/
/*	                    else : bucket is bad			*/
/*									*/
/************************************************************************/

uLong ix_checkfhd (Rab *rab)

{
  Fhd *fhd;
  uLong *ckpt, cksm;
  Rsz i;

  /* Check the header's checksum */

  fhd = rab -> fhd;
  cksm = 0 - (fhd -> cksm);
  ckpt = (uLong *) fhd;
  for (i = 0; i < rab -> fhdrsz; i += sizeof cksm) cksm += *(ckpt ++);
  if (cksm != fhd -> cksm) {
    ix_errorlog (rab, "file header has invalid checksum");
    return (IX_INVFHDCKSM);
  }

  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  Make a new cache entry						*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	cacher = where to return pointer to entry			*/
/*									*/
/*    Output:								*/
/*									*/
/*	makecache = IX_SUCCESS : successful				*/
/*	            else : error status					*/
/*	rab -> cache = address of new cache entry			*/
/*									*/
/************************************************************************/

static uLong makecache (Rab *rab)

{
  Cache *cache, **cachl, **cl, *ce;

  /* If we have exceeded maximum limit, free off an old entry. */
  /* If there aren't any old ones, overrun the limit.          */

  cache = NULL;
  if (rab -> maxcache < 0) {
    cl = &(rab -> cache);
    for (ce = *cl; ce != NULL; ce = *cl) {
      if ((ce -> bkt != NULL) && !(ce -> write)) {
        cache = ce;
        cachl = cl;
      }
      cl = &(ce -> next);
    }
  }

  /* Either unlink the old one or allocate a new one */

  if (cache != NULL) {				/* see if we got an old one */
    *cachl = cache -> next;			/* if so, unlink it */
  } else {
    cache = ix_calloc (sizeof *cache);		/* no old ones, alloc new */
    if (cache == NULL) return (IX_NOMEMORY);	/* maybe ran out of memory */
    cache -> bkt = ix_malloc (rab -> fhd -> bks); /* alloc bucket mem */
    if (cache -> bkt == NULL) {
      ix_free (cache);				/* ran out of memory */
      return (IX_NOMEMORY);
    }
    rab -> maxcache --;				/* decrement counter */
  }

  /* Either way, fix it up for new bucket */

  cache -> bkt -> vbn = (Rsz) (-1);
  cache -> next = rab -> cache;
  rab -> cache = cache;

  return (IX_SUCCESS);
}

/************************************************************************/
/*									*/
/*  Find entry in cache and move it to top if found			*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = address of rab						*/
/*	vbn = vbn of entry to look for					*/
/*	cacher = where to return pointer to entry found			*/
/*									*/
/*    Output:								*/
/*									*/
/*	findcache = IX_SUCCESS : entry found				*/
/*	            IX_CACHENTNTFND : entry not found			*/
/*	            else : error status					*/
/*									*/
/************************************************************************/

static uLong findcache (Rab *rab, Vbn vbn, Cache **cacher)

{
  Cache **cl, *cp;

  cl = &(rab -> cache);				/* save last link */
  for (cp = *cl; cp != NULL; cp = *cl) {	/* scan through list */
    if (cp -> vbn == vbn) {			/* see if vbn matches */
      *cl = cp -> next;				/* ok, unlink entry from list */
      cp -> next = rab -> cache;		/* link it back on top */
      rab -> cache = cp;
      *cacher = cp;				/* return entry pointer */
      return (IX_SUCCESS);			/* return success status */
    }
    cl = &(cp -> next);				/* save last link */
  }
  return (IX_CACHENTNTFND);			/* end, return not found sts */
}

/************************************************************************/
/*									*/
/*  Count the number of writes pending in cache				*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_countwrites = number of writes pending in cache		*/
/*									*/
/************************************************************************/

int ix_countwrites (Rab *rab)

{
  Cache *cp;
  int wc;

  wc = 0;
  for (cp = rab -> cache; cp != NULL; cp = cp -> next) wc += cp -> write;
  return (wc);
}
