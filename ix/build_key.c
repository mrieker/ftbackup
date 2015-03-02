/************************************************************************/
/*									*/
/*  This routine builds a new key definition into a file		*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <string.h>

static uLong build_key (Rab *rab, Vbn vbn, Rsz krf, Rbf *prbf);

/************************************************************************/
/*									*/
/*  Build new key definition into file					*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = address of record access block				*/
/*	krf = new key of reference number				*/
/*	ksz = size of key						*/
/*	kof = offset of key in record					*/
/*	newmrs = 0 : keep old maximum record size			*/
/*	      else : set up new maximum record size			*/
/*									*/
/*  Output:								*/
/*									*/
/*	build_key = IX_SUCCESS : new key built				*/
/*	                  else : error					*/
/*									*/
/************************************************************************/

uLong ix_build_key (void *rabv, Rsz krf, Rsz ksz, Rsz kof, Rsz newmrs)

{
  Fhd *newfhd;
  uLong sts;
  Rab *rab;
  Rbf prbf[65536];
  Rsz newfhdrsz, oldfhdrsz, oldmrs;
  Vbn vbn;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  /* Make sure new maximum record size isn't too big */

  oldmrs = rab -> fhd -> mrs;
  if (newmrs == 0) newmrs = oldmrs;
  if ((rab -> fhd -> bks < newmrs + sizeof (Bkt) + 2 * sizeof (Rec)) 
   || (newmrs < oldmrs)) return (IX_INVRECSIZE);

  /* Make sure key size is non-zero and key doesn't go off end of record */

  if ((ksz == 0) || (ksz + kof > newmrs)) return (IX_INVKEYSIZE);

  /* Make sure key of reference is one greater than we have now */

  if (krf != rab -> fhd -> nky) return (IX_INVKEYREF);

  /* Make sure there is room for the larger file header */

  oldfhdrsz = rab -> fhdrsz;
  newfhdrsz = (Rbf *)(newfhd -> khd + krf + 1) - (Rbf *)(newfhd);
  if (newfhdrsz > rab -> fhdbsz) return (IX_INVFILHDR);

  /* Release any previous sequential search context */

  ix_memen (1);
  ix_relcrp (rab, &(rab -> crp));
  rab -> nosqf = 0;

  /* Make a new file header with one more key */

  newfhd = ix_calloc (newfhdrsz);
  memcpy (newfhd, rab -> fhd, oldfhdrsz);

  newfhd -> khd[krf].vbn = 0;
  newfhd -> khd[krf].ksz = ksz;
  newfhd -> khd[krf].kof = kof;
  newfhd -> nky = krf + 1;

  ix_free (rab -> fhd);
  rab -> fhd    = newfhd;
  rab -> fhdrsz = newfhdrsz;
  sts = ix_writefhd (rab);
  if (sts != IX_SUCCESS) goto rtn;

  /* If the old maximum record size contains the new key, insert */
  /* all primary key tree records into new alternate key tree    */

  if (ksz + kof <= oldmrs) {
    sts = ix_lockkhd (rab, rab -> fhd -> khd + krf, 1); /* alt key hdr write */
    if (sts == IX_SUCCESS) {
      vbn = rab -> fhd -> khd -> vbn;		/* get root primary vbn */
      sts = build_key (rab, vbn, krf, prbf);	/* build alt key */
    }
  }

rtn:
  if (sts != IX_SUCCESS) rab -> fhdrsz = oldfhdrsz;
  return (ix_flush (rab, sts, sts == IX_SUCCESS));
}

/************************************************************************/
/*									*/
/*  This routine inserts all the records from a given primary bucket 	*/
/*  and all its children into an alternate key tree			*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab  = pointer to rab						*/
/*	vbn  = primary bucket's vbn					*/
/*	krf  = alternate key number					*/
/*	prbf = temporary record buffer					*/
/*	primary and alternate key headers locked			*/
/*									*/
/*    Output:								*/
/*									*/
/*	build_key = status						*/
/*	*prbf = scratch							*/
/*	records inserted into alternate key tree			*/
/*	primary and alternate key headers still locked			*/
/*									*/
/************************************************************************/

static uLong build_key (Rab *rab, Vbn vbn, Rsz krf, Rbf *prbf)

{
  Bkt *pbkt;
  Idx idx;
  Khd *khd, *pkhd;
  uLong sts;
  Rbf *pbas, *rbf1, *rbf2, *rbf3, *xrbf;
  Rec *prec;
  Rsz pkof, prsz, rsz1, rsz2, rsz3, xrsz;

  khd  = rab -> fhd -> khd + krf;		/* point to alt key header */
  pkhd = rab -> fhd -> khd;			/* point to pri key header */
  pkof = pkhd -> kof;				/* get offset of pri key */

  /* Read primary bucket into memory */

  sts = ix_readbkt (rab, pkhd, vbn, &pbkt);
  if (sts != IX_SUCCESS) return (sts);

  /* Insert all those records into new alternate key */

  for (idx = 0; pbkt -> rec[idx].size != 0; idx ++) {
    prec = pbkt -> rec + idx;			/* point to primary rec descr */
    pbas = ((Rbf *)pbkt) + prec -> offset;	/* point to base of pri rec */

    rsz1 = pkof + prec -> keysize;		/* first part is everthing */
    rbf1 = prbf;				/* up to primary key, */
						/* including compressed key */

    rsz2 = pkhd -> ksz - prec -> keysize;	/* second part is what was */
    rbf2 = rbf1 + rsz1;				/* compressed out of pri key */

    rsz3 = prec -> size - rsz1;			/* third part is the rest */
    rbf3 = rbf2 + rsz2;				/* of the record after key */

    memcpy (rbf1, pbas, rsz1);			/* copy first part */
    memset (rbf2, 0, rsz2);			/* zero fill second part */
    memcpy (rbf3, pbas + rsz1, rsz3);		/* copy third part */

    prsz = prec -> size + rsz2;			/* get primary record size */

    xrsz = khd -> ksz;				/* alt keys use just the key */
    xrbf = prbf + khd -> kof;			/* ... for their "record" */
    if (xrbf + xrsz <= prbf + prsz) {		/* if record contains the key */
      while ((xrsz > 0) && (xrbf[xrsz-1] == 0)) xrsz --; /* compress it */
      sts = ix_insert_key (rab, khd, xrsz, 0, 	/* insert it */
                           xrsz, xrbf, prec -> keysize, prbf + pkof, 
                           &(prec -> rfa));
      if (sts != IX_SUCCESS) break;
    }
  }

  /* Flush all those writes to disk, but don't flush the header yet */

  if (sts == IX_SUCCESS) sts = ix_flushwrites (rab, 0);

  /* Now process each child of the primary bucket */

  for (idx = 0; (sts == IX_SUCCESS) && (pbkt -> rec[idx].size != 0); idx ++) {
    vbn = pbkt -> rec[idx].left;
    if (vbn != 0) sts = build_key (rab, vbn, krf, prbf);
  }
  if (sts == IX_SUCCESS) {
    vbn = pbkt -> rec[idx].left;
    if (vbn != 0) sts = build_key (rab, vbn, krf, prbf);
  }

  /* Release primary bucket and return status */

  ix_relbkt (rab, pkhd, pbkt);
  return (sts);
}
