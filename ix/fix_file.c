/************************************************************************/
/*									*/
/*  This routine fixes a file						*/
/*									*/
/*  It does this by creating a new output file then copying the 	*/
/*  records from all the good primary buckets to the output file	*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <stdio.h>
#include <string.h>

/************************************************************************/
/*									*/
/*  Fix corrupt file							*/
/*									*/
/*    Input:								*/
/*									*/
/*	ifspec = filespec of input file					*/
/*	ofspec = filespec of output file				*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_fix_file = status						*/
/*									*/
/************************************************************************/

uLong ix_fix_file (const Byte *ifspec, 
                   const Byte *ofspec)

{
  Bkt *bkt;
  int i, krf, total;
  Kat *kat;
  uLong sts;
  Rab *irab, *orab;
  Rbf *rbf;
  Rsz *kof, *ksz, nky, obks, rsz1, rsz2, rsz3;
  Vbn ivbn;

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
  for (krf = 0; krf < nky; krf ++) {
    printf ("[%d] ksz %u, kof %u, kat %x\n", krf, ksz[krf], kof[krf], kat[krf]);
  }

  obks = irab -> fhd -> bks;
  sts = ix_create_file3 (ofspec, nky, ksz, kof, kat, obks, 
                         irab -> fhd -> mrs, 10 * nky, (void **)&orab, 
                         IX_SHARE_N, 0, NULL);

  ix_free (ksz);
  ix_free (kof);
  ix_free (kat);

  if (sts != IX_SUCCESS) {
    ix_close_file (irab);
    return (sts);
  }

  obks = orab -> fhd -> bks;

  ix_messlog (orab, "bucket size %u, block size %u", obks, orab -> fhd -> bls);

  /* Scan input file for good primary key buckets */
  /* and write the records to output file         */

  rbf = ix_malloc (irab -> fhd -> mrs);		/* alloc record buffer */
  total = 0;

  for (ivbn = irab -> fhdbsz / irab -> fhd -> bls; ivbn < irab -> fhd -> eof; 
                              ivbn += irab -> fhd -> bks / irab -> fhd -> bls) {

    /* Read next bucket in file.  Skip it if bad or not primary key. */

    sts = ix_readbktfix (irab, irab -> fhd -> khd, ivbn, &bkt);
    if ((sts != IX_SUCCESS) || (bkt -> krf != 1)) continue;

    /* Ok, scan each record in the bucket and copy to output file */

    for (i = 0; bkt -> rec[i].size != 0; i ++) {

      /* First segment is up to and including compressed key */

      rsz1 = irab -> fhd -> khd[0].kof + bkt -> rec[i].keysize;

      /* Second segment is the number of zeroes compressed off key */

      rsz2 = irab -> fhd -> khd[0].ksz - bkt -> rec[i].keysize;

      /* Third segment is the rest of the record after the key */

      rsz3 = bkt -> rec[i].size - rsz1;

      /* Make an uncompressed record */

      memcpy (rbf, ((Rbf *) bkt) + bkt -> rec[i].offset, rsz1);
      memset (rbf + rsz1, 0, rsz2);
      memcpy (rbf + rsz1 + rsz2, 
              ((Rbf *) bkt) + bkt -> rec[i].offset + rsz1, rsz3);

      /* Insert record in output file */

      sts = ix_insert_rec (orab, (Rsz) (rsz1 + rsz2 + rsz3), rbf);
      if (sts != IX_SUCCESS) {
        ix_errorlog (orab, "error copying record to output file - %s", 
                                                              ix_errlist (sts));
        ix_relbkt (irab, irab -> fhd -> khd, bkt);
        ix_close_file (irab);
        ix_close_file (orab);
        goto done;
      }
    }
    ix_free (bkt);
    ix_messlog (irab, "bucket %u contains %d record(s)", ivbn, i);
    total += i;
  }

  ix_messlog (orab, "total of %d records written", total);
  ix_close_file (irab);
  sts = ix_close_file (orab);
  if (sts != IX_SUCCESS) ix_errorlog (orab, "error closing output file\n%s", 
                                                              ix_errlist (sts));

done:
  ix_free (rbf);
  return (sts);
}
