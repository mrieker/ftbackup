/************************************************************************/
/*									*/
/*  This program sequentially deletes all the records from a file	*/
/*									*/
/************************************************************************/

#include "ixinternal.h"
#include <stdio.h>

int main (int argc, char *argv[])

{
  IX_Rbf recbuf[32256];
  IX_Rsz reclen;
  IX_Vbn reccount[16];
  int i, recnum, stopat;
  unsigned int sts;
  Rab *ixrab;
  
  if (argc != 2) {
    fprintf (stderr, "usage: %s <filename>\n", argv[0]);
    return (1);
  }

  printf ("opening %s ...\n", argv[1]);  
  sts = ix_open_file (argv[1], 0, 10, &ixrab);
  if (sts != IX_SUCCESS) {
    fprintf (stderr, "error opening %s - %s\n", argv[1], ix_errlist (sts));
    return (2);
  }

  printf ("verifying %s ...\n", argv[1]);
  sts = ix_validate_file (ixrab, reccount);
  if (sts != IX_SUCCESS) {
    fprintf (stderr, "error validating file - %s\n", ix_errlist (sts));
    return (3);
  }
  printf ("... file has %d records\n", reccount[0]);
  stopat = reccount[0];
    
  recnum = 0;
  while (recnum < stopat) {
    if ((++ recnum % 1000) == 0) printf ("%d ...\n", recnum);
    sts = ix_search_seq (ixrab, 1, sizeof recbuf, recbuf, &reclen, 0, NULL);
    if (sts != IX_SUCCESS) {
      fprintf (stderr, "error reading record %d - %s\n", 
                                                      recnum, ix_errlist (sts));
      return (4);
    }

    if (ixrab -> crp.idx != 0) {
      fprintf (stderr, "have non-zero index for record %d\n", recnum);
      return (5);
    }
    if (ixrab -> crp.bkt -> rec[0].left != 0) {
      fprintf (stderr, "have non-zero left link for record %d\n", recnum);
      return (7);
    }
    for (i = 0; i < ixrab -> crp.depth; i ++) if (ixrab -> crp.paridx[i] != 0) {
      fprintf (stderr, "depth[%d] non-zero index for record %d\n", i, recnum);
      return (6);
    }

    sts = ix_remove_rec (ixrab);
    if (sts != IX_SUCCESS) {
      fprintf (stderr, "error deleting record %d - %s\n", 
                                                      recnum, ix_errlist (sts));
      return (3);
    }
  }

  printf ("verifying %s ...\n", argv[1]);
  sts = ix_validate_file (ixrab, reccount);
  if (sts != IX_SUCCESS) {
    fprintf (stderr, "error validating file - %s\n", ix_errlist (sts));
    return (3);
  }
  printf ("... file has %d records\n", reccount[0]);
  
  return (0);
}
  
  
  
  
