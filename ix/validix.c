/************************************************************************/
/*									*/
/*  This program validates the contents of a file			*/
/*									*/
/************************************************************************/

#include "ix.h"

#include <stdio.h>

int main (int argc, char **argv)

{
  int i, n, sts;
  void *rab;
  IX_Rsz bks, mrs, kof[256], ksz[256], nky;
  IX_Vbn counts[256], depths[256];

  /* Get filespec from first arg */

  if (argc < 2) {
    printf ("Usage: %s <filespec>\n", argv[0]);
    return (1);
  }

  for (i = 1; i < argc; i ++) {
    sts = ix_open_file (argv[i], 1, 10, &rab);
    if (sts != IX_SUCCESS) {
      printf ("Open file %s - %s\n", argv[i], ix_errlist (sts));
      continue;
    }

    ix_inquire_file (rab, &nky, ksz, kof, &bks, &mrs);
    printf ("  Number of keys: %d\n", nky);
    printf ("     Bucket size: %d\n", bks);
    printf ("    Max rec size: %d\n", mrs);

    ix_close_file (rab);
    sts = ix_open_file (argv[i], 1, 10 * nky, &rab);
    if (sts != IX_SUCCESS) {
      printf ("Re-open file %s - %s\n", argv[i], ix_errlist (sts));
      continue;
    }

    printf ("Validating %s ...\n", argv[i]);

    sts = ix_validate_file (rab, counts, depths);
    if (sts != IX_SUCCESS) return (3);
    for (n = 0; n < nky; n ++) 
               printf ("  Key %d:  size %d, offset %d, records %u, depth %u\n", 
                                       n, ksz[n], kof[n], counts[n], depths[n]);

    ix_close_file (rab);
    printf ("\n");
  }

  return (0);
}
