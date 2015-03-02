/************************************************************************/
/*									*/
/*  This routine is called to close an open file			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

/************************************************************************/
/*									*/
/*  Close file								*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = address of rab of file to be closed			*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_close_file = status						*/
/*	rabv is no longer valid						*/
/*									*/
/************************************************************************/

uLong ix_close_file (void *rabv)

{
  uLong sts;
  Rab *rab;

  rab = rabv;

  /* Close off any batch */

  sts = ix_setbatch (rab, 0);

  /* Release current record pointer */

  ix_memen (1);
  ix_relcrp (rab, &(rab -> crp));
  ix_os_unlockcrp (rab);

  /* Wipe cache */

  ix_wipecache (rab);

  /* Close the file */

  ix_os_clofil (rab);

  /* Release memory */

  ix_free (rab -> fhd);
  ix_free (rab);
  ix_memen (0);

  return (sts);
}
