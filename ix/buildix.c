/************************************************************************/
/*									*/
/*  Test program to build a new key to a file				*/
/*									*/
/*    Input:								*/
/*									*/
/*	argv[1] = filespec						*/
/*	argv[2] = new key number					*/
/*	argv[3] = new key size						*/
/*	argv[4] = new key offset					*/
/*	argv[5] = new max record size (0 to keep same)			*/
/*									*/
/************************************************************************/

#include "ix.h"

#include <stdio.h>

int main (int argc, char *argv[])

{
  IX_Rsz newkrf, newksz, newkof, newmrs;
  IX_uLong sts;
  void *rab;

  if (argc != 6) {
    fprintf (stderr, 
             "usage: %s <file> <newkrf> <newksz> <newkof> <newmrs>\n", argv[0]);
    return (1);
  }

  newkrf = atoi (argv[2]);
  newksz = atoi (argv[3]);
  newkof = atoi (argv[4]);
  newmrs = atoi (argv[5]);
  sts = ix_open_file (argv[1], 0, 12, &rab);
  sts = ix_build_key (rab, newkrf, newksz, newkof, newmrs);
  fprintf (stderr, "Status: %s\n", ix_errlist (sts));
  ix_close_file (rab);
  return (sts != IX_SUCCESS);  
}
