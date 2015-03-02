/************************************************************************/
/*									*/
/*  Test program to compress file					*/
/*									*/
/*    Input:								*/
/*									*/
/*	argv[1] = input filespec					*/
/*	argv[2] = output filespec					*/
/*	argv[3] = new bucket size (0 to keep same)			*/
/*	argv[4] = fill percentage					*/
/*									*/
/************************************************************************/

#include "ix.h"

#include <stdio.h>
#include <stdlib.h>

int main (int argc, char *argv[])

{
  int fillpct, newbks;
  unsigned int sts;

  if (argc != 5) {
    fprintf (stderr, 
              "usage: %s <in_file> <out_file> <new_bks> <fill_pct>\n", argv[0]);
    return (1);
  }

  newbks  = atoi (argv[3]);
  fillpct = atoi (argv[4]);
  sts = ix_compress_file (argv[1], argv[2], newbks, fillpct);
  fprintf (stderr, "Status: %s\n", ix_errlist (sts));
  return (sts != IX_SUCCESS);  
}
