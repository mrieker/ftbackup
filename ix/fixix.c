/************************************************************************/
/*									*/
/*  Test program to fix file						*/
/*									*/
/*    Input:								*/
/*									*/
/*	argv[1] = input filespec					*/
/*	argv[2] = output filespec					*/
/*									*/
/************************************************************************/

#include "ix.h"

#include <stdio.h>

int main (int argc, char *argv[])

{
  unsigned int sts;

  if (argc != 3) {
    fprintf (stderr, "usage: %s <in_file> <out_file>\n", argv[0]);
    return (1);
  }

  sts = ix_fix_file (argv[1], argv[2]);
  fprintf (stderr, "Status: %s\n", ix_errlist (sts));
  return (sts != IX_SUCCESS);  
}
