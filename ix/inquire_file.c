/************************************************************************/
/*									*/
/*  This routine is called to inquire about an open file		*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

/************************************************************************/
/*									*/
/*  Inquire file							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = as returned by ix_create_file or ix_open_file		*/
/*	nky = where to return number of keys to (or NULL)		*/
/*	ksz = where to return key sizes to (or NULL)			*/
/*	kof = where to return key offsets to (or NULL)			*/
/*	bks = where to return bucket size to (or NULL)			*/
/*	mrs = where to return maximum record size to (or NULL)		*/
/*									*/
/*    Output:								*/
/*									*/
/*	*nky = number of keys						*/
/*	*ksz = key sizes						*/
/*	*kof = key offsets						*/
/*	*bks = bucket size						*/
/*	*mrs = maximum record size					*/
/*									*/
/************************************************************************/

void ix_inquire_file (void *rabv, 
                      Rsz *nky, 
                      Rsz *ksz, 
                      Rsz *kof, 
                      Rsz *bks, 
                      Rsz *mrs)

{
  uLong i;
  Rab *rab;

  rab = rabv;

  /* Return requested information from the rab */

  if (nky != NULL) *nky = rab -> fhd -> nky;
  for (i = 0; i < rab -> fhd -> nky; i ++) {
    if (ksz != NULL) ksz[i] = rab -> fhd -> khd[i].ksz;
    if (kof != NULL) kof[i] = rab -> fhd -> khd[i].kof;
  }
  if (bks != NULL) *bks = rab -> fhd -> bks;
  if (mrs != NULL) *mrs = rab -> fhd -> mrs;
}
