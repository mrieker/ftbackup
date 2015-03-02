/************************************************************************/
/*									*/
/*  This routine is called to modify the batch level			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

/************************************************************************/
/*									*/
/*  Set batch level							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rab = pointer to rab						*/
/*	newlevel > 0 : start batch mode if not already			*/
/*	         < 0 : stop batch mode and discard changes		*/
/*	        == 0 : stop batch mode and apply changes		*/
/*									*/
/*    Output:								*/
/*									*/
/*	ix_setbatch = IX_SUCCESS : success				*/
/*	                    else : error status (from flushing writes)	*/
/*									*/
/************************************************************************/

uLong ix_setbatch (void *rabv, int newlevel)

{
  Rab *rab;

  rab = rabv;

  if (rab -> logbuf != NULL) rab -> logbuf[0] = 0;

  ix_memen (1);					/* enable memory access */
  rab -> batchlvl = (newlevel > 0);		/* set new batch enable */
  return (ix_flush (rab, IX_SUCCESS, newlevel >= 0)); /* return success status (discard if newlevel < 0) */
}

/************************************************************************/
/*									*/
/*  Inquire batch level							*/
/*									*/
/*    Input:								*/
/*									*/
/*	rabv = pointer to rab						*/
/*									*/
/*    Output:								*/
/*									*/
/*	*curlevel = current batch level					*/
/*	*writes   = number of pending writes				*/
/*									*/
/************************************************************************/

void ix_inqbatch (void *rabv, int *curlevel, int *writes)

{
  Rab *rab;

  rab = rabv;

  if (curlevel != NULL) *curlevel = rab -> batchlvl;
  if (writes   != NULL) *writes = ix_countwrites (rab);
}
