/************************************************************************/
/*									*/
/*  These are the memory allocation routines used on VMS when compiled 	*/
/*  with MEMORY_VMS.  They assume that all routines use ix_malloc, 	*/
/*  ix_calloc and ix_free instead of the standard C routines.		*/
/*									*/
/*  They will set the page protections to read-only when not in use.  	*/
/*  All main level routines call ix_memen (1) to enable memory on 	*/
/*  entry and ix_memen (0) to disable memory on exit.			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"
#include prtdef

#ifdef VAX
#pragma builtins
#endif

typedef struct Page { struct Page *next;
                      uLong size;
                      uByte *adrs;
                    } Page;

globalvalue ss$_badparam, ss$_unsupported;

static uLong zoneid = 0;
static Page *pages = NULL;

static uLong getpage (uLong *numpages, uByte **baseaddr);
static uLong freepage (uLong *numpages, uByte **baseaddr);

/************************************************************************/
/*									*/
/* This routine write enables or disables the pages			*/
/*									*/
/*   Input:								*/
/*									*/
/*	write = 0 : set pages read-only					*/
/*	        1 : set pages read/write				*/
/*									*/
/************************************************************************/

void ix_memen (int write)

{
  uLong inadr[2];
  uLong prot;
  Page *page;
  uLong sts;

  static const uLong extsize = (65536 / 512) - 1;

  /* Set up a zone if we haven't already */

  if (zoneid == 0) {
    sts = lib$create_vm_zone (&zoneid, 0, 0, 0, &extsize, 0, 0, 0, 0, 0, 0, 
                              getpage, freepage);
    if (!(sts & 1)) lib$stop (sts);
  }

  /* Determinte read-only or read/write */

#ifdef PROTECT
  prot = PRT$C_UR;				/* assume read-only */
  if (write) prot = PRT$C_UW;			/* maybe read/write */

  /* Set protection on all the pages we have control of */

  for (page = pages; page != NULL; page = page -> next) { /* do each block */
    inadr[0] = page -> adrs;			/* set up address range */
    inadr[1] = page -> adrs + page -> size - 1;
    sts = sys$setprt (inadr, 0, 0, prot, 0);	/* change protection */
    if (!(sts & 1)) lib$stop (sts);		/* barf if error */
  }
#endif
}

/************************************************************************/
/*									*/
/* This routine allocates protected memory				*/
/*									*/
/*   Input:								*/
/*									*/
/*	size = number of bytes to allocate				*/
/*									*/
/*   Output:								*/
/*									*/
/*	memalloc = address of memory block				*/
/*									*/
/************************************************************************/

void *ix_calloc (uLong size)

{
  Byte *where;

  where = ix_malloc (size);			/* allocate memory */
#ifdef VAX
  _MOVC5 (0, (Byte *) 0, 0, size, where);	/* clear it to zeroes */
#else
  ots$zero (where, size);
#endif

  return (where);				/* return pointer */
}

void *ix_malloc (uLong size)

{
  uLong *adrs;
  uLong isize, sts;

  isize = size + 8;				/* get total size wanted */
  sts = lib$get_vm (&isize, &adrs, &zoneid);	/* allocate memory */
  if (!(sts & 1)) lib$stop (sts);		/* abort if error */

  adrs[0] = isize;				/* save size allocated */
  adrs[1] = ~ isize;				/* (twice for double check) */

  adrs += 2;					/* return pointer */
  return (adrs);
}

/************************************************************************/
/*									*/
/* This routine deallocates protected memory				*/
/*									*/
/*   Input:								*/
/*									*/
/*	where = as returned by memalloc					*/
/*									*/
/************************************************************************/

void ix_free (void *where)

{
  uLong *adrs;
  uLong isize, sts;

  sts = ss$_badparam;				/* assume bad address given */
  if (where != 0) {
    adrs = where;
    adrs -= 2;
    isize = adrs[0];				/* get size */
    if (adrs[1] + isize + 1 == 0) sts = lib$free_vm (&isize, &adrs, &zoneid);
  }
  if (!(sts & 1)) lib$stop (sts);		/* abort if error */
}

/************************************************************************/
/*									*/
/* This internal routine gets new pages					*/
/*									*/
/*   Input:								*/
/*									*/
/*	*numpages = number of page(let)s				*/
/*	*baseaddr = where to return page pointer			*/
/*									*/
/*   Output:								*/
/*									*/
/*	*baseaddr = base address of pages allocated			*/
/*									*/
/************************************************************************/

static uLong getpage (uLong *numpages, uByte **baseaddr)

{
  uLong npages, retadr[2];
  uLong sts;
  Page *page;

  npages = *numpages + 1;			/* get pagelets needed + 1 */
						/* (hopefully, always a full */
						/*  number of pages on Alpha) */
  sts = sys$expreg (npages, retadr, 0, 0);	/* attempt expand P0 region */
  if (sts & 1) {
    *baseaddr = retadr[0] + 512;		/* success, return base + 512 */
    page = retadr[0];				/* use first 512 bytes as hdr */
    page -> size = npages * 512;		/* total size of everything */
    page -> adrs = retadr[0];			/* base of pages */
    page -> next = pages;			/* link to next group of pgs */
    pages = page;				/* this one is on top now */
  }
  return (sts);
}

/************************************************************************/
/*									*/
/* This internal routine frees old pages				*/
/*									*/
/************************************************************************/

static uLong freepage (uLong *numpages, uByte **baseaddr)

{
  return (ss$_unsupported);
}
