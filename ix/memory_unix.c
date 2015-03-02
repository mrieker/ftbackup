/************************************************************************/
/*									*/
/*  These routines wrap around the standard calloc/malloc/free 		*/
/*  routines to keep track of where, when and how much memory was 	*/
/*  allocated.								*/
/*									*/
/*  To use them, put #include "mallo.h" in your source file and use 	*/
/*  CALLOC, MALLOC, FREE instead of calloc, malloc, free.		*/
/*									*/
/*  Also, global variable mallo_total contains the total memory 	*/
/*  allocated at any given time, and mallo_peak contains the peak 	*/
/*  amount of allocated memory.						*/
/*									*/
/*  Routine mallo_display can be called to display the current memory 	*/
/*  allocations.							*/
/*									*/
/*  These routines inhibit signal / ast delivery during the malloc / 	*/
/*  calloc / free calls, thus making them re-entrant.			*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <malloc.h>
#include <stdlib.h>
#include <time.h>

#define inhint() 0
#define enbint(flag)

typedef struct Mem { time_t garda[2];
                     struct Mem *next;
                     char *file;
                     int line;
                     int size;
                     time_t time;
                     time_t cksm;
                     time_t *data;
                     time_t gardb[2];
                   } Mem;

static Mem *memlist = NULL;			/* list of allocated memory */
static int *trash = NULL;			/* something to abort with */

/************************************************************************/
/*									*/
/*  Memory enable							*/
/*									*/
/*    Input:								*/
/*									*/
/*	write = 0 : write-disable memory				*/
/*	        1 : write-enable memory					*/
/*									*/
/*    Note:								*/
/*									*/
/*	When write = 0, we set all the checksums.  Then when write = 	*/
/*	1, we check all the checksums, thus detecting any corruption 	*/
/*	while writing was "disabled".					*/
/*									*/
/************************************************************************/

void ix_mallo_memen (int write, char *file, int line)

{
#ifdef PROTECT
  int intsts;
  int i, newsize;
  Mem *mem;
  time_t cksm, *data;

  intsts = inhint ();
  for (mem = memlist; mem != NULL; mem = mem -> next) {
    if ((mem -> garda[0] != mem -> time) || (mem -> garda[1] != mem -> time) 
     || (mem -> gardb[0] != mem -> time) || (mem -> gardb[1] != mem -> time)) {
      ix_errorlog (NULL, "guard overwritten at %x in %s %d", mem, file, line);
      *trash = 0;
    }
    data = mem -> data;
    newsize = mem -> size / sizeof (time_t) + 5;
    if ((data[0] != mem -> time) || (data[newsize-2] != mem -> time) 
     || (data[1] != mem -> time) || (data[newsize-1] != mem -> time)) {
      ix_errorlog (NULL, "guards overwritten at %x in %s %d", data, file, line);
      *trash = 0;
    }
    cksm = 0;
    for (i = 2; i < newsize-2; i ++) cksm += data[i];
    if (!write) mem -> cksm = cksm;
    else if (cksm != mem -> cksm) {
      ix_errorlog (NULL, "memory corrupted at %x in %s %d", data, file, line);
      *trash = 0;
    }
  }
  enbint (intsts);
#endif
}

/************************************************************************/
/*									*/
/*  Allocate memory and clear it					*/
/*									*/
/************************************************************************/

void *ix_mallo_calloc (int nele, int size, char *file, int line)

{
  int intsts;
  int newsize;
  Mem *mem;
  time_t *data;

  intsts = inhint ();				/* inhibit interrupt delivery */
  mem = (Mem *) malloc (sizeof *mem);		/* allocate overhead node */
  if (mem == NULL) {
    ix_errorlog (NULL, "calloc no memory at %s %d", file, line);
    *trash = 0;
  }
  mem -> file = file;				/* save source file name */
  mem -> line = line;				/* save source line number */
  mem -> size = nele * size;			/* save total size allocated */
  mem -> time = time (NULL);			/* save time allocated */
  newsize = nele * size / sizeof (time_t) + 5;	/* allocate memory */
  mem -> data = data = (time_t *) calloc (1, newsize * sizeof (time_t));
  if (data == NULL) {
    ix_errorlog (NULL, "calloc no memory at %s %d", file, line);
    *trash = 0;
  }
  data[0] = mem -> time;			/* put in guards */
  data[1] = mem -> time;
  data[newsize-2] = mem -> time;
  data[newsize-1] = mem -> time;
  mem -> garda[0] = mem -> time;
  mem -> garda[1] = mem -> time;
  mem -> gardb[0] = mem -> time;
  mem -> gardb[1] = mem -> time;
  mem -> next = memlist;			/* put on top of list */
  memlist = mem;
  enbint (intsts);				/* enable interrupt delivery */

  return ((void *)(mem -> data + 2));		/* return memory pointer */
}

/************************************************************************/
/*									*/
/*  Allocate memory and dont clear it					*/
/*									*/
/************************************************************************/

void *ix_mallo_malloc (int size, char *file, int line)

{
  int intsts;
  int i, newsize;
  Mem *mem;
  time_t *data;

  intsts = inhint ();				/* inhibit interrupt delivery */
  mem = (Mem *) malloc (sizeof *mem);		/* allocate overhead node */
  if (mem == NULL) {
    ix_errorlog (NULL, "malloc no memory at %s %d", file, line);
    *trash = 0;
  }
  mem -> file = file;				/* save source file name */
  mem -> line = line;				/* save source line number */
  mem -> size = size;				/* save total size allocated */
  mem -> time = time (NULL);			/* save time allocated */
  newsize = size / sizeof (time_t) + 5;		/* allocate memory */
  mem -> data = data = (time_t *) malloc (newsize * sizeof (time_t));
  if (data == NULL) {
    ix_errorlog (NULL, "calloc no memory at %s %d", file, line);
    *trash = 0;
  }
  data[0] = mem -> time;			/* put in guards */
  data[1] = mem -> time;
  data[newsize-2] = mem -> time;
  data[newsize-1] = mem -> time;
  mem -> garda[0] = mem -> time;
  mem -> garda[1] = mem -> time;
  mem -> gardb[0] = mem -> time;
  mem -> gardb[1] = mem -> time;
  for (i = 2; i < newsize-2; i ++) data[i] = mem -> time; /* guarbage fill it */
  mem -> next = memlist;			/* put on top of list */
  memlist = mem;
  enbint (intsts);				/* enable interrupt delivery */

  return ((void *)(mem -> data + 2));		/* return memory pointer */
}

/************************************************************************/
/*									*/
/*  Free memory								*/
/*									*/
/************************************************************************/

void ix_mallo_free (void *datav, char *file, int line)

{
  int intsts;
  int newsize;
  Mem **lmem, *mem;
  time_t *data;

  data = ((time_t *) datav) - 2;		/* back up before guards */

  intsts = inhint ();				/* inhibit interrupt delivery */
  for (lmem = &memlist; 			/* scan memory list */
       (mem = *lmem) != NULL; 
       lmem = &(mem -> next)) {
    if ((mem -> garda[0] != mem -> time) || (mem -> garda[1] != mem -> time) 
     || (mem -> gardb[0] != mem -> time) || (mem -> gardb[1] != mem -> time)) {
      ix_errorlog (NULL, "guard overwritten at %x in %s %d", mem, file, line);
      *trash = 0;
    }
    if (data == mem -> data) break;		/* stop when found */
  }

  if (mem == NULL) {				/* error if not found */
    ix_errorlog (NULL, "free (%x) invalid in %s %d", datav, file, line);
    *trash = 0;
  }

  newsize = mem -> size / sizeof (time_t) + 5;	/* check guards */
  if ((data[0] != mem -> time) || (data[newsize-2] != mem -> time) 
   || (data[1] != mem -> time) || (data[newsize-1] != mem -> time)) {
    ix_errorlog (NULL, "free (%x) guards overwritten in %s %d", 
                                                             datav, file, line);
    *trash = 0;
  }

  *lmem = mem -> next;				/* ok, unlink it */
  free (mem -> data);				/* free data node */
  free (mem);					/* free overhead node */
  enbint (intsts);				/* enable interrupt delivery */
}
