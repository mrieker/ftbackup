#include <stdlib.h>
#include <string.h>

void *mallo_malloc (int size, char *file, int line)
{
  void *p = (void *)malloc (size);
  if (p != NULL) memset (p, 0x69, size);
  return (p);
}
void *mallo_calloc (int nele, int size, char *file, int line) { return ((void *)calloc (nele, size)); }
void  mallo_free (void *datav, char *file, int line)          { free (datav);                         }
