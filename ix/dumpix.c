/************************************************************************/
/*									*/
/*  This program reads the input file sequentially and 			*/
/*  writes the output to standard output in a dump format		*/
/*									*/
/************************************************************************/

#include "ix.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void bin2hex (IX_uLong value, int width, IX_Byte *outbuf);
static int dec2bin (IX_Byte *inpbuf);

int main (int argc, char **argv)

{
  IX_Byte *argp;
  IX_Byte outbuf[256];
  int fwd, krf, i, j, k, l, little_endian, sts;
  IX_Rbf *recbuf;
  IX_Rsz bks, mrs, kof[256], ksz[256], nky, rsz;
  IX_Vbn count, n, skip;
  union {
    IX_uLong l;
    IX_uByte b[4];
  } endetect;
  void *rab;

  /* Check for any command line args */

  if (argc < 2) {
    printf ("Usage: %s file\n", argv[0]);
    printf ("          -b       - big-endian format\n");
    printf ("          -c count - count of records to dump\n");
    printf ("          -f       - forward scan\n");
    printf ("          -k krf   - key-of-reference\n");
    printf ("          -l       - little-endian format\n");
    printf ("          -r       - reverse scan\n");
    printf ("          -s skip  - number of records to skip\n");
    return (1);
  }

  /* Get command line args */

  endetect.l = 1;
  little_endian = endetect.b[0];
  fwd   = 1;
  krf   = 0;
  count = (IX_Vbn) (-1);
  skip  = 0;
  outbuf[0] = 0;
  for (i = 1; i < argc; i ++) {
    argp = argv[i];
    if (*argp != '-') strcpy (outbuf, argp);
    if (strcmp (argp, "-b") == 0) little_endian = 0;
    if (strcmp (argp, "-c") == 0) count = dec2bin (argv[++i]);
    if (strcmp (argp, "-f") == 0) fwd   = 1;
    if (strcmp (argp, "-k") == 0) krf   = dec2bin (argv[++i]);
    if (strcmp (argp, "-l") == 0) little_endian = 1;
    if (strcmp (argp, "-r") == 0) fwd   = 0;
    if (strcmp (argp, "-s") == 0) skip  = dec2bin (argv[++i]);
  }

  /* Open file */

  sts = ix_open_file (outbuf, 1, 10, &rab);
  printf ("Open file %s - %s\n", outbuf, ix_errlist (sts));
  if (sts != IX_SUCCESS) return (1);

  ix_inquire_file (rab, &nky, ksz, kof, &bks, &mrs);
  printf ("Number of keys: %d\n", nky);
  printf ("   Bucket size: %d\n", bks);
  printf ("  Max rec size: %d\n", mrs);
  for (n = 0; n < nky; n ++) 
                    printf ("Key %d:  size %d, offset %d\n", n, ksz[n], kof[n]);

  recbuf = (IX_Rbf *) malloc (mrs);

  /* Dump file record by record */

  n = 0;
  sts = ix_rewind (rab, (IX_uWord) krf);
  if (sts == IX_SUCCESS) while ((sts = ix_search_seq (rab, fwd, 
                                  mrs, recbuf, &rsz, 0, NULL)) == IX_SUCCESS) {
    if (++ n <= skip) continue;
    fprintf (stdout, "\nRecord %d - %d (hex %x) bytes\n\n", n, rsz, rsz);
    for (i = 0; i < rsz; i += 16) {
      if (little_endian) {
        j = 37;
        outbuf[37] = ' ';
        bin2hex (i, 8, outbuf + 38);
        outbuf[46] = ' ';
        outbuf[47] = ' ';
        outbuf[48] = '"';
        k = 49;
        for (l = 0; (l < 16) && (i + l < rsz); l ++) {
          if ((l & 3) == 0) outbuf[--j] = ' ';
          j -= 2;
          bin2hex (recbuf[i+l], 2, outbuf + j);
          outbuf[k] = recbuf[i+l];
          if ((outbuf[k] < ' ') || (outbuf[k] == 127)) outbuf[k] = '.';
          k ++;
        }
        memset (outbuf, ' ', j);
      } else {
        outbuf[0] = ' ';
        bin2hex (i, 8, outbuf + 1);
        outbuf[9] = ' ';
        j = 10;
        outbuf[48] = '"';
        k = 49;
        for (l = 0; (l < 16) && (i + l < rsz); l ++) {
          if ((l & 3) == 0) outbuf[j++] = ' ';
          bin2hex (recbuf[i+l], 2, outbuf + j);
          j += 2;
          outbuf[k] = recbuf[i+l];
          if ((outbuf[k] < ' ') || (outbuf[k] == 127)) outbuf[k] = '.';
          k ++;
        }
        memset (outbuf + j, ' ', 48 - j);
      }
      outbuf[k++] = '"';
      outbuf[k++] = '\n';
      fwrite (outbuf, k, 1, stdout);
    }
    if (n - skip >= count) break;
  }
  printf ("\nSearch status - %s\n", ix_errlist (sts));
  if ((sts == IX_RECNOTFOUND) || (sts == IX_SUCCESS)) return (0);
  return (1);
}

/* Convert a binary value to a hexadecimal string */

static void bin2hex (IX_uLong value, int width, IX_Byte *outbuf)

{
  int i;

  outbuf += width;
  for (i = 0; i < width; i ++) {
    *(-- outbuf) = "0123456789ABCDEF"[value&15];
    value /= 16;
  }
}

/* Convert a decimal string to binary */

static int dec2bin (IX_Byte *inpbuf)

{
  int n, v;

  v = 0;
  for (n = 0; inpbuf[n] != 0; n ++) {
    if ((inpbuf[n] < '0') || (inpbuf[n] > '9')) return (-1);
    v = v * 10 + inpbuf[n] - '0';
  }
  return (v);
}
