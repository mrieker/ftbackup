#include "ixinternal.h"

/************************************************************************/
/*									*/
/*  Compare keys, padding the shorter one with nulls			*/
/*									*/
/*    Input:								*/
/*									*/
/*	ksz1 = size of key 1						*/
/*	kbf1 = address of key 1						*/
/*	rfa1 = address of rfa for key 1					*/
/*	ksz2 = size of key 2						*/
/*	kbf2 = address of key 2						*/
/*	rfa2 = address of rfa for key 2					*/
/*	kat  = key attributes						*/
/*	       IX_KAT_CASE : treat upper case as lower case		*/
/*	       IX_KAT_REV  : highest address byte is most significant	*/
/*									*/
/*    Output:								*/
/*									*/
/*	compare_keys = 1 : key 1 .gt. key 2				*/
/*	               0 : key 1 .eq. key 2				*/
/*	              -1 : key 1 .lt. key 2				*/
/*									*/
/*    Note:  								*/
/*									*/
/*	The comparison is unsigned					*/
/*	The shorter key is treated as if it were null padded		*/
/*									*/
/************************************************************************/

int ix_compare_keys (Rsz ksz1, const Rbf *kbf1, Rfa *rfa1, 
                     Rsz ksz2, const Rbf *kbf2, Rfa *rfa2, 
                     Kat kat)

{
  Rbf c1, c2;
  Rbf const *p1, *p2;
  Rsz i, s1, s2;

  p1 = kbf1;
  p2 = kbf2;
  s1 = ksz1;
  s2 = ksz2;

  switch (kat & (IX_KAT_CASE | IX_KAT_REV)) {

    /* case-sensitive, forward */

    case 0: {

      for (i = 0; (i < s1) && (i < s2); i ++) {
        c1 = *(p1 ++);
        c2 = *(p2 ++);
        if (c1 != c2) goto c1nec2;
      }

      for (; i < s1; i ++) if (*(p1 ++) > 0) return (1);
      for (; i < s2; i ++) if (*(p2 ++) > 0) return (-1);

      break;
    }

    /* case-sensitive, reverse */

    case IX_KAT_REV: {

      p1 += s1;
      p2 += s2;

      if (s1 > s2) {
        for (i = s1; i > s2; -- i) {
          if (*(-- p1) != 0) return (1);
        }
      } else {
        for (i = s2; i > s1; -- i) {
          if (*(-- p2) != 0) return (-1);
        }
      }

      for (; i > 0; -- i) {
        c1 = *(-- p1);
        c2 = *(-- p2);
        if (c1 != c2) goto c1nec2;
      }

      break;
    }

    /* case-insensitive, forward */

    case IX_KAT_CASE: {

      for (i = 0; (i < s1) && (i < s2); i ++) {
        c1 = *(p1 ++);
        c2 = *(p2 ++);
        if ((c1 >= 'A') && (c1 <= 'Z')) c1 += 'a' - 'A';
        if ((c2 >= 'A') && (c2 <= 'Z')) c2 += 'a' - 'A';
        if (c1 != c2) goto c1nec2;
      }

      for (; i < s1; i ++) if (*(p1 ++) > 0) return (1);
      for (; i < s2; i ++) if (*(p2 ++) > 0) return (-1);

      break;
    }

    /* case-insensitive, reverse */

    case IX_KAT_CASE | IX_KAT_REV: {

      p1 += s1;
      p2 += s2;

      if (s1 > s2) {
        for (i = s1; i > s2; -- i) {
          if (*(-- p1) != 0) return (1);
        }
      } else {
        for (i = s2; i > s1; -- i) {
          if (*(-- p2) != 0) return (-1);
        }
      }

      for (; i > 0; -- i) {
        c1 = *(-- p1);
        c2 = *(-- p2);
        if ((c1 >= 'A') && (c1 <= 'Z')) c1 += 'a' - 'A';
        if ((c2 >= 'A') && (c2 <= 'Z')) c2 += 'a' - 'A';
        if (c1 != c2) goto c1nec2;
      }

      break;
    }
  }

  /* If key values matched, compare the rfa's */

  if ((rfa1 != NULL) && (rfa2 != NULL)) {
    if (rfa1 -> vbn < rfa2 -> vbn) return (-1);
    if (rfa1 -> vbn > rfa2 -> vbn) return (1);
    if (rfa1 -> seq < rfa2 -> seq) return (-1);
    if (rfa1 -> seq > rfa2 -> seq) return (1);
  }

  return (0);

  /* C1 != C2, return difference */

c1nec2:
  if (c1 < c2) return (-1);
  return (1);
}
