void memmove (char *dst, char *src, long len)

{
  long l;

  /* Nop if moving to same place or if length = 0 */

  l = src - dst;
  if ((l == 0) || (len == 0)) return;

  /* If src ends before dst begins, or dst ends */
  /* before src begins, non-overlapped move     */

  if ((src + len <= dst) || (dst + len <= src)) {
    memcpy (dst, src, len);
    return;
  }

  /* See if moving to lower memory */

  if (l > 0) {					/* pos when src > dst */
    while (l < len) {
      memcpy (dst, src, l);			/* move non-overlapped */
      src += l;					/* increment src pointer */
      dst += l;					/* increment dst pointer */
      len -= l;					/* decrement length */
    }
    memcpy (dst, src, len);			/* move last part */
    return;
  }

  /* Moving to higher memory */

  dst += len;					/* point to ends */
  src += len;

  l = -l;					/* get positive difference */
  while (len > l) {
    src -= l;					/* point to beg of non-overlap */
    dst -= l;
    memcpy (dst, src, l);			/* copy it */
    len -= l;					/* decrement length */
  }
  src -= len;					/* move last part */
  dst -= len;
  memcpy (dst, src, len);
  return;
}
