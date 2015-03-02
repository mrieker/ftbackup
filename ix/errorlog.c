/************************************************************************/
/*									*/
/*  Print a message in the error log					*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <stdio.h>
#include <stdarg.h>

static void logit (Byte code, Rab *rab, Byte *ctl, va_list ap);

void ix_errorlog (Rab *rab, Byte *ctl, ...)

{
  va_list ap;

  va_start (ap, ctl);
  logit ('E', rab, ctl, ap);
  va_end (ap);
}

void ix_messlog (Rab *rab, Byte *ctl, ...)

{
  va_list ap;

  va_start (ap, ctl);
  logit ('I', rab, ctl, ap);
  va_end (ap);
}

static void logit (Byte code, Rab *rab, Byte *ctl, va_list ap)

{
  Byte tmpbuf[256];
  uLong i;

  if ((rab == NULL) || (rab -> logbuf == NULL)) {
    fprintf (stderr, "%%IX-%c, %s: ", code, rab -> fspec);
    vfprintf (stderr, ctl, ap);
    fprintf (stderr, "\n");
  } else {
    vsprintf (tmpbuf, ctl, ap);
    i = strlen (rab -> logbuf);
    if (i + strlen (tmpbuf) + 3 <= rab -> logsiz) {
      if (i != 0) rab -> logbuf[i++] = '\n';
      rab -> logbuf[i++] = code;
      strcpy (rab -> logbuf + i, tmpbuf);
    } else if (i + 3 <= rab -> logsiz) {
      if (i != 0) rab -> logbuf[i++] = '\n';
      rab -> logbuf[i++] = '+';
      rab -> logbuf[i++] = 0;
    }
  }
}
