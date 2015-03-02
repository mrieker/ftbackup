/************************************************************************/
/*									*/
/*  These routines return error message text strings corresponding to 	*/
/*  an error status code						*/
/*									*/
/* ------  -----------------------------------	---------  --------	*/
/* V1.0    ORIGINAL VERSION			03-AUG-93  M.RIEKER	*/
/* ------  -----------------------------------	---------  --------	*/
/*									*/
/************************************************************************/

#include "ixinternal.h"

#include <errno.h>
#include <stdio.h>

#if defined (VMS)

static struct { int code; char *text; } errtbl[] = {
	EPERM		, "Not owner", 
	ENOENT		, "No such file or directory", 
	ESRCH		, "No such process", 
	EINTR		, "Interrupted system call", 
	EIO		, "I/O error", 
	ENXIO		, "No such device or address", 
	E2BIG		, "Arg list too long", 
	ENOEXEC		, "Exec format error", 
	EBADF		, "Bad file number", 
	ECHILD		, "No children", 
	EAGAIN		, "No more processes", 
	ENOMEM		, "Not enough core", 
	EACCES		, "Permission denied", 
	EFAULT		, "Bad address", 
	ENOTBLK		, "Block device required", 
	EBUSY		, "Mount device busy", 
	EEXIST		, "File exists", 
	EXDEV		, "Cross-device link", 
	ENODEV		, "No such device", 
	ENOTDIR		, "Not a directory", 
	EISDIR		, "Is a directory", 
	EINVAL		, "Invalid argument", 
	ENFILE		, "File table overflow", 
	EMFILE		, "Too many open files", 
	ENOTTY		, "Not a typewriter", 
	ETXTBSY		, "Text file busy", 
	EFBIG		, "File too large", 
	ENOSPC		, "No space left on device", 
	ESPIPE		, "Illegal seek", 
	EROFS		, "Read-only file system", 
	EMLINK		, "Too many links", 
	EPIPE		, "Broken pipe", 
	EDOM		, "Math argument", 
	ERANGE		, "Result too large", 
	EWOULDBLOCK	, "I/O operation would block channel", 
	EINPROGRESS	, "Operation now in progress", 
	EALREADY	, "Operation already in progress", 
	ENOTSOCK	, "Socket operation on non-socket", 
	EDESTADDRREQ	, "Destination address required", 
	EMSGSIZE	, "Message too long", 
	EPROTOTYPE	, "Protocol wrong type for socket", 
	ENOPROTOOPT	, "Protocol not available", 
	EPROTONOSUPPORT	, "Protocol not supported", 
	ESOCKTNOSUPPORT	, "Socket type not supported", 
	EOPNOTSUPP	, "Operation not supported on socket", 
	EPFNOSUPPORT	, "Protocol family not supported",  
	EAFNOSUPPORT	, "Address family not supported", 
	EADDRINUSE	, "Address already in use", 
	EADDRNOTAVAIL	, "Can't assign requested address", 
	ENETDOWN	, "Network is down", 
	ENETUNREACH	, "Network is unreachable", 
	ENETRESET	, "Network dropped connection on reset", 
	ECONNABORTED	, "Software caused connection abort", 
	ECONNRESET	, "Connection reset by peer", 
	ENOBUFS		, "No buffer space available", 
	EISCONN		, "Socket is already connected", 
	ENOTCONN	, "Socket is not connected", 
	ESHUTDOWN	, "Can't send after socket shutdown", 
	ETOOMANYREFS	, "Too many references: can't splice",  
	ETIMEDOUT	, "Connection timed out", 
	ECONNREFUSED	, "Connection refused", 
	ELOOP		, "Too many levels of symbolic links", 
	ENAMETOOLONG	, "File name too long", 
	EHOSTDOWN	, "Host is down", 
	EHOSTUNREACH	, "No route to host", 
#ifdef ENOTEMPTY
	ENOTEMPTY	, "Directory not empty", 
#endif
#ifdef EPROCLIM
	EPROCLIM	, "Too many processes", 
#endif
#ifdef EUSERS
	EUSERS		, "Too many users", 
#endif
#ifdef EDQUOT
	EDQUOT		, "Disk quota exceeded", 
#endif
#ifdef ENOMSG
	ENOMSG		, "No message of desired type", 
#endif
#ifdef EIDRM
	EIDRM		, "Identifier removed", 
#endif
#ifdef EALIGN
	EALIGN		, "Alignment error", 
#endif
#ifdef ESTALE
	ESTALE		, "Stale NFS file handle", 
#endif
#ifdef EREMOTE
	EREMOTE		, "Too many levels of remote in path", 
#endif
#ifdef ENOLCK
	ENOLCK		, "No locks available", 
#endif
#ifdef ENOSYS
	ENOSYS		, "Function not implemented", 
#endif
#ifdef EFTYPE
	EFTYPE		, "Inappropriate operation for file type", 
#endif
#ifdef ECANCELED
	ECANCELED	, "Operation canceled", 
#endif
#ifdef EFAIL
	EFAIL		, "Cannot start operation", 
#endif
#ifdef EINPROG
	EINPROG		, "Asynchronous operation in progress", 
#endif
#ifdef ENOTSUP
	ENOTSUP		, "Function not implemented", 
#endif
#ifdef EDEADLK
	EDEADLK		, "Resource deadlock avoided", 
#endif
#ifdef ENWAIT
	ENWAIT		, "No waiting processes", 
#endif
#ifdef EILSEQ
	EILSEQ		, "Illegal byte sequence", 
#endif
#ifdef EBADCAT
	EBADCAT 	, "Bad message catalogue format", 
#endif
#ifdef EBADMSG
	EBADMSG		, "Corrupted message detected", 
#endif
	0		, NULL };

#endif

/* This routine returns an error message for errno */

#ifndef WIN32
Byte *ix_unixerr ()

{
  static Byte buf[256];

#ifdef VMS
  int i;

  if (errno == EVMSERR) return (ix_vmserr (vaxc$errno));
  for (i = 0; errtbl[i].text != NULL; i ++)
                           if (errno == errtbl[i].code) return (errtbl[i].text);
  sprintf (buf, "errno %d", errno);
  return (buf);
#else
  char *p;
  int saverrno;

  saverrno = errno;
  errno = 0;
  p = strerror (saverrno);
  if (errno == 0) {
    errno = saverrno;
    return (p);
  }
  sprintf (buf, "errno %d", saverrno);
  errno = saverrno;
  return (buf);
#endif
}
#endif

/* This VMS-only routine returns the error message for the VMS status */

#ifdef VMS
Byte *ix_vmserr (uLong sts)

{
  static Byte buf[256];
  struct { Long size; Byte *adrs; } des = { 255, buf };

  sys$getmsg (sts, &des.size, &des, 1, 0);
  buf[des.size] = 0;
  return (buf);
}
#endif

/* This WIN32-only routine returns the error message for the WIN32 status */

#ifdef WIN32
Byte *ix_win32err (uLong sts)

{
  static Byte buf[256];

  sprintf (buf, "WIN32 status %u", sts);
  return (buf);
}
#endif


/* This routine returns IX error message text address */

#define NERR 74

static Byte *ix_errlist_text[NERR] = {
	"IX status 65536", 			/* 65536 */
	"Normal successful completion", 	/* 65537 SUCCESS */
	"New key value", 			/* 65538 NEWKEYVALUE */
	"Error creating file", 			/* 65539 CREATERROR */
	"Error writing file", 			/* 65540 WRITERROR */
	"Invalid key size", 			/* 65541 INVKEYSIZE */
	"Error positioning file", 		/* 65542 SEEKERROR */
	"Error reading file", 			/* 65543 READERROR */
	"Invalid bucket format", 		/* 65544 INVBKTFMT */
	"Invalid record size", 			/* 65545 INVRECSIZE */
	"Error closing file", 			/* 65546 CLOSERROR */
	"No current record", 			/* 65547 NOCURREC */
	"Invalid bucket checksum", 		/* 65548 INVBKTCKSM */
	"Invalid bucket pointer", 		/* 65549 INVBKTPNTR */
	"Error opening file", 			/* 65550 OPENERROR */
	"Invalid file header", 			/* 65551 INVFILHDR */
	"Record read but truncated", 		/* 65552 TRUNCATED */
	"Record not found", 			/* 65553 RECNOTFOUND */
	"Invalid key of reference", 		/* 65554 INVKEYREF */
	"Invalid search type", 			/* 65555 INVSEARCH */
	"Index too deep", 			/* 65556 IDXTOODEEP */
	"Bucket in use", 			/* 65557 BKTINUSE */
	"Bad alternate bucket", 		/* 65558 BADALTBKT */
	"Bucket not checked out of cache", 	/* 65559 NOTCHECKEDOUT */
	"Bucket not checked in to cache", 	/* 65560 NOTCHECKEDIN */
	"Invalid file header checksum", 	/* 65561 INVFHDCKSM */
	"Cache entry not found", 		/* 65562 CACHENTNTFND */
	"No such file", 			/* 65563 NOSUCHFILE */
	"Invalid bucket vbn", 			/* 65564 INVBKTVBN */
	"Ran out of memory", 			/* 65565 NOMEMORY */
	"Record too short for buffer", 		/* 65566 RECTOOSHORT */
	"File opened by another stream",	/* 65567 FILELOCKED */
	"Record accessed by another stream",	/* 65568 RECORDLOCKED */
	"Error performing lock function",	/* 65569 LOCKERROR */
	"File and/or record is read-only",	/* 65570 READONLY */
	"Record deleted by another stream"	/* 65571 RECDELETED */
};

Byte *ix_errlist (uLong status)

{
  static Byte buff[32];

  if ((status > IX_BASE) && (status < NERR + IX_BASE)) 
                                       return (ix_errlist_text[status-IX_BASE]);
  sprintf (buff, "Unknown IX status %u", status);
  return (buff);
}
