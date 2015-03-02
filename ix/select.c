
#include "ixinternal.h"

typedef struct IX_Sel { int opcode;
                        struct IX_Sel *boola;
                        struct IX_Sel *boolb;
                        IX_Rsz compfsz;
                        IX_Rsz *compfof;
                        IX_Rsz compksz;
                        IX_Rbf *compkbf;
                      } IX_Sel;

uLong ix_search_sel (void *rabv, IX_Sel *sel, int func, 
                     IX_Rsz rsz, IX_Rbf *rbf, IX_Rsz *rrsz)

{
  Rab *rab;

  rab = rabv;

  /* Determine key of reference to be used */

  krf = ??;
  ksz = ??;
  kbf = ??;

  /* Search file for next matching record */

search_next:
  if (func == IX_SEL_FIRST) 
           sts = ix_search_key (rab, IX_KEY_GEF, krf, ksz, kbf, rsz, rbf, rrsz);
  else if (func == IX_SEL_NEXT) 
                         sts = ix_search_seq (rab, 1, rsz, rbf, rrsz, ksz, kbf);
  else {
    ??ILLEGAL FUNCTION CODE;
    return (IX_??);
  }

  func = IX_SEL_NEXT;

  if (sts != IX_SUCCESS) return (sts);

  /* See if it matches the given list */

  if (!match (rsz, rbf, sel)) goto search_next;
  return (sts);
}

static int match (Rsz rsz, Rbf *rbf, Sel *sel)

{
  if (sel == NULL) return (1);

  switch (sel -> opcode) {

    /* Boolean opcodes */

    case IX_SEL_AND: return (match (rsz, rbf, sel -> boola) 
                          && match (rsz, rbf, sel -> boolb));
    case IX_SEL_OR: return (match (rsz, rbf, sel -> boola) 
                         || match (rsz, rbf, sel -> boolb));

    /* Comparison opcodes */

    case IX_SEL_LT: return (ix_compare_keys (sel -> compfsz, 
                                             rbf + sel -> compfof, 
                                             NULL, 
                                             sel -> compksz, 
                                             sel -> compkbf, 
                                             NULL) < 0);

    case IX_SEL_GT: return (ix_compare_keys (sel -> compfsz, 
                                             rbf + sel -> compfof, 
                                             NULL, 
                                             sel -> compksz, 
                                             sel -> compkbf, 
                                             NULL) > 0);

    case IX_SEL_EQ: return (ix_compare_keys (sel -> compfsz, 
                                             rbf + sel -> compfof, 
                                             NULL, 
                                             sel -> compksz, 
                                             sel -> compkbf, 
                                             NULL) == 0);

    case IX_SEL_NE: return (ix_compare_keys (sel -> compfsz, 
                                             rbf + sel -> compfof, 
                                             NULL, 
                                             sel -> compksz, 
                                             sel -> compkbf, 
                                             NULL) != 0);

    case IX_SEL_LE: return (ix_compare_keys (sel -> compfsz, 
                                             rbf + sel -> compfof, 
                                             NULL, 
                                             sel -> compksz, 
                                             sel -> compkbf, 
                                             NULL) <= 0);

    case IX_SEL_GE: return (ix_compare_keys (sel -> compfsz, 
                                             rbf + sel -> compfof, 
                                             NULL, 
                                             sel -> compksz, 
                                             sel -> compkbf, 
                                             NULL) >= 0);
  }

  /* Who knows what it is */

  ix_errorlog (??);
  return (0);
}
