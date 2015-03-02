#
# makefile for libix.a routines
#

OBJ_DIR=BIN
LIB_DIR=BIN
BIN_DIR=BIN

CC=cc -Wall -Werror -g -O2 -c
LD=cc -g
RM=rm -f
AR=ar rcv
RANLIB=ranlib
MV=mv

LINKS=	$(LIB_DIR)/libix.a

OBJS= \
	$(OBJ_DIR)/batch.o         \
	$(OBJ_DIR)/close_file.o    \
	$(OBJ_DIR)/compare_keys.o  \
	$(OBJ_DIR)/compress_file.o \
	$(OBJ_DIR)/create_file.o   \
	$(OBJ_DIR)/diskio.o        \
	$(OBJ_DIR)/errlist.o       \
	$(OBJ_DIR)/errorlog.o      \
	$(OBJ_DIR)/fix_file.o      \
	$(OBJ_DIR)/inquire_file.o  \
	$(OBJ_DIR)/insert_rec.o    \
	$(OBJ_DIR)/modify_rec.o    \
	$(OBJ_DIR)/open_file.o     \
	$(OBJ_DIR)/os.o    	   \
	$(OBJ_DIR)/remove_rec.o    \
	$(OBJ_DIR)/search_rec.o    \
	$(OBJ_DIR)/spsc_mallo.o    \
	$(OBJ_DIR)/validate_file.o

# default is to make everyting

default: $(LIB_DIR)/libix.a \
	$(BIN_DIR)/comprix \
	$(BIN_DIR)/dumpix \
	$(BIN_DIR)/fixix \
	$(BIN_DIR)/validix

# build library

$(LIB_DIR)/libix.a: $(OBJS)
	$(RM)     libix.a
	$(AR)     libix.a $(OBJS)
	$(RANLIB) libix.a
	$(MV)     libix.a $(LIB_DIR)

# Make the test programs

$(BIN_DIR)/comprix: $(OBJ_DIR)/comprix.o $(LINKS)
	$(LD) -o comprix $(OBJ_DIR)/comprix.o $(LINKS) $(LD_RTL)
	$(MV) comprix $(BIN_DIR)

$(BIN_DIR)/dumpix: $(OBJ_DIR)/dumpix.o $(LINKS)
	$(LD) -o dumpix $(OBJ_DIR)/dumpix.o $(LINKS) $(LD_RTL)
	$(MV) dumpix $(BIN_DIR)

$(BIN_DIR)/fixix: $(OBJ_DIR)/fixix.o $(LINKS)
	$(LD) -o fixix $(OBJ_DIR)/fixix.o $(LINKS) $(LD_RTL)
	$(MV) fixix $(BIN_DIR)

$(BIN_DIR)/validix: $(OBJ_DIR)/validix.o $(LINKS)
	$(LD) -o validix $(OBJ_DIR)/validix.o $(LINKS) $(LD_RTL)
	$(MV) validix $(BIN_DIR)

# Make the object files

$(OBJ_DIR)/batch.o: batch.c
	$(CC) -DSPSC batch.c -o batch.o
	$(MV) batch.o $(OBJ_DIR)

$(OBJ_DIR)/build_key.o: build_key.c
	$(CC) -DSPSC build_key.c -o build_key.o
	$(MV) build_key.o $(OBJ_DIR)

$(OBJ_DIR)/close_file.o: close_file.c
	$(CC) -DSPSC close_file.c -o close_file.o
	$(MV) close_file.o $(OBJ_DIR)

$(OBJ_DIR)/compare_keys.o: compare_keys.c
	$(CC) -DSPSC compare_keys.c -o compare_keys.o
	$(MV) compare_keys.o $(OBJ_DIR)

$(OBJ_DIR)/compress_file.o: compress_file.c
	$(CC) -DSPSC compress_file.c -o compress_file.o
	$(MV) compress_file.o $(OBJ_DIR)

$(OBJ_DIR)/comprix.o: comprix.c
	$(CC) -DSPSC comprix.c -o comprix.o
	$(MV) comprix.o $(OBJ_DIR)

$(OBJ_DIR)/create_file.o: create_file.c
	$(CC) -DSPSC create_file.c -o create_file.o
	$(MV) create_file.o $(OBJ_DIR)

$(OBJ_DIR)/diskio.o: diskio.c
	$(CC) -DSPSC diskio.c -o diskio.o
	$(MV) diskio.o $(OBJ_DIR)

$(OBJ_DIR)/dumpix.o: dumpix.c
	$(CC) -DSPSC dumpix.c -o dumpix.o
	$(MV) dumpix.o $(OBJ_DIR)

$(OBJ_DIR)/errlist.o: errlist.c
	$(CC) -DSPSC errlist.c -o errlist.o
	$(MV) errlist.o $(OBJ_DIR)

$(OBJ_DIR)/errorlog.o: errorlog.c
	$(CC) -DSPSC errorlog.c -o errorlog.o
	$(MV) errorlog.o $(OBJ_DIR)

$(OBJ_DIR)/fixix.o: fixix.c
	$(CC) -DSPSC fixix.c -o fixix.o
	$(MV) fixix.o $(OBJ_DIR)

$(OBJ_DIR)/fix_file.o: fix_file.c
	$(CC) -DSPSC fix_file.c -o fix_file.o
	$(MV) fix_file.o $(OBJ_DIR)

$(OBJ_DIR)/inquire_file.o: inquire_file.c
	$(CC) -DSPSC inquire_file.c -o inquire_file.o
	$(MV) inquire_file.o $(OBJ_DIR)

$(OBJ_DIR)/insert_rec.o: insert_rec.c
	$(CC) -DSPSC insert_rec.c -o insert_rec.o
	$(MV) insert_rec.o $(OBJ_DIR)

$(OBJ_DIR)/memmove.o: memmove.c
	$(CC) -DSPSC memmove.c -o memmove.o
	$(MV) memmove.o $(OBJ_DIR)

$(OBJ_DIR)/modify_rec.o: modify_rec.c
	$(CC) -DSPSC modify_rec.c -o modify_rec.o
	$(MV) modify_rec.o $(OBJ_DIR)

$(OBJ_DIR)/open_file.o: open_file.c
	$(CC) -DSPSC open_file.c -o open_file.o
	$(MV) open_file.o $(OBJ_DIR)

$(OBJ_DIR)/os.o: os.c
	$(CC) -DSPSC os.c -o os.o
	$(MV) os.o $(OBJ_DIR)

$(OBJ_DIR)/remove_rec.o: remove_rec.c
	$(CC) -DSPSC remove_rec.c -o remove_rec.o
	$(MV) remove_rec.o $(OBJ_DIR)

$(OBJ_DIR)/search_rec.o: search_rec.c
	$(CC) -DSPSC search_rec.c -o search_rec.o
	$(MV) search_rec.o $(OBJ_DIR)

$(OBJ_DIR)/spsc_mallo.o: spsc_mallo.c
	$(CC) -DSPSC spsc_mallo.c -o spsc_mallo.o
	$(MV) spsc_mallo.o $(OBJ_DIR)

$(OBJ_DIR)/validate_file.o: validate_file.c
	$(CC) -DSPSC validate_file.c -o validate_file.o
	$(MV) validate_file.o $(OBJ_DIR)

$(OBJ_DIR)/validix.o: validix.c
	$(CC) -DSPSC validix.c -o validix.o
	$(MV) validix.o $(OBJ_DIR)
