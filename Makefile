
CFLAGS   ?= -Wall -Werror -O2
STATIC   ?=

#
#  Get version number from GIT
#
HAVEGITDIR := $(wildcard .git)
ifeq ($(HAVEGITDIR),)
	VERFILE     := $(wildcard *.ver)
	COMMITHASH  := $(patsubst %_0.ver,%,$(patsubst %_1.ver,%,$(VERFILE)))
	COMMITDATE  := $(shell cat $(VERFILE))
	COMMITCLEAN := $(patsubst %_0.ver,0,$(patsubst %_1.ver,1,$(VERFILE)))
else
	COMMITHASH  := $(strip $(shell git log -1 | grep ^commit | sed 's/commit//g'))
	COMMITDATE  := $(strip $(shell git log -1 --date=iso | grep ^Date: | sed 's/Date://g'))
	COMMITCLEAN := $(strip $(shell git status | grep -q modified: ; echo $$?))
	VERFILE     := $(COMMITHASH)_$(COMMITCLEAN).ver
endif

#
#  Make distribution version number from GIT commit date
#
DISTVER   := $(subst -,,$(firstword $(COMMITDATE)))$(subst 0,+,$(subst 1,,$(COMMITCLEAN)))

#
#  List of files that go into distribution
#
DISTFILES := \
    cryptopp562.files.cpp.patch \
    cryptopp562.misc.h.patch    \
    cryptopp562.zip             \
    ftbackup.cpp                \
    ftbackup.h                  \
    ftbackup.html               \
    ftbreader.cpp               \
    ftbreader.h                 \
    ftbwriter.cpp               \
    ftbwriter.h                 \
    INSTALL.txt                 \
    Makefile                    \
    $(VERFILE)

#
#  Make list of source files that go into executable
#
LIBFILES := -lpthread -lrt -lz -lstdc++ -lm
SRCFILES := ftbackup.cpp ftbreader.cpp ftbwriter.cpp cryptopp562/libcryptopp.a ix/BIN/libix.a
ifeq ($(STATIC),)
else
    CFLAGS := $(CFLAGS) -static
endif

#
#  Build the executable (default target)
#
ftbackup: $(SRCFILES) ftbackup.h ftbreader.h ftbwriter.h $(VERFILE)
	cc $(CFLAGS) -o ftbackup \
		-DGITCOMMITHASH='"$(COMMITHASH)"' \
		-DGITCOMMITDATE='"$(COMMITDATE)"' \
		-DGITCOMMITCLEAN=$(COMMITCLEAN)   \
		$(SRCFILES) $(LIBFILES)

#
#  Clean up
#
clean:
	rm -rf cryptopp562
	rm -f  ftbackup
	rm -rf ftbackup-$(DISTVER)*
	rm -rf ix/BIN

#
#  Build distribution tarball
#
dist: ftbackup ftbackup-$(DISTVER).tar.gz

ftbackup-$(DISTVER).tar.gz: $(DISTFILES)
	rm -rf ftbackup-$(DISTVER)
	mkdir -p ftbackup-$(DISTVER)
	cp -l $(DISTFILES) ftbackup-$(DISTVER)
	tar czvf ftbackup-$(DISTVER).tar.gz ftbackup-$(DISTVER)

#
#  Make version file to remember what version we compiled in to executable
#
$(VERFILE):
	rm -f *.ver
	echo $(COMMITDATE) > $(VERFILE)

#
#  Compile crypto library
#
cryptopp562/libcryptopp.a: cryptopp562.zip
	rm -rf cryptopp562
	mkdir cryptopp562
	cd cryptopp562 ; unzip ../cryptopp562.zip
	patch cryptopp562/files.cpp < cryptopp562.files.cpp.patch
	patch cryptopp562/misc.h < cryptopp562.misc.h.patch
	cd cryptopp562 ; $(MAKE) libcryptopp.a

#
#  Compile IX library
#
ix/BIN/libix.a: ix/*.c ix/*.h ix/makefile
	mkdir -p ix/BIN
	cd ix ; make

