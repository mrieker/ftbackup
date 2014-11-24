
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

DISTVER   := $(subst -,,$(firstword $(COMMITDATE)))$(subst 0,+,$(subst 1,,$(COMMITCLEAN)))
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

SRCFILES := ftbackup.cpp ftbreader.cpp ftbwriter.cpp cryptopp562/libcryptopp.a

ftbackup: $(SRCFILES) ftbackup.h ftbreader.h ftbwriter.h $(VERFILE)
	cc -Wall -Werror -O2 -g -o ftbackup \
		-DGITCOMMITHASH='"$(COMMITHASH)"' \
		-DGITCOMMITDATE='"$(COMMITDATE)"' \
		-DGITCOMMITCLEAN=$(COMMITCLEAN)   \
		$(SRCFILES) -lpthread -lrt -lz -lstdc++

clean:
	rm -rf cryptopp562
	rm -f  ftbackup
	rm -rf ftbackup-$(DISTVER)*

dist: ftbackup ftbackup-$(DISTVER).tar.gz

ftbackup-$(DISTVER).tar.gz: $(DISTFILES)
	rm -rf ftbackup-$(DISTVER)
	mkdir -p ftbackup-$(DISTVER)
	cp -l $(DISTFILES) ftbackup-$(DISTVER)
	tar czvf ftbackup-$(DISTVER).tar.gz ftbackup-$(DISTVER)

$(VERFILE):
	rm -f *.ver
	echo $(COMMITDATE) > $(VERFILE)

cryptopp562/libcryptopp.a: cryptopp562.zip
	rm -rf cryptopp562
	mkdir cryptopp562
	cd cryptopp562 ; unzip ../cryptopp562.zip
	patch cryptopp562/files.cpp < cryptopp562.files.cpp.patch
	patch cryptopp562/misc.h < cryptopp562.misc.h.patch
	cd cryptopp562 ; $(MAKE) libcryptopp.a

