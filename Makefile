
COMMITHASH  := $(strip $(shell git log -1 | grep ^commit | sed 's/commit//g'))
COMMITDATE  := $(strip $(shell git log -1 --date=iso | grep ^Date: | sed 's/Date://g'))
COMMITCLEAN := $(strip $(shell git status | grep -q modified: ; echo $$?))

VERFILE = $(COMMITHASH)_$(COMMITCLEAN).ver

ftbackup: ftbackup.cpp ftbackup.h ftbreader.h ftbreader.cpp ftbwriter.h ftbwriter.cpp $(VERFILE) cryptopp562/libcryptopp.a
	cc -Wall -Werror -O2 -g -o ftbackup \
		-DGITCOMMITHASH='"$(COMMITHASH)"' \
		-DGITCOMMITDATE='"$(COMMITDATE)"' \
		-DGITCOMMITCLEAN=$(COMMITCLEAN)   \
		ftbackup.cpp ftbreader.cpp ftbwriter.cpp cryptopp562/libcryptopp.a \
		-lpthread -lrt -lz -lstdc++

$(VERFILE):
	rm -f *.ver
	touch $(VERFILE)

cryptopp562/libcryptopp.a: cryptopp562
	cd cryptopp562 ; $(MAKE) libcryptopp.a

cryptopp562: cryptopp562.zip
	rm -rf cryptopp562
	mkdir cryptopp562
	cd cryptopp562 ; unzip ../cryptopp562.zip
	patch cryptopp562/files.cpp < crpytopp562.files.cpp.patch
	patch cryptopp562/misc.h < cryptopp562.misc.h.patch

