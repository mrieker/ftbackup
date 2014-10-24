
COMMITHASH  := $(strip $(shell git log -1 | grep ^commit | sed 's/commit//g'))
COMMITDATE  := $(strip $(shell git log -1 --date=iso | grep ^Date: | sed 's/Date://g'))
COMMITCLEAN := $(strip $(shell git status | grep -q modified: ; echo $$?))

VERFILE = $(COMMITHASH)_$(COMMITCLEAN).ver

ftbackup: ftbackup.cpp ftbackup.h ftbreader.h ftbreader.cpp ftbwriter.h ftbwriter.cpp $(VERFILE)
	cc -Wall -Werror -O2 -g -o ftbackup \
		-DGITCOMMITHASH='"$(COMMITHASH)"' \
		-DGITCOMMITDATE='"$(COMMITDATE)"' \
		-DGITCOMMITCLEAN=$(COMMITCLEAN)   \
		ftbackup.cpp ftbreader.cpp ftbwriter.cpp \
		-lpthread -lrt -lz -lstdc++

$(VERFILE):
	rm -f *.ver
	touch $(VERFILE)

