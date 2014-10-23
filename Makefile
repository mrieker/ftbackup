
ftbackup: ftbackup.cpp ftbackup.h ftbreader.h ftbreader.cpp ftbwriter.h ftbwriter.cpp
	cc -Wall -Werror -g -o ftbackup ftbackup.cpp ftbreader.cpp ftbwriter.cpp -lpthread -lrt -lz -lstdc++

