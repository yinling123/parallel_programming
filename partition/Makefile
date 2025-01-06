CCOMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I system -Wno-deprecated -O2
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs

all: partition

partition: partition.cpp
	mpic++ -std=c++11 -I/mnt/sevenT/yinboh/software/hadoop/hadoop-2.10.1/include -I/mnt/sevenT/yinboh/software/jdk/jdk1.8.0_144/include -I/mnt/sevenT/yinboh/software/jdk/jdk1.8.0_144/include/linux -I system -Wno-deprecated -O2 \
-L/mnt/sevenT/yinboh/software/hadoop/hadoop-2.10.1/lib/native \
-L/mnt/sevenT/yinboh/software/jdk/jdk1.8.0_144/jre/lib/amd64/server \
-Wl,-rpath,/mnt/sevenT/yinboh/software/jdk/jdk1.8.0_144/jre/lib/amd64/server \
partition.cpp -lhdfs -ljvm -lpthread -o partition

clean:
	-rm partition
