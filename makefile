# This is for compiling the c code into a library.
# The Scala code needs to be compiled first since javah needs the bytecode
# to generate the c header file for JNI integration.

LIBS_HOME=/usr/local/Cellar/scala/2.12.7/libexec/lib
CP=$(LIBS_HOME)/scala-library.jar
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home

all:
	javah -cp $(CP):target/scala-2.12/classes:. backend.CBackend
	g++ -dynamiclib -O3 -I/usr/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin -I. src/main/cpp/CBackend.cpp src/main/cpp/CubeArrays.cpp src/main/cpp/Keys.cpp -o libCBackend.dylib

clean:
	rm -f libCBackend.dylib backend_CBackend.h time_test

time_test: src/main/cpp/main.cpp
	g++ -O3 src/main/cpp/main.cpp src/main/cpp/CubeArrays.cpp src/main/cpp/Keys.cpp -o time_test
