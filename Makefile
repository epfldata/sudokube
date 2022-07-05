# This is for compiling the c code into a library.
# The Scala code needs to be compiled first since javah needs the bytecode
# to generate the c header file for JNI integration.

CP=~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.12.7.jar
#JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
#JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_281.jdk/Contents/Home
osname=$(shell uname | tr A-Z a-z)
jni_include=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(osname)
lib_src=src/main/cpp/CBackend.cpp src/main/cpp/CubeArrays.cpp
$(info osname is $(osname))
ifeq ($(osname), darwin)
	lib=libCBackend.dylib
else
	lib=libCBackend.so
endif

libCBackend: $(lib)
backend_CBackend.h: src/main/scala/backend/CBackend.scala
	$(info java home is $(JAVA_HOME))
ifeq ($(JAVA_HOME),)
	$(error JAVA_HOME is not set)
endif
	sbt compile
	javah -cp $(CP):target/scala-2.12/classes:. backend.CBackend

libCBackend.so: backend_CBackend.h $(lib_src)
	g++ -std=c++11 -shared -fPIC -O3 $(jni_include) -I. $(lib_src) -o libCBackend.so

libCBackend.dylib: backend_CBackend.h $(lib_src)
	g++ -std=c++11 -dynamiclib -O3 $(jni_include) -I. $(lib_src) -o libCBackend.dylib

clean:
	rm -f libCBackend.so libCBackend.dylib backend_CBackend.h time_test

time_test: src/main/cpp/main.cpp
	g++ -O3 src/main/cpp/main.cpp src/main/cpp/CubeArrays.cpp  -o time_test
