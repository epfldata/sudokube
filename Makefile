# This is for compiling the c code into a library.
# The Scala code needs to be compiled first since javah needs the bytecode
# to generate the c header file for JNI integration.

CP=~/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.12.7.jar
#JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
#JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_281.jdk/Contents/Home
osname=$(shell uname | tr A-Z a-z)
jni_include=-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(osname)
liborig_src=src/main/cpp/CBackend.cpp src/main/cpp/CubeArrays.cpp 
liborig_h=src/main/cpp/Keys.h 
librowstore_src=src/main/cpp/RowStoreCBackend.cpp src/main/cpp/RowStore.cpp
librowstore_h=src/main/cpp/RowStore.h src/main/cpp/common.h
$(info osname is $(osname))
ifeq ($(osname), darwin)
liborig=libCBackend.dylib
librowstore=libRowStoreCBackend.dylib
else
liborig=libCBackend.so
librowstore=libRowStoreCBackend.so
endif
ifeq ($(JAVA_HOME),)
$(error JAVA_HOME is not set)
endif

all: libCBackend libRowStoreCBackend

libCBackend: $(liborig)
libRowStoreCBackend: $(librowstore)

backend_CBackend.h: src/main/scala/backend/CBackend.scala
	$(info java home is $(JAVA_HOME))
	sbt compile
	javah -cp $(CP):target/scala-2.12/classes:. backend.CBackend

backend_RowStoreCBackend.h: src/main/scala/backend/RowStoreCBackend.scala
	$(info java home is $(JAVA_HOME))
	sbt compile
	javah -cp $(CP):target/scala-2.12/classes:. backend.RowStoreCBackend


libCBackend.so: backend_CBackend.h $(liborig_src) $(liborig_h)
	g++ -std=c++17 -shared -fPIC -O3 $(jni_include) -I. $(liborig_src) -o libCBackend.so
libRowStoreCBackend.so: backend_RowStoreCBackend.h $(librowstore_src) $(librowstore_h)
	g++ -std=c++17 -shared -fPIC -O3 $(jni_include) -I. $(librowstore_src) -o libRowStoreCBackend.so



libCBackend.dylib: backend_CBackend.h $(liborig_src) $(liborig_h)
	g++ -std=c++17 -dynamiclib -O3 $(jni_include) -I. $(liborig_src) -o libCBackend.dylib
libRowStoreCBackend.dylib: backend_RowStoreCBackend.h $(librowstore_src) $(librowstore_h)
	g++ -std=c++17 -dynamiclib -O3 $(jni_include) -I. $(librowstore_src) -o libRowStoreCBackend.dylib


clean:
	rm -f *.so *.dylib *.h

