# Sudokube
## Requirements
This project has the following dependencies:
- sbt 1.2.7
- JDK Version 8
- gcc
- make

## Instructions to build the shared library
- Set the environment variable `JAVA_HOME` to the home directory of the JDK installation. The folder `${JAVA_HOME}/include` must contain the header file `jni.h`
- Run `make` from the root directory of the project

## Instructions to run
- Run `sbt test` from the root directory of the project to run all the tests
- Run `sbt "runMain <mainclass>"` to run some class containing the main() method, for example, `example.Demotxt`



