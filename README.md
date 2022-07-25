# Sudokube
## Requirements
This project has the following dependencies:
- sbt
- JDK Version 8
- gcc
- make

## Instructions to build the shared library libCBackend
- Set the environment variable `JAVA_HOME` to the home directory of the JDK installation. The folder `${JAVA_HOME}/include` must contain the header file `jni.h`
- Run `make` from the root directory of the project

## Instructions to run
- Run `sbt test` from the root directory of the project to run all the tests
- Run `sbt "runMain <classname>"` to run some class containing the main method, for example, `example.Demotxt`

## Generate data and build data cube
- [New York Parking Violations Dataset](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/kvfd-bves)
	+ Run `dataloading-scripts/nyc.sh`
- [Star Schema Benchmark](https://github.com/eyalroz/ssb-dbgen)
	+ Follow instructions to build `ssb-dbgen` in the same folder containing the `sudokube` repository. In our scripts, we use `../ssb-dbgen` from the root directory of our project to access the generator.
	+ Run `dataloading-scripts/ssb.sh`
- Warmup Dataset
	+ Run `sbt "runMain frontend.generators.Warmup"`

## Run Experiments from our paper
- sbt --error 'set showSuccess := false' "runMain experiments.Experimenter Fig7"
- sbt --error 'set showSuccess := false' "runMain experiments.Experimenter Tab1"
- sbt --error 'set showSuccess := false' "runMain experiments.Experimenter Fig8"
- sbt --error 'set showSuccess := false' "runMain experiments.Experimenter Fig9"
- sbt --error 'set showSuccess := false' "runMain experiments.Experimenter Fig10"
- sbt --error 'set showSuccess := false' "runMain experiments.Experimenter Fig11"


