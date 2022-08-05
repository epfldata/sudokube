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
In order to reproduce the experiment with fixed queries (Fig 12) exactly, we have fixed the seed of the random generator
that is used in deciding what cuboids are materialized to zero. This can be disabled by editing the files `src/main/scala/frontend/generators/NYC.scala` and  `src/main/scala/frontend/generators/SSB.scala` before generating the data cube.
- [New York Parking Violations Dataset](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/kvfd-bves)
	+ Run `dataloading-scripts/nyc.sh`
- [Star Schema Benchmark](https://github.com/eyalroz/ssb-dbgen)
	+ Follow instructions to build `ssb-dbgen` in the same folder containing the `sudokube` repository. In our scripts, we use `../ssb-dbgen` from the root directory of our project to access the generator.
	+ Run `dataloading-scripts/ssb.sh`
- Warmup Dataset
	+ Run `sbt "runMain frontend.generators.Warmup"`


## Run Experiments from our paper
- sbt --error 'runMain experiments.Experimenter Fig7'
- sbt --error 'runMain experiments.Experimenter Tab1'
- sbt --error 'runMain experiments.Experimenter Fig8-RMS'
- sbt --error 'runMain experiments.Experimenter Fig8-SMS'
- sbt --error 'runMain experiments.Experimenter Fig9-RMS'
- sbt --error 'runMain experiments.Experimenter Fig9-SMS'
- sbt --error 'runMain experiments.Experimenter Fig10-RMS'
- sbt --error 'runMain experiments.Experimenter Fig10-SMS'
- sbt --error 'runMain experiments.Experimenter Fig11'
- sbt --error 'runMain experiments.Experimenter Fig12-NYC'
- sbt --error 'runMain experiments.Experimenter Fig12-SSB'
