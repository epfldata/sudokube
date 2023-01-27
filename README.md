# Sudokube
Sudokube is a data cube system that supports fast aggregation queries on high-dimensional data. Like traditional data cubes,
Sudokube supports OLAP operations such as roll-up, drill-down, slicing and dicing, but even on high-dimensional data that cannot be supported at interactive
speeds using previous technology. For high-dimensional data, the full materialization involving all possible projections is not possible due to storage and compute limitations.
When only some projections can be materialized, current approaches evaluate queries from the smallest materialized projection that contains the query, which in practice can be slow for large volumes of data.
Sudokube, on the other hand, tries to approximate query results from all available projections, incrementally improving the results as more projections are processed in an online fashion.

Technical details can be found in our VLDB'22 paper, [High-dimensional Data Cubes](https://vldb.org/pvldb/volumes/15/paper/High-dimensional%20Data%20Cubes)
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
The complete reproducibility package can be found under [experiments/vldb2022_sudokube_reproducibility.zip.](experiments/vldb2022_sudokube_reproducibility.zip)
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
