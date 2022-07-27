#!/bin/bash

wget https://data.cityofnewyork.us/api/views/kvfd-bves/rows.tsv -O tabledata/nyc/2021.tsv &
wget https://data.cityofnewyork.us/api/views/p7t3-5i9s/rows.tsv -O tabledata/nyc/2020.tsv &
wget https://data.cityofnewyork.us/api/views/faiq-9dfq/rows.tsv -O tabledata/nyc/2019.tsv &
wget https://data.cityofnewyork.us/api/views/a5td-mswe/rows.tsv -O tabledata/nyc/2018.tsv &
wget https://data.cityofnewyork.us/api/views/2bnn-yakx/rows.tsv -O tabledata/nyc/2017.tsv &
wget https://data.cityofnewyork.us/api/views/kiv2-tbus/rows.tsv -O tabledata/nyc/2016.tsv &
wget https://data.cityofnewyork.us/api/views/c284-tqph/rows.tsv -O tabledata/nyc/2015.tsv &
wget https://data.cityofnewyork.us/api/views/jt7v-77mi/rows.tsv -O tabledata/nyc/2014.tsv &

wait

mkdir -p tabledata/nyc/uniq

#merge all years into single file
(cd tabledata/nyc && ls *.tsv | xargs -Ifile sh -c 'tail -n+2 $1 >> all' -- file)

#remove the unmerged files
(cd tabledata/nyc && rm *.tsv)

#Generate unique values for every column
(cd tabledata/nyc/uniq && seq 1 38 | xargs -P8 -n1 ../cutuniq.sh all)

#split into 1000 files for parallel processing
(cd tabledata/nyc && ./split.sh)

#remove the merged file all
(cd tabledata/nyc && rm all)

#Build Datacube
sbt --error 'set showSuccess := false' "runMain frontend.generators.NYC"
