#!/bin/bash

hosts=`cat hosts | xargs`
hostsWithCores=`echo $hosts | xargs -n1 printf "1/%s,"`

echo "HOSTS=$hosts with cores $hostsWithCores"

timestamp=`date +"%Y%m/%d/%H%M%S"`
numIters=100
echo $timestamp
#Run experiments
parallel --no-run-if-empty --colsep ' ' --env PATH  -S $hostsWithCores --wd /var/data/sudokube/sudokube --tag --lb sbt --error \"runMain {1} {2} $timestamp $numIters\" :::: scripts/jobs/allexpts
bash scripts/syncFolder.sh "$hosts" expdata
endtime=`date +"%Y%m/%d/%H%M%S"`
echo "Experiment started at $timestamp and finished at $endtime"