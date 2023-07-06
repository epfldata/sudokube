#!/bin/bash

hosts=`cat hosts | xargs`
hostsWithCores=`echo $hosts | xargs -n1 printf "1/%s,"`

echo "HOSTS=$hosts with cores $hostsWithCores"
if [ "$#" -eq 0 ]; then
    echo "Please provide the file containing all the jobs to be executed."
    exit;
fi
jobname=$1

timestamp=`date +"%Y%m/%d/%H%M%S"`
numIters=100
echo $timestamp
#Run experiments
parallel --no-run-if-empty --colsep ' ' --env PATH  -S $hostsWithCores --wd /var/data/sudokube/sudokube --tag --lb sbt --error \"runMain {1} {2} $timestamp $numIters\" :::: scripts/jobs/${jobname}
bash scripts/syncFolder.sh "$hosts" expdata
endtime=`date +"%Y%m/%d/%H%M%S"`
echo "Experiment started at $timestamp and finished at $endtime"