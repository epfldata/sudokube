#!/bin/bash

hosts=`cat hosts | xargs`
hostsWithCores=`echo $hosts | xargs -n1 printf "1/%s,"`

echo "HOSTS=$hosts with cores $hostsWithCores"

timestamp=`date +"%Y%m/%d/%H%M%S"`
echo $timestamp
#Run experiments
parallel --env PATH  -S $hostsWithCores --wd /var/data/sudokube/sudokube --tag --lb sbt --error \"runMain experiments.IPFExperimenter {1} $timestamp\" :::: scripts/jobs/ipfexpts
bash scripts/syncFolder.sh "$hosts" expdata