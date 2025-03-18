#!/bin/bash

hosts=$1
folder=$2

#Collect
for h in $hosts
do
	rsync -rptW --inplace   $h:/var/data/sudokube/sudokube/$folder/ /var/data/sudokube/sudokube/$folder
done	

#Distribute
for h in $hosts
do
	rsync -rptW --inplace  /var/data/sudokube/sudokube/$folder/ $h:/var/data/sudokube/sudokube/$folder
done	