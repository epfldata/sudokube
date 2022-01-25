#!/bin/bash

plots/datasets.py

plots/mat-stats.py expdata/MultiStorage_NYC_rms2_25.csv
plots/mat-stats.py expdata/MultiStorage_NYC_sms_25.csv

#LPP Full QSize
plots/lpp-full-qsize-time.py 

#LPP Online QSize
#LPP Full Cubes
#LPP Online Cubes

#------

#Uniform Full QSize
plots/us-full-qsize-time.py
plots/us-full-qsize-err.py

#Uniform Online QSize
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_SSB-sf100_sms_15_25_3-qs.csv"
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_SSB-sf100_rms2_15_25_3-qs.csv"

plots/us-online-qsize.py 


#Uniform Full Cubes
plots/us-full-cubename-time.py
plots/us-full-cubename-err.py

#Uniform Online Cubes
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_NYC_rms2-cubes.csv"
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_NYC_sms-cubes.csv"
plots/us-online-cubename.py 

#-------

#Microbench
#sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_mb_15_Exponential_all-qs.csv"
#sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_mb_15_LogNormal_all-qs.csv"
#sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_mb_15_Normal_all-qs.csv"
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_mb_15_Uniform_all-qs.csv"
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_mb_15_all-data.csv"

#plots/us-online-microbench.py expdata/current/US-Online_mb_15_Exponential_all-qs-ERROR.csv
#plots/us-online-microbench.py expdata/current/US-Online_mb_15_LogNormal_all-qs-ERROR.csv
#plots/us-online-microbench.py expdata/current/US-Online_mb_15_Normal_all-qs-ERROR.csv
plots/us-online-microbench-qsize.py expdata/current/US-Online_mb_15_Uniform_all-qs-ERROR.csv
plots/us-online-microbench-data.py expdata/current/US-Online_mb_15_all-data-ERROR.csv

