#!/bin/bash

plots/datasets.py

plots/mat-stats.py expdata/MultiStorage_NYC_rms2_25.csv
plots/mat-stats.py expdata/MultiStorage_NYC_sms_25.csv

#LPP Full QSize
plots/lpp-full-qsize-time-hist.py expdata/current/LP-Full_SSB-sf100_sms_15_25_3-qs.csv
plots/lpp-full-qsize-time-hist.py expdata/current/LP-Full_SSB-sf100_rms2_15_25_3-qs.csv

#LPP Online QSize

sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter LP-Online_SSB-sf100_sms_15_25_3-qs.csv"
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter LP-Online_SSB-sf100_rms2_15_25_3-qs.csv"
plots/lpp-online-qsize.py expdata/current/LP-Online_SSB-sf100_sms_15_25_3-qs-ERROR.csv
plots/lpp-online-qsize.py expdata/current/LP-Online_SSB-sf100_rms2_15_25_3-qs-ERROR.csv


#LPP Full Cubes
#LPP Online Cubes

#------

#Uniform Full QSize
plots/us-full-qsize-time-hist.py expdata/current/US-Full_SSB-sf100_rms2_15_25_3-qs.csv
plots/us-full-qsize-time-hist.py expdata/current/US-Full_SSB-sf100_sms_15_25_3-qs.csv

#Uniform Online QSize
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_SSB-sf100_sms_15_25_3-qs.csv"
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_SSB-sf100_rms2_15_25_3-qs.csv"
plots/us-online-qsize.py expdata/current/US-Online_SSB-sf100_sms_15_25_3-qs-ERROR.csv
plots/us-online-qsize.py expdata/current/US-Online_SSB-sf100_rms2_15_25_3-qs-ERROR.csv


#Uniform Full Cubes
plots/us-full-cubename-time-hist.py expdata/current/US-Full_NYC_rms2-cubes.csv
plots/us-full-cubename-time-hist.py expdata/current/US-Full_NYC_sms-cubes.csv

#Uniform Online Cubes
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_NYC_rms2-cubes.csv"
plots/us-online-cubename.py expdata/current/US-Online_NYC_rms2-cubes-ERROR.csv
sbt --error 'set showSuccess := false' "runMain experiments.plotters.OnlinePlotter US-Online_NYC_sms-cubes.csv"
plots/us-online-cubename.py expdata/current/US-Online_NYC_sms-cubes-ERROR.csv

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

