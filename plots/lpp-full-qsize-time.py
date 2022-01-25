#!/usr/bin/env python 

import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt
import numpy as np

plt.rcParams["figure.figsize"] = (8,6)

fig, (ax1,ax2) = plt.subplots(2,1)

def plot(input,ax):
	
	filename = input[16:-4]
	print(filename)

	def mean(x):
	    return sum(x) / len(x)

	naivefetch = defaultdict(lambda: [])
	naiveprepare = defaultdict(lambda: [])

	lpp_prepare =defaultdict(lambda: [])
	lpp_fetch = defaultdict(lambda: [])
	lpp_old_solve = defaultdict(lambda: [])
	lpp_new_solve = defaultdict(lambda: [])


	with open(input) as fh:
		header = [h.strip() for h in fh.next().split(',')]
		reader = csv.DictReader(fh, fieldnames=header)
		data = list(reader)
		timescale=1000.0*1000.0
		cname0=''
		for row in data:
			key = int(row['QSize'])
			cname0 = row['CubeName']
	
			nfetch = int(row['NFetchTime(us)'])/timescale
			nprep = int(row['NPrepareTime(us)'])/timescale
			lpprep = int(row['LPPrepareTime(us)'])/timescale
			lpfetch = int(row['LPFetchTime(us)'])/timescale
			lpsolve_new = (int(row['Init SliceSparse']) + int(row['ComputeBounds SliceSparse']))/timescale 
	
			naivefetch[key].append(nfetch)
			naiveprepare[key].append(nprep)
	
			lpp_fetch[key].append(lpfetch)
			lpp_new_solve[key].append(lpsolve_new)
			lpp_prepare[key].append(lpprep)

	cname=cname0.split('_')
	mod=int(cname[3])+int(cname[4])+1-int(cname[2])
	title="{}_{}_{}_{}".format(cname[0],cname[1],cname[2],mod)
	keys = sorted(naivefetch.keys())
	
	nfetch_avg = np.array(map(lambda x: mean(x[1]), sorted(naivefetch.iteritems(),key = lambda kv: kv[0])))
	nprep_avg = np.array(map(lambda x:  mean(x[1]), sorted(naiveprepare.iteritems(),key = lambda kv: kv[0])))
	lpp_prep_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_prepare.iteritems(),key = lambda kv: kv[0])))
	lpp_fetch_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_fetch.iteritems(),key = lambda kv: kv[0])))
	lpp_new_solve_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_new_solve.iteritems(), key = lambda kv: kv[0])))
	ax.title.set_text(title)

	N = len(keys)
	X = np.arange(0, N)    # set up a array of x-coordinates

	w=1.0/9

	ax.bar(X-3*w , lpp_prep_avg , w, bottom = 0.001, label='LPP Prepare', color='limegreen')
	ax.bar(X-2*w, nprep_avg, w, bottom = 0.001, label='Naive Prepare', color='darkgreen')
	ax.bar(X-w , lpp_fetch_avg, w, bottom = 0.001, label='LPP Fetch', color='turquoise')
	ax.bar(X ,nfetch_avg, w, label='Naive Fetch', bottom=0.001, color='blue')
	ax.bar(X+2*w , lpp_prep_avg + lpp_fetch_avg + lpp_new_solve_avg, w, bottom = 0.001, label='LPP Total', color='tomato')
	ax.bar(X+3*w , nprep_avg+nfetch_avg, w, label='Naive Total', bottom=0.001, color='red')
	ax.bar(X+w, lpp_new_solve_avg, w, bottom = 0.001, label='LPP Solve', color='gray')

	ax.set_ylabel('Time(s)')
	ax.set_xticks(X)
	ax.set_xticklabels(keys)
	ax.set_yscale('log')
	

plot('expdata/current/LP-Full_SSB-sf100_rms2_15_25_3-qs.csv', ax1)
plot('expdata/current/LP-Full_SSB-sf100_sms_15_25_3-qs.csv', ax2)

plt.subplots_adjust(bottom=0.25,hspace=0.4)
plt.xlabel('Query Dimensionality')
handles, labels = ax2.get_legend_handles_labels()
fig.legend(handles, labels, ncol=4, loc='upper center', fontsize='small', bbox_to_anchor=(0.5,0.13))
plt.savefig('figs/lpp-full-qsize-time.pdf',bbox_inches = 'tight')
