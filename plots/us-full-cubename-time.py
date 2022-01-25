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

def plot(input, ax):

	filename = input[16:-4]
	print(filename)

	def mean(x):
	    return sum(x) / len(x)

	naivefetch = defaultdict(lambda: [])
	naiveprepare = defaultdict(lambda: [])
	solverprepare =defaultdict(lambda: [])
	solverfetch = defaultdict(lambda: [])
	solversolve = defaultdict(lambda: [])


	with open(input) as fh:
		header = [h.strip() for h in fh.next().split(',')]
		reader = csv.DictReader(fh, fieldnames=header)
		data = list(reader)
		timescale=1000.0*1000.0
		title = ''
		for row in data:
			keyfull = row['Name'].split('_')
			#print(keyfull)
			mod = int(keyfull[3])-int(keyfull[2])+1
			modstr = str(mod).zfill(2)
			title="{}-{}".format(keyfull[0],keyfull[1])
			key = "{}_{}".format(keyfull[2], modstr)
			
			nfetch = int(row['NFetchTime(us)'])/timescale
			nprep = int(row['NPrepareTime(us)'])/timescale
			uprep = int(row['UPrepareTime(us)'])/timescale
			ufetch = int(row['UFetchTime(us)'])/timescale
			usolve = int(row['CoMoment3 SolveTime(us)'])/timescale

			naivefetch[key].append(nfetch)
			naiveprepare[key].append(nprep)
			solverfetch[key].append(ufetch)
			solversolve[key].append(usolve)
			solverprepare[key].append(uprep)

	keys = sorted(naivefetch.keys())
	
	nfetch_avg = np.array(map(lambda x: mean(x[1]), sorted(naivefetch.iteritems(),key = lambda kv: kv[0])))
	nprep_avg = np.array(map(lambda x:  mean(x[1]), sorted(naiveprepare.iteritems(),key = lambda kv: kv[0])))
	uprep_avg = np.array(map(lambda x: mean(x[1]), sorted(solverprepare.iteritems(),key = lambda kv: kv[0])))
	ufetch_avg = np.array(map(lambda x: mean(x[1]), sorted(solverfetch.iteritems(),key = lambda kv: kv[0])))
	usolve_avg = np.array(map(lambda x: mean(x[1]), sorted(solversolve.iteritems(), key = lambda kv: kv[0])))

	print(nprep_avg)
	print(uprep_avg)

	N = len(keys)
	X = np.arange(0, N)    # set up a array of x-coordinates
	w=1.0/9
	
	ax.bar(X-3*w , uprep_avg, bottom=0.001, width=w, label='Solver Prepare', color='limegreen')
	ax.bar(X-2*w , nprep_avg , bottom = 0.001, width=w,label='Naive Prepare', color='darkgreen')
	ax.bar(X-w , ufetch_avg, bottom=0.001, width=w, label='Solver Fetch', color='turquoise')
	ax.bar(X , nfetch_avg, bottom = 0.001, width=w,label='Naive Fetch', color='blue')
	ax.bar(X+2*w , uprep_avg+ufetch_avg+usolve_avg, width=w, bottom=0.001, label='Solver Total', color='tomato')
	ax.bar(X+3*w , nprep_avg + nfetch_avg, bottom = 0.001, width=w,label='Naive Total', color='red')
	ax.bar(X+w , usolve_avg, bottom=0.001, width=w, label='Solver Solve', color='gray')

	ax.set_ylabel('Time(s)')
	ax.set_xticks(X)
	ax.set_xticklabels(keys)
	ax.set_yscale('log')
	ax.title.set_text(title)
plot('expdata/current/US-Full_NYC_rms2-cubes.csv', ax1)
plot('expdata/current/US-Full_NYC_sms-cubes.csv', ax2)

plt.subplots_adjust(bottom=0.25,hspace=0.4)
plt.xlabel('logn_dmin')
handles, labels = ax2.get_legend_handles_labels()
fig.legend(handles, labels, ncol=4, loc='upper center', fontsize='small', bbox_to_anchor=(0.5,0.13))
plt.savefig('figs/us-full-cubename-time.pdf',bbox_inches = 'tight')