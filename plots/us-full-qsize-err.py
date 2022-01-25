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

	errors = defaultdict(lambda: [])
	

	cnames=filename[8:-3].split('_')
	mod = int(cnames[4]) + int(cnames[3]) + 1 - int(cnames[2])
	title="{}_{}_{}_{}".format(cnames[0], cnames[1], cnames[2], mod)

	with open(input) as fh:
		header = [h.strip() for h in fh.next().split(',')]
		reader = csv.DictReader(fh, fieldnames=header)
		data = list(reader)
		timescale=1000.0 * 1000.0
		for row in data:
			key = int(row['QSize'])
			err = float(row['CoMoment3 Err'])
			
			errors[key].append(err)
			
	keys = sorted(errors.keys())
	
	maxX = max(max(errors.values()))
	for s in sorted(errors.iteritems(), key=lambda kv: kv[0]):
		maxv = max(s[1])
		values, base = np.histogram(s[1], bins=10, range=(0.0, maxv+0.00001))
		total = sum(values)
		cumulative = map(lambda x: float(x)/total, np.insert(np.cumsum(values),0,0))

		ax.plot(base, cumulative, label=s[0])
	ax.set_ylabel('CDF')
	ax.set_xlim([-maxX/12.0, maxX])
	ax.title.set_text(title)
	
plot('expdata/current/US-Full_SSB-sf100_rms2_15_25_3-qs.csv', ax1)
plot('expdata/current/US-Full_SSB-sf100_sms_15_25_3-qs.csv', ax2)

plt.subplots_adjust(bottom=0.25,hspace=0.4)
plt.xlabel('Error')
handles, labels = ax2.get_legend_handles_labels()
fig.legend(handles, labels, ncol=4, loc='upper center', bbox_to_anchor=(0.5,0.13))
plt.savefig('figs/us-full-qsize-err.pdf',bbox_inches = 'tight')