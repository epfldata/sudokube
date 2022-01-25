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
	

	with open(input) as fh:
		header = [h.strip() for h in fh.next().split(',')]
		reader = csv.DictReader(fh, fieldnames=header)
		data = list(reader)
		timescale=1000.0*1000.0
		title = ''
		for row in data:
			keyfull = row['Name'].split('_')
			#print("key0={} A={} B={} A-B+1={}".format(key0, key0[2], key0[1],int(key0[2])-int(key0[1])+1))
			mod = int(keyfull[3])-int(keyfull[2])+1
			modstr = str(mod).zfill(2)
			title="{}_{}".format(keyfull[0],keyfull[1])
			key = "{}_{}".format(keyfull[2], modstr)
				
			err = float(row['CoMoment3 Err'])
		
			errors[key].append(err)
			
	keys = sorted(errors.keys())
	

	colors = [ 'limegreen', 'red', 'green', 'blue', 'darkolivegreen']
	styles = ['dotted', 'solid', 'solid', 'solid', 'dashed']

	maxX = max(max(errors.values()))
	for i,s in enumerate(sorted(errors.iteritems(), key=lambda kv: kv[0])):
		maxv = max(s[1])
		values, base = np.histogram(s[1], bins=10, range=(0.0, maxv+0.00001))
		total = sum(values)
		cumulative = map(lambda x: float(x)/total, np.insert(np.cumsum(values),0,0))
		ax.plot(base, cumulative, label=s[0],c=colors[i],ls=styles[i])
	ax.set_ylabel('CDF')
	ax.set_xlim([-maxX/12.0, maxX])
	ax.title.set_text(title)


plot('expdata/current/US-Full_NYC_rms2-cubes.csv', ax1)
plot('expdata/current/US-Full_NYC_sms-cubes.csv', ax2)


plt.subplots_adjust(bottom=0.25,hspace=0.4)
plt.xlabel('Error')
handles, labels = ax2.get_legend_handles_labels()
fig.legend(handles, labels, ncol=5, loc='upper center', fontsize='small', bbox_to_anchor=(0.5,.13))
plt.savefig('figs/us-full-cubename-err.pdf',bbox_inches = 'tight')