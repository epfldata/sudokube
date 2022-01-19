#!/usr/bin/env python 

import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt
import numpy as np

plt.rcParams["figure.figsize"] = (8,4)
input =  sys.argv[1]

filename = input[16:-4]
print(filename)

def mean(x):
    return sum(x) / len(x)

dofs = defaultdict(lambda: [])
errors = defaultdict(lambda: [])
naivefetch = defaultdict(lambda: [])
naiveprepare = defaultdict(lambda: [])
solverprepare =defaultdict(lambda: [])
solverfetch = defaultdict(lambda: [])
solversolve = defaultdict(lambda: [])

cnames=filename[8:-3].split('_')
mod = int(cnames[4]) + int(cnames[3]) + 1 - int(cnames[2])
title0="{}_{}_{}_{}".format(cnames[0], cnames[1], cnames[2], mod)

with open(input) as fh:
	header = [h.strip() for h in fh.next().split(',')]
	reader = csv.DictReader(fh, fieldnames=header)
	data = list(reader)
	timescale=1000.0 * 1000.0
	for row in data:
		key = int(row['QSize'])
		dof = int(row['DOF'])
		err = float(row['CoMoment3 Err'])
		nfetch = int(row['NFetchTime(us)'])/timescale
		nprep = int(row['NPrepareTime(us)'])/timescale
		uprep = int(row['UPrepareTime(us)'])/timescale
		ufetch = int(row['UFetchTime(us)'])/timescale
		usolve = int(row['CoMoment3 SolveTime(us)'])/timescale

		dofs[key].append(dof)
		errors[key].append(err)
		naivefetch[key].append(nfetch)
		naiveprepare[key].append(nprep)
		solverfetch[key].append(ufetch)
		solversolve[key].append(usolve)
		solverprepare[key].append(uprep)

keys = sorted(map(lambda x: x[0], naivefetch.iteritems()))
#print(keys)
#dof_avg = map(lambda x: mean(x[1]) , sorted(dofs.iteritems(), key = lambda kv: kv[0]))
#err_avg = map(lambda x: mean(x[1]), sorted(errors.iteritems(), key = lambda kv: kv[0]))
nfetch_avg = np.array(map(lambda x: mean(x[1]), sorted(naivefetch.iteritems(),key = lambda kv: kv[0])))
nprep_avg = np.array(map(lambda x:  mean(x[1]), sorted(naiveprepare.iteritems(),key = lambda kv: kv[0])))
uprep_avg = np.array(map(lambda x: mean(x[1]), sorted(solverprepare.iteritems(),key = lambda kv: kv[0])))
ufetch_avg = np.array(map(lambda x: mean(x[1]), sorted(solverfetch.iteritems(),key = lambda kv: kv[0])))
usolve_avg = np.array(map(lambda x: mean(x[1]), sorted(solversolve.iteritems(), key = lambda kv: kv[0])))

#print("DOF", dof_avg)
#print("Error", err_avg)
#print("Naive Fetch", nfetch_avg)
#print("Naive Prepare", nprep_avg)
#print("Solver Prepare", uprep_avg)
#print("Solver Fetch", ufetch_avg)
#print("Solver Solve", usolve_avg)


N = len(keys)
X = np.arange(0, N)    # set up a array of x-coordinates
w=1.0/9
fig, ax = plt.subplots()

ax.bar(X-3*w , uprep_avg, bottom=0.001, width=w, label='Solver Prepare', color='limegreen')
ax.bar(X-2*w , nprep_avg , bottom = 0.001, width=w,label='Naive Prepare', color='darkgreen')
ax.bar(X-w , ufetch_avg, bottom=0.001, width=w, label='Solver Fetch', color='turquoise')
ax.bar(X , nfetch_avg, bottom = 0.001, width=w,label='Naive Fetch', color='blue')
ax.bar(X+2*w , uprep_avg+ufetch_avg+usolve_avg, width=w, bottom=0.001, label='Solver Total', color='tomato')
ax.bar(X+3*w , nprep_avg + nfetch_avg, bottom = 0.001, width=w,label='Naive Total', color='red')
ax.bar(X+w , usolve_avg, bottom=0.001, width=w, label='Solver Solve', color='gray')

ax.set_ylabel('Time(s)')
ax.set_xlabel('Query Dimensionality')
ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_yscale('log')
# Shrink current axis by 30%
box = ax.get_position()
ax.set_position([box.x0, box.y0+0.2, box.width , box.height * 0.8])
plt.title(title0)
ax.legend(loc='lower center', ncol=4, bbox_to_anchor=(0.5, -0.4), fontsize='x-small')
#plt.show()
plt.savefig('figs/'+filename+'-time.pdf',bbox_inches = 'tight',pad_inches = 0.1)

"""
######################

fig, ax = plt.subplots()
data = map(lambda kv: kv[1], sorted(errors.iteritems(), key=lambda kv: kv[0]))

ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_xlabel('Query Dimensionality')

ax.boxplot(data)
ax.set_ylabel('Error')
plt.title(title0)
plt.savefig('figs/'+filename+'-err.pdf')

######################
"""
fig,ax = plt.subplots()
maxX = max(max(errors.values()))
for s in sorted(errors.iteritems(), key=lambda kv: kv[0]):
	maxv = max(s[1])
	values, base = np.histogram(s[1], bins=10, range=(0.0, maxv+0.00001))
	total = sum(values)
	cumulative = map(lambda x: float(x)/total, np.insert(np.cumsum(values),0,0))

	ax.plot(base, cumulative, label=s[0])
ax.set_xlabel('Error')
ax.set_ylabel('CDF')
ax.set_xlim([-maxX/12.0, maxX])
ax.legend(loc='lower right')
plt.title(title0)
plt.savefig('figs/'+filename+'-err.pdf',bbox_inches = 'tight',pad_inches = 0.1)
