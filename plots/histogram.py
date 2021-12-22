#!/usr/bin/env python 

import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt
import numpy as np


input =  sys.argv[1]

filename = input[8:-4]
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


with open(input) as fh:
	header = [h.strip() for h in fh.next().split(',')]
	reader = csv.DictReader(fh, fieldnames=header)
	data = list(reader)
	print(header)
	
	for row in data:
		key = int(row['QSize'])
		dof = int(row['DOF'])
		err = float(row['CoMoment3 Err'])
		nfetch = int(row['NFetchTime(us)'])
		nprep = int(row['NPrepareTime(us)'])
		uprep = int(row['UPrepareTime(us)'])
		ufetch = int(row['UFetchTime(us)'])
		usolve = int(row['CoMoment3 AddTime(us)']) + int(row['CoMoment3 FillTime(us)']) + int(row['CoMoment3 SolveTime(us)'])

		dofs[key].append(dof)
		errors[key].append(err)
		naivefetch[key].append(nfetch)
		naiveprepare[key].append(nprep)
		solverfetch[key].append(ufetch)
		solversolve[key].append(usolve)
		solverprepare[key].append(uprep)


keys = sorted(map(lambda x: x[0], dofs.iteritems()))
#dof_avg = map(lambda x: mean(x[1]) , sorted(dofs.iteritems(), key = lambda kv: kv[0]))
#err_avg = map(lambda x: mean(x[1]), sorted(errors.iteritems(), key = lambda kv: kv[0]))
nfetch_avg = map(lambda x: mean(x[1]), sorted(naivefetch.iteritems(),key = lambda kv: kv[0]))
nprep_avg = map(lambda x:  mean(x[1]), sorted(naiveprepare.iteritems(),key = lambda kv: kv[0]))
uprep_avg = np.array(map(lambda x: mean(x[1]), sorted(solverprepare.iteritems(),key = lambda kv: kv[0])))
ufetch_avg = np.array(map(lambda x: mean(x[1]), sorted(solverfetch.iteritems(),key = lambda kv: kv[0])))
usolve_avg = map(lambda x: mean(x[1]), sorted(solversolve.iteritems(), key = lambda kv: kv[0]))

#print("DOF", dof_avg)
#print("Error", err_avg)
#print("Naive Fetch", nfetch_avg)
#print("Naive Prepare", nprep_avg)
#print("Solver Prepare", uprep_avg)
#print("Solver Fetch", ufetch_avg)
#print("Solver Solve", usolve_avg)


N = len(keys)
X = np.arange(0, N)    # set up a array of x-coordinates

w=0.25
fig, ax = plt.subplots()

ax.bar(X , uprep_avg, w, bottom = 1, label='Solver Prepare', color='tomato')
ax.bar(X , ufetch_avg, w, bottom = uprep_avg, label='Solver Fetch', color='turquoise')
ax.bar(X , usolve_avg, w, bottom = uprep_avg + ufetch_avg, label='Solver Solve', color='limegreen')

ax.bar(X-w , nprep_avg, w, label='Naive Prepare', bottom=1, color='red')
ax.bar(X-w , nfetch_avg, w, bottom = nprep_avg, label='Naive Fetch', color='blue')

ax.set_ylabel('Time(us)')
ax.set_xlabel('Query Size')
ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_yscale('log')
# Shrink current axis by 30%
box = ax.get_position()
ax.set_position([box.x0, box.y0+0.2, box.width , box.height * 0.8])

ax.legend(loc='lower center', ncol=2, bbox_to_anchor=(0.5, -0.4))
#plt.show()
plt.savefig('figs/'+filename+'-time.pdf')


######################

fig, ax = plt.subplots()
data = map(lambda kv: kv[1], sorted(errors.iteritems(), key=lambda kv: kv[0]))

ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_xlabel('Query Size')

ax.boxplot(data)
ax.set_ylabel('Error')

plt.savefig('figs/'+filename+'-err.pdf')