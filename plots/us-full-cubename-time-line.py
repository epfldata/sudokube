#!/usr/bin/env python 

import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt
import numpy as np


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


with open(input) as fh:
	header = [h.strip() for h in fh.next().split(',')]
	reader = csv.DictReader(fh, fieldnames=header)
	data = list(reader)
	print(header)
	print(len(data))
	onemill=1000.0 * 1000.0
	for row in data:
		keyfull = row['Name'].split('_')
		#print("key0={} A={} B={} A-B+1={}".format(key0, key0[2], key0[1],int(key0[2])-int(key0[1])+1))
		mod = int(keyfull[2])-int(keyfull[1])+int(keyfull[3])+1
		modstr = str(mod).zfill(2)
		key = "{}_{}".format(keyfull[1], modstr)
		dof = int(row['DOF'])
		err = float(row['CoMoment3 Err'])
		nfetch = int(row['NFetchTime(us)'])/onemill
		nprep = int(row['NPrepareTime(us)'])/onemill
		uprep = int(row['UPrepareTime(us)'])/onemill
		ufetch = int(row['UFetchTime(us)'])/onemill
		usolve = int(row['CoMoment3 SolveTime(us)'])/onemill

		dofs[key].append(dof)
		errors[key].append(err)
		naivefetch[key].append(nfetch)
		naiveprepare[key].append(nprep)
		solverfetch[key].append(ufetch)
		solversolve[key].append(usolve)
		solverprepare[key].append(uprep)

keys = sorted(map(lambda x: x[0], naivefetch.iteritems()))
print(keys)
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

fig, ax = plt.subplots()

print(uprep_avg+ufetch_avg)
ax.plot(X , uprep_avg+ufetch_avg, label='Solver Prepare + Fetch', color='gray')
ax.plot(X , usolve_avg, label='Solver Solve', color='limegreen')
ax.plot(X , uprep_avg+ufetch_avg+usolve_avg,lw=2, label='Solver Total', color='darkgreen')
ax.plot(X , nprep_avg + nfetch_avg, lw=2,label='Naive Prepare + Fetch', color='red')

ax.set_ylabel('Time(s)')
ax.set_xlabel('CubeName')
ax.set_xticks(X)
ax.set_xticklabels(keys,rotation='vertical')
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
ax.set_xlabel('CubeName')

ax.boxplot(data)
ax.set_ylabel('Error')

plt.savefig('figs/'+filename+'-err.pdf')
