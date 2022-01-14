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

dofs_old = defaultdict(lambda: [])
errors_old = defaultdict(lambda: [])
dofs_new = defaultdict(lambda: [])
errors_new = defaultdict(lambda: [])
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
	print(header)
	onemill=1000.0 * 1000.0
	for row in data:
		key = int(row['QSize'])
		dof_new = int(row['DOF SliceSparse'])
		err_new = float(row['Error SliceSparse'])

		dof_old = int(row['DOF Sparse'])
		err_old = float(row['Error Sparse'])
		nfetch = int(row['NFetchTime(us)'])/onemill
		nprep = int(row['NPrepareTime(us)'])/onemill
		lpprep = int(row['LPPrepareTime(us)'])/onemill
		lpfetch = int(row['LPFetchTime(us)'])/onemill
		lpsolve_new = (int(row['Init SliceSparse']) + int(row['ComputeBounds SliceSparse']))/onemill 
		lpsolve_old = (int(row['Init Sparse']) + int(row['ComputeBounds Sparse']))/onemill

		dofs_old[key].append(dof_old)
		errors_old[key].append(err_old)
		dofs_new[key].append(dof_new)
		errors_new[key].append(err_new)

		naivefetch[key].append(nfetch)
		naiveprepare[key].append(nprep)
		lpp_fetch[key].append(lpfetch)
		lpp_old_solve[key].append(lpsolve_old)
		lpp_new_solve[key].append(lpsolve_new)
		lpp_prepare[key].append(lpprep)


keys = sorted(map(lambda x: x[0], naivefetch.iteritems()))
#dof_avg = map(lambda x: mean(x[1]) , sorted(dofs.iteritems(), key = lambda kv: kv[0]))
#err_avg = map(lambda x: mean(x[1]), sorted(errors.iteritems(), key = lambda kv: kv[0]))
nfetch_avg = np.array(map(lambda x: mean(x[1]), sorted(naivefetch.iteritems(),key = lambda kv: kv[0])))
nprep_avg = np.array(map(lambda x:  mean(x[1]), sorted(naiveprepare.iteritems(),key = lambda kv: kv[0])))
lpp_prep_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_prepare.iteritems(),key = lambda kv: kv[0])))
lpp_fetch_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_fetch.iteritems(),key = lambda kv: kv[0])))
lpp_old_solve_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_old_solve.iteritems(), key = lambda kv: kv[0])))
lpp_new_solve_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_new_solve.iteritems(), key = lambda kv: kv[0])))

#print("DOF", dof_avg)
#print("Error", err_avg)
#print("Naive Fetch", nfetch_avg)
#print("Naive Prepare", nprep_avg)
#print("Solver Prepare", uprep_avg)
#print("Solver Fetch", ufetch_avg)
#print("Solver Solve", usolve_avg)


N = len(keys)
X = np.arange(0, N)    # set up a array of x-coordinates

#w=0.25
fig, ax = plt.subplots()


ax.plot(X , lpp_prep_avg+lpp_fetch_avg, label='LPP Prepare + Fetch', color='gray')
ax.plot(X , lpp_old_solve_avg, label='LPP Old Solve', color='limegreen')
ax.plot(X, lpp_new_solve_avg, label='LPP New Solve', color='turquoise')
ax.plot(X , lpp_prep_avg+lpp_fetch_avg+lpp_old_solve_avg,lw=2, label='LPP Old Total', color='darkgreen')
ax.plot(X, lpp_prep_avg+lpp_fetch_avg+lpp_new_solve_avg, lw=2, label='LPP New Total', color='blue')
ax.plot(X , nprep_avg + nfetch_avg, lw=2,label='Naive Prepare + Fetch', color='red')

ax.set_ylabel('Time(s)')
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
data_old = map(lambda kv: kv[1], sorted(errors_old.iteritems(), key=lambda kv: kv[0]))

ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_xlabel('Query Size')

ax.boxplot(data_old)
ax.set_ylabel('Error')

plt.savefig('figs/'+filename+'-errold.pdf')

fig, ax = plt.subplots()
data_new = map(lambda kv: kv[1], sorted(errors_new.iteritems(), key=lambda kv: kv[0]))

ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_xlabel('Query Size')

ax.boxplot(data_new)
ax.set_ylabel('Error')

plt.savefig('figs/'+filename+'-errnew.pdf')