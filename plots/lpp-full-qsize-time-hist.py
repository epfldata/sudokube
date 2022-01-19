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
	timescale=1000.0*1000.0
	cname0=''
	for row in data:
		key = int(row['QSize'])
		cname0 = row['CubeName']
		dof_new = int(row['DOF SliceSparse'])
		err_new = float(row['Error SliceSparse'])

		#dof_old = int(row['DOF Sparse'])
		#err_old = float(row['Error Sparse'])
		nfetch = int(row['NFetchTime(us)'])/timescale
		nprep = int(row['NPrepareTime(us)'])/timescale
		lpprep = int(row['LPPrepareTime(us)'])/timescale
		lpfetch = int(row['LPFetchTime(us)'])/timescale
		lpsolve_new = (int(row['Init SliceSparse']) + int(row['ComputeBounds SliceSparse']))/timescale 
		#lpsolve_old = (int(row['Init Sparse']) + int(row['ComputeBounds Sparse']))/timescale

		#dofs_old[key].append(dof_old)
		#errors_old[key].append(err_old)
		dofs_new[key].append(dof_new)
		errors_new[key].append(err_new)

		naivefetch[key].append(nfetch)
		naiveprepare[key].append(nprep)
		lpp_fetch[key].append(lpfetch)
		#lpp_old_solve[key].append(lpsolve_old)
		lpp_new_solve[key].append(lpsolve_new)
		lpp_prepare[key].append(lpprep)

cname=cname0.split('_')
mod=int(cname[3])+int(cname[4])+1-int(cname[2])
title="{}_{}_{}_{}".format(cname[0],cname[1],cname[2],mod)
keys = sorted(map(lambda x: x[0], naivefetch.iteritems()))
#dof_avg = map(lambda x: mean(x[1]) , sorted(dofs.iteritems(), key = lambda kv: kv[0]))
#err_avg = map(lambda x: mean(x[1]), sorted(errors.iteritems(), key = lambda kv: kv[0]))
nfetch_avg = np.array(map(lambda x: mean(x[1]), sorted(naivefetch.iteritems(),key = lambda kv: kv[0])))
nprep_avg = np.array(map(lambda x:  mean(x[1]), sorted(naiveprepare.iteritems(),key = lambda kv: kv[0])))
lpp_prep_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_prepare.iteritems(),key = lambda kv: kv[0])))
lpp_fetch_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_fetch.iteritems(),key = lambda kv: kv[0])))
#lpp_old_solve_avg = np.array(map(lambda x: mean(x[1]), sorted(lpp_old_solve.iteritems(), key = lambda kv: kv[0])))
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

w=1.0/9
fig, ax = plt.subplots()

ax.bar(X-3*w , lpp_prep_avg , w, bottom = 0.001, label='LPP Prepare', color='limegreen')
ax.bar(X-2*w, nprep_avg, w, bottom = 0.001, label='Naive Prepare', color='darkgreen')
ax.bar(X-w , lpp_fetch_avg, w, bottom = 0.001, label='LPP Fetch', color='turquoise')
ax.bar(X ,nfetch_avg, w, label='Naive Fetch', bottom=0.001, color='blue')
ax.bar(X+2*w , lpp_prep_avg + lpp_fetch_avg + lpp_new_solve_avg, w, bottom = 0.001, label='LPP Total', color='tomato')
ax.bar(X+3*w , nprep_avg+nfetch_avg, w, label='Naive Total', bottom=0.001, color='red')
ax.bar(X+w, lpp_new_solve_avg, w, bottom = 0.001, label='LPP Solve', color='gray')

ax.set_ylabel('Time(s)')
ax.set_xlabel('Query Dimensionality')
ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_yscale('log')
# Shrink current axis by 30%
box = ax.get_position()
ax.set_position([box.x0, box.y0+0.2, box.width , box.height * 0.8])

ax.legend(loc='lower center', ncol=4, bbox_to_anchor=(0.5, -0.4), fontsize='x-small')
#plt.show()
plt.title(title)
plt.savefig('figs/'+filename+'-time.pdf',bbox_inches = 'tight',pad_inches = 0.1)


######################

"""fig, ax = plt.subplots()
data_old = map(lambda kv: kv[1], sorted(errors_old.iteritems(), key=lambda kv: kv[0]))

ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_xlabel('Query Size')

ax.boxplot(data_old)
ax.set_ylabel('Error')
plt.title(title)
plt.savefig('figs/'+filename+'-errold.pdf')

data_new = map(lambda kv: kv[1], sorted(errors_new.iteritems(), key=lambda kv: kv[0]))

fig, ax = plt.subplots()

ax.set_xticks(X)
ax.set_xticklabels(keys)
ax.set_xlabel('Query Dimensionality')

ax.boxplot(data_new)
ax.set_ylabel('Error')
ax.set_yscale('log')
plt.title(title)
plt.savefig('figs/'+filename+'-errnew.pdf')

######################
"""

fig,ax = plt.subplots()
maxX = max(max(errors_new.values()))
for s in sorted(errors_new.iteritems(), key=lambda kv: kv[0]):
	maxv = max(s[1])
	values, base = np.histogram(s[1], bins=10, range=(0.0, maxv+0.00001))
	total = sum(values)
	cumulative = map(lambda x: float(x)/total, np.insert(np.cumsum(values),0,0))
	ax.plot(base, cumulative, label=s[0])
ax.set_xlabel('Error')
ax.set_ylabel('CDF')
ax.set_xlim([-maxX/12.0, maxX])
#ax.set_yscale('log')
ax.legend(loc='lower right')
plt.title(title)
plt.savefig('figs/'+filename+'-errnew.pdf',bbox_inches = 'tight',pad_inches = 0.1)