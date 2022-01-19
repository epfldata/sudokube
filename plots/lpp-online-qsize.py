#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np

plt.rcParams["figure.figsize"] = (8,4)
input = sys.argv[1]
filename = input[16:-4]
isError = filename.endswith('ERROR')
print(filename)

alldata = list(open(input))
numseries = len(alldata)/2
series = []
cnames=filename[10:-9].split('_')
mod = int(cnames[4]) + int(cnames[3]) + 1 - int(cnames[2])
title0="{}_{}_{}_{}".format(cnames[0], cnames[1], cnames[2], mod)
for i in range(0, numseries):
    time = map(lambda x: float(x), alldata[2*i].split(',')[1:])
    data0 = alldata[2*i + 1].split(',')
    name = data0[0]
    data = map(lambda x: float(x), data0[1:])
    series.append([time, data, name])


fig, ax = plt.subplots()

for s in sorted(series, key=lambda tdn: int(tdn[2])):
    ax.plot(s[0], s[1], label=s[2],marker='o' if(len(s[1])) < 3 else '')

plt.title(title0)    
ax.set_xlabel('Time(s)')
ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
ax.set_xscale('log')
plt.legend()
plt.savefig('figs/'+filename+'.pdf',bbox_inches = 'tight',pad_inches = 0.1)