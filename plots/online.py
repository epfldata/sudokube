#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np

input = sys.argv[1]
isError = int(sys.argv[2])
filename = input[8:-4]


alldata = list(open(input))
numseries = len(alldata)/2
series = []
for i in range(0, numseries):
    time = map(lambda x: float(x), alldata[2*i].split(',')[1:])
    data0 = alldata[2*i + 1].split(',')
    name = data0[0]
    data = map(lambda x: float(x), data0[1:])
    series.append([time, data, name])


fig, ax = plt.subplots()
for s in series:
    ax.plot(s[0], s[1], label=s[2])
ax.set_xlabel('Time(s)')
ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
plt.legend()
plt.savefig('figs/'+filename+'.pdf')