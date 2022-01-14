#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np

input = sys.argv[1]
filename = input[8:-4]
print(filename)

title0 = filename[13:-3]
alldata = list(open(input))
numseries = len(alldata)
series = []


for i in range(0, numseries):
    data0 = alldata[i].split(',')
    name = data0[0]
    data = map(lambda x: float(x), data0[1:])
    series.append([name, data])


fig, ax = plt.subplots()
header = series[0]
total = series[-1]
ss= sorted(series[1:-1], key=lambda nd:nd[0])
for s in ss:
    ax.plot(header[1], s[1], label=s[0])
ax.plot(header[1], total[1], label='Total', ls='dashed', color='gray')
plt.title(title0)    
ax.set_yscale('log')
ax.set_xlabel('#Dimensions')
ax.set_ylabel('#Cuboids')
plt.legend()
plt.savefig('figs/'+filename+'.pdf')
