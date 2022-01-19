#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np
plt.rcParams["figure.figsize"] = (8,3)

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
#for s in ss:
#    
ax.plot(header[1], ss[0][1], label=ss[0][0])
ax.plot(header[1], ss[1][1], label=ss[1][0])
ax.plot(header[1], total[1], label='Total', ls='dashed', color='gray')
ax.plot(header[1], ss[2][1], label=ss[2][0])
ax.plot(header[1], ss[3][1], label=ss[3][0])
plt.title(title0)    
ax.set_yscale('log')
ax.set_xlabel('#Dimensions')
ax.set_ylabel('#Cuboids')
plt.legend(loc='upper left',ncol=2,fontsize='small')
plt.savefig('figs/'+filename+'.pdf',bbox_inches = 'tight',pad_inches = 0.1)
