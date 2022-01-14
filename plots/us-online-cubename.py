#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np

input = sys.argv[1]
filename = input[16:-4]
isError = filename.endswith('ERROR')
print(filename)

alldata = list(open(input))
numseries = len(alldata)/2
series = []
title0=''
for i in range(0, numseries):
    time = map(lambda x: float(x), alldata[2*i].split(',')[1:])
    data0 = alldata[2*i + 1].split(',')
    name0 = data0[0].split('_')
    title0 = name0[0]
    mod = int(name0[2])+int(name0[3])+1-int(name0[1])
    modstr = str(mod).zfill(2)
    name = [name0[1], modstr]
    data = map(lambda x: float(x), data0[1:])
    series.append([time, data, name])


fig, ax = plt.subplots()
ss = sorted(series, key=lambda tdn: tdn[2][0]*100 + tdn[2][1])
colors = ['tomato', 'red', 'brown', 'turquoise', 'dodgerblue', 'blue', 'limegreen', 'green', 'darkgreen']
styles = ['dotted', 'dashed', 'solid', 'dotted', 'dashed', 'solid', 'dotted', 'dashed', 'solid']
for s in zip(ss, colors, styles):
    ax.plot(s[0][0], s[0][1], label="{}_{}".format(s[0][2][0], s[0][2][1]), color=s[1], ls=s[2])

plt.title(title0)    
ax.set_xlabel('Time(s)')
ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
plt.legend()
plt.savefig('figs/'+filename+'.pdf')


"""
fixed_logN = sorted(filter(lambda tdn: tdn[2][0]=='15', series), key=lambda tdn: tdn[2][1])
fixed_mod = sorted(filter(lambda tdn: tdn[2][1]=='10',series), key=lambda tdn: tdn[2][0])


fig, ax = plt.subplots()
for s in fixed_logN:
    ax.plot(s[0], s[1], label=s[2][1])
plt.title(title0+"_15_*")    
ax.set_xlabel('Time(s)')
ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
plt.legend()
plt.savefig('figs/'+filename+'-fixedLogN.pdf')

fig, ax = plt.subplots()
for s in fixed_mod:
    ax.plot(s[0], s[1], label=s[2][0])
ax.set_xlabel('Time(s)')
plt.title(title0+"_*_10")
ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
plt.legend()
plt.savefig('figs/'+filename+'-fixedMod.pdf')
"""