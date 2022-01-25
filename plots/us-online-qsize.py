#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np

plt.rcParams["figure.figsize"] = (8,6)
fig, (ax1,ax2) = plt.subplots(2,1)

def plot(input, ax):
    filename = input[16:-4]
    isError = filename.endswith('ERROR')
    print(filename)

    alldata = list(open(input))
    numseries = len(alldata)/2
    series = []

    qsindex = filename.index('qs')-1
    cnames=filename[10:qsindex].split('_')
    mod = int(cnames[4]) + int(cnames[3]) + 1 - int(cnames[2])
    title="{}_{}_{}_{}".format(cnames[0], cnames[1], cnames[2], mod)

    for i in range(0, numseries):
        time = map(lambda x: float(x), alldata[2*i].split(',')[1:])
        data0 = alldata[2*i + 1].split(',')
        name = data0[0]
        data = map(lambda x: float(x), data0[1:])
        series.append([time, data, name])


    for s in sorted(series, key=lambda tdn: int(tdn[2])):
        ax.plot(s[0], s[1], label=s[2])
    ax.title.set_text(title)
    ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
    ax.set_xscale('log')

plot('expdata/current/US-Online_SSB-sf100_rms2_15_25_3-qs-ERROR.csv',ax1)
plot('expdata/current/US-Online_SSB-sf100_sms_15_25_3-qs-ERROR.csv',ax2)

plt.subplots_adjust(bottom=0.25,hspace=0.4)
plt.xlabel('Time(s)')
handles, labels = ax2.get_legend_handles_labels()
fig.legend(handles, labels, ncol=4, loc='upper center', bbox_to_anchor=(0.5,0.13))
plt.savefig('figs/us-online-qsize.pdf',bbox_inches = 'tight')
