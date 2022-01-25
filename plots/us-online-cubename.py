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
    title=''
    for i in range(0, numseries):
        time = map(lambda x: float(x), alldata[2*i].split(',')[1:])
        data0 = alldata[2*i + 1].split(',')
        name0 = data0[0].split('_')
        title = "{}_{}".format(name0[0],name0[1])
        mod = int(name0[3])+1-int(name0[2])
        modstr = str(mod).zfill(2)
        name = [name0[2], modstr]
        data = map(lambda x: float(x), data0[1:])
        series.append([time, data, name])


    ss = sorted(series, key=lambda tdn: tdn[2][0]*100 + tdn[2][1])
    colors = [ 'limegreen', 'red', 'green', 'blue', 'darkolivegreen']
    styles = ['dotted', 'solid', 'solid', 'solid', 'dashed']
    for s in zip(ss, colors, styles):
        ax.plot(s[0][0], s[0][1], label="{}_{}".format(s[0][2][0], s[0][2][1]), color=s[1], ls=s[2])

    ax.set_ylabel('Error' if isError==1 else 'Degrees of Freedom')
    #ax.set_xscale('log')
    ax.title.set_text(title)

plot('expdata/current/US-Online_NYC_rms2-cubes-ERROR.csv',ax1)
plot('expdata/current/US-Online_NYC_sms-cubes-ERROR.csv',ax2)

plt.subplots_adjust(bottom=0.25,hspace=0.4)
plt.xlabel('Time(s)')
handles, labels = ax2.get_legend_handles_labels()
fig.legend(handles, labels, ncol=5, loc='upper center', fontsize='small', bbox_to_anchor=(0.5,0.13))
plt.savefig('figs/us-online-cubename.pdf',bbox_inches = 'tight')
