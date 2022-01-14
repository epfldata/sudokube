#!/usr/bin/env python 

import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
import numpy as np

X = [93, 600, 60, 6]
Y = [429, 193, 193, 193]
fig, ax = plt.subplots()

ax.scatter(X, Y)

ax.annotate('NYC', (100, 440))
ax.annotate('SSB-1', (6, 200))
ax.annotate('SSB-10', (35, 160))
ax.annotate('SSB-100', (540, 200))

x1 = [0, 93, 93]
y1 = [429, 429, 0]
ax.plot(x1, y1, ls='dashed', color='gray')

x1 = [0, 600, 600]
y1 = [193, 193, 0]
ax.plot(x1, y1, ls='dashed', color='gray')

x1 = [0, 60, 60]
y1 = [193, 193, 0]
ax.plot(x1, y1, ls='dashed', color='gray')

x1 = [0, 6, 6]
y1 = [193, 193, 0]
ax.plot(x1, y1, ls='dashed', color='gray')

ax.set_xlabel('#Rows(Millions)')
ax.set_ylabel('#Dimensions')
plt.xlim([0, 610])
plt.ylim([0, 500])
plt.savefig('figs/datasets.pdf')
