#!/usr/bin/env python
"""
Plot envelope delays in SC3VS.
Created on Nov 7, 2013

@author: behry
"""

import logging
import logging.handlers
import os
from collections import defaultdict
import re
from obspy import UTCDateTime
import numpy as np
from scipy.stats import scoreatpercentile
import matplotlib.pyplot as plt
import json
from mpl_toolkits.basemap import Basemap
from matplotlib.colorbar import ColorbarBase
from matplotlib.colors import Normalize
from matplotlib.pyplot import cm
from matplotlib.patches import Rectangle


def envelope_delays(fin, delayfile, maxcount=1000000, new=False):
    """
    Evaluate the envelope log file that is produced by scvsmag.
    """
    logging.basicConfig(filename='parse_envelope_log.log', filemode='w',
                        level=logging.DEBUG)
    if new:
        pat = r'(\S+ \S+) \[envelope/info/VsMagnitude\] Current time: (\S+);'
        pat += r' Envelope: timestamp: (\S+) waveformID: (\S+)'
        cnt = 0
        streams = defaultdict(dict)
        delays = defaultdict(list)
        f = open(fin)
        first = 9999999999999999 #None
        last = 0 #None
        while True:
            line = f.readline()
            if not line: break
            if cnt > maxcount: break
            match = re.search(pat, line)
            if match:
                ttmp = match.group(1)
                dt, t = ttmp.split()
                year, month, day = map(int, dt.split('/'))
                hour, min, sec = map(int, t.split(':'))
                logtime = UTCDateTime(year, month, day, hour, min, sec)
                currentTime = UTCDateTime(match.group(2))
                # the timestamp marks the beginning of the data window
                # so we have to add one second to get the time of the end
                # of the data window
                timestamp = UTCDateTime(match.group(3)) + 1.
                ts_string = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
                wID = match.group(4)
                station = wID.split('.')[0] + '.' + wID.split('.')[1]
                net = wID.split('.')[0]
                tdiff = currentTime - timestamp
                streams[wID][ts_string] = currentTime
                # We are looking for the time that is required to have 3 s of
                # envelopes following a P arrival. We therefore have to add the
                # time difference of the envelope 3 s after the arrival of the
                # current one which is equivalent to measuring the time
                # difference to the arrival time of the envelope 3 s before
                # the latest one.
                if len(streams[wID].keys()) >= 4:
                    try:
                        old_ts = (timestamp - 3.).strftime("%Y-%m-%dT%H:%M:%S")
                        old_ct = streams[wID][old_ts]
                        tdiff += (currentTime - old_ct)
                        # tdiff = currentTime - old_ct
                    except Exception, e:
                        logging.debug('%s %s: %s' % (wID, old_ts, e))
                        continue
                else:
                    continue
                #if cnt == 0:
                first = np.min([first, timestamp])
                #if cnt == maxcount-1:
                last = np.max([last, timestamp])
                delays[station].append(tdiff)
                cnt += 1
            else:
                print "problem with line %d" % cnt
                print line
                break
        print first
        print last
        f.close()
        fh = open(delayfile, 'w')
        json.dump({'delays':delays,'first':str(first),'last':str(last)}, fh)
        fh.close()
    else:
        fh = open(delayfile)
        tmp = json.load(fh)
        delays = tmp['delays']
        first = tmp['first']
        last = tmp['last']
        fh.close()
    return delays, first, last

def plot_hist(delays, first, last, stations=False, noshow=False):
    """
    Plot a histogram of the envelope delays for all stations combined.
    """
    alldelays = []
    for _s in delays.keys():
        alldelays += delays[_s]

    flat =  [elem for elem in alldelays if elem <30 and elem>0]
    med = np.median(flat)
    percentile16 = scoreatpercentile(flat, 16)
    percentile84 = scoreatpercentile(flat, 84)

    Npicks=[]
    delays_list = delays.copy()
    delays_cumul_list = delays.copy()

    for _n in delays.keys():
        tmpx = np.sort([elem for elem in delays[_n] if elem<=30 and elem>=0])
        tmpx = np.append(tmpx,med)
        tmpy = np.cumsum(tmpx)
        tmpy = tmpy/tmpy[-1]
        Npicks.append( np.mean( tmpy ) )
        delays_list[_n] = list(tmpx)
        delays_cumul_list[_n] = list(tmpy*100.)

    sorted_keys = np.asarray(delays.keys())
    sorted_keys = (sorted_keys[ np.argsort(Npicks)[::-1] ]).tolist()
    print('station list from worst to best')
    nstation = -1
    for g in range(0, len(sorted_keys), 9):


        fig = plt.figure()
        plt.clf()
        ax = fig.add_subplot(111)
        ax.text(0.5, 0.5, str(g/9),
            horizontalalignment='center',
            verticalalignment='center',
            zorder=-999,
            alpha=0.4,fontsize=200, color='grey',
            transform=ax.transAxes)

        ylim = [1,100]
        if len(alldelays) > 0:
            tmp = np.cumsum(np.sort(flat))/np.sum(flat)*100
            #ax.fill_between(np.sort(flat)[1:],0.00000001, np.diff(tmp), label='All (hist.)', linewidth=2.0, color='grey', alpha=0.8)
            ax.semilogy(np.sort(flat), tmp, label='All (cumul.)', linewidth=2.0, color='grey', alpha=0.9)

            n, bins, patches = ax.hist(alldelays, bins=np.arange(0, 30, 0.1),bottom=np.min(tmp),
                                   label='All (hist.)', color='grey', alpha=0.8, rwidth=2.0, normed=stations, histtype='step',log=True)
            ylim = [np.min(n[-10:]),100.]

        keys = sorted_keys[g:g+7]
        if stations:
            for ik,k in enumerate(keys):
                nstation+=1
                print(str(g/9)+"."+str(ik)+": "+str(k)+" ("+str(nstation)+")")
                ax.semilogy(delays_list[k], delays_cumul_list[k], label=k)



        ax.add_patch(Rectangle((med-percentile84/2,ylim[0]),
            percentile84, ylim[-1],
            zorder=0, alpha=0.2, facecolor='grey', linewidth=0, label=r'%$\stackrel{ile}{84th}$: '+str(percentile84)+'s' ))
        ax.add_patch(Rectangle((med-percentile16/2,ylim[0]),
            percentile16, ylim[-1],
            zorder=0, alpha=0.5, facecolor='grey', linewidth=0, label=r'%$\stackrel{ile}{16th}$: '+str(percentile16)+'s' ))
        ax.add_patch(Rectangle((med, ylim[0]),
            0., ylim[-1],
            zorder=0, edgecolor='grey',facecolor='grey',linewidth=3,label='Median: %.1f s' % (med)))

        ax.set_title(r''+'Distribution of envelope delays \n $\stackrel{From\ '+str(first)+'}{To\ '+str(last)+'}$')
        ax.set_xlabel('Envelope delays [s]')
        ax.set_ylabel('% of delays')
        ax.legend(loc=4, fancybox=True, framealpha=0.5)
        plt.grid()
        ax.set_ylim(ylim)
        plt.savefig('envelope_delays_group'+str(g/9)+'.pdf')
        if not noshow:
            plt.show()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Evaluate the envelope log \
    file produced by scvsmag.")
    parser.add_argument('-i', '--fin', help='Envelope log file to be parsed.')
    parser.add_argument('-o', '--fout', help='Path of a json file to save the \
    envelope delays for every station.')
    parser.add_argument('--new', help='If true parse the envelope log file. \
    Otherwise plot the histogram for the delays given in the output file.',
    action='store_true')
    parser.add_argument('--stations', help='If true plot each station separately. \
    Otherwise plot all station together.',
    action='store_true')
    parser.add_argument('--noshow', help='If true save each station plot without showing. \
    Otherwise plot all station together.',
    action='store_true')
    args = parser.parse_args()
    args = parser.parse_args()
    delays, first, last = envelope_delays(args.fin, args.fout, maxcount=1000000,
                             new=args.new)
    plot_hist(delays, first, last, stations=args.stations, noshow=args.noshow)

