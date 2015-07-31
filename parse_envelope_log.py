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
        first = None
        last = None
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
                if cnt == 0:
                    first = timestamp
                if cnt == maxcount:
                    last = timestamp
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
        json.dump(delays, fh)
        fh.close()
    else:
        fh = open(delayfile)
        delays = json.load(fh)
        fh.close()
    return delays

def plot_hist(delays):
    """
    Plot a histogram of the envelope delays for all stations combined.
    """
    fig = plt.figure()
    ax = fig.add_subplot(111)
    alldelays = []
    for _s in delays.keys():
        alldelays += delays[_s]
    if len(alldelays) > 0:
        n, bins, patches = ax.hist(alldelays, bins=np.arange(0, 30, 0.5),
                                   color='green', histtype='bar', rwidth=1.0)
    ax.set_xlabel('Envelope delays [s]')
    med = np.median(alldelays)
    percentile25 = scoreatpercentile(alldelays, 25)
    percentile75 = scoreatpercentile(alldelays, 75)
    ax.text(0.6, 0.80, 'Median: %.1f s' % (med), horizontalalignment='left',
            transform=ax.transAxes, color='black')
    ax.text(0.6, 0.75, '25th percentile: %.1f s' % (percentile25), horizontalalignment='left',
            transform=ax.transAxes, color='black')
    ax.text(0.6, 0.70, '75th percentile: %.1f s' % (percentile75), horizontalalignment='left',
            transform=ax.transAxes, color='black')
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
    args = parser.parse_args()
    delays = envelope_delays(args.fin, args.fout, maxcount=1000000,
                             new=args.new)
    plot_hist(delays)

