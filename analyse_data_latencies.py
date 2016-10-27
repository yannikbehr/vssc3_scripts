#!/usr/bin/env python
"""
Plot data latencies in the SeisComp3 system
Created on Jun 19, 2013

@author: behry
"""
from collections import defaultdict
import sqlite3

import matplotlib
matplotlib.use('WXAgg')
import wx
import matplotlib.pyplot as plt
import numpy as np
from Scientific.IO.NetCDF import NetCDFFile as Dataset
from matplotlib.colors import LightSource
from mpl_toolkits.basemap import Basemap
from matplotlib.colorbar import ColorbarBase
from matplotlib.colors import Normalize
from matplotlib.pyplot import cm
from scipy.stats import scoreatpercentile

def get_data_latencies(dbfn):
    conn = sqlite3.connect(dbfn)
    c = conn.cursor()
    # delay2 is the database generated by scqc
    c.execute('''select network,station,delay from latency limit 1000000;''')
    datadict_delays = defaultdict(list)
    for entry in c.fetchall():
        name = '.'.join(entry[0:2])
        datadict_delays[name].append(entry[2])
    return datadict_delays

def background_map(ax):
    m = Basemap(projection='merc', llcrnrlat=43.5,
                urcrnrlat=49, llcrnrlon=4, urcrnrlon=12, lat_ts=47,
                resolution='i', ax=ax)
    m.drawmapboundary(fill_color='lightblue', zorder=0)
    m.fillcontinents(zorder=0)
    etopofn = '/home/behry/uni/data/etopo1_central_europe_gmt.grd'
    etopodata = Dataset(etopofn, 'r')
    z = etopodata.variables['z'][:]
    x_range = etopodata.variables['x_range'][:]
    y_range = etopodata.variables['y_range'][:]
    spc = etopodata.variables['spacing'][:]
    lats = np.arange(y_range[0], y_range[1], spc[1])
    lons = np.arange(x_range[0], x_range[1], spc[0])
    topoin = z.reshape(lats.size, lons.size, order='C')
    # transform to nx x ny regularly spaced 5km native projection grid
    nx = int((m.xmax - m.xmin) / 5000.) + 1; ny = int((m.ymax - m.ymin) / 5000.) + 1
    topodat, x, y = m.transform_scalar(np.flipud(topoin), lons, lats, nx, ny, returnxy=True)
    ls = LightSource(azdeg=300, altdeg=15, hsv_min_sat=0.2, hsv_max_sat=0.3,
                     hsv_min_val=0.2, hsv_max_val=0.3)
    # shade data, creating an rgb array.
    rgb = ls.shade(np.ma.masked_less(topodat / 1000.0, 0.0), cm.gist_gray_r)
    # m.contourf(x, y, topodat, cmap=cm.gist_earth)
    m.imshow(rgb)
    m.drawmeridians(np.arange(6, 12, 2), labels=[0, 0, 0, 1], color='white',
                    linewidth=0.5, zorder=0)
    m.drawparallels(np.arange(44, 50, 2), labels=[1, 0, 0, 0], color='white',
                    linewidth=0.5, zorder=0)
    m.drawcoastlines(zorder=2)
    m.drawcountries(linewidth=1.5, zorder=2)
    m.drawrivers(color='lightblue')
    return m

def plot_data_latencies(delays, fout, delaysfile, map=False, fnmap=None,
                        networks=['*']):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    dl = defaultdict(list)
    for _ns in delays.keys():
        net, stat = _ns.split('.')
        if net in networks or '*' in networks:
            dl[net] += delays[_ns]
    if len(dl.keys()) > 0:
        n, bins, patches = ax.hist([dl[_n] for _n in dl.keys()],
                                   bins=np.arange(-30, 30, 0.5),
                                   histtype='barstacked',
                                   label=[_n for _n in dl.keys()],
                                   rwidth=1.0)
        ax.set_xlabel('Data latencies [s]')
        ax.legend()
        plt.savefig(fout, dpi=300)
        plt.show()
    else:
        print 'No data found for ', networks

    # Plot a map showing the median for each station
    if map:
        cmap = cm.get_cmap('RdBu_r')
        fig = plt.figure()

        ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        m = background_map(ax)
        m.set_axes_limits()
        dataX = []
        dataY = []
        names = []
        for _stat in stat_dict.keys():
            net, lat, lon, med = stat_dict[_stat]
            x, y = m(lon, lat)
            cl = cmap((med - 1.) / 10.)
            m.plot(x, y, color=cl, marker='^', zorder=3, mec='white', ms=8,
                   picker=5)
            dataX.append(x)
            dataY.append(y)
            names.append('%s.%s: %.1f' % (net, _stat, med))
        cax = fig.add_axes([0.83, 0.1, 0.05, 0.8])
        cb = ColorbarBase(cax, cmap=cmap,
                      norm=Normalize(vmin=1., vmax=10.))
        cb.set_label('Median data delays [s]')

        # add a pop-up window showing the station and its value
        tooltip = wx.ToolTip(tip='')
        tooltip.Enable(False)
        tooltip.SetDelay(0)
        fig.canvas.SetToolTip(tooltip)
        def onMotion(event):
            line2d = event.artist
            x = line2d.get_xdata()[0]
            y = line2d.get_ydata()[0]
            found = False
            for i in xrange(len(dataX)):
                radius = 5
                if abs(x - dataX[i]) < radius and abs(y - dataY[i]) < radius:
                    tip = '%s' % names[i]
                    tooltip.SetTip(tip)
                    tooltip.Enable(True)
                    found = True
                    break
            if not found:
                tooltip.Enable(False)

        fig.canvas.mpl_connect('pick_event', onMotion)
        if fnmap is not None:
            plt.savefig(fnmap, dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == '__main__':
    fout = '/tmp/deleteme.png'
    fout_map = '/tmp/deleteme_map.png'
    delaysfile = '/tmp/deleteme_delays.npz'
    plot_data_latencies(get_data_latencies('data_latencies.sqlite3'), fout, delaysfile,
                        map=False, fnmap=fout_map)
