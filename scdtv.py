#!/usr/bin/env python
"""
Plot delays in SC3.
Created on Nov 15, 2016

@author: fmassin
"""
import matplotlib
from collections import defaultdict
#matplotlib.use('WXAgg')
#import wx
import matplotlib.pyplot as plt
import numpy as np
#from Scientific.IO.NetCDF import NetCDFFile as Dataset
from scipy.io.netcdf  import netcdf_file as Dataset
from matplotlib.colors import LightSource
from mpl_toolkits.basemap import Basemap
from matplotlib.colorbar import ColorbarBase
from matplotlib.colors import Normalize
from matplotlib.pyplot import cm
from scipy.stats import scoreatpercentile
from matplotlib.patches import Rectangle
from obspy import read_inventory
from obspy.clients.fdsn import Client
from obspy import UTCDateTime
from obspy.core.inventory.util import Equipment


from parse_envelope_log import envelope_delays
from analyse_data_latencies import get_data_latencies
from vssc3_pick_delays import PickDelay

def get_pick_delays(jsonfile, host=None, database=None, user=None,
                   pwd=None, port=None, new=False, dbtype=None,
                   starttime=None, endtime=None, pmethodid=None,
                   networks=['*'], stations=[]):

    pd = PickDelay()
    pd.get_pick_delays(jsonfile, host, database, user,
                       pwd, port, new, dbtype,
                       starttime, endtime, pmethodid)

    delays = defaultdict(list)
    picktimes = defaultdict(list)
    first=9999999999999999.
    last=0.

    for k in pd.stations.keys():
        if k.split('.')[0] in networks or '*' in networks :
            pass
        elif k.split('.')[1] in stations or '*' in stations :
            pass
        else:
            continue
        for _e in pd.stations[k]:
            _d, _pct, _pt = _e
            delays[k].append(_d)
            picktimes[k].append(UTCDateTime(_pt))

        first=np.min([ np.min( picktimes[k] ), first ])
        last=np.max([ np.max( picktimes[k] ), last ])

    return delays, first, last, picktimes


def boost_inventory(clienturl = 'http://165.98.224.52:8081/',
                    toaddtoinventory = '/Users/fmassin/Google Drive/Projects/SED-EEW/INETER/documentation on INETER/telemetry/Sismic Stations.csv',
                    separator = '|',
                    boost_level = ['sta'],
                    file=''):


    import os.path
    if os.path.isfile(file) :
        print('Boosted inventory read from file.')
        inventory = read_inventory(file)
    else:
        print('Read from client and boost.')
        client = Client(clienturl)
        starttime = UTCDateTime()-60*60*24*360
        endtime = UTCDateTime()
        inventory = client.get_stations(network="*", station="*", level="RESP",
                                        starttime=starttime, endtime=endtime,
                                        lat=11.984562, lon=-86.168006,
                                        minradius=0.0,maxradius=5.)
    stationsfile = [x.split(separator) for x in open(toaddtoinventory).readlines()]
    for n,net in enumerate(inventory):
        for s,sta in enumerate(net):
            for d,desc in enumerate(stationsfile):
                if desc[1] == sta.code :
                    if boost_level in ['c', 'cha', 'chan', 'channel'] :
                        for c,cha in enumerate(sta):
                            cha.data_logger.description=desc[10]
                            cha.sensor.description = desc[11]
                            cha.telemetry=Equipment(description=desc[14][:-1],resource_id=desc[3])
                    else :
                        sta.telemetry=Equipment(description=desc[14][:-1],resource_id=desc[3])
                        sta.data_logger=Equipment(description=desc[10])
                        sta.sensor=Equipment(description=desc[15]) #11
    if file:
        print('Writes',file)
        inventory.write(file, format='STATIONXML')
    return inventory

def inv_split(inventory, sorted_keys, Sdl, Ndl, summarises=False, fout=None):

    inventories = list()
    if summarises:
        print('From worst to best')

    if True:
        for _in,_n in enumerate(sorted_keys):
            flag=False
            for n,net in enumerate(inventory):
                for s,sta in enumerate(net):
                    for s,cha in enumerate(sta) :
                        cha.code = cha.code.strip( '_select' )

                    if str(_n) == str(sta.code) :
                        flag=True
                        if summarises:
                            print(sta.code, net.code)
                        for s,cha in enumerate(sta) :
                            cha.code+='_select'

                    elif str(_n) == str(net.code) :
                        flag=True
                        if summarises:
                            print(net.code, sta.code)
                        for s,cha in enumerate(sta) :
                            cha.code+='_select'
                    else:
                        try:
                            sta.sensor.description
                            if str(_n) == str(sta.sensor.description):
                                flag=True
                                if summarises:
                                    print(sta.sensor.description, net.code, sta.code)
                                for s,cha in enumerate(sta) :
                                    cha.code+='_select'
                        except:
                            pass
                        try:
                            sta.data_logger.description
                            if str(_n) == str(sta.data_logger.description):
                                flag=True
                                if summarises:
                                    print(sta.data_logger.description, net.code, sta.code)
                                for s,cha in enumerate(sta) :
                                    cha.code+='_select'
                        except:
                            pass
                        try:
                            sta.telemetry.description
                            if  str(_n) == str(sta.telemetry.description) :
                                flag=True
                                if summarises:
                                    print(sta.telemetry.description, net.code, sta.code)
                                for s,cha in enumerate(sta) :
                                    cha.code+='_select'
                        except:
                            pass
                        try:
                            sta[0].sample_rate
                            for s,cha in enumerate(sta) :
                                if str(_n) == str(cha.sample_rate) :
                                    flag=True
                                    if summarises:
                                        print(cha.sample_rate, net.code, sta.code)
                                    cha.code+='_select'
                        except:
                            pass
            if flag:
                inventories.append(inventory.select(channel="*_select"))
                inventories[-1].source = str(_n)+' ($\~{dt}$ '+str(Sdl[_in])[:3]+'s)'
                inventories[-1].key = _n
                filename = inventories[-1].source.replace(' ', '_')
                filename = filename.strip( '.' )
                filename = filename.strip( '/' )
                filename += '.kml'
                #if summarises:
                #    print(inventories[-1])
                if fout:
                    print(fout+'_'+filename)
                    #inv.write( filename, format="KML")

    return inventories


def delay_split(delays_in, inventory=read_inventory(), fout=None, summarises=False,norms_hists=False,
                networks=['*'], telemetries=[], data_loggers=[], sensors=[], sample_rates=[], stations=[] ):

    delays = defaultdict(list)
    delays['All'] = []
    for _ns in delays_in.keys():
        net, stat = _ns.split('.')
        tele='None'
        logg='None'
        sensor='None'
        sp='None'
        station_inv=inventory.select(network=net,station=stat)
        if len(station_inv)>0:
            sp = np.max([ test.sample_rate for test in station_inv[0][0] ])
            if hasattr(station_inv[0][0], 'telemetry'):
                tele=station_inv[0][0].telemetry.description
            if hasattr(station_inv[0][0], 'data_logger'):
                logg=station_inv[0][0].data_logger.description
            #if hasattr(station_inv[0][0][0], 'data_logger'):
            #    logg=station_inv[0][0][0].data_logger.description
            if hasattr(station_inv[0][0], 'sensor'):
                sensor=station_inv[0][0].sensor.description

            if '' == tele:
                tele='None'
            if '' == logg:
                logg='None'
            if '' == sensor:
                sensor='None'

        if tele in telemetries or '*' in telemetries :
            #if  'None' != tele:
            delays[tele] += delays_in[_ns]
        elif logg in data_loggers or '*' in data_loggers :
            #if 'None' != logg:
            delays[logg] += delays_in[_ns]
        elif sensor in sensors or '*' in sensors :
            #if 'None' != sensor:
            delays[sensor] += delays_in[_ns]
        elif sp in sample_rates or '*' in sample_rates :
            try:
                int(sp)
                delays[str(sp)] += delays_in[_ns]
            except:
                pass
        elif str(stat) in stations or '*' in stations :
            #if 'None' != stat:
            delays[stat] += delays_in[_ns]
        elif str(net) in networks or '*' in networks :
            #if 'None' != net:
            delays[net] += delays_in[_ns]

        delays['All'] += delays_in[_ns]



    flat =  [elem for elem in delays['All'] if elem <=300 and elem>-1]
    statistics = [np.nanmedian(flat) ,          #delays['All'])
                  np.nanstd(flat) ,             #delays['All'])
                  scoreatpercentile(flat, 16) , #delays['All'], 16)#flat, 16)#
                  scoreatpercentile(flat, 84) ,
                  scoreatpercentile(flat, 99.9) ,
                  scoreatpercentile(flat, .1) ] #delays['All'], 84)#flat, 84)#

    Ndl=list(np.zeros(len(delays))+statistics[4])
    Sdl=list(np.zeros(len(delays))+statistics[4])
    delays_x = delays.copy()
    delays_c = delays.copy()

    for _in, _n in enumerate(delays.keys()):
        tmpx = np.sort([elem for elem in delays[_n] if elem<=statistics[4] and elem>=statistics[5]])
        #tmpx = np.sort(np.append(tmpx,statistics[5]))
        tmpx = np.sort(np.append(tmpx,-1.))
        tmpy = np.arange(len(tmpx))*1.
        if not str(_n) == str('All'):
            Ndl[_in] =  len( tmpx) #/np.nanmax(tmpy) ) )
            Sdl[_in] =  np.nanmean( tmpx) #/np.nanmax(tmpy) ) )
        delays_x[_n] = list(tmpx)
        delays_c[_n] = list(tmpy)
        if norms_hists:
            delays_c[_n] /= np.nanmax(tmpy)

    sorted_keys = np.asarray(delays.keys())
    sorted_keys = (sorted_keys[ np.argsort(Sdl)[::-1] ]).tolist()
    sorted_keys.remove('All')

    Sdl=list(np.zeros(len(sorted_keys))+statistics[4])
    Ndl=list(np.zeros(len(sorted_keys))+statistics[4])
    for _in, _n in enumerate(sorted_keys):
        tmpx = np.sort([elem for elem in delays[_n] if elem<=statistics[4] and elem>=statistics[5]])
        Sdl[_in] =  np.nanmean( tmpx)
        Ndl[_in] =  len(tmpx)

    inventories=inv_split(inventory, sorted_keys, Sdl, Ndl, summarises, fout)


    big_enough = 5 # there is 6 colors no more
    for i, _n in enumerate( sorted_keys ):
        if i > big_enough:
            sorted_keys[big_enough] = str(len(sorted_keys)-big_enough)+" more"
            for _d in delays[ _n ]:
                delays[ sorted_keys[big_enough] ].append( _d )
            tmpx = np.sort([elem for elem in delays[ sorted_keys[big_enough] ] if elem<=statistics[4] and elem>=statistics[5]])
            tmpx = np.sort(np.append(tmpx,statistics[4]))
            tmpy = np.arange(len(tmpx))*1.
            Ndl[i] = len( tmpx) #/np.nanmax(tmpy) ) )
            Sdl[i] = np.nanmean( tmpx) #/np.nanmax(tmpy) ) )
            delays_x[ sorted_keys[big_enough] ] = list(tmpx)
            delays_c[ sorted_keys[big_enough] ] = list(tmpy)
            if norms_hists:
                delays_c[sorted_keys[big_enough]] /= np.nanmax(tmpy)

    sorted_keys = sorted_keys[: np.min([ big_enough+1, len(sorted_keys) ]) ]

    return delays, sorted_keys, delays_x, delays_c, statistics, Sdl, inventories



def maps(delays_in, fout=None, inventory=read_inventory(), summarises=False,
         networks=[], telemetries=[], data_loggers=[], sensors=[],
         sample_rates=[], stations=[], norms_hists=False, lat_lim=None, lon_lim=None, **kwargs):

    delays, sorted_keys, delays_x, delays_c, statistics, Ndl, inventories = delay_split(delays_in, inventory,
                                                                      fout, summarises, norms_hists,
                                                                      networks, telemetries, data_loggers,
                                                                      sensors, sample_rates, stations)

    colors = ['b', 'g', 'r','c', 'm', 'y', 'k']
    flat=[statistics[4]]
    fig=(inventory.select(network='NU')).plot(projection='local',label=False, color='w',  **kwargs)
    #(plt.gca()).scatter(0,0, s=100, marker='v', edgecolor='k', label='Off', color='w')

    for i, inv in reversed(list(enumerate(inventories))):

        if i<6:
            #if not fig:
            #    fig = inv.plot(projection="local", color=colors[i] , label=False)
            #else:
            inv.plot(fig=fig, projection="local", color=colors[min([i, 6])] , label=False, **kwargs)

            (plt.gca()).scatter(0,0, s=100, marker='v', edgecolor='k', label=inv.source, color=colors[i])

        elif i>=6 :
            #if not fig:
            #    fig = inv.plot(projection="local", color=colors[min([i, 6])], label=False)
            #else:
            inv.plot(fig=fig, projection="local", color=colors[6] , label=False, **kwargs)

            for _d in delays[inv.key]:
                flat.append( _d )

    flat = [elem for elem in flat if elem<=30 and elem>=-1]
    if len(inventories) > 6 :
        (plt.gca()).scatter(0,0,
                            marker='v', s=100, edgecolor='k',
                            label=str(len(inventories)-6+1)+' more types ('+str(np.nanmedian(flat))[:3]+'s)' ,
                            color=colors[6])

    (plt.gca()).legend(loc='lower left', fancybox=True, framealpha=0.5)
    handles, labels = (plt.gca()).get_legend_handles_labels()
    i=labels.index(labels[-1])
    handles.insert(0, handles.pop(i))
    labels.insert(0, labels.pop(i))
    (plt.gca()).legend(reversed(handles), reversed(labels), scatterpoints=1, loc='upper right', fancybox=True, framealpha=0.5)


    if lat_lim:
        (plt.gca()).set_ylim(lat_lim)
    if lon_lim:
        (plt.gca()).set_xlim(lon_lim)
    if fout:
        print('saves in',fout+'_map.pdf')
        #plt.savefig(fout, dpi=300)
    plt.show()
    return inventories

def hist(delays_in, fout=None, inventory=read_inventory(), summarises=False,norms_hists=False,
         networks=[], telemetries=[], data_loggers=[], sensors=[], sample_rates=[], stations=[],
         splits_hists=False, hists_types='step', cums_hists=True, hists='old', ax=None,
         first=None, last=None, fsize=14, delays_types=None,
         delays_bis=None, delays_prime=None,
         lines_styles=['-','--',':']):

    if ax:
        fig = plt.gcf()
    else:
        fig = plt.figure()
        ax = fig.add_subplot(111)
    #fig.tight_layout()

    lw=0
    if hists_types in ['step']:
        lw=1

    delays, sorted_keys, delays_x, delays_c, statistics, Ndl, inventories = delay_split(delays_in, inventory,
                                                                  fout, summarises, norms_hists,
                                                                  networks, telemetries, data_loggers,
                                                                  sensors, sample_rates, stations)
    #print(delays)
    #print('OK')
    #return
    if len(delays.keys()) > 0:
        if hists == 'exp':
            memhist = np.zeros(1000)
            for _in, _n in enumerate(sorted_keys):
                xh = np.linspace(-1., statistics[4], 1000)
                tmp = np.linspace(0., 1.*len(delays[_n]), num=len(delays[_n]) )
                if norms_hists:
                    tmp /= len(delays['All'])

                #np.cumsum(np.sort(flat))/np.sum(flat)
                tmpcum = np.interp(xh, np.sort(delays[_n]), tmp) #use delays_c[_n]
                tmphist = tmpcum.copy()*1.
                tmphist[100:-50] = tmpcum[150:] - tmpcum[:-150]
                tmphist[:100] = tmpcum[50:150] - tmpcum[50]
                tmphist[-50:] = tmpcum[-1] - tmpcum[-150:-100]
                #tmphist /= len(sorted_keys)

                if hists_types in ['barstacked']:
                    try:
                        tmphist += memhist
                    except:
                        pass
                elif hists_types in ['bar', 'step', 'stepfilled']:
                    pass

                if hists_types in [ 'step']:
                    ax.plot(xh,tmphist, label=_n+' ('+str(Ndl[_in])[:3]+')', linewidth=2.0)
                else:
                    ax.fill_between(xh, memhist, tmphist, label=_n+' ('+str(Ndl[_in])[:3]+')', linewidth=0)

                memhist=np.nan_to_num(tmphist)

        elif hists in ['c', 'cum', 'cumulated']:


            for _in, _n in enumerate( sorted_keys):
                ax.plot(delays_x[_n],
                        delays_c[_n],
                        label=_n+' ($\~{dt}$ '+str(Ndl[_in])[:3]+'s)',
                        alpha=.8)

            ax.fill_between(delays_x['All'],
                            np.nanmax(delays_c['All'])+np.nanmax(delays_c['All'])/100,
                            delays_c['All'],
                            label='All ('+str(len(delays['All']))+' delays)',
                            linewidth=2.0,
                            facecolor='None')

        else:
            weights = [ list(np.ones([len(delays_x[_n])])) for _n in sorted_keys]
            if norms_hists:
                weights = [ list(np.ones([len(delays_x[_n])])*(1.*len(delays_x['All']))/(1.*len(delays_x[_n]))) for _n in sorted_keys]

            ax.hist([delays_x[_n] for _n in sorted_keys],#delays.keys()],
                    bins=np.logspace(np.log10(statistics[5]), np.log10(statistics[4]), 100),#
                    weights=weights,
                    histtype=hists_types,
                    label=[_n+' ($\~{dt}$ '+str(Ndl[_in])[:3]+'s)' for _in, _n in enumerate(sorted_keys)],
                    normed=norms_hists, linewidth=lw, rwidth=1.) #

            #n, bins, patches =
            ax.hist(delays_x['All'],
                    bins=np.logspace(np.log10(statistics[5]), np.log10(statistics[4]), 100),#
                    label='All ('+str(len(delays['All']))+' delays)' ,
                    histtype='step', color='black',
                    normed=norms_hists,
                    linewidth=1.5,rwidth=1.)

            #ax.fill_between(bins[:-1]+np.diff(bins)/2.,
            #                np.nanmax(n)+np.nanmax(n)/100,
            #                n,#                            label='All ($\~{dt}$ '+str(np.nanmedian(delays['All']))[:3]+'s)' , #linewidth=2.0,
            #                facecolor='white', edgecolor=None,linewidth=0)
            #ax.set_ylim(top=np.nanmax(n))

        if norms_hists:
            ax.set_ylim(top=1.)

        ax.set_xlim(left= (np.min(ax.get_xlim())+np.diff(ax.get_xlim())/100.)[0])
        ylim = ax.get_ylim()

        ax.set_xlim([statistics[5],statistics[4]])
        ax.set_xscale('log')
        ax.set_xlim([statistics[5],statistics[4]])

        ax.add_patch(Rectangle((statistics[0], ylim[0]),
            0., np.max([1., ylim[-1]]),
            zorder=-1, edgecolor='grey',facecolor='grey',linewidth=3,label='$\sigma$: %.1f s' % (statistics[0])))

        ax.add_patch(Rectangle((statistics[2],ylim[0]),
            statistics[3]-statistics[2], np.max([1., ylim[-1]]),
            zorder=-2, alpha=0.6, facecolor='grey', linewidth=0, label=r'84$^{th}$: %.1f s' % (statistics[3]) ))
        #ax.add_patch(Rectangle((med-std,ylim[0]),
        #    std*2, ylim[-1],
        #    zorder=0, alpha=0.2, facecolor='grey', linewidth=0, label=r'$\sigma$: '+str(std)+'s' ))
        ax.add_patch(Rectangle((0,ylim[0]),
            statistics[2], np.max([1., ylim[-1]]),
            zorder=-3, alpha=0.2, facecolor='grey', linewidth=0, label=r'16$^{th}$: %.1f s ' % (statistics[2]) ))



    if norms_hists:
        ylabel='Percent'
    else:
        ylabel='Count'
    if hists in ['c', 'cum', 'cumulated']:
        ylabel=ylabel+' (cumulated)'
    else:
        ylabel=ylabel+' (binned)'

    ax.set_ylabel(ylabel)
    ax.set_xlabel('Delay times [s]')

    if  first and last:
        ax.set_title(r''+'Delays distributions\n $\stackrel{From\ '+str(first)+'}{To\ '+str(last)+'}$')
    elif first:
        ax.set_title(r''+'Delays distributions\n After\ '+str(first))
    elif last:
        ax.set_title(r''+'Delays distributions\n Before\ '+str(last))
    else:
        ax.set_title(r''+'Delays distributions')

    handles, labels = ax.get_legend_handles_labels()
    l_1end=labels[-1]
    l_2end=labels[-2]
    l_3end=labels[-3]

    #handles.insert(0, handles.pop(i))
    #labels.insert(0, labels.pop(i))

    #i=labels.index(labels[-1])
    #handles.insert(1, handles.pop(i))
    #labels.insert(1, labels.pop(i))

    #i=labels.index(labels[-1])
    #handles.insert(1, handles.pop(i))
    #labels.insert(1, labels.pop(i))

    #i=labels.index(labels[-1])
    #handles.insert(1, handles.pop(i))
    #labels.insert(1, labels.pop(i))
    if hists in ['c', 'cum', 'cumulated']:
        ax.legend(handles, labels, loc=4, fancybox=True, framealpha=0.5,prop={'size':fsize})
    else:
        ax.legend(handles, labels, loc=1, fancybox=True, framealpha=0.5,prop={'size':fsize})
    ax.grid()

    if fout:
        print('saves in',fout+'_hist.pdf')
        #plt.savefig(fout, dpi=300)
    plt.show()


    plt.show()

    return inventories




if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Evaluate the envelope log \
    file produced by scvsmag.")
    parser.add_argument('-i', '--fin', help='Envelope log file to be parsed.')
    parser.add_argument('-o', '--fout', help='Path of a file to save the \
    delays if necesary.')
    parser.add_argument('--new', help='If true parse the envelope log file. \
    Otherwise plot the histogram for the delays given in the output file.',
    action='store_true')
    parser.add_argument('--station', help='If true plot each station separately. \
    Otherwise plot all together.',
    action='store_true')
    parser.add_argument('--network', help='If true plot each network separately. \
    Otherwise plot all together.',
    action='store_true')
    parser.add_argument('--datalogger', help='If true plot each datalogger separately. \
    Otherwise plot all together.',
    action='store_true')
    parser.add_argument('--communication', help='If true plot each communication separately. \
    Otherwise plot all together.',
    action='store_true')
    parser.add_argument('--noshow', help='If true save plot(s) without showing.',
    action='store_true')
    args = parser.parse_args()

    #delays, first, last = envelope_delays(args.fin, args.fout, maxcount=1000000,
    #                         new=args.new)

    #delays = get_data_latencies(args.fin) # 'data_latencies.sqlite3'),


    fout = '/tmp/deleteme.png'
    fout_map = '/tmp/deleteme_map.png'
    delaysfile = '/tmp/deleteme_delays.npz'
    plot(get_data_latencies(args.fin), fout, delaysfile,
                        map=False, fnmap=fout_map)
