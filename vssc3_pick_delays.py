#!/usr/bin/env python
"""
Measure pick delays in VS(SC3).
Created on Nov 7, 2013

@author: behry
"""
import psycopg2
import psycopg2.extras
import MySQLdb
from obspy import UTCDateTime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import json
from collections import defaultdict
#import ipdb
from scipy.stats import scoreatpercentile

class PickDelay:

    def __init__(self):
        self.stations = defaultdict(list)

    def get_pick_delays(self, fout, host, database, user, passwd, port,
                        starttime=UTCDateTime(0), endtime=UTCDateTime(),
                        new=True, dbtype='postgresql', pmethodid='Trigger'):
        if new:
            if dbtype == 'postgresql':
                con = psycopg2.connect(database=database, user=user, port=port,
                                        password=passwd, host=host)
                mark = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
                query = """select pick.m_creationinfo_creationtime +
                (pick.m_creationinfo_creationtime_ms || ' microseconds')::interval as pick_creation,
                pick.m_time_value + (pick.m_time_value_ms || ' microseconds')::interval as pick_time,
                pick.m_creationinfo_creationtime +
                (pick.m_creationinfo_creationtime_ms || ' microseconds')::interval
                - (pick.m_time_value + (pick.m_time_value_ms || ' microseconds')::interval) as tdiff,
                pick.m_waveformid_networkcode as network, pick.m_waveformid_stationcode as station
                from pick """
                query += "where pick.m_methodid = '%s' " % (pmethodid)
                query += "and pick.m_creationinfo_creationtime > "
                query += "'%s'::timestamp " % (starttime.strftime("%Y-%m-%d %H:%M:%S"))
                query += "and pick.m_creationinfo_creationtime <= "
                query += "'%s'::timestamp" % (endtime.strftime("%Y-%m-%d %H:%M:%S"))
            elif dbtype == 'mysql':
                con = MySQLdb.connect(host=host, user=user, passwd=passwd,
                                      db=database, port=port)
                mark = con.cursor()
                # There is a bug in mySQL/mySQLForPython that returns None
                # instead of the date for date_add functions. See bug report:
                # http://sourceforge.net/p/mysql-python/bugs/108/
                query = """select creationInfo_creationTime as pick_creation,
                time_value as pick_time,
                date_add(creationInfo_creationTime, interval creationInfo_creationTime_ms microsecond) -
                date_add(time_value,interval time_value_ms microsecond) as tdiff,
                waveformID_networkCode as network, waveformID_stationCode as station
                from Pick """
                query += "where Pick.methodID = '%s' " % (pmethodid)
                query += "and Pick.creationInfo_creationTime > "
                query += "'%s' " % (starttime.strftime("%Y-%m-%d %H:%M:%S"))
                query += "and Pick.creationInfo_creationTime <= "
                query += "'%s'" % (endtime.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                raise Exception('Only postgresql and mysql are currently supported')
            mark.execute(query)
            result = mark.fetchall()
            con.close()
            for _e in result:
                pct, pt, tdiff, net, stat = _e
                station = '%s.%s' % (net, stat)
                if dbtype == 'postgresql':
                    self.stations[station].append((tdiff.total_seconds(),
                                                   pct.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                                   pt.strftime("%Y-%m-%dT%H:%M:%S.%f")))
                elif dbtype == 'mysql':
                    self.stations[station].append((float(tdiff),
                                                   pct.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                                   pt.strftime("%Y-%m-%dT%H:%M:%S.%f")))
            fh = open(fout, 'w')
            json.dump(self.stations, fh)
            fh.close()
        else:
            fh = open(fout)
            self.stations = json.load(fh)
            fh.close()

    def summary(self):
        min_del = 1e38
        max_del = -1e38
        min_pct = UTCDateTime()
        max_pct = UTCDateTime(0)
        for _s in self.stations:
            for _e in self.stations[_s]:
                _d, _pct, _pt = _e
                _pct = UTCDateTime(_pct)
                _pt = UTCDateTime(_pt)
                if _pct > max_pct:
                    max_pct = _pct
                    max_pct_st = _s
                    max_pct_del = _d
                if _pct < min_pct:
                    min_pct = _pct
                    min_pct_st = _s
                    min_pct_del = _d
                if _d < min_del:
                    min_del = _d
                    min_del_st = _s
                    min_del_pct = _pct
                if _d > max_del:
                    max_del = _d
                    max_del_st = _s
                    max_del_pct = _pct

        print "Earliest pick: %s %s (delay: %.2f)" % (min_pct_st, min_pct, min_pct_del)
        print "Latest pick: %s %s (delay: %.2f)" % (max_pct_st, max_pct, max_pct_del)
        print "Minimum delay: %s %s (delay: %.2f)" % (min_del_st, min_del_pct, min_del)
        print "Maximum delay: %s %s (delay: %.2f)" % (max_del_st, max_del_pct, max_del)

    def plot_delays(self, fout, networks=[], noshow=False):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        flat = []
        flat_pct= []
        delays = defaultdict(list)
        stations_corresponding_to_delays = defaultdict(list)
        pct_corresponding_to_delays = defaultdict(list)
        for _s in self.stations.keys():
            net, stat = _s.split('.')
            if net in networks or '*' in networks:
                for _e in self.stations[_s]:
                    _d, _pct, _pt = _e
                    delays[net].append(_d)
                    flat.append(_d)
                    pct_corresponding_to_delays[net].append(UTCDateTime(_pct))
                    flat_pct.append(UTCDateTime(_pct))
                    stations_corresponding_to_delays[net].append(stat)
        Npicks=[]
        for _n in delays.keys():
            Npicks.append(len(delays[_n]))
        sorted_keys = np.asarray(delays.keys())
        sorted_keys = (sorted_keys[ np.argsort(Npicks)[::-1] ]).tolist()

        if args.summary:
            print "Summary by network"
            for _n in sorted_keys:
                min_pct_st = stations_corresponding_to_delays[_n][np.argmin(pct_corresponding_to_delays[_n])]
                min_pct_del = delays[_n][np.argmin(pct_corresponding_to_delays[_n])]
                min_pct = np.min(pct_corresponding_to_delays[_n])

                max_pct_st = stations_corresponding_to_delays[_n][np.argmax(pct_corresponding_to_delays[_n])]
                max_pct_del = delays[_n][np.argmax(pct_corresponding_to_delays[_n])]
                max_pct = np.max(pct_corresponding_to_delays[_n])

                min_del_st = stations_corresponding_to_delays[_n][np.argmin(delays[_n])]
                min_del_pct = pct_corresponding_to_delays[_n][np.argmin(delays[_n])]
                min_del = np.min(delays[_n])

                max_del_st = stations_corresponding_to_delays[_n][np.argmax(delays[_n])]
                max_del_pct = pct_corresponding_to_delays[_n][np.argmax(delays[_n])]
                max_del = np.max(delays[_n])

                print "- %s (%i picks) -" % (_n, len(delays[_n]) )
                print "%s earliest pick: %s %s (delay: %.2f)" % (_n, min_pct_st, min_pct, min_pct_del)
                print "%s latest pick: %s %s (delay: %.2f)" % (_n, max_pct_st, max_pct, max_pct_del)
                print "%s minimum delay: %s %s (delay: %.2f)" % (_n, min_del_st, min_del_pct, min_del)
                print "%s maximum delay: %s %s (delay: %.2f)" % (_n, max_del_st, max_del_pct, max_del)


        big_enough = np.min([ 15, np.sum( Npicks > np.max(Npicks)/20.) ])
        for i, _n in enumerate( sorted_keys ):
            if i > big_enough:
                sorted_keys[big_enough] = str(len(sorted_keys)-big_enough+1)+" more"
                for _d in delays[ _n ]:
                    delays[ sorted_keys[big_enough] ].append( _d )
        sorted_keys = sorted_keys[: np.min([ big_enough+1, len(sorted_keys) ]) ]

        if len(delays.keys()) > 0:
            n, bins, patches = ax.hist([delays[_n] for _n in sorted_keys], #delays.keys()],
                                           bins=np.arange(-30, 30, 1.0),
                                           histtype='barstacked',
                                           label=[_n for _n in sorted_keys], #delays.keys()],
                                           rwidth=1.0)

            first = np.min(flat_pct)
            last  = np.max(flat_pct)

            med = np.median(flat)
            percentile16 = scoreatpercentile(flat, 16)
            percentile84 = scoreatpercentile(flat, 84)
            ylim = ax.get_ylim()

            ax.add_patch(Rectangle((med-percentile84/2,ylim[0]),
                percentile84, ylim[-1],
                zorder=0, alpha=0.2, facecolor='grey', linewidth=0, label=r'%$\stackrel{ile}{84th}$: '+str(percentile84)+'s' ))
            ax.add_patch(Rectangle((med-percentile16/2,ylim[0]),
                percentile16, ylim[-1],
                zorder=0, alpha=0.5, facecolor='grey', linewidth=0, label=r'%$\stackrel{ile}{16th}$: '+str(percentile16)+'s' ))
            ax.add_patch(Rectangle((med, ylim[0]),
                0., ylim[-1],
                zorder=0, edgecolor='grey',facecolor='grey',linewidth=3,label='Median: %.1f s' % (med)))

            ax.set_title(r''+'Distribution of picks delays \n $\stackrel{From\ '+str(first)+'}{To\ '+str(last)+'}$')
            ax.set_xlabel('Pick delay [s]')
            ax.set_ylabel('Count')
            ax.legend(loc=2, fancybox=True)
            plt.grid()
            plt.savefig(fout, dpi=300)
            if noshow :
                pass
            else:
                plt.show()

        else:
            print 'No data found for ', networks

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help="Server name hosting the database.")
    parser.add_argument('--port', help="Port number to connect to database.", type=int)
    parser.add_argument('-u', '--user', help="User name for database.")
    parser.add_argument('-p', '--pwd', help="Password for database.")
    parser.add_argument('-d', '--database', help="Database name.")
    parser.add_argument('--dbtype', help="Database type ('postgresql' or 'mysql').")
    parser.add_argument('-o', '--jsonfile', help="Path for json file with pick delays.")
    parser.add_argument('--plotfile', help="Path for png file showing pick delay distribution.",
                        default=None)
    parser.add_argument('--new', help="Reread data from database even if json file exists.",
                        action='store_true')
    parser.add_argument('--noshow', help="Saves plot without showing.",
                        action='store_true')
    parser.add_argument('--start', help="Give start time for the query e.g. 2014-12-31T12:11:05",
                        default='1970-01-01T00:00:00')
    parser.add_argument('--end', help="Give end time for the query e.g. 2014-12-31T12:11:05",
                        default=UTCDateTime().strftime("%Y-%m-%dT%H:%M:%SZ"))
    parser.add_argument('--networks', help="Give a comma separated list of \
    network codes that you want to plot, e.g. 'CH,MN'")
    parser.add_argument('--pickmethod', help="Give the abbreviation that \
    describes your automatic picks. Defaults to 'Trigger'.", default='Trigger')
    parser.add_argument('--summary', help="Print a summary of the results.",
                        action='store_true')
    args = parser.parse_args()
    pd = PickDelay()
    pd.get_pick_delays(args.jsonfile, args.host, args.database, args.user,
                       args.pwd, args.port, new=args.new, dbtype=args.dbtype,
                       starttime=UTCDateTime(args.start),
                       endtime=UTCDateTime(args.end), pmethodid=args.pickmethod)
    if args.networks:
        networks = []
        for _n in args.networks.split(','):
            networks.append(_n)
    else:
        networks = ['*']
    if args.summary:
        pd.summary()
    if args.plotfile is not None:
        pd.plot_delays(args.plotfile, networks=networks, noshow=args.noshow)
