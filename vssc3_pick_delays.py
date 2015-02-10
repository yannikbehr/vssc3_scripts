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
import json
from collections import defaultdict


class PickDelay:

    def __init__(self):
        self.stations = defaultdict(list)

    def get_pick_delays(self, fout, host, database, user, passwd, port,
                        starttime=UTCDateTime(0), endtime=UTCDateTime(),
                        new=True, dbtype='postgresql'):
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
                from pick
                where pick.m_methodid = 'Trigger' """
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
                from Pick
                where Pick.methodID = 'Trigger'"""
            else:
                raise Exception('Only postgresql and mysql are currently supported')
            mark.execute(query)
            result = mark.fetchall()
            con.close()
            for _e in result:
                pct, pt, tdiff, net, stat = _e
                station = '%s.%s' % (net, stat)
                if dbtype == 'postgresql':
                    self.stations[station].append(tdiff.total_seconds())
                elif dbtype == 'mysql':
                    self.stations[station].append(float(tdiff))
            fh = open(fout, 'w')
            json.dump(self.stations, fh)
            fh.close()
        else:
            fh = open(fout)
            self.stations = json.load(fh)
            fh.close()

    def plot_delays(self, fout, networks=[]):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        delays = defaultdict(list)
        for _s in self.stations.keys():
            net, stat = _s.split('.')
            if net in networks or '*' in networks:
                delays[net] += self.stations[_s]
        if len(delays.keys()) > 0:
            n, bins, patches = ax.hist([delays[_n] for _n in delays.keys()],
                                           bins=np.arange(-30, 30, 0.5),
                                           histtype='barstacked',
                                           label=[_n for _n in delays.keys()],
                                           rwidth=1.0)
            ax.set_xlabel('Pick delays [s]')
            ax.legend()
            plt.savefig(fout, dpi=300)
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
    parser.add_argument('--plotfile', help="Path for png file showing pick delay distribution.")
    parser.add_argument('--new', help="Reread data from database even if json file exists.",
                        action='store_true')
    parser.add_argument('--start', help="Give start time for the query e.g. 2014-12-31T12:11:05",
                        default='1970-01-01T00:00:00')
    parser.add_argument('--end', help="Give end time for the query e.g. 2014-12-31T12:11:05",
                        default=UTCDateTime().strftime("%Y-%m-%dT%H:%M:%SZ"))
    args = parser.parse_args()
    pd = PickDelay()
    pd.get_pick_delays(args.jsonfile, args.host, args.database, args.user, args.pwd,
                       args.port, new=args.new, dbtype=args.dbtype,
                       starttime=UTCDateTime(args.start), endtime=UTCDateTime(args.end))
    pd.plot_delays(args.plotfile, networks=['*'])
