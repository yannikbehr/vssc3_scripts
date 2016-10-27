#!/usr/bin/env python
"""
Measure origin creation times, that is the time between the creation of the
last pick and the creation of the first origin, in VS(SC3).
Created on Jun 3, 2013

@author: behry
"""

import psycopg2
import psycopg2.extras
import MySQLdb
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from obspy import UTCDateTime
import numpy as np
from scipy.stats import scoreatpercentile


class OriginCT:
    def __init__(self):
        self.events = []
        self.ots = []
        self.delays_ct = []
        self.delays_t = []
        self.odb_delays = []
        self.pick_delays = []

    def get_delays(self, fout, host, database, user, passwd, port, author=None,
                   agency=None, latmin=None, latmax=None, lonmin=None,
                   lonmax=None, minmag=None, smiid=None,
                   starttime=UTCDateTime(0), endtime=UTCDateTime(),
                   new=True, dbtype='postgresql'):
        if new:
            if dbtype == 'postgresql':
                con = psycopg2.connect(database=database, user=user, password=passwd,
                                        port=port, host=host)
                print "Database connection established"
                mark = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
                query = """select eobject.m_publicid as eventid, oobject.m_publicid as originid,
                origin.m_creationinfo_creationtime + (origin.m_creationinfo_creationtime_ms || ' microseconds')::interval as origin_ct,
                max(pick.m_creationinfo_creationtime + (pick.m_creationinfo_creationtime_ms || ' microseconds')::interval) as latest_pick_ct,
                max(pick.m_time_value + (pick.m_time_value_ms || ' microseconds')::interval) as latest_pick_t,
                originobject._timestamp as origin_in_db, max(pickobject._timestamp) as latest_pick_in_db,
                origin.m_creationinfo_creationtime + (origin.m_creationinfo_creationtime_ms || ' microseconds')::interval -
                max(pick.m_creationinfo_creationtime+ (pick.m_creationinfo_creationtime_ms || ' microseconds')::interval) as locationdelay,
                originobject._timestamp  - max(pickobject._timestamp) as locationdelay_in_db,
                originobject._timestamp - origin.m_creationinfo_creationtime + (origin.m_creationinfo_creationtime_ms || ' microseconds')::interval as origin_to_db_delay

                from arrival inner join origin on arrival._parent_oid = origin._oid
                inner join publicobject as pobject on pobject.m_publicid = arrival.m_pickid
                inner join pick on pick._oid = pobject._oid
                inner join publicobject as oobject on oobject._oid = origin._oid
                inner join originreference on originreference.m_originid = oobject.m_publicid
                inner join event on event._oid = originreference._parent_oid
                inner join publicobject as eobject on eobject._oid = event._oid
                inner join object as originobject on origin._oid = originobject._oid
                inner join object as pickobject on pick._oid = pickobject._oid
                inner join publicobject as mobject on event.m_preferredmagnitudeid = mobject.m_publicid
                inner join magnitude on mobject._oid = magnitude._oid

                where origin.m_evaluationmode='automatic' and arrival.m_weight > 0
                and
                origin._oid in ( select min(origin._oid)
                                 from origin inner join publicobject as oobject on oobject._oid = origin._oid
                                 inner join originreference on originreference.m_originid = oobject.m_publicid
                                 inner join event on event._oid = originreference._parent_oid
                                 group by event._oid)
                """
                if author is not None:
                    query += """ and origin.m_creationinfo_author like '%s%%'""" % author
                if agency is not None:
                    query += """ and origin.m_creationinfo_agencyid = '%s'""" % agency
                if smiid is not None:
                    query += """ and event.m_publicid like '%s%%'""" % smiid
                if latmin is not None and latmax is not None:
                    query += """ and origin.m_latitude_value > %.1f and origin.m_latitude_value < %.1f""" % (latmin, latmax)
                if lonmin is not None and lonmax is not None:
                    query += """ and origin.m_longitude_value > %.1f and origin.m_longitude_value < %.1f""" % (lonmin, lonmax)
                if minmag is not None:
                    query += """ and magnitude.m_magnitude_value >= %.1f """ % minmag
                query += "and origin.m_creationinfo_creationtime > "
                query += "'%s'::timestamp " % (starttime.strftime("%Y-%m-%d %H:%M:%S"))
                query += "and origin.m_creationinfo_creationtime <= "
                query += "'%s'::timestamp" % (endtime.strftime("%Y-%m-%d %H:%M:%S"))
                query += """ group by eventid, originid, origin_ct, origin_in_db, origin_to_db_delay order by  eventid desc"""
            elif dbtype == 'mysql':
                con = MySQLdb.connect(host=host, user=user, passwd=passwd,
                                      db=database, port=port)
                print "Database connection established"
                mark = con.cursor()
                query = """select eobject.publicID as eventid, oobject.publicID as originid,
                date_add(Origin.creationInfo_creationTime, interval Origin.creationInfo_creationTime_ms microsecond) as origin_ct,
                max(date_add(Pick.creationInfo_creationTime, interval Pick.creationInfo_creationTime_ms microsecond)) as latest_pick_ct,
                max(date_add(Pick.time_value, interval Pick.time_value_ms microsecond)) as latest_pick_t,
                originobject._timestamp as origin_in_db, max(pickobject._timestamp) as latest_pick_in_db,
                unix_timestamp(date_add(Origin.creationInfo_creationTime, interval Origin.creationInfo_creationTime_ms microsecond)) -
                unix_timestamp(max(date_add(Pick.creationInfo_creationTime, interval Pick.creationInfo_creationTime_ms microsecond))) as locationdelay,
                originobject._timestamp  - max(pickobject._timestamp) as locationdelay_in_db,
                originobject._timestamp - unix_timestamp(date_add(Origin.creationInfo_creationTime, interval Origin.creationInfo_creationTime_ms microsecond)) as origin_to_db_delay

                from Arrival inner join Origin on Arrival._parent_oid = Origin._oid
                inner join PublicObject as pobject on pobject.publicID = Arrival.pickID
                inner join Pick on Pick._oid = pobject._oid
                inner join PublicObject as oobject on oobject._oid = Origin._oid
                inner join OriginReference on OriginReference.originID = oobject.publicID
                inner join Event on Event._oid = OriginReference._parent_oid
                inner join PublicObject as eobject on eobject._oid = Event._oid
                inner join Object as originobject on Origin._oid = originobject._oid
                inner join Object as pickobject on Pick._oid = pickobject._oid
                inner join PublicObject as mobject on Event.preferredMagnitudeID = mobject.publicID
                inner join Magnitude on mobject._oid = Magnitude._oid

                where Origin.evaluationMode='automatic' and Arrival.weight > 0
                and
                Origin._oid in ( select min(Origin._oid)
                                 from Origin inner join PublicObject as oobject on oobject._oid = Origin._oid
                                 inner join OriginReference on OriginReference.originID = oobject.publicID
                                 inner join Event on Event._oid = OriginReference._parent_oid
                                 group by Event._oid)
                """
                if author is not None:
                    query += """ and Origin.creationInfo_author like '%s%%'""" % author
                if agency is not None:
                    query += """ and Origin.creationInfo_agencyID = '%s'""" % agency
                if smiid is not None:
                    query += """ and Event.publicID like '%s%%'""" % smiid
                if latmin is not None and latmax is not None:
                    query += """ and Origin.latitude_value > %.1f and Origin.latitude_value < %.1f""" % (latmin, latmax)
                if lonmin is not None and lonmax is not None:
                    query += """ and Origin.longitude_value > %.1f and Origin.longitude_value < %.1f""" % (lonmin, lonmax)
                if minmag is not None:
                    query += """ and Magnitude.magnitude_value >= %.1f """ % minmag
                query += "and Origin.creationInfo_creationTime > "
                query += "'%s'" % (starttime.strftime("%Y-%m-%d %H:%M:%S"))
                query += "and Origin.creationInfo_creationTime <= "
                query += "'%s'" % (endtime.strftime("%Y-%m-%d %H:%M:%S"))
                query += """ group by eventid, originid, origin_ct, origin_in_db, origin_to_db_delay order by  eventid desc"""
            mark.execute(query)
            while True:
                result = mark.fetchmany()
                if not result:
                    break
                for _e in result:
                    evid, oid, oct, pct, pt, octdb, pctdb, odelay, odelay_db, o2db = _e
                    if dbtype == 'postgresql':
                        self.delays_t.append(UTCDateTime(oct) - UTCDateTime(pt))
                        self.delays_ct.append(odelay.total_seconds())
                        self.events.append(evid)
                        self.ots.append(UTCDateTime(oct))
                        self.pick_delays.append(UTCDateTime(pct) - UTCDateTime(pt))
                        self.odb_delays.append(odelay_db.total_seconds())
                    elif dbtype == 'mysql':
                        self.delays_ct.append(float(odelay))
                        self.events.append(evid)
                        self.odb_delays.append(float(odelay_db))
                        self.ots.append(UTCDateTime(oct))
            print "Fastest event %s; delay %.3f s" \
            % (self.events[np.argmin(self.delays_ct)], np.min(self.delays_ct))
            print "Slowest event %s; delay %.3f s" \
            % (self.events[np.argmax(self.delays_ct)], np.max(self.delays_ct))
            np.savez(fout, delays=np.array(self.delays_ct), ots=np.array(self.ots))
            con.close()
        else:
            a = np.load(fout)
            self.delays_ct = a['delays']
            #self.ots=a['ots']

    def plot_delays(self, fout, noshow=False):
        # Plot the time difference between the creation time of the first origin
        # of an event and the creation time of the latest pick object
        fig = plt.figure()
        ax1 = plt.gca() #fig.add_subplot(111)
        if False:
            ax2 = fig.add_axes(ax1.get_position(True), sharex=ax1, frameon=False)
            n, bins, patches = ax2.hist(self.delays_t, bins=np.arange(0, 30, 0.5), color='green')
            ax2.yaxis.tick_left()
            ax2.yaxis.set_label_position('right')
            ax2.yaxis.set_offset_position('right')
            ax2.xaxis.set_visible(False)
            ax2.set_ylabel('Origin creation - pick creation', color='b')
            for tl in ax2.get_yticklabels():
                tl.set_color('g')
        n, bins, patches = ax1.hist(self.delays_ct, bins=np.arange(0, 30., 0.5),
                                    color='blue', rwidth=1.0,
                                    label=r''+str(len(self.delays_ct))+' events')# $\stackrel{from }{to}$') #'+str(np.min(self.ots))+'}{to '+str(np.max(self.ots))+'}$')

        med = np.nanmedian(self.delays_ct)
        percentile16 = scoreatpercentile(self.delays_ct, 16)
        percentile84 = scoreatpercentile(self.delays_ct, 84)
        ylim = ax1.get_ylim()

        if False:
            ax1.text(0.6, 0.7, 'Median: %.1f s' % (med), horizontalalignment='left',
                    transform=ax1.transAxes, color='blue')
            ax1.text(0.6, 0.65, '16th percentile: %.1f s' % (percentile16), horizontalalignment='left',
                    transform=ax1.transAxes, color='blue')
            ax1.text(0.6, 0.6, '84th percentile: %.1f s' % (percentile84), horizontalalignment='left',
                    transform=ax1.transAxes, color='blue')

            ax1.text(0.6, 0.7, 'Median: %.1f s \n16th percentile: %.1f s \n84th percentile: %.1f s' % (med, percentile16, percentile84),
                    horizontalalignment='left', transform=ax1.transAxes, color='blue',
                    bbox=dict(facecolor='none', edgecolor='blue', boxstyle='round') )

            ax1.plot([med, percentile84], [ylim[-1]*.98, ylim[-1]*.98], '-', linewidth=3, label=r'84th percenile: '+str(percentile84)+'s' )
            ax1.plot([med, percentile16], [ylim[-1]*.98, ylim[-1]*.98], '-', linewidth=6, label=r'16th percenile: '+str(percentile16)+'s')
            ax1.plot([med], [ylim[-1]*.98], '+', linewidth=9, label='Median: %.1f s' % (med))

        ax1.add_patch(Rectangle((med-percentile84/2,ylim[0]),
            percentile84, ylim[-1],
            zorder=0,alpha=0.2,facecolor='grey',linewidth=0,label=r'%$\stackrel{ile}{84th}$: '+str(percentile84)+'s' ))
        ax1.add_patch(Rectangle((med-percentile16/2,ylim[0]),
            percentile16, ylim[-1],
            zorder=0,alpha=0.5,facecolor='grey',linewidth=0,label=r'%$\stackrel{ile}{16th}$: '+str(percentile16)+'s' ))
        ax1.add_patch(Rectangle((med, ylim[0]),
            0.,ylim[-1],
            zorder=0,edgecolor='grey',facecolor='grey',linewidth=3,label='Median: %.1f s' % (med)))


        ax1.set_xlim( [ 0., ax1.get_xlim()[-1] ] )
        ax1.set_title(r''+'Distribution of event declaration times \n $\stackrel{To}{From}$')#+str(np.min(self.ots))+'}{to '+str(np.max(self.ots))+'}$')')
        ax1.set_xlabel('Event declaration time [s]')
        ax1.set_ylabel('Count')
        ax1.legend(fancybox=True)
        plt.grid()
        plt.savefig(fout, dpi=300)
        if noshow :
            pass
        else:
            plt.show()

        if False:
            # Plot the time difference between the arrival of the first origin
            # and the last pick at the database
            fig1 = plt.figure()
            ax = fig1.add_subplot(111)
            n, bins, patches = ax.hist(self.odb_delays, bins=np.arange(0, 30, 0.5), color='blue')
            ax.set_ylabel('Origin - pick creation in DB')
            ax.set_xlabel('Time [s]')
            plt.savefig(fout, dpi=300)

        if False:
            # Plot the time difference between the pick timestamp and its creation time
            fig1 = plt.figure()
            ax = fig1.add_subplot(111)
            n, bins, patches = ax.hist(self.pick_delays, bins=np.arange(0, 30, 0.5), color='blue')
            ax.set_ylabel('Pick occurrence  - pick creation')
            ax.set_xlabel('Time [s]')
            plt.savefig(fout, dpi=300, bbox_inches='tight')
            plt.show()

        if False:
            # Print some statistics to stdout
            print "Number of events: %d" % len(self.events)
            print "Earliest event: %s" % np.min(self.ots)
            print "Latest event: %s" % np.max(self.ots)
            print "min ct: %.2f; max ct: %.2f" % (min(self.delays_ct), max(self.delays_ct))
            print "Event with longest delay: %s" % self.events[np.argmax(self.delays_ct)]
            print "Origin time of event with longest delay: %s" % self.ots[np.argmax(self.delays_ct)]

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help="Server name hosting the database.")
    parser.add_argument('--port', help="Port number to connect to database.", type=int)
    parser.add_argument('-u', '--user', help="User name for database.")
    parser.add_argument('-p', '--pwd', help="Password for database.")
    parser.add_argument('-d', '--database', help="Database name.")
    parser.add_argument('--dbtype', help="Database type ('postgresql' or 'mysql').")
    parser.add_argument('-o', '--npfile', help="Path for numpy file with pick delays.")
    parser.add_argument('--plotfile', help="Path for png file showing pick delay distribution.")
    parser.add_argument('--new', help="Reread data from database even if json file exists.",
                        action='store_true')
    parser.add_argument('--noshow', help="Saves plot without showing.",
                        action='store_true')
    flt = parser.add_argument_group('SQL filter')
    flt.add_argument('--start', help="Give start time for the query e.g. 2014-12-31T12:11:05",
                        default='1970-01-01T00:00:00')
    flt.add_argument('--end', help="Give end time for the query e.g. 2014-12-31T12:11:05",
                        default=UTCDateTime().strftime("%Y-%m-%dT%H:%M:%SZ"))
    flt.add_argument('--author', help="Author of an origin.", default=None)
    flt.add_argument('--agency', help="Agency of an origin.", default=None)
    flt.add_argument('--latmin', help="Minimum latitude for spatial filter.",
                     default=None, type=float)
    flt.add_argument('--latmax', help="Maximum latitude for spatial filter.",
                     default=None, type=float)
    flt.add_argument('--lonmin', help="Minimum longitude for spatial filter.",
                     default=None, type=float)
    flt.add_argument('--lonmax', help="Maximum longitude for spatial filter.",
                     default=None, type=float)
    flt.add_argument('--smiid', help="Filter based on smi identifier.", default=None)
    flt.add_argument('--minmag', help="Minimum magnitude.", default=None, type=float)

    args = parser.parse_args()
    oct = OriginCT()
    oct.get_delays(args.npfile, args.host, args.database, args.user, args.pwd,
                    args.port, author=args.author, agency=args.agency,
                    latmin=args.latmin, latmax=args.latmax, lonmin=args.lonmin,
                    lonmax=args.lonmax, minmag=args.minmag, smiid=args.smiid,
                    new=args.new, dbtype=args.dbtype,
                    starttime=UTCDateTime(args.start),
                    endtime=UTCDateTime(args.end))
    oct.plot_delays(args.plotfile, args.noshow)
