#!/usr/bin/env python
"""
This module measures different types of data latencies and writes the results
into an sqlite3 database for later analysis. It listens for WaveformQuality
messages so scqc has to be running. For the difference between 'delay' and
'latency' see the SeisComP3 documentation of scqc.

@author: Roman Racine, Yannik Behr
"""

import sqlite3
import sys
import time
import traceback

import seiscomp3.Client


class QCListener(seiscomp3.Client.Application):
    def __init__(self, argc, argv):
        seiscomp3.Client.Application.__init__(self, argc, argv)
        self.setPrimaryMessagingGroup("LISTENER_GROUP")
        self.addMessagingSubscription("QC")
        self.setDatabaseEnabled(False, False)
        self.setMessagingUsername("")
        self.dbfn = 'data_latencies.sqlite3'
        self.dbcon = None
        self.dbcs = None
        self.runtime = 24 * 3600
        self.start = None

    def validateParameters(self):
        try:
            if seiscomp3.Client.Application.validateParameters(self) == False:
                return False
            if self.commandline().hasOption('dbname'):
                self.dbfn = self.commandline().optionString('dbname')
            if self.commandline().hasOption('runtime'):
                self.runtime = self.commandline().optionDouble('runtime') * 3600
            return True
        except:
            info = traceback.format_exception(*sys.exc_info())
            for i in info: sys.stderr.write(i)
            return False

    def createCommandLineDescription(self):
        try:
            try:
                self.commandline().addGroup("DBOUT")
                self.commandline().addStringOption("DBOUT", "dbname", "Filename to store sqlite3 database in.")
                self.commandline().addDoubleOption("DBOUT", "runtime", "Time (in hours) after which to stop.")
            except:
                seiscomp3.Logging.warning("caught unexpected error %s" % sys.exc_info())
        except:
            info = traceback.format_exception(*sys.exc_info())
            for i in info: sys.stderr.write(i)

    def init(self):
        if not seiscomp3.Client.Application.init(self):
            return False
        seiscomp3.Logging.debug("Opening database file %s" % self.dbfn)
        self.dbcon = sqlite3.connect(self.dbfn)
        self.dbcs = self.dbcon.cursor()
        # table with an averaged value per station
        try:
            self.dbcs.execute('''CREATE TABLE delay (network text, station text,
            location text, channel text, delay real, time text)''')
            self.dbcs.execute('''CREATE TABLE latency (network text, station text,
            location text, channel text, delay real, time text)''')
        except sqlite3.OperationalError:
            seiscomp3.Logging.info("Tables 'delay' and 'latency' already exist.")
        self.enableTimer(int(self.runtime))
        return True

    def done(self):
        seiscomp3.Client.Application.done(self)
        seiscomp3.Logging.debug("Closing database file %s" % self.dbfn)
        if self.dbcon is not None:
            self.dbcon.close()

    def handleTimeout(self):
        self.exit(0)

    def handleMessage(self, msg):
        try:
            dm = seiscomp3.Core.DataMessage.Cast(msg)
            if dm:
                for obj in dm:
                    wq = seiscomp3.DataModel.WaveformQuality.Cast(obj)
                    if wq:
                        if wq.type() == 'report':
                            # handle report messages
                            param = wq.parameter()
                            value = wq.value()
                            timestamp = wq.end()

                            # load Waveform ID (wID), ChannelCode (cc),Location Code (lc)
                            # network Code (nc), station Code (sc)
                            wID = wq.waveformID()
                            nc = wID.networkCode()
                            sc = wID.stationCode()
                            lc = wID.locationCode()
                            cc = wID.channelCode()
                            if param == 'delay':
                                logstr = "%s.%s.%s.%s (%s): %s = %f" % (nc, sc, lc, cc, timestamp.iso(), param, value)
                                seiscomp3.Logging.info(logstr)
                                self.dbcs.execute("insert into delay values (?,?,?,?,?,datetime('now'))", (nc, sc, lc, cc, value))
                                self.dbcon.commit()
                                # c.execute('INSERT delay (station,channel,delay,time) values(%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE delay= if(%s < delay, %s, 0.9*delay + 0.1 * %s),time=NOW()', [sc, cc, value, value, value, value])
                            if param == 'latency':
                                logstr = "%s.%s.%s.%s (%s): %s = %f" % (nc, sc, lc, cc, timestamp.iso(), param, value)
                                seiscomp3.Logging.info(logstr)
                                self.dbcs.execute("insert into latency values (?,?,?,?,?,datetime('now'))" , (nc, sc, lc, cc, value))
                                self.dbcon.commit()
        except:
            info = traceback.format_exception(*sys.exc_info())
            for i in info: sys.stderr.write(i)

app = QCListener(len(sys.argv), sys.argv)
sys.exit(app())
