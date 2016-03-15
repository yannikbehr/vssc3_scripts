#!/usr/bin/python

import sys, seiscomp3.Client, time, traceback
# import MySQLdb


#
# These values need to be provided by the user
#

host = 'localhost' 
user = 'USER OF DATABASE'
password = 'PASS' 
database = 'PUT THE DB YOU WAT THIS WRITTEN' 

#######################################
# db = MySQLdb.connect(host=host,user=user,passwd=password,db=database)
# c = db.cursor()   
class QCListener(seiscomp3.Client.Application):
  def __init__(self, argc, argv):
    seiscomp3.Client.Application.__init__(self, argc, argv)
    self.setPrimaryMessagingGroup("LISTENER_GROUP")
    self.addMessagingSubscription("QC")
    self.setDatabaseEnabled(False,False)
    self.setMessagingUsername("")

  def handleMessage(self, msg):
#    if first_run:
#      first_run = False
#      time.sleep(1000)
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
              if nc == 'HP':
    		 if param == 'delay':
        	  print "%s.%s.%s.%s (%s): %s = %f" % (nc, sc, lc, cc, timestamp.iso(), param, value)
                 if param == 'latency':
                  print "%s.%s.%s.%s (%s): %s = %f" % (nc, sc, lc, cc, timestamp.iso(), param, value)    
              
              # this statement creates a table with an averaged value per station
            #    c.execute('INSERT delay (station,channel,delay,time) values(%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE delay= if(%s < delay, %s, 0.9*delay + 0.1 * %s),time=NOW()',[sc,cc,value,value,value,value])
            # db.commit()
              # this statement creates a table which contains all past values (to make statistics)
            #    c.execute('INSERT delay2 (station,channel,delay,time) values(%s,%s,%s,NOW())',[sc,cc,value])
            # db.commit()
    except:
         info = traceback.format_exception(*sys.exc_info())
	 for i in info: sys.stderr.write(i)

app = QCListener(len(sys.argv), sys.argv)
sys.exit(app())
