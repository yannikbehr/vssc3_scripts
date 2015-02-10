# VS(SC3) scripts
Scripts to analyse the performance of VS(SC3). To run them you will need the following 
additional Python packages installed:

psycopg2
pscycopg2.extras
MySQLdb
obspy
numpy
matplotlib

The '-h' option will give you help on the command line options. 

Example:
To 
sc3vs_pick_delays.py --host 127.0.0.1 \
-u sysop -p sysop --port 9998 -d seiscomp3 --dbtype mysql --new \
-o /tmp/single_station_pk_delays_ro.txt --plotfile=/tmp/single_station_pk_delays_ro.png
