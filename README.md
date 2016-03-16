# VS(SC3) scripts

Scripts to analyse the performance of VS(SC3). To run them you will need the following
additional Python packages installed:

psycopg2  
pscycopg2.extras  
MySQLdb  
obspy  
numpy  
matplotlib  

The '-h' option will give you help on the command line options.

## Examples

To measure data latencies run the following command (note this depends on scqc running):

    ./data_latency.py

To get the distribution of pick delays run the following command:

    ./vssc3_pick_delays.py --host your.database.host -u username -p password --port yourdbport -d yourdbname --dbtype mysql --networks 'CH,MN,GU' --new -o data-output-file.txt --plotfile=data-plot.png

And to get the distribution of origin creation times run the following:

    ./vssc3_origin_creation_time.py --host your.database.host -u username -p password --port yourdbport -d yourdbname --dbtype mysql --new -o data-output-file.npz --plotfile=data-plot.png

## Notes on analysing remote databases
If the scripts are installed on a different machine than the one your database is
running on you can use ssh's port forwarding feature to access the database
(provided you have ssh access of course). Let's assume the database is running
on 'dbhost.some.url' and it can be accessed through the server 'gateway.some.url'
to which you have ssh access (username: 'sshuser'). Let's further assume the
database type is MySQL listening on its default port 3306 and the database name is 'testdb' which we can access as user 'testuser' with password 'testpasswd'. To forward access to
this machine to the local port '9999' run the following command in a new window:

      ssh -N -L 9999:dbhost.some.url:3306 -l sshuser gateway.some.url

You can then run the pick delay analysis as follows:

    vssc3_pick_delays.py --host 127.0.0.1 -u testuser -p testpasswd --port 9999 -d testdb --dbtype mysql --new -o pick_delays.txt --plotfile=pick_delays.png
