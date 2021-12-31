taosdump is a tool application to support dump out data from the running TDengine server or cluster and restore the dumped data into a running TDengine server or cluster.

taosdump can be used to dump the database(s), super table(s), or normal table(s) as the logical unit to backup. taosdump can backup specified data records of the database(s), super table(s) or normal table(s) with a specified time period.

taosdump is a logical backup tool. It does not intend to or to be expected to back up any raw data, the environment settings, hardware information, the configuration of server, or the topology of the cluster.

taosdump uses the [Apache AVRO](https://avro.apache.org/) as the data file format to store the backup data.

taosdump can backup data to the specified directory. If no location is specified, taosdump will back up the data to the current directory by default. If the specified location already has data files, taosdump will prompt the user that the backup action may be overwritten again. If you see the prompt, please proceed with caution.

taosdump backups can specify all databases with the -A or --all-databases parameter; or use the -D db1,db2,... parameter; or -D db1, db2, ... -D db1, db2, ... -D db1, db2, ... -D dbname stbname1 stbname2 tbname1 tbname2 ... parameters, note that the first parameter of this input sequence must be the database name and only the database name can be specified here. Note that the first parameter of this input sequence is the database name, and only one database is supported, and the second and subsequent parameters are the names of the super or normal tables in the database, separated by spaces.

taosdump restores data using -i and the path where the data file is located as an argument to backup the data file under the specified path. As mentioned earlier, you should not use the same directory to backup different data sets, nor should you backup the same data set multiple times in the same path, otherwise the backup data will be overwritten or backed up multiple times.

TDengine servers or clusters usually contain a system database named log, the data in this database is the data that TDengine runs itself, and taosdump does not back up the log library by default. If you have a specific need to back up the log database, you can use the -a or --allow-sys command line parameter.

taosdump uses the TDengine stmt binding API internally for writing recovery data, and currently uses 16384 as a write batch to improve data recovery performance. If the backup data has more columns of data, it may cause WAL size exceeds limit error, you can try by adjusting to a smaller value with -B parameter.

Following is the list of taosdump command line arguments in details:
```
Usage: taosdump [OPTION...] dbname [tbname ...]
  or:  taosdump [OPTION...] --databases db1,db2,...
  or:  taosdump [OPTION...] --all-databases
  or:  taosdump [OPTION...] -i inpath
  or:  taosdump [OPTION...] -o outpath

  -h, --host=HOST            Server host dumping data from. Default is
                             localhost.
  -p, --password             User password to connect to server. Default is
                             taosdata.
  -P, --port=PORT            Port to connect
  -u, --user=USER            User name used to connect to server. Default is
                             root.
  -c, --config-dir=CONFIG_DIR   Configure directory. Default is /etc/taos
  -i, --inpath=INPATH        Input file path.
  -o, --outpath=OUTPATH      Output file path.
  -r, --resultFile=RESULTFILE   DumpOut/In Result file path and name.
  -a, --allow-sys            Allow to dump system database
  -A, --all-databases        Dump all databases.
  -D, --databases=DATABASES  Dump inputted databases. Use comma to separate
                             databases' name.
  -N, --without-property     Dump database without its properties.
  -s, --schemaonly           Only dump tables' schema.
  -y, --answer-yes           Input yes for prompt. It will skip data file
                             checking!
  -d, --avro-codec=snappy    Choose an avro codec among null, deflate, snappy,
                             and lzma.
  -S, --start-time=START_TIME   Start time to dump. Either epoch or
                             ISO8601/RFC3339 format is acceptable. ISO8601
                             format example: 2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00:000+0800 or '2017-10-01
                             00:00:00.000+0800'
  -E, --end-time=END_TIME    End time to dump. Either epoch or ISO8601/RFC3339
                             format is acceptable. ISO8601 format example:
                             2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00.000+0800 or '2017-10-01
                             00:00:00.000+0800'
  -B, --data-batch=DATA_BATCH   Number of data per insert statement. Default
                             value is 16384.
  -T, --thread_num=THREAD_NUM   Number of thread for dump in file. Default is
                             5.
  -g, --debug                Print debug info.
  -?, --help                 Give this help list
      --usage                Give a short usage message
  -V, --version              Print program version

Mandatory or optional arguments to long options are also mandatory or optional
for any corresponding short options.

Report bugs to <support@taosdata.com>.
```
