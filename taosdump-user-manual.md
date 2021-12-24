taosdump is a tool application to support dump out data from the running TDengine server or cluster and restore the dumped data into a running TDengine server or cluster.

taosdump can be used to dump the database(s), super table(s), or normal table(s) as the logical unit to backup. taosdump can backup specified data records of the database(s), super table(s) or normal table(s) with a specified time period.

taosdump is a logical backup tool. It does not intend to or to be expected to back up any raw data, the environment settings, hardware information, the configuration of server, or the topology of the cluster.

taosdump uses the [Apache AVRO](https://avro.apache.org/) as the data file format to store the backup data.

taosdump can backup the data to the specified directory. If you don't specify the location, taosdump will back up the data to the current directory by default. taosdump will prompt the user if the specified location already has any data file taosdump generated may be overwritten by backup action again. Please be careful if you see a prompt.

Following is the list of taosdump command line arguments in details:
```
$ taosdump --help
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
  -c, --config-dir=CONFIG_DIR   Configure directory. Default is
                             /etc/taos/taos.cfg.
  -e, --encode=ENCODE        Input file encoding.
  -i, --inpath=INPATH        Input file path.
  -o, --outpath=OUTPATH      Output file path.
  -r, --resultFile=RESULTFILE   DumpOut/In Result file path and name.
  -a, --allow-sys            Allow to dump system database
  -A, --all-databases        Dump all databases.
  -D, --databases=DATABASES  Dump inputted databases. Use comma to separate
                             databases' name.
  -N, --without-property     Dump schema without properties.
  -s, --schemaonly           Only dump schema.
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
  -B, --data-batch=DATA_BATCH   Number of data point per insert statement. Default
                             value is 16384.
  -L, --max-sql-len=SQL_LEN  Max length of one sql. Default is 65480.
  -t, --table-batch=TABLE_BATCH   Number of table dumpout into one output file.
                             Default is 1.
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
