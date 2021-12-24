taosdump是一个支持从运行中的TDengine服务器或集群中备份数据并将备份的数据恢复到相同或另一个运行中的TDengine服务器或集群中的工具应用程序。

taosdump可以用数据库、超级表或普通表作为逻辑数据单元备份。taosdump可以数据库、超级表和普通表中备份指定时间段内的数据记录。

taosdump是一个逻辑备份工具。它不打算或不应被期望用于备份任何原始数据、环境设置、硬件信息、服务端配置或集群的拓扑结构。

taosdump使用[Apache AVRO](https://avro.apache.org/)作为数据文件格式来存储备份数据。

taosdump可以将数据备份到指定的目录中。如果不指定位置，taosdump默认会将数据备份到当前目录。如果指定的位置已经有数据文件，taosdump会提示用户可能会被备份动作再次覆盖。如果你看到提示，请小心操作。

以下为 taosdump 详细命令行参数列表：
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
