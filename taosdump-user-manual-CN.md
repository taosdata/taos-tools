taosdump是一个支持从运行中的TDengine服务器或集群中备份数据并将备份的数据恢复到相同或另一个运行中的TDengine服务器或集群中的工具应用程序。

taosdump可以用数据库、超级表或普通表作为逻辑数据单元备份。taosdump可以数据库、超级表和普通表中备份指定时间段内的数据记录。

taosdump是一个逻辑备份工具。它不打算或不应被期望用于备份任何原始数据、环境设置、硬件信息、服务端配置或集群的拓扑结构。

taosdump使用[Apache AVRO](https://avro.apache.org/)作为数据文件格式来存储备份数据。

taosdump可以将数据备份到指定的目录中。如果不指定位置，taosdump默认会将数据备份到当前目录。如果指定的位置已经有数据文件，taosdump会提示用户可能会被备份动作再次覆盖。如果您看到提示，请小心操作。

taosdump 备份可以指定 -A 或 --all-databases 参数指定所有数据库；或使用 -D db1,db2,... 参数备份指定的多个数据库；或者使用 dbname stbname1 stbname2 tbname1 tbname2 ... 参数方式备份指定数据库中的某些个超级表或普通表，注意这种输入序列第一个参数为数据库名称，且只支持一个数据库，第二个和之后的参数为该数据库中的超级表或普通表名称，中间以空格分隔。

taosdump 恢复数据使用 -i 加上数据文件所在路径作为参数进行备份指定路径下的数据文件。如前面提及，不应该使用同一个目录备份不同数据集合，也不应该在同一路径多次备份同一数据集，否则备份数据会造成覆盖或多次备份。

TDengine 服务器或集群通常会包含一个系统数据库，名为 log，这个数据库内的数据为 TDengine 自我运行的数据，taosdump 默认不会对 log 库进行备份。如果有特定需求对 log 库进行备份，可以使用 -a 或 --allow-sys 命令行参数。

taosdump 内部使用 TDengine stmt binding API 进行恢复数据的写入，为提高数据恢复性能，目前使用 16384 为一次写入批次。如果备份数据中有比较多列数据，可能会导致产生 WAL size exceeds limit 错误，此时可以通过使用 -B 参数调整为一个更小的值进行尝试。

以下为 taosdump 详细命令行参数列表：
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
