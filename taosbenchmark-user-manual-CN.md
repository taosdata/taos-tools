# taosBenchmark

## 简介

taosBenchmark 是一个用于 TDengine 的性能测试的应用程序。taosBenchmark 可以测试 TDengine 的插入、查询和订阅功能，它可以模拟由大量设备产生的大量数据，还可以灵活地控制列的数量，数据类型、线程等。以前叫 taosdemo，现在改名为为 taosBenchmark，安装包提供 taosdemo 作为 taosBenchmark 的软链接。
配置，一种是命令行配置，另一种是[json文件](#taosbenchmarkjson-configuration-file)。

## taosBenchmark CLI 选项

| 选项名称                                               | 描述                                                    |
|:---------------------------------------------------|-------------------------------------------------------|
| [-f/--file](#taosbenchmark json-configuration-file) | json 配置文件                                             |
| -c/--config-dir                                    | 配置文件所在的目录，默认路径是 /etc/taos/                             |
| -h/--host                                          | 用于连接 taosd 服务器的 FQDN，默认值为 localhost。                      |
| -P/--port                                          | 用于连接 taosd 服务器的端口号，默认值为 6030。                            |
| -I/--interface                                     | taosBenchmark 如何插入数据，选项有 taosc、rest、stmt、sml，默认值为 taosc。 |
| -u/--user                                          | 用于连接 taosd 服务器的用户名，默认值为 root。                            |
| -p/--password                                      | 用于连接 taosd 服务器的密码，默认值为 taosdata。                         |
| -o/--output                                        | 指定结果输出文件的路径，默认值为 ./output.txt。                         |
| -T/--thread                                            | 指定插入数据的线程数，默认值为 8                                      |
| [-i/--insert-interval](#-i-insert-interval)        | 隔行插入模式的插入间隔，单位为 ms，默认值为 0。                              |
| -S/--timestampstep                                          | 每个子表中每条记录的插入时间戳步长，单位是 ms，默认值是 1                         |
| [-B/--interlace-rows](#-b-interlace-rows)          | 向子表插入交错行的数量                                           |
| -r/--rec-per-req                                   | 每次插入请求的记录数，默认值为 30000                                  |
| -t/--tables                                        | 子表的数量，默认值为 10000。                                      |
| -n/--records                                       | 每个子表插入的记录数，默认值为 10000。                                 |
| -d/--database                                           | 数据库的名称，默认值为 test。                                      |
| [-l/--columns](#-l--columns)                       | 子表的列数，将使用int数据类型的列。                                   |
| [-A/--tag-type](#-a-tag-type)                      | 子表的标签的数据类型。                                           |
| [-b/--data-type](#-b--data-type)                   | 子表的列的数据类型                                             |
| -w/--binwidth                                      | nchar和二进制数据类型的默认长度，默认值为 64。                            |
| -m/--table-prefix                                  | 子表名称的前缀，默认值为 d                                         |
| -E/--escape-character                                          | 在稳定表和子表名称中使用转义字符，可选。                                  |
| -C/--chinese                                            | nchar和binary是基本的Unicode中文字符，可选。                       |
| [-N/--normal-table](#-n-normal-table)              | 只创建正常表，不创建超级表，可选。                                     |
| -M/--random                                        | 数据源是随机的，可选。                                           |
| -x/--aggr-func                                     | 插入后查询聚合函数，可选。                                         |
| -y/--answer-yes                                    | 通过确认提示继续，可选。                                          |
| [-R/--disorder-range](#-r-disorder-range)          | 失序时间戳的范围，基于数据库的精度，默认值为 1000                            |
| [-O/--disorder](#-o-disorder)                      | 插入无序时间戳的数据的比例，默认为 0。                                   |
| -a/--replica                                       | 创建数据库时的副本数量，默认值为 1。                                    |
| -V/--version                                       | 显示版本信息并退出                                             |
| -?/--help                                          | 显示帮助信息并退出。                                            |

## taosBenchmark json 配置文件

### 1、插入 json 配置文件

```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 4,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "interlace_rows": 100,
    "num_of_records_per_req": 100,
    "prepared_rand": 10000,
    "chinese":"no",
    "databases": [{
    "dbinfo": {
      "name": "db",
      "drop": "yes",
      "replica": 1,
      "days": 10,
      "cache": 16,
      "blocks": 8,
      "precision": "ms",
      "keep": 3650,
      "minRows": 100,
      "maxRows": 4096,
      "comp":2,
      "walLevel":1,
      "cachelast":0,
      "quorum":1,
      "fsync":3000,
      "update": 0
      },
      "super_tables": [{
        "name": "stb",
        "child_table_exists":"no",
        "childtable_count": 100,
        "childtable_prefix": "stb_",
        "escape_character": "yes",
        "batch_create_tbl_num": 5,
        "data_source": "rand",
        "insert_mode": "taosc",
        "line_protocol": "line",
        "insert_rows": 100000,
        "childtable_limit": 10,
        "childtable_offset":100,
        "interlace_rows": 0,
        "insert_interval":0,
        "disorder_ratio": 0,
        "disorder_range": 1000,
        "timestamp_step": 10,
        "start_timestamp": "2020-10-01 00:00:00.000",
        "sample_format": "csv",
        "sample_file": "./sample.csv",
        "use_sample_ts": "no",
        "tags_file": "",
        "columns": [{"type": "INT", "name": "id"}, {"type": "DOUBLE", "count":10}, {"type": "BINARY", "len": 16, "count":3}, {"type": "BINARY", "len": 32, "count":6}],
        "tags": [{"type": "TINYINT", "count":2, "max": 10, "min": 98}, {"type": "BINARY", "len": 16, "count":5, "values":["beijing","shanghai"]}]
        }]
      }]
}
```

### 参数

| 组 | 选项名称 | 描述 |
| ------------ | --------------------------------------- | ------------------------------------------------------------ |
| | filetype | 文件类型，指定哪种测试，对于插入测试，需要插入。
| |cfgdir | taos 配置文件所在的目录，默认值是 /etc/taos。
| | host | taosd 服务器的 FQDN，默认为 localhost。
| | port | taosd 服务器的端口号，默认为 6030。
| |user | 连接 taosd 服务器的用户名，默认为 root。
| |password | 连接 taosd 服务器的密码，默认为 taosdata。
| | thread_count | 插入和创建表的线程数，默认为 8。
| | result_file | 保存输出结果的文件路径，默认为 ./output.txt。
| | confirm_parameter_prompt | 在执行过程中传递确认提示，默认为无。
| |[insert_interval](#-i-insert-interval) | 插入隔行扫描模式的间隔时间，默认值为 0
| |[interlace_rows](#-b-interlace-rows) | 每个子表的交错行数，默认值为 0。
| | num_of_records_per_req | 每个请求中的记录数，默认值为 30000。
| | [prepare_rand](#prepare_rand) | 随机产生的数据数量，默认值为 10000 |
| | chinese | nchar 和 binary 都是 rand 中文，默认值为否。
| dbinfo | name | 数据库名称，必填
| dbinfo | drop | 插入测试前是否删除数据库，默认值为是。
| dbinfo | replica | 复制的数量，默认值是 1。
| dbinfo | days | 在文件中存储数据的时间跨度，默认值为 10。
| dbinfo | cache | 内存块的大小，单位是 MB，默认值是 16。
| dbinfo | blocks | 每个 vnode(tsdb) 中的缓存大小的内存块的数量，默认值为 6。
| dbinfo | precision | 数据库时间精度，默认值为 "ms" | dbinfo | keep | 数据库时间精度。
| dbinfo | keep | 保留数据的天数，默认值为 3650。
| dbinfo | minRows     | 文件块中的最小记录数，默认值为 100
| dbinfo | minRows | 文件块中的最大记录数，默认值为 4096
| dbinfo | comp | 文件压缩标志，默认值为 2。
| dbinfo | walLevel | wal 级别，默认值是 1。
| dbinfo | cachelast | 是否允许将每个表的最后一条记录保留在内存中，默认值为 0
| dbinfo | quorum | 异步写需要的确认次数，默认为 1
| dbinfo | fsync | 当 wal 设置为 2 时，fsync 的间隔时间，单位为 ms，默认值为 3000。
| dbinfo | update | 是否支持数据更新，默认值为 0。
| super_tables | name | 超级表的名称，必须填写。
| super_tables | child_table_exists                      | 子表是否已经存在，默认为否。
| super_tables | child_table_count | 子表的数量，必填
| super_tables | childtable_prefix                       | 子表名称的前缀，必须填写。
| Super_tables | escape_character | 超级表和子表的名称包括转义字符，默认为否。
| Super_tables | batch_create_tbl_num | 为每个请求创建的子表数量，默认为 10。
| super_tables | data_source                             |  数据资源类型 |
| super_tables | insert_mode                             | 插入模式，选项：taosc, rest, stmt, sml，默认为 taosc。
| super_tables | line_protocol | 仅当 insert_mode 为 sml 时有效，选项：line, telnet, json, 默认为 line。
| super_tables | insert_rows | 每个子表的记录数，默认为 0。
| super_tables | childtable_offset                       | 子表的偏移量，只有当 drop 为 no，child_table_exists 为 yes 时才有效。
| super_tables | childtable_limit | 插入数据的子表数量，仅当 drop 为 no 且 child_table_exists 为 yes 时有效。
| super_tables | interlace_rows | 每个子表的间隔行，默认为 0。
| super_tables | insert_interval | 两个请求之间的插入时间间隔，当 interlace_rows 大于 0 时有效。
| super_tables | [disorder_ratio](#-r-disorder-ratio) | 紊乱时间戳的数据比例，默认为 0
| super_tables | [disorder_range](#-r--disorder-range) | 无序时间戳的范围，只有当 disorder_ratio 大于 0 时才有效，默认为 1000。
| super_tables | timestamp_step | 每条记录的时间戳步骤，默认为 1。
| super_tables | start_timestamp | 每个子表的时间戳起始值，默认值是现在。
| super_tables | sample_format | 样本数据文件的类型，现在只支持 csv。
| super_tables | sample_file | 样本文件，仅当 sample_source 为 "sample "时有效。
| super_tables| use_sample_ts | 样本文件是否包含时间戳，默认为否。
| super_tables | tags_file | 标签数据样本文件，仅支持 taosc、rest insert模式。
| columns/tags | type  | 数据类型，必填
| columns/tags | len | 数据长度，对 nchar 和 binary 有效，默认为 8。
| columns/tags | count | 该列的连续数，默认为 1。
| columns/tags | name | 这一列的名称，连续的列名将是 name_#{number}。
| columns/tags | min | 数字数据类型列/标签的最小值
| columns/tags | max | 数字数据类型列/标签的最大值
| columns/tags | values | nchar/binary 列/标签的值，将从值中随机选择。

### 2、查询测试 json 配置文件

```json
{
  "filetype": "query",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "confirm_parameter_prompt": "no",
  "databases": "db",
  "query_times": 2,
  "query_mode": "taosc",
  "specified_table_query": {
    "query_interval": 1,
    "concurrent": 3,
    "sqls": [
      {
        "sql": "select last_row(*) from stb0 ",
        "result": "./query_res0.txt"
      },
      {
        "sql": "select count(*) from stb00_1",
        "result": "./query_res1.txt"
      }
     ]
   },
   "super_table_query": {
     "stblname": "stb1",
     "query_interval": 1,
     "threads": 3,
     "sqls": [
     {
       "sql": "select last_row(ts) from xxxx",
       "result": "./query_res2.txt"
      }
     ]
   }
}
```

### 查询测试 JSON 文件的参数

| 组 | 选项 | 描述 |
| --------------------------------------- | ------------------------ | ------------------------------------------------------------ |
| | filetype | 文件类型，指定哪种测试，对于查询测试，需要
| |cfgdir | taosd 配置文件所在的目录。
| | host | taosd 服务器的 FQDN，默认为 localhost。
| | port | taosd 服务器的端口号，默认为 6030。
| |user | 连接 taosd 服务器的用户名，默认为 root。
| |password | 连接 taosd 服务器的密码，默认为 taosdata。
| |confirm_parameter_prompt | 在执行过程中传递确认提示，默认为否。
| | database |数据库的名称，需要
| |query_times | 查询次数 |
| |query mode |查询模式，选项：taosc 和 rest，默认为 taosc。
| specified_table_query/super_table_query | query_interval | 查询时间间隔，单位是秒，默认是 0
| specified_table_query/super_table_query | concurrent/threads | 执行 SQL 的线程数，默认为 1。
| super_table_query | stblname | supertable name, required |超级表名称。
| sqls | [sql](#sql) | SQL 命令，必填
| sqls | result | 查询结果的结果文件，没有则为空。

### 3、订阅 json 配置文件

```json
{
  "filetype":"subscribe",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "databases": "db",
  "confirm_parameter_prompt": "no",
  "specified_table_query":
    {
      "concurrent":1,
      "interval":0,
      "restart":"yes",
      "keepProgress":"yes",
      "sqls": [
        {
          "sql": "select * from stb00_0 ;",
          "result": "./subscribe_res0.txt"
        }
        ]
      },
      "super_table_query":
      {
        "stblname": "stb0",
        "threads":1,
        "interval":10000,
        "restart":"yes",
        "keepProgress":"yes",
        "sqls": [
        {
          "sql": "select * from xxxx where ts > '2021-02-25 11:35:00.000' ;",
          "result": "./subscribe_res1.txt"
        }]
      }
}
```

### 订阅测试 JSON 文件的参数

| 组 | 选项 | 描述 |
| --------------------------------------- | ------------------------ | ------------------------------------------------------------ |
| | filetype | 文件类型，指定哪种测试，对于订阅测试，需要
| |cfgdir | taosd 配置文件的目录。
| | host | taosd 服务器的 FQDN，默认为 localhost。
| | port | taosd 服务器的端口号，默认为 6030。
| |user | 连接 taosd 服务器的用户名，默认为 root。
| |password | 连接 taosd 服务器的密码，默认为 taosdata。
| |databases | 数据库名称，需要
| |confirm_parameter_prompt | 在执行过程中是否通过确认提示。
| specified_table_query/super_table_query | concurrent/threads | 执行 SQL 的线程数，默认为 1。
| specified_table_query/super_table_query | interval | 执行订阅的时间间隔，默认为 0。
| specified_table_query/super_table_query | restart | no: 继续之前的订阅，yes: 开始新的订阅。
| specified_table_query/super_table_query | keepProgress | 是否保留订阅的进度。
| specified_table_query/super_table_query | resubAfterConsume | 是否取消订阅，然后再次订阅？
| super_table_query | stblname | supertable 的名称，必须的。
| sqls | [sql](#sql) | SQL 命令，必填
| sqls | result | 查询结果的结果文件，没有则为空。

- #### -i/--插值间隔

  只有当隔行扫描(-B/-interlace-rows)大于0时才起作用。该
  意味着线程在为每个子表插入隔行扫描记录后，会在该时间段内睡觉。
  隔行扫描的时间。

- #### -B/--interlace-rows

  如果它的值为 0，意味着逐步插入，线程将逐个插入到
  子表。如果它的值大于零，线程将首先插入
  到第一个子表的行数，然后是第二个、第三个，以此类推。
  以此类推，当所有的子表都被插入了该行数后，线程将从第一个子表重新开始插入。
  将从第一个子表重新开始插入，依次进行。

- #### -l/--columns

  如果同时设置了-l/--columns和-b/--data-type，它将检查列数是否达到
      列号是否达到 -b/--数据类型的 -l/--columns，如果是的话。
      忽略这个选项，或者它将继续增加列数以达到这个数字
      所有的 int 数据类型。

- #### -A/--tag-type

  设置超级表的标签类型，nchar 和 binary 也可以设置长度，例如
  如

  ```
  taosBenchmark -A INT,DOUBLE,NCHAR,BINARY(16)
  ```

  默认是INT,BINARY(16)。

- #### -b/--数据类型

  与-A/--tag-type相同，但用于列，默认为FLOAT,INT,FLOAT

- #### -O/--紊乱度

  乱序时间戳的比例，最大为50

- #### -R/--失序范围

  只有当-O/--disorder大于0时才有效，并且disorder意味着
  时间戳将在该范围内减少随机数毫秒。
  生成。

- #### prepared_rand

  作为数据源预先生成的随机数据的数量，小的prepare_rand
  会节省内存，但会减少数据种类。

- #### sql

  对于超级表的查询 SQL，在 SQL 命令中保留 "xxxx"，程序会自动将其替换为超级表的所有子表名。
      替换为超级表中所有的子表名。

