## taosBenchmark

### 简介
taosBenchmark 是TDengine进行性能测试的工具应用程序。taosBenchmark可以进行TDengine的写入、查询和订阅功能的性能测试。taosBenchmark可以模拟大量设备产生海量数据的场景，可以通过taosBenchmark参数灵活控制表的列数、数据类型、并发线程数量等。taosBenchmark支持两种配置参数的方法，一种是命令行，另一种是配置json文件。原为taosdemo，现在更名为taosBenchmark，并且安装包也提供了作为 taosBenchmark 软连接的 taosdemo。

### taosBenchmark 命令行参数：
```

-f: 指定taosBenchmark所需参数的配置json文件。使用该参数时，其他命令行参数失效，可选项。

-c：配置文件taos.cfg所在的路径。因为taosBenchmark通过包含taos的动态库，去链接taosd服务，所以需要做好配置文件。缺省值路径是 "/etc/taos"

-h：连接taosd服务的FQDN。缺省值为localhost。

-P：连接taosd服务的端口号。缺省值为6030。

-u：连接taosd服务的用户名。缺省值为root。

-p：连接taosd服务的密码。缺省值为taosdata。

-I：taosBenchmark插入数据的方式，可选项为taosc, rest, stmt, sml。缺省值为taosc。

-o：指定执行结果输出文件的路径。缺省值为./output.txt。

-T：指定插入数据时的并发线程数，不影响插入的数据总量，缺省值为8。

-i：指定每次请求插入的时间间隔，单位为毫秒(ms)，缺省值为0。

-S：指定每张子表的每条记录插入时间戳步长，单位为毫秒，缺省值为1。

-B：子表交错写入的行数，若为0，则按子表顺序依次写入，缺省值为0。

-r：每条插入请求包含的记录条数，等同于批次数，缺省值为30000。

-t：子表的个数，缺省值为10000。

-n：每张子表的插入记录数。缺省值为10000。

-d：指定插入数据的数据库名称，缺省值为test。

-l：指定子表普通列的个数，不包含时间戳列，全为INT类型。与-b无法同时使用。

-A：指定子表标签列的类型，逗号分隔，NCHAR和BINARY类型可以指定长度，类如INT,NCHAR\(18\)，缺省值为INT,BINARY。

-b：指定子表普通列的类型，无法和-l同时使用，逗号分隔，NCHAR和BINARY类型可以指定长度，类如INT,NCHAR\(18\)，缺省值为FLOAT,INT,FLOAT。

-w：指定默认NCHAR和BINARY类型的长度，优先级低于-A/-b，缺省值为64。

-m：指定子表名的前缀，缺省值为d。

-E：指定超级表与子表名是否使用转义符包含，可选项，默认不配置。

-C：指定NCHAR和BINARY的数据是否为unicode基础中文集，可选项，默认不配置。

-N：指定是否不建超级表，即全为子表，可选项，默认不配置。

-M：指定插入数据是否为随机数据源，若其他改变表结构的参数配置，该参数自动生效，可选项，缺省值不配置。

-x：插入数据完成后是否进行部分聚合函数查询操作，可选项，默认不配置。

-y：指定打印完配置信息后是否需要回车键来继续，可选项，默认为需要。

-O：插入数据时间戳乱序的比例，缺省值为0。

-R：若插入数据时间戳乱序比例不为0时，规定乱序的时间戳范围，单位为毫秒(ms)。缺省值为1000。

-a：指定创建数据库时副本的个数，缺省值为1。

-V：打印taosBenchmark的版本信息。

--help：打印命令行参数介绍。

```

### taosBenchmark json配置文件：

#### 写入性能测试json配置文件：

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
        "use_sameple_ts": "no",
        "tags_file": "",
        "columns": [{"type": "INT"}, {"type": "DOUBLE", "count":10}, {"type": "BINARY", "len": 16, "count":3}, {"type": "BINARY", "len": 32, "count":6}],
        "tags": [{"type": "TINYINT", "count":2}, {"type": "BINARY", "len": 16, "count":5}]
        }]
      }]
}
```

#### 参数说明：

```

"filetype": taosBenchmark实例进行哪种功能测试。"insert"表示数据插入功能。必选项。

"cfgdir": 配置文件taos.cfg所在的路径。因为taosBenchmark通过包含taos的动态库，去链接taosd服务，所以需要做好配置文件。可选项，缺省是 "/etc/taos"路径。

"host": taosd服务的FQDN。可选项，缺省是"localhost"。

"port": taosd服务的端口号。可选项，缺省是6030。

"user": 用户名。可选项，缺省是"root"。

"password": 密码。可选项，缺省是"taosdata"。

"thread_count": 插入数据和建表时的并发线程数。可选项，缺省是8。

"result_file": 测试完成后结果保存文件。可选项，缺省是本实例启动目录下的"./output.txt"。

"confirm_parameter_prompt": 执行过程中提示是否确认，为no时，执行过程无需手工输入enter。可选项，缺省是no。

"insert_interval": 两次发送请求的间隔时间。可选项，缺省是0，代表无人工设置的时间间隔，单位为ms。优先级低于超级表的insert_interval配置。

"interlace_rows": 设置轮询插入每个单表数据的条目数，可选项，缺省是0。

"num_of_records_per_req": 每条请求数据内容包含的插入数据记录数目，可选项，缺省值为30000。

"prepared_rand": 随机生成的数据的个数，取值范围为大于1的正整数，缺省值为10000。调小可以控制taosBenchmark占用内存。

"chinese": 随机生成的nchar或者binary的内容是否为中文，可选项"yes" 和 "no", 默认为"no"

"databases": [{

"dbinfo": { 

"name": 数据库名称。必选项。

"drop": 如果数据库已经存在，"yes"：删除后重建；"no"：不删除，直接使用。可选项，缺省是"yes"。

"replica": 副本个数，可选项。1 - 3，缺省是1。

"days": 数据文件存储数据的时间跨度，可选项。缺省是10天。

"cache": 内存块的大小，单位是MB，可选项。缺省是16MB。

"blocks": 每个VNODE（TSDB）中有多少cache大小的内存块，可选项。缺省是6块。

"precision": 数据库时间精度，可选项。"ms"：毫秒， “us”：微秒。"ns": 纳秒。缺省是"ms"。

"keep": 数据保留的天数，可选项。缺省是3650天。

"minRows": 文件块中记录的最小条数，可选项。缺省是100。

"maxRows": 文件块中记录的最大条数，可选项。缺省是4096.

"comp": 文件压缩标志位，可选项。0：关闭，1:一阶段压缩，2:两阶段压缩。缺省是2。

"walLevel": WAL级别，可选项。1：写wal, 但不执行fsync; 2：写wal, 而且执行fsync。缺省是1。

"cachelast": 允许在内存中保留每张表的最后一条记录。1表示允许。

"quorum": 异步写入成功所需应答之法定数，1-3，可选项。缺省是1。

"fsync": 当wal设置为2时，执行fsync的周期，单位是毫秒，最小为0，表示每次写入，立即执行fsync. 最大为180000，可选项。缺省是3000。

"update": 支持数据更新，0：否；1：是。可选项。缺省是0。 },

"super_tables": [{

"name": 超级表名称，必选项。

"child_table_exists": 子表是否已经存在，"yes"：是；"no"：否。指定"是"后，不再建子表，后面相关子表的参数就无效了。可选项，缺省是"no"。database 设置 drop = yes 时，无论配置文件内容，此参数将自动置为 no。

"childtable_count": 建立子表个数 。该值需要大于0。当child_table_exists为"no"时，必选项，否则就是无效项。

"childtable_prefix": 子表名称前缀。当child_table_exists为"no"时，必选项，否则就是无效项。确保数据库中表名没有重复。

"escape_character": 子表名是否包含转义字符。"yes": 包含; "no": 不包含。可选项，缺省是"no"。

"batch_create_tbl_num": 一个sql批量创建子表的数目。

"data_source": 插入数据来源，"rand"：实例随机生成；"sample"：从样例文件中读取，近当insert_mode为taosc和rest时有效。可选项。缺省是"rand"。

"insert_mode": 插入数据接口，"taosc"：调用TDengine的c接口；"rest"：使用restful接口；"stmt"：使用 stmt （参数绑定）接口; "sml": 使用schemaless 。可选项。缺省是“taosc”。

"line_protocol": 只有在insert_mode为sml时生效，可选项为"line", "telnet", "json", 默认为"line"。

"insert_rows": 插入记录数，0：不插入数据，只建表；>0：每个子表插入记录数，完成后实例退出。可选项，缺省是0。

"childtable_offset": 插入数据时，子表起始值。只在drop=no && child_table_exists= yes，该字段生效。

"childtable_limit": 插入数据时，子表从offset开始，偏移的表数目。使用者可以运行多个 taosBenchmark 实例（甚至可以在不同的机器上）通过使用不同的 childtable_offset 和 childtable_limit 配置值来实现同时写入相同数据库相同超级表下多个子表。只在drop=no && child_table_exists= yes，该字段生效。

"interlace_rows": 跟上面的配置一致，不过该处的配置优先，每个stable可以有自己单独的配置。最大不超过 num_of_records_per_req。

"insert_interval": 两次发送请求的间隔时间。可选项，缺省是0，代表无人工设置的时间间隔，单位为ms。

"disorder_ratio": 插入数据时的乱序百分比，可选项，缺省是0。

"disorder_range": 乱序百分比不为0时，乱序时间戳范围，单位：ms。可选项，缺省是1000，即1秒或1000毫秒。

"timestamp_step": 每个子表中记录时间戳的步长，单位与数据库的precision匹配。可选项，缺省是1。

"start_timestamp": 子表中记录时间戳的起始值，支持"2020-10-01 00:00:00.000"和“now”两种格式，可选项，缺省是“now”。

"sample_format": 当插入数据源选择“sample”时，sample文件的格式，"csv"：csv格式，每列的值与子表的columns保持一致，但不包含第1列的时间戳。可选项，缺省是”csv”。目前仅仅支持csv格式的sample文件。

"sample_file": sample文件，包含路径和文件名。当插入数据源选择“sample”时，该项为必选项。

"use_sample_ts": sample文件是否包含第一列时间戳，可选项: "yes" 和 "no", 若为"yes"，则插入数据量为sample文件内数据量，默认 "no"。

"tags_file": 子表tags值文件，只能是csv文件格式，且必须与超级表的tags保持一致。当该项为非空时，表示子表的tags值从文件中获取；为空时，实例随机生成。可选项，缺省是空。

"columns": [{

超级表的column列表，最大支持4096列（指所有普通列+超级列总和）。默认的第一列为时间类型，程序自动添加，不需要手工添加。

"type": 该列的数据类型 ，必选项。

"len": 该列的长度，只有type是BINARY或NCHAR时有效，可选项，缺省值是8。

"count": 该类型的连续列个数，可选项，缺省是1。

}],

"tags": [{

超级表的tags列表，type不能是timestamp类型， 最大支持128个。

"type": 该列的数据类型 ，必选项。

"len": 该列的长度，只有type是BINARY或NCHAR时有效，可选项，缺省值是8。

"count": 该类型的连续列个数，可选项，缺省是1。

}]
```
**注意：当tag的type为json时，count为json tag内的key数量，len为json tag内value string的长度**


#### 查询性能测试json配置文件：

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
#### 参数说明：
```

"filetype": 本taosBenchmark实例进行哪种功能测试。"query"表示数据查询功能。必选项。

"cfgdir": 配置文件taos.cfg所在的路径。因为taosBenchmark通过包含taos的动态库，去链接taosd服务，所以需要做好配置文件。可选项，缺省是 "/etc/taos"路径。

"host": taosd服务的FQDN。可选项，缺省是“localhost“。

"port": taosd服务的端口号。可选项，缺省是6030。

"user": 用户名。可选项，缺省是“root“。

"password": 密码。可选项，缺省是“taosdata"。

"confirm_parameter_prompt": 执行过程中提示是否确认，为no时，执行过程无需手工输入enter。可选项，缺省是no。

"databases": 数据库名称。必选项。

"query_times": 每种查询类型的查询次数

"query_mode": 查询数据接口，"taosc"：调用TDengine的c接口；“resetful”：使用restfule接口。可选项。缺省是“taosc”。

"specified_table_query": { 指定表的查询

"query_interval": 执行sqls的间隔，单位是秒。可选项，缺省是0。

"concurrent": 并发执行sqls的线程数，可选项，缺省是1。每个线程都执行所有的sqls。

"sqls": 可以添加多个sql语句，最多支持100条。

"sql": 查询语句。必选项。

"result": 查询结果写入的文件名。可选项，缺省是空，表示查询结果不写入文件。

"super_table_query": 对超级表中所有子表的查询

"stblname": 超级表名称。必选项。

"query_interval": 执行sqls的间隔，单位是秒。可选项，缺省是0。

"threads": 并发执行sqls的线程数，可选项，缺省是1。每个线程负责一部分子表，执行所有的sqls。

"sql": "select count(*) from xxxx"。查询超级表内所有子表的查询语句，其中表名必须写成 “xxxx”，实例会自动替换成子表名。

"result": 查询结果写入的文件名。可选项，缺省是空，表示查询结果不写入文件。

```
**注意：每条sql语句后的保存结果的文件不能重名，且生成结果文件时，文件名会附加线程号。**

**查询结果显示：如果查询线程结束一次查询距开始执行时间超过30秒打印一次查询次数、用时和QPS。所有查询结束时，汇总打印总的查询次数和QPS。**


#### 订阅性能测试json文件配置：

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
      "mode":"sync", 
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
        "mode":"sync", 
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
#### 参数说明：
```

"filetype": 本taosBenchmark实例进行哪种功能测试。"subscribe"表示数据查询功能。必选项。

"cfgdir": 配置文件taos.cfg所在的路径。因为taosBenchmark通过包含taos的动态库，去链接taosd服务，所以需要做好配置文件。可选项，缺省是 "/etc/taos"路径。

"host": taosd服务的FQDN。可选项，缺省是“localhost“。

"port": taosd服务的端口号。可选项，缺省是6030。

"user": 用户名。可选项，缺省是“root“。

"password": 密码。可选项，缺省是“taosdata"。

"databases": 数据库名称。必选项。**

"confirm_parameter_prompt": 执行过程中提示是否确认，为no时，执行过程无需手工输入enter。可选项，缺省是no。

"specified_table_query": 指定表的订阅。

"concurrent": 并发执行sqls的线程数，可选项，缺省是1。每个线程都执行所有的sqls。

"mode": 订阅模式。目前支持同步和异步订阅，缺省是sync。

"interval": 执行订阅的间隔，单位是秒。可选项，缺省是0。

"restart": 订阅重启。"yes"：如果订阅已经存在，重新开始，"no": 继续之前的订阅。(请注意执行用户需要对 dataDir 目录有读写权限)

"keepProgress": 保留订阅信息进度。yes表示保留订阅信息，no表示不保留。该值为yes，restart为no时，才能继续之前的订阅。

"resubAfterConsume": 配合 keepProgress 使用，在订阅消费了相应次数后调用 unsubscribe 取消订阅并再次订阅。

"sql": 查询语句。必选项。

"result": 查询结果写入的文件名。可选项，缺省是空，表示查询结果不写入文件。

"super_table_query": 对超级表中所有子表的订阅。

"stblname": 超级表名称。必选项。

"threads": 并发执行sqls的线程数，可选项，缺省是1。每个线程都执行所有的sqls。

"mode": 订阅模式。

"interval": 执行sqls的间隔，单位是秒。可选项，缺省是0。

"restart": 订阅重启。"yes"：如果订阅已经存在，重新开始，"no": 继续之前的订阅。

"keepProgress": 保留订阅信息进度。yes表示保留订阅信息，no表示不保留。该值为yes，restart为no时，才能继续之前的订阅。

"resubAfterConsume": 配合 keepProgress 使用，在订阅消费了相应次数后调用 unsubscribe 取消订阅并再次订阅。

"sql": " select count(*) from xxxx "。查询语句，其中表名必须写成 “xxxx”，实例会自动替换成子表名。

 "result": 查询结果写入的文件名。可选项，缺省是空，表示查询结果不写入文件。 注意：每条sql语句后的保存结果的文件不能重名，且生成结果文件时，文件名会附加线程号。
```
