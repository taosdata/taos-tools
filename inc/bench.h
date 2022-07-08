/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __DEMO__
#define __DEMO__

#define _GNU_SOURCE
#define CURL_STATICLIB
#define ALLOW_FORBID_FUNC

#ifdef LINUX
#include <argp.h>
#include <inttypes.h>
#ifndef _ALPINE
#include <error.h>
#endif
#include <semaphore.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>
#include <netdb.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <syscall.h>
#include <unistd.h>
#include <wordexp.h>
#include <sys/ioctl.h>
#include <signal.h>

#elif DARWIN
#include <argp.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <netdb.h>

#elif defined(WIN32) || defined(WIN64)

#include "os.h"

#endif


#include <regex.h>
#include <stdio.h>
#include <assert.h>
#include <toolscJson.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


// temporary flag for 3.0 development TODO need to remove in future
#define ALLOW_FORBID_FUNC

#include "taos.h"
#include "toolsdef.h"

#if defined(WIN32) || defined(WIN64)
#define strcasecmp _stricmp
#define strncasecmp _strnicmp
#endif

#ifndef TSDB_DATA_TYPE_VARCHAR
#define TSDB_DATA_TYPE_VARCHAR 8
#endif

#ifndef TSDB_DATA_TYPE_VARBINARY
#define TSDB_DATA_TYPE_VARBINARY 16
#endif

#ifndef TSDB_DATA_TYPE_DECIMAL
#define TSDB_DATA_TYPE_DECIMAL 17
#endif

#ifndef TSDB_DATA_TYPE_MEDIUMBLOB
#define TSDB_DATA_TYPE_MEDIUMBLOB 19
#endif

#ifndef TSDB_DATA_TYPE_MAX
#define TSDB_DATA_TYPE_MAX 20
#endif

#define REQ_EXTRA_BUF_LEN 1024
#define RESP_BUF_LEN      4096
#define SQL_BUFF_LEN      1024

#define STR_INSERT_INTO "INSERT INTO "

#define HEAD_BUFF_LEN \
    TSDB_MAX_COLUMNS * 24  // 16*MAX_COLUMNS + (192+32)*2 + insert into ..

#define BUFFER_SIZE       TSDB_MAX_ALLOWED_SQL_LEN
#define FETCH_BUFFER_SIZE 100 * TSDB_MAX_ALLOWED_SQL_LEN
#define COND_BUF_LEN      (BUFFER_SIZE - 30)

#define OPT_ABORT         1    /* –abort */
#define MAX_RECORDS_PER_REQ 65536
#define MAX_FILE_NAME_LEN 256  // max file name length on linux is 255.
#define MAX_PATH_LEN      4096
#define DEFAULT_START_TIME  1500000000000
#define MAX_SQL_LEN         1048576
#define TELNET_TCP_PORT     6046
#define INT_BUFF_LEN        12
#define BIGINT_BUFF_LEN     21
#define SMALLINT_BUFF_LEN   8
#define TINYINT_BUFF_LEN    6
#define BOOL_BUFF_LEN       6
#define FLOAT_BUFF_LEN      22
#define DOUBLE_BUFF_LEN     42
#define JSON_BUFF_LEN       20
#define TIMESTAMP_BUFF_LEN  21
#define PRINT_STAT_INTERVAL 30 * 1000

#define MAX_QUERY_SQL_COUNT 100

#define MAX_JSON_BUFF 6400000

#define INPUT_BUF_LEN     256
#define EXTRA_SQL_LEN     256
#define SMALL_BUFF_LEN    8
#define DATATYPE_BUFF_LEN (SMALL_BUFF_LEN * 3)
#define SML_MAX_BATCH          65536 * 32
#define DEFAULT_NTHREADS       8
#define DEFAULT_CHILDTABLES    10000
#define DEFAULT_PORT           6030
#define DEFAULT_DATABASE       "test"
#define DEFAULT_TB_PREFIX      "d"
#define DEFAULT_OUTPUT         "./output.txt"
#define DEFAULT_BINWIDTH       64
#define DEFAULT_PREPARED_RAND  10000
#define DEFAULT_REQ_PER_REQ    30000
#define DEFAULT_INSERT_ROWS    10000
#define DEFAULT_DISORDER_RANGE 1000
#define DEFAULT_CREATE_BATCH   10
#define DEFAULT_SUB_INTERVAL   10000
#define DEFAULT_QUERY_INTERVAL 10000
#define BARRAY_MIN_SIZE 8
#define SML_LINE_SQL_SYNTAX_OFFSET 7

#if _MSC_VER <= 1900
#define __func__ __FUNCTION__
#endif

#if defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif

#define debugPrint(fp, fmt, ...)                                             \
    do {                                                                     \
        if (g_arguments->debug_print) {                                      \
            struct tm      Tm, *ptm;                                         \
            struct timeval timeSecs;                                         \
            time_t         curTime;                                          \
            toolsGetTimeOfDay(&timeSecs);                                    \
            curTime = timeSecs.tv_sec;                                       \
            ptm = toolsLocalTime(&curTime, &Tm);                                \
            fprintf(fp, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,    \
                    (int32_t)timeSecs.tv_usec);                              \
            fprintf(fp, "DEBG: ");                                           \
            fprintf(fp, "%s(%d) ", __FILE__, __LINE__);                      \
            fprintf(fp, "" fmt, __VA_ARGS__);                                \
        }                                                                    \
    } while (0)

#define infoPrint(fp, fmt, ...)                                          \
    do {                                                                 \
        struct tm      Tm, *ptm;                                         \
        struct timeval timeSecs;                                         \
        time_t         curTime;                                          \
        toolsGetTimeOfDay(&timeSecs);                                    \
        curTime = timeSecs.tv_sec;                                       \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        fprintf(fp, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,    \
                (int32_t)timeSecs.tv_usec);                              \
        fprintf(fp, "INFO: " fmt, __VA_ARGS__);                          \
    } while (0)

#define performancePrint(fp, fmt, ...)                                       \
    do {                                                                     \
        if (g_arguments->performance_print) {                                \
            struct tm      Tm, *ptm;                                         \
            struct timeval timeSecs;                                         \
            time_t         curTime;                                          \
            toolsGetTimeOfDay(&timeSecs);                                    \
            curTime = timeSecs.tv_sec;                                       \
            ptm = toolsLocalTime(&curTime, &Tm);                                \
            fprintf(fp, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,    \
                    (int32_t)timeSecs.tv_usec);                              \
            fprintf(fp, "PERF: " fmt, __VA_ARGS__);                          \
        }                                                                    \
    } while (0)

#define errorPrint(fp, fmt, ...)                                         \
    do {                                                                 \
        struct tm      Tm, *ptm;                                         \
        struct timeval timeSecs;                                         \
        time_t         curTime;                                          \
        toolsGetTimeOfDay(&timeSecs);                                    \
        curTime = timeSecs.tv_sec;                                       \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        fprintf(fp, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,    \
                (int32_t)timeSecs.tv_usec);                              \
        fprintf(fp, "\033[31m");                                         \
        fprintf(fp, "ERROR: ");                                          \
        if (g_arguments->debug_print) {                                  \
            fprintf(fp, "%s(%d) ", __FILE__, __LINE__);                  \
        }                                                                \
        fprintf(fp, "" fmt, __VA_ARGS__);                                \
        fprintf(fp, "\033[0m");                                          \
    } while (0)

enum TEST_MODE {
    INSERT_TEST,     // 0
    QUERY_TEST,      // 1
    SUBSCRIBE_TEST,  // 2
};

enum enumSYNC_MODE { SYNC_MODE, ASYNC_MODE, MODE_BUT };

enum enum_TAOS_INTERFACE {
    TAOSC_IFACE,
    REST_IFACE,
    STMT_IFACE,
    SML_IFACE,
    SML_REST_IFACE,
    INTERFACE_BUT
};

typedef enum enumQUERY_CLASS {
    SPECIFIED_CLASS,
    STABLE_CLASS,
    CLASS_BUT
} QUERY_CLASS;

typedef enum enumQUERY_TYPE {
    NO_INSERT_TYPE,
    INSERT_TYPE,
    QUERY_TYPE_BUT
} QUERY_TYPE;

enum _show_db_index {
    TSDB_SHOW_DB_NAME_INDEX,
    TSDB_SHOW_DB_CREATED_TIME_INDEX,
    TSDB_SHOW_DB_NTABLES_INDEX,
    TSDB_SHOW_DB_VGROUPS_INDEX,
    TSDB_SHOW_DB_REPLICA_INDEX,
    TSDB_SHOW_DB_QUORUM_INDEX,
    TSDB_SHOW_DB_DAYS_INDEX,
    TSDB_SHOW_DB_KEEP_INDEX,
    TSDB_SHOW_DB_CACHE_INDEX,
    TSDB_SHOW_DB_BLOCKS_INDEX,
    TSDB_SHOW_DB_MINROWS_INDEX,
    TSDB_SHOW_DB_MAXROWS_INDEX,
    TSDB_SHOW_DB_WALLEVEL_INDEX,
    TSDB_SHOW_DB_FSYNC_INDEX,
    TSDB_SHOW_DB_COMP_INDEX,
    TSDB_SHOW_DB_CACHELAST_INDEX,
    TSDB_SHOW_DB_PRECISION_INDEX,
    TSDB_SHOW_DB_UPDATE_INDEX,
    TSDB_SHOW_DB_STATUS_INDEX,
    TSDB_MAX_SHOW_DB
};

// -----------------------------------------SHOW TABLES CONFIGURE
// -------------------------------------

enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

typedef struct BArray {
    size_t   size;
    uint32_t capacity;
    uint32_t elemSize;
    void*    pData;
} BArray;

typedef struct TAOS_POOL_S {
    int    size;
    int    current;
    TAOS **taos_list;
} TAOS_POOL;

typedef struct SField {
    uint8_t  type;
    char     name[TSDB_COL_NAME_LEN + 1];
    uint32_t length;
    bool     none;
    bool     null;
    void *   data;
    int64_t  max;
    int64_t  min;
    tools_cJSON *  values;
    bool     sma;
} Field;

typedef struct SSuperTable_S {
    char *   stbName;
    bool     random_data_source;  // rand_gen or sample
    bool     escape_character;
    bool     use_metric;
    char *   childTblPrefix;
    bool     childTblExists;
    uint64_t childTblCount;
    uint64_t batchCreateTableNum;  // 0: no batch,  > 0: batch table number in
                                   // one sql
    bool     autoCreateTable;
    uint16_t iface;  // 0: taosc, 1: rest, 2: stmt
    uint16_t lineProtocol;
    uint64_t childTblLimit;
    uint64_t childTblOffset;

    //  int          multiThreadWriteOneTbl;  // 0: no, 1: yes
    uint32_t interlaceRows;  //
    int      disorderRatio;  // 0: no disorder, >0: x%
    int      disorderRange;  // ms, us or ns. according to database precision

    uint64_t insert_interval;
    uint64_t insertRows;
    uint64_t timestamp_step;
    int64_t  startTimestamp;
    char     sampleFile[MAX_FILE_NAME_LEN];
    char     tagsFile[MAX_FILE_NAME_LEN];
    uint32_t partialColumnNum;
    char *   partialColumnNameBuf;
    BArray * cols;
    BArray * tags;
    char **  childTblName;
    char *   colsOfCreateChildTable;
    uint32_t lenOfTags;
    uint32_t lenOfCols;

    char *sampleDataBuf;
    bool  useSampleTs;
    char *tagDataBuf;
    bool  tcpTransfer;
    bool  non_stop;
    char *comment;
    int   delay;
    int   file_factor;
    char *rollup;
    bool no_check_for_affected_rows;
} SSuperTable;

typedef struct SDbCfg_S {
    int32_t minRows;  // 0 means default
    int32_t maxRows;  // 0 means default
    int     comp;
    int     walLevel;
    int     cacheLast;
    int     fsync;
    int     replica;
    int     update;
    int     buffer;
    int     keep;
    int     days;
    int     cache;
    int     blocks;
    int     quorum;
    int     strict;
    int     precision;
    int     sml_precision;
    int     page_size;
    int     pages;
    int     vgroups;
    int     single_stable;
    char *  retentions;

} SDbCfg;

typedef struct SSTREAM_S {
    char stream_name[TSDB_TABLE_NAME_LEN];
    char stream_stb[TSDB_TABLE_NAME_LEN];
    char trigger_mode[BIGINT_BUFF_LEN];
    char watermark[BIGINT_BUFF_LEN];
    char source_sql[TSDB_MAX_SQL_LEN];
    bool drop;
} SSTREAM;

typedef struct SDataBase_S {
    char *       dbName;
    bool         drop;  // 0: use exists, 1: if exists, drop then new create
    SDbCfg       dbCfg;
    BArray*      superTbls;
    BArray*      streams;
} SDataBase;

typedef struct SSQL_S {
    char *command;
    char result[MAX_FILE_NAME_LEN];
    int64_t* delay_list;
} SSQL;

typedef struct SpecifiedQueryInfo_S {
    uint64_t  queryInterval;  // 0: unlimited  > 0   loop/s
    uint32_t  concurrent;
    uint32_t  asyncMode;          // 0: sync, 1: async
    uint64_t  subscribeInterval;  // ms
    uint64_t  queryTimes;
    bool      subscribeRestart;
    int       subscribeKeepProgress;
    BArray*   sqls;
    int       resubAfterConsume[MAX_QUERY_SQL_COUNT];
    int       endAfterConsume[MAX_QUERY_SQL_COUNT];
    TAOS_SUB *tsub[MAX_QUERY_SQL_COUNT];
    char      topic[MAX_QUERY_SQL_COUNT][32];
    int       consumed[MAX_QUERY_SQL_COUNT];
    TAOS_RES *res[MAX_QUERY_SQL_COUNT];
    uint64_t  totalQueried;
    bool      mixed_query;
} SpecifiedQueryInfo;

typedef struct SuperQueryInfo_S {
    char      stbName[TSDB_TABLE_NAME_LEN];
    uint64_t  queryInterval;  // 0: unlimited  > 0   loop/s
    uint32_t  threadCnt;
    uint32_t  asyncMode;          // 0: sync, 1: async
    uint64_t  subscribeInterval;  // ms
    bool      subscribeRestart;
    int       subscribeKeepProgress;
    uint64_t  queryTimes;
    uint64_t  childTblCount;
    int       sqlCount;
    char      sql[MAX_QUERY_SQL_COUNT][BUFFER_SIZE + 1];
    char      result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
    int       resubAfterConsume;
    int       endAfterConsume;
    TAOS_SUB *tsub[MAX_QUERY_SQL_COUNT];
    char **   childTblName;
    uint64_t  totalQueried;
} SuperQueryInfo;

typedef struct SQueryMetaInfo_S {
    SpecifiedQueryInfo specifiedQueryInfo;
    SuperQueryInfo     superQueryInfo;
    uint64_t           totalQueried;
    uint64_t           query_times;
    uint64_t           response_buffer;
    bool               reset_query_cache;
    uint16_t           iface;
    char*              dbName;
} SQueryMetaInfo;

typedef struct SArguments_S {
    uint8_t            taosc_version;
    char *             metaFile;
    int32_t            test_mode;
    char *             host;
    uint16_t           port;
    uint16_t           telnet_tcp_port;
    char *             user;
    char *             password;
    bool               answer_yes;
    bool               debug_print;
    bool               performance_print;
    bool               chinese;
    char *             output_file;
    uint32_t           binwidth;
    uint32_t           intColumnCount;
    uint32_t           connection_pool;
    uint32_t           nthreads;
    uint32_t           table_threads;
    uint64_t           prepared_rand;
    uint32_t           reqPerReq;
    uint64_t           insert_interval;
    bool               demo_mode;
    bool               aggr_func;
    struct sockaddr_in serv_addr;
    TAOS_POOL *        pool;
    uint64_t           g_totalChildTables;
    uint64_t           g_actualChildTables;
    uint64_t           g_autoCreatedChildTables;
    uint64_t           g_existedChildTables;
    FILE *             fpOfInsertResult;
    BArray *           databases;
    char *             base64_buf;
#ifdef LINUX
    sem_t              cancelSem;
#endif
    bool               terminate;
} SArguments;

typedef struct SThreadInfo_S {
    TAOS *     taos;
    TAOS_STMT *stmt;
    uint64_t * bind_ts;
    uint64_t * bind_ts_array;
    char *     bindParams;
    char *     is_null;
    uint32_t   threadID;
    uint64_t   start_table_from;
    uint64_t   end_table_to;
    uint64_t   ntables;
    uint64_t   tables_created;
    char *     buffer;
    uint64_t   counter;
    uint64_t   st;
    uint64_t   et;
    uint64_t   samplePos;
    uint64_t   totalInsertRows;
    uint64_t   totalQueried;
    uint64_t   totalAffectedRows;
    int64_t   totalDelay;
    uint64_t   querySeq;
    TAOS_SUB * tsub;
    char **    lines;
    int32_t    sockfd;
    uint32_t   db_index;
    uint32_t   stb_index;
    char **    sml_tags;
    tools_cJSON *    json_array;
    tools_cJSON *    sml_json_tags;
    uint64_t   start_time;
    uint64_t   max_sql_len;
    FILE *     fp;
    char       filePath[MAX_PATH_LEN];
    BArray*    delayList;
    uint64_t*  query_delay_list;
    double     avg_delay;
} threadInfo;

typedef struct SQueryThreadInfo_S {
    int start_sql;
    int end_sql;
    int threadId;
    BArray*  query_delay_list;
    int   sockfd;
    TAOS* taos;
    int64_t total_delay;
} queryThreadInfo;

typedef void (*FSignalHandler)(int signum, void *sigInfo, void *context);

/* ************ Global variables ************  */
extern char *         g_aggreFuncDemo[];
extern char *         g_aggreFunc[];
extern SArguments *   g_arguments;
extern SQueryMetaInfo g_queryInfo;
extern bool           g_fail;
extern char           configDir[];
extern tools_cJSON *  root;
extern uint64_t       g_memoryUsage;

#define min(a, b) (((a) < (b)) ? (a) : (b))
#define tstrncpy(dst, src, size)       \
    do {                               \
        strncpy((dst), (src), (size)); \
        (dst)[(size)-1] = 0;           \
    } while (0)
#define BARRAY_GET_ELEM(array, index) ((void*)((char*)((array)->pData) + (index) * (array)->elemSize))
/* ************ Function declares ************  */
/* benchCommandOpt.c */
void commandLineParseArgument(int argc, char *argv[]);
void modify_argument();
void init_argument();
void queryAggrFunc();
void parse_field_datatype(char *dataType, BArray *fields, bool isTag);
/* demoJsonOpt.c */
int getInfoFromJsonFile();
/* demoUtil.c */
int     compare(const void *a, const void *b);
void    encode_base_64();
int     init_taos_list();
TAOS *  select_one_from_pool(char *db_name);
void    cleanup_taos_list();
int64_t toolsGetTimestampMs();
int64_t toolsGetTimestampUs();
int64_t toolsGetTimestampNs();
int64_t toolsGetTimestamp(int32_t precision);
void    toolsMsleep(int32_t mseconds);
void    replaceChildTblName(char *inSql, char *outSql, int tblIndex);
void    setupForAnsiEscape(void);
void    resetAfterAnsiEscape(void);
char *  taos_convert_datatype_to_string(int type);
int     taos_convert_string_to_datatype(char *type, int length);
int     taosRandom();
void    tmfree(void *buf);
void    tmfclose(FILE *fp);
void    fetchResult(TAOS_RES *res, threadInfo *pThreadInfo);
void    prompt(bool NonStopMode);
void    ERROR_EXIT(const char *msg);
int     postProceSql(char *sqlstr, char* dbName, int precision, int iface, int protocol, bool tcp, int sockfd, char* filePath);
int     queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet, bool check);
int     regexMatch(const char *s, const char *reg, int cflags);
int     convertHostToServAddr(char *host, uint16_t port,
                              struct sockaddr_in *serv_addr);
int     getAllChildNameOfSuperTable(TAOS *taos, char *dbName, char *stbName,
                                    char ** childTblNameOfSuperTbl,
                                    int64_t childTblCountOfSuperTbl);
void*   benchCalloc(size_t nmemb, size_t size, bool record);
BArray* benchArrayInit(size_t size, size_t elemSize);
void* benchArrayPush(BArray* pArray, void* pData);
void* benchArrayDestroy(BArray* pArray);
void benchArrayClear(BArray* pArray);
void* benchArrayGet(const BArray* pArray, size_t index);
void* benchArrayAddBatch(BArray* pArray, void* pData, int32_t nEles);
#ifdef LINUX
int32_t bsem_wait(sem_t* sem);
void benchSetSignal(int32_t signum, FSignalHandler sigfp);
#endif
int taos_convert_type_to_length(uint8_t type);
int64_t taos_convert_datatype_to_default_max(uint8_t type);
int64_t taos_convert_datatype_to_default_min(uint8_t type);
/* demoInsert.c */
int  insertTestProcess();
void postFreeResource();
/* demoQuery.c */
int queryTestProcess();
/* demoSubscribe.c */
int subscribeTestProcess();
#endif
