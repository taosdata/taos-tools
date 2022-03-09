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

#ifdef LINUX
#include <argp.h>
#include <inttypes.h>
#ifndef _ALPINE
#include <error.h>
#endif
#include <pthread.h>
#include <regex.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>
#else
#include <regex.h>
#include <stdio.h>
#endif

#include <argp.h>
#include <assert.h>
#include <cJSONDEMO.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <syscall.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>

// temporary flag for 3.0 development TODO need to remove in future
#define ALLOW_FORBID_FUNC

#include "taos.h"
#include "taoserror.h"
#include "toolsdef.h"

#define REQ_EXTRA_BUF_LEN 1024
#define RESP_BUF_LEN 4096
#define SQL_BUFF_LEN 1024

#define STR_INSERT_INTO "INSERT INTO "

#define MAX_RECORDS_PER_REQ 32766

#define HEAD_BUFF_LEN \
    TSDB_MAX_COLUMNS * 24  // 16*MAX_COLUMNS + (192+32)*2 + insert into ..

#define BUFFER_SIZE TSDB_MAX_ALLOWED_SQL_LEN
#define FETCH_BUFFER_SIZE 100 * TSDB_MAX_ALLOWED_SQL_LEN
#define COND_BUF_LEN (BUFFER_SIZE - 30)
#define COL_BUFFER_LEN ((TSDB_COL_NAME_LEN + 15) * TSDB_MAX_COLUMNS)

#define OPT_ABORT 1            /* –abort */
#define MAX_FILE_NAME_LEN 256  // max file name length on linux is 255.
#define MAX_PATH_LEN 4096

#define DEFAULT_START_TIME 1500000000000
#define TELNET_TCP_PORT 6046
#define INT_BUFF_LEN 12
#define BIGINT_BUFF_LEN 21
#define SMALLINT_BUFF_LEN 8
#define TINYINT_BUFF_LEN 6
#define BOOL_BUFF_LEN 6
#define FLOAT_BUFF_LEN 22
#define DOUBLE_BUFF_LEN 42
#define JSON_BUFF_LEN 20
#define TIMESTAMP_BUFF_LEN 21
#define PRINT_STAT_INTERVAL 30 * 1000

#define MAX_DB_COUNT 8
#define MAX_SUPER_TABLE_COUNT 200

#define MAX_QUERY_SQL_COUNT 100

#define MAX_JSON_BUFF 6400000

#define INPUT_BUF_LEN 256
#define EXTRA_SQL_LEN 256
#define SMALL_BUFF_LEN 8
#define DATATYPE_BUFF_LEN (SMALL_BUFF_LEN * 3)

#define DEFAULT_NTHREADS 8
#define DEFAULT_CHILDTABLES 10000
#define DEFAULT_HOST "localhost"
#define DEFAULT_PORT 6030
#define DEFAULT_DATABASE "test"
#define DEFAULT_TB_PREFIX "d"
#define DEFAULT_OUTPUT "./output.txt"
#define DEFAULT_BINWIDTH 64
#define DEFAULT_PREPARED_RAND 10000
#define DEFAULT_REQ_PER_REQ 30000
#define DEFAULT_INSERT_ROWS 10000
#define DEFAULT_DISORDER_RANGE 1000
#define DEFAULT_CREATE_BATCH 10
#define DEFAULT_SUB_INTERVAL 10000
#define DEFAULT_QUERY_INTERVAL 10000

#define SML_LINE_SQL_SYNTAX_OFFSET 7

#if _MSC_VER <= 1900
#define __func__ __FUNCTION__
#endif

#define debugPrint(fmt, ...)                                                  \
    do {                                                                      \
        if (g_arguments->debug_print) {                                       \
            struct tm      Tm, *ptm;                                          \
            struct timeval timeSecs;                                          \
            time_t         curTime;                                           \
            gettimeofday(&timeSecs, NULL);                                    \
            curTime = timeSecs.tv_sec;                                        \
            ptm = localtime_r(&curTime, &Tm);                                 \
            fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",               \
                    ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, \
                    ptm->tm_sec, (int32_t)timeSecs.tv_usec);                  \
            fprintf(stderr, "DEBG: ");                                        \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                   \
            fprintf(stderr, "" fmt, __VA_ARGS__);                             \
        }                                                                     \
    } while (0)

#define infoPrint(fmt, ...)                                                  \
    do {                                                                     \
        struct tm      Tm, *ptm;                                             \
        struct timeval timeSecs;                                             \
        time_t         curTime;                                              \
        gettimeofday(&timeSecs, NULL);                                       \
        curTime = timeSecs.tv_sec;                                           \
        ptm = localtime_r(&curTime, &Tm);                                    \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,        \
                (int32_t)timeSecs.tv_usec);                                  \
        fprintf(stderr, "INFO: " fmt, __VA_ARGS__);                          \
    } while (0)

#define performancePrint(fmt, ...)                                            \
    do {                                                                      \
        if (g_arguments->performance_print) {                                 \
            struct tm      Tm, *ptm;                                          \
            struct timeval timeSecs;                                          \
            time_t         curTime;                                           \
            gettimeofday(&timeSecs, NULL);                                    \
            curTime = timeSecs.tv_sec;                                        \
            ptm = localtime_r(&curTime, &Tm);                                 \
            fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",               \
                    ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, \
                    ptm->tm_sec, (int32_t)timeSecs.tv_usec);                  \
            fprintf(stderr, "PERF: " fmt, __VA_ARGS__);                       \
        }                                                                     \
    } while (0)

#define errorPrint(fmt, ...)                                                 \
    do {                                                                     \
        struct tm      Tm, *ptm;                                             \
        struct timeval timeSecs;                                             \
        time_t         curTime;                                              \
        gettimeofday(&timeSecs, NULL);                                       \
        curTime = timeSecs.tv_sec;                                           \
        ptm = localtime_r(&curTime, &Tm);                                    \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,        \
                (int32_t)timeSecs.tv_usec);                                  \
        fprintf(stderr, "\033[31m");                                         \
        fprintf(stderr, "ERROR: ");                                          \
        if (g_arguments->debug_print) {                                      \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                  \
        }                                                                    \
        fprintf(stderr, "" fmt, __VA_ARGS__);                                \
        fprintf(stderr, "\033[0m");                                          \
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

typedef struct TAOS_POOL_S {
    int    size;
    int    current;
    TAOS **taos_list;
} TAOS_POOL;

typedef struct SSuperTable_S {
    char *   stbName;
    bool     random_data_source;  // rand_gen or sample
    bool     escape_character;
    bool     use_metric;
    char *   childTblPrefix;
    bool     childTblExists;
    int64_t  childTblCount;
    uint64_t batchCreateTableNum;  // 0: no batch,  > 0: batch table number in
                                   // one sql
    bool     autoCreateTable;
    uint16_t iface;  // 0: taosc, 1: rest, 2: stmt
    uint16_t lineProtocol;
    uint64_t childTblLimit;
    uint64_t childTblOffset;

    //  int          multiThreadWriteOneTbl;  // 0: no, 1: yes
    int32_t interlaceRows;  //
    int     disorderRatio;  // 0: no disorder, >0: x%
    int     disorderRange;  // ms, us or ns. according to database precision

    uint64_t insert_interval;
    int64_t  insertRows;
    int64_t  timestamp_step;
    int      tsPrecision;
    int64_t  startTimestamp;
    char     sampleFile[MAX_FILE_NAME_LEN];
    char     tagsFile[MAX_FILE_NAME_LEN];

    int32_t  columnCount;
    int32_t  partialColumnNum;
    char *   partialColumnNameBuf;
    char *   col_type;
    bool *   col_null;
    int32_t *col_length;
    int32_t  tagCount;
    char *   tag_type;
    bool *   tag_null;
    int32_t *tag_length;
    char **  childTblName;
    char *   colsOfCreateChildTable;
    int32_t  lenOfTags;
    int32_t  lenOfCols;

    char *sampleDataBuf;
    bool  useSampleTs;
    char *tagDataBuf;
    // bind param batch
    TAOS_BIND **tag_bind_array;
    char *      stmt_buffer;
    char **     stmt_col_string_grid;
    char **     stmt_tag_string_grid;
    bool        tcpTransfer;
} SSuperTable;

typedef struct {
    char    name[TSDB_DB_NAME_LEN];
    char    create_time[32];
    int64_t ntables;
    int32_t vgroups;
    int16_t replica;
    int16_t quorum;
    int16_t days;
    char    keeplist[64];
    int32_t cache;  // MB
    int32_t blocks;
    int32_t minrows;
    int32_t maxrows;
    int8_t  wallevel;
    int32_t fsync;
    int8_t  comp;
    int8_t  cachelast;
    char    precision[SMALL_BUFF_LEN];  // time resolution
    int8_t  update;
    char    status[16];
} SDbInfo;

typedef struct SDbCfg_S {
    //  int       maxtablesPerVnode;
    int32_t minRows;  // 0 means default
    int32_t maxRows;  // 0 means default
    int     comp;
    int     walLevel;
    int     cacheLast;
    int     fsync;
    int     replica;
    int     update;
    int     keep;
    int     days;
    int     cache;
    int     blocks;
    int     quorum;
    int     precision;
    int     sml_precision;
} SDbCfg;

typedef struct SDataBase_S {
    char *       dbName;
    bool         drop;  // 0: use exists, 1: if exists, drop then new create
    SDbCfg       dbCfg;
    uint64_t     superTblCount;
    SSuperTable *superTbls;
} SDataBase;

typedef struct SpecifiedQueryInfo_S {
    uint64_t  queryInterval;  // 0: unlimited  > 0   loop/s
    uint32_t  concurrent;
    int       sqlCount;
    uint32_t  asyncMode;          // 0: sync, 1: async
    uint64_t  subscribeInterval;  // ms
    uint64_t  queryTimes;
    bool      subscribeRestart;
    int       subscribeKeepProgress;
    char      sql[MAX_QUERY_SQL_COUNT][BUFFER_SIZE + 1];
    char      result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
    int       resubAfterConsume[MAX_QUERY_SQL_COUNT];
    int       endAfterConsume[MAX_QUERY_SQL_COUNT];
    TAOS_SUB *tsub[MAX_QUERY_SQL_COUNT];
    char      topic[MAX_QUERY_SQL_COUNT][32];
    int       consumed[MAX_QUERY_SQL_COUNT];
    TAOS_RES *res[MAX_QUERY_SQL_COUNT];
    uint64_t  totalQueried;
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
    int64_t   childTblCount;
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
    int64_t            query_times;
    uint64_t           response_buffer;
    bool               reset_query_cache;
} SQueryMetaInfo;

typedef struct SArguments_S {
    char *             metaFile;
    int32_t            test_mode;
    char *             host;
    int16_t            port;
    int16_t            telnet_tcp_port;
    char *             user;
    char *             password;
    bool               answer_yes;
    bool               debug_print;
    bool               performance_print;
    bool               chinese;
    char *             output_file;
    int32_t            binwidth;
    int32_t            intColumnCount;
    int32_t            nthreads_pool;
    int32_t            nthreads;
    int64_t            prepared_rand;
    uint32_t           reqPerReq;
    int64_t            insert_interval;
    bool               demo_mode;
    bool               aggr_func;
    int32_t            dbCount;
    struct sockaddr_in serv_addr;
    TAOS_POOL *        pool;
    int64_t            g_totalChildTables;
    int64_t            g_actualChildTables;
    int64_t            g_autoCreatedChildTables;
    int64_t            g_existedChildTables;
    FILE *             fpOfInsertResult;
    SDataBase *        db;
    char *             base64_buf;
} SArguments;

typedef struct delayNode_S {
    uint64_t            value;
    struct delayNode_S *next;
} delayNode;

typedef struct delayList_S {
    uint64_t   size;
    delayNode *head;
    delayNode *tail;
} delayList;

typedef struct SThreadInfo_S {
    TAOS *     taos;
    TAOS_STMT *stmt;
    int64_t *  bind_ts;
    int64_t *  bind_ts_array;
    char *     bindParams;
    char *     is_null;
    int        threadID;
    uint64_t   start_table_from;
    uint64_t   end_table_to;
    int64_t    ntables;
    int64_t    tables_created;
    char *     buffer;
    int64_t    counter;
    uint64_t   st;
    uint64_t   et;
    uint64_t   lastTs;
    int64_t    samplePos;
    uint64_t   totalInsertRows;
    uint64_t   totalQueried;
    uint64_t   totalAffectedRows;
    uint64_t   cntDelay;
    uint64_t   totalDelay;
    uint64_t   maxDelay;
    uint64_t   minDelay;
    uint64_t   querySeq;
    TAOS_SUB * tsub;
    char **    lines;
    int32_t    sockfd;
    int32_t    db_index;
    int32_t    stb_index;
    char **    sml_tags;
    cJSON *    json_array;
    cJSON *    sml_json_tags;
    int64_t    start_time;
    int64_t    max_sql_len;
    FILE *     fp;
    char       filePath[MAX_PATH_LEN];
    delayList  delayList;
} threadInfo;

/* ************ Global variables ************  */
extern char *         g_aggreFuncDemo[];
extern char *         g_aggreFunc[];
extern SArguments *   g_arguments;
extern SQueryMetaInfo g_queryInfo;
extern bool           g_fail;
extern char           configDir[];
extern cJSON *        root;
extern uint64_t       g_memoryUsage;

#define min(a, b) (((a) < (b)) ? (a) : (b))
#define tstrncpy(dst, src, size)       \
    do {                               \
        strncpy((dst), (src), (size)); \
        (dst)[(size)-1] = 0;           \
    } while (0)
/* ************ Function declares ************  */
/* benchCommandOpt.c */
void commandLineParseArgument(int argc, char *argv[]);
void modify_argument();
void init_argument();
void queryAggrFunc();
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
void    taosMsleep(int32_t mseconds);
void    replaceChildTblName(char *inSql, char *outSql, int tblIndex);
void    setupForAnsiEscape(void);
void    resetAfterAnsiEscape(void);
char *  taos_convert_datatype_to_string(int type);
int     taos_convert_string_to_datatype(char *type);
int     taosRandom();
void    tmfree(void *buf);
void    tmfclose(FILE *fp);
void    fetchResult(TAOS_RES *res, threadInfo *pThreadInfo);
void    prompt();
void    ERROR_EXIT(const char *msg);
int     postProceSql(char *sqlstr, threadInfo *pThreadInfo);
int     queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet);
int     regexMatch(const char *s, const char *reg, int cflags);
int     convertHostToServAddr(char *host, uint16_t port,
                              struct sockaddr_in *serv_addr);
int     getAllChildNameOfSuperTable(TAOS *taos, char *dbName, char *stbName,
                                    char ** childTblNameOfSuperTbl,
                                    int64_t childTblCountOfSuperTbl);
void    delay_list_init(delayList *list);
void    delay_list_destroy(delayList *list);
/* demoInsert.c */
int  insertTestProcess();
void postFreeResource();
/* demoOutput.c */
void printfInsertMetaToFileStream(FILE *fp);
void printStatPerThread(threadInfo *pThreadInfo);
void printfQueryMeta();
void display_delay_list(delayList *list);
/* demoQuery.c */
int queryTestProcess();
/* demoSubscribe.c */
int subscribeTestProcess();
#endif
