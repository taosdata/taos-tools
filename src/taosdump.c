/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <iconv.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/prctl.h>
// #include <error.h>
#include <inttypes.h>
#include <argp.h>
#include <dirent.h>
#include <wordexp.h>
#include <assert.h>
#include <termios.h>
#include <sys/time.h>
#include <limits.h>

#include "taos.h"
#include "taosdef.h"
#include "taoserror.h"

#include <avro.h>
#include <jansson.h>

#define MAX_FILE_NAME_LEN       256             // max file name length on linux is 255
#define MAX_PATH_LEN            4096            // max path length on linux is 4095
#define COMMAND_SIZE            65536
#define MAX_RECORDS_PER_REQ     32766

static char    **g_tsDumpInSqlFiles     = NULL;
static char      g_tsCharset[63] = {0};
static char      g_configDir[MAX_FILE_NAME_LEN] = "/etc/taos";

static char    **g_tsDumpInAvroTagsTbs = NULL;
static char    **g_tsDumpInAvroNtbs = NULL;
static char    **g_tsDumpInAvroFiles = NULL;

static void print_json_aux(json_t *element, int indent);

// for tstrncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))

#define tstrncpy(dst, src, size) \
  do {                              \
    strncpy((dst), (src), (size));  \
    (dst)[(size)-1] = 0;            \
  } while (0)

#define tfree(x)         \
  do {                   \
    if (x) {             \
      free((void *)(x)); \
      x = 0;             \
    }                    \
  } while (0)

#define atomic_add_fetch_64(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
#define SET_THREAD_NAME(name)  do {prctl(PR_SET_NAME, (name));} while(0)

static int  converStringToReadable(char *str, int size, char *buf, int bufsize);
static int  convertNCharToReadable(char *str, int size, char *buf, int bufsize);

typedef struct {
  short bytes;
  int8_t type;
} SOColInfo;

#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) \
      fprintf(stderr, "DEBG: "fmt, __VA_ARGS__); } while(0)

#define debugPrint2(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) \
      fprintf(stderr, ""fmt, __VA_ARGS__); } while(0)

#define verbosePrint(fmt, ...) \
    do { if (g_args.verbose_print) \
        fprintf(stderr, "VERB: "fmt, __VA_ARGS__); } while(0)

#define performancePrint(fmt, ...) \
    do { if (g_args.performance_print) \
        fprintf(stderr, "PERF: "fmt, __VA_ARGS__); } while(0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

static bool isStringNumber(char *input)
{
    int len = strlen(input);
    if (0 == len) {
        return false;
    }

    for (int i = 0; i < len; i++) {
        if (!isdigit(input[i]))
            return false;
    }

    return true;
}

// -------------------------- SHOW DATABASE INTERFACE-----------------------
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

// -----------------------------------------SHOW TABLES CONFIGURE -------------------------------------
enum _show_tables_index {
    TSDB_SHOW_TABLES_NAME_INDEX,
    TSDB_SHOW_TABLES_CREATED_TIME_INDEX,
    TSDB_SHOW_TABLES_COLUMNS_INDEX,
    TSDB_SHOW_TABLES_METRIC_INDEX,
    TSDB_SHOW_TABLES_UID_INDEX,
    TSDB_SHOW_TABLES_TID_INDEX,
    TSDB_SHOW_TABLES_VGID_INDEX,
    TSDB_MAX_SHOW_TABLES
};

// ---------------------------------- DESCRIBE STABLE CONFIGURE ------------------------------
enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

#define COL_NOTE_LEN        4
#define COL_TYPEBUF_LEN     16
#define COL_VALUEBUF_LEN    32

typedef struct {
    char field[TSDB_COL_NAME_LEN];
    int type;
    int length;
    char note[COL_NOTE_LEN];
    char value[COL_VALUEBUF_LEN];
    char *var_value;
} ColDes;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    int columns;
    int tags;
    ColDes cols[];
} TableDef;

extern char version[];

#define DB_PRECISION_LEN   8
#define DB_STATUS_LEN      16

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    bool belongStb;
    char stable[TSDB_TABLE_NAME_LEN];
} TableInfo;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    char stable[TSDB_TABLE_NAME_LEN];
} TableRecord;

typedef struct {
    bool isStb;
    bool belongStb;
    int64_t dumpNtbCount;
    TableRecord **dumpNtbInfos;
    TableRecord tableRecord;
} TableRecordInfo;

typedef struct {
    char     name[TSDB_DB_NAME_LEN];
    char     create_time[32];
    int64_t  ntables;
    int32_t  vgroups;
    int16_t  replica;
    int16_t  quorum;
    int16_t  days;
    char     keeplist[32];
    //int16_t  daysToKeep;
    //int16_t  daysToKeep1;
    //int16_t  daysToKeep2;
    int32_t  cache; //MB
    int32_t  blocks;
    int32_t  minrows;
    int32_t  maxrows;
    int8_t   wallevel;
    int32_t  fsync;
    int8_t   comp;
    int8_t   cachelast;
    char     precision[DB_PRECISION_LEN];   // time resolution
    int8_t   update;
    char     status[DB_STATUS_LEN];
    int64_t  dumpTbCount;
    TableRecordInfo **dumpTbInfos;
} SDbInfo;

enum enWHICH {
    WHICH_UNKNOWN = 0,
    WHICH_SQL,
    WHICH_AVRO_TBTAGS,
    WHICH_AVRO_NTB,
    WHICH_AVRO_DATA
};

typedef struct {
    pthread_t threadID;
    int32_t   threadIndex;
    char      dbName[TSDB_DB_NAME_LEN];
    char      stbName[TSDB_TABLE_NAME_LEN];
    int       precision;
    TAOS      *taos;
    int64_t   rowsOfDumpOut;
    int64_t   count;
    int64_t   from;
    int64_t   stbSuccess;
    int64_t   stbFailed;
    int64_t   ntbSuccess;
    int64_t   ntbFailed;
    int64_t   recSuccess;
    int64_t   recFailed;
    enum enWHICH    which;
} threadInfo;

typedef struct {
    int64_t   totalRowsOfDumpOut;
    int64_t   totalChildTblsOfDumpOut;
    int32_t   totalSuperTblsOfDumpOut;
    int32_t   totalDatabasesOfDumpOut;
} resultStatistics;


enum enAvro_Codec {
    AVRO_CODEC_START = 0,
    AVRO_CODEC_NULL = AVRO_CODEC_START,
    AVRO_CODEC_DEFLATE,
    AVRO_CODEC_SNAPPY,
    AVRO_CODEC_LZMA,
    AVRO_CODEC_UNKNOWN,
    AVRO_CODEC_INVALID
};

char *g_avro_codec[] = {
    "null",
    "deflate",
    "snappy",
    "lzma",
    "unknown",
    "invalid"
};

/* avro sectin begin */
#define RECORD_NAME_LEN     64
#define FIELD_NAME_LEN      64

typedef struct FieldStruct_S {
    char name[FIELD_NAME_LEN];
    int type;
    bool nullable;
    bool is_array;
    int array_type;
} FieldStruct;

typedef struct RecordSchema_S {
    char name[RECORD_NAME_LEN];
    char *fields;
    int  num_fields;
} RecordSchema;

/* avro section end */

static uint64_t g_uniqueID = 0;
static int64_t g_totalDumpOutRows = 0;
static int64_t g_totalDumpInRecSuccess = 0;
static int64_t g_totalDumpInRecFailed = 0;
static int64_t g_totalDumpInStbSuccess = 0;
static int64_t g_totalDumpInStbFailed = 0;
static int64_t g_totalDumpInNtbSuccess = 0;
static int64_t g_totalDumpInNtbFailed = 0;

SDbInfo **g_dbInfos = NULL;
TableInfo *g_tablesList = NULL;

const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";

/* Program documentation. */
static char doc[] = "";
/* "Argp example #4 -- a program with somewhat more complicated\ */
/*         options\ */
/*         \vThis part of the documentation comes *after* the options;\ */
/*         note that the text is automatically filled, but it's possible\ */
/*         to force a line-break, e.g.\n<-- here."; */

/* A description of the arguments we accept. */
static char args_doc[] = "dbname [tbname ...]\n--databases db1,db2,... \n--all-databases\n-i inpath\n-o outpath";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
    // connection option
    {"host", 'h', "HOST",    0,  "Server host dumping data from. Default is localhost.", 0},
    {"user", 'u', "USER",    0,  "User name used to connect to server. Default is root.", 0},
    {"password", 'p', 0,    0,  "User password to connect to server. Default is taosdata.", 0},
    {"port", 'P', "PORT",        0,  "Port to connect", 0},
    // input/output file
    {"outpath", 'o', "OUTPATH",     0,  "Output file path.", 1},
    {"inpath", 'i', "INPATH",      0,  "Input file path.", 1},
    {"resultFile", 'r', "RESULTFILE",  0,  "DumpOut/In Result file path and name.", 1},
    {"config-dir", 'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/taos", 1},
    // dump unit options
    {"all-databases", 'A', 0, 0,  "Dump all databases.", 2},
    {"databases", 'D', "DATABASES", 0,  "Dump inputted databases. Use comma to separate databases\' name.", 2},
    {"allow-sys",   'a', 0, 0,  "Allow to dump system database", 2},
    // dump format options
    {"schemaonly", 's', 0, 0,  "Only dump tables' schema.", 2},
    {"without-property", 'N', 0, 0,  "Dump database without its properties.", 2},
    {"answer-yes", 'y', 0, 0,  "Input yes for prompt. It will skip data file checking!", 3},
    {"avro-codec", 'd', "snappy", 0,  "Choose an avro codec among null, deflate, snappy, and lzma.", 4},
    {"start-time",    'S', "START_TIME",  0,  "Start time to dump. Either epoch or ISO8601/RFC3339 format is acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 or 2017-10-0100:00:00:000+0800 or '2017-10-01 00:00:00.000+0800'",  8},
    {"end-time",      'E', "END_TIME",    0,  "End time to dump. Either epoch or ISO8601/RFC3339 format is acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 or 2017-10-0100:00:00.000+0800 or '2017-10-01 00:00:00.000+0800'",  9},
    {"data-batch",  'B', "DATA_BATCH",  0,  "Number of data per insert statement. Default value is 16384.", 10},
//    {"max-sql-len", 'L', "SQL_LEN",     0,  "Max length of one sql. Default is 65480.", 10},
    {"thread_num",  'T', "THREAD_NUM",  0,  "Number of thread for dump in file. Default is 5.", 10},
    {"debug",   'g', 0, 0,  "Print debug info.", 15},
    {0}
};

#define HUMAN_TIME_LEN      28

/* Used by main to communicate with parse_opt. */
typedef struct arguments {
    // connection option
    char    *host;
    char    *user;
    char    password[SHELL_MAX_PASSWORD_LEN];
    uint16_t port;
    // output file
    char     outpath[MAX_FILE_NAME_LEN];
    char     inpath[MAX_FILE_NAME_LEN];
    // result file
    char    *resultFile;
    // dump unit option
    bool     all_databases;
    bool     databases;
    char    *databasesSeq;
    // dump format option
    bool     schemaonly;
    bool     with_property;
    bool     answer_yes;
    bool     avro;
    int      avro_codec;
    int64_t  start_time;
    char     humanStartTime[HUMAN_TIME_LEN];
    int64_t  end_time;
    char     humanEndTime[HUMAN_TIME_LEN];
    char     precision[8];

    int32_t  data_batch;
    int32_t  max_sql_len;
    bool     allow_sys;
    // other options
    int32_t  thread_num;
    int      abort;
    char   **arg_list;
    int      arg_list_len;
    bool     isDumpIn;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;

    int      dumpDbCount;
} SArguments;

/* Our argp parser. */
static error_t parse_opt(int key, char *arg, struct argp_state *state);

static struct argp argp = {options, parse_opt, args_doc, doc};
static resultStatistics g_resultStatistics = {0};
static FILE *g_fpOfResult = NULL;
static int g_numOfCores = 1;

struct arguments g_args = {
    // connection option
    NULL,
    "root",
    "taosdata",
    0,          // port
    // outpath and inpath
    "",
    "",
    "./dump_result.txt", // result_file
    // dump unit option
    false,      // all_databases
    false,      // databases
    NULL,       // databasesSeq
    // dump format option
    false,      // schemaonly
    true,       // with_property
    false,      // answer_yes
    true,       // avro
    AVRO_CODEC_SNAPPY,  // avro_codec
    -INT64_MAX + 1, // start_time
    {0},        // humanStartTime
    INT64_MAX,  // end_time
    {0},        // humanEndTime
    "ms",       // precision
    MAX_RECORDS_PER_REQ / 2,    // data_batch
    TSDB_MAX_SQL_LEN,   // max_sql_len
    false,      // allow_sys
    // other options
    8,          // thread_num
    0,          // abort
    NULL,       // arg_list
    0,          // arg_list_len
    false,      // isDumpIn
    false,      // debug_print
    false,      // verbose_print
    false,      // performance_print
        0,      // dumpDbCount
};

// get taosdump commit number version
#ifndef TAOSDUMP_COMMIT_SHA1
#define TAOSDUMP_COMMIT_SHA1 "unknown"
#endif

#ifndef TAOSTOOLS_TAG
#define TAOSTOOLS_TAG "0.1.0"
#endif

#ifndef TAOSDUMP_STATUS
#define TAOSDUMP_STATUS "unknown"
#endif


static uint64_t getUniqueIDFromEpoch()
{
    struct timeval tv;

    gettimeofday(&tv, NULL);

    uint64_t id =
        (unsigned long long)(tv.tv_sec) * 1000 +
        (unsigned long long)(tv.tv_usec) / 1000;

    atomic_add_fetch_64(&g_uniqueID, 1);
    id += g_uniqueID;

    debugPrint("%s() LN%d unique ID: %"PRIu64"\n",
            __func__, __LINE__, id);

    return id;
}

void prompt() {
    if (!g_args.answer_yes) {
        printf("         Press enter key to continue or Ctrl-C to stop\n\n");
        (void)getchar();
    }
}

char* strToLower(char *dst, const char *src) {
  int esc = 0;
  char quote = 0, *p = dst, c;

  assert(dst != NULL);

  for (c = *src++; c; c = *src++) {
    if (esc) {
      esc = 0;
    } else if (quote) {
      if (c == '\\') {
        esc = 1;
      } else if (c == quote) {
        quote = 0;
      }
    } else if (c >= 'A' && c <= 'Z') {
      c -= 'A' - 'a';
    } else if (c == '\'' || c == '"') {
      quote = c;
    }
    *p++ = c;
  }

  *p = 0;
  return dst;
}

int setConsoleEcho(bool on)
{
#define ECHOFLAGS (ECHO | ECHOE | ECHOK | ECHONL)
    int err;
    struct termios term;

    if (tcgetattr(STDIN_FILENO, &term) == -1) {
        perror("Cannot get the attribution of the terminal");
        return -1;
    }

    if (on)
        term.c_lflag |= ECHOFLAGS;
    else
        term.c_lflag &= ~ECHOFLAGS;

    err = tcsetattr(STDIN_FILENO, TCSAFLUSH, &term);
    if (err == -1 || err == EINTR) {
        perror("Cannot set the attribution of the terminal");
        return -1;
    }

    return 0;
}

static void printVersion() {
    char taostools_ver[] = TAOSTOOLS_TAG;
    char taosdump_commit[] = TAOSDUMP_COMMIT_SHA1;
    char taosdump_status[] = TAOSDUMP_STATUS;

    if (strlen(taosdump_status) == 0) {
        printf("taosdump version %s_%s\n",
                taostools_ver, taosdump_commit);
    } else {
        printf("taosdump version %s_%s, status:%s\n",
                taostools_ver, taosdump_commit, taosdump_status);
    }
}

void errorWrongValue(char *program, char *wrong_arg, char *wrong_value)
{
    fprintf(stderr, "%s %s: %s is an invalid value\n", program, wrong_arg, wrong_value);
    fprintf(stderr, "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorUnrecognized(char *program, char *wrong_arg)
{
    fprintf(stderr, "%s: unrecognized options '%s'\n", program, wrong_arg);
    fprintf(stderr, "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorPrintReqArg(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option requires an argument -- '%s'\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorPrintReqArg2(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option requires a number argument '-%s'\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorPrintReqArg3(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option '%s' requires an argument\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static char *typeToStr(int type) {
    switch(type) {
        case TSDB_DATA_TYPE_BOOL:
            return "bool";
        case TSDB_DATA_TYPE_TINYINT:
            return "tinyint";
        case TSDB_DATA_TYPE_SMALLINT:
            return "smallint";
        case TSDB_DATA_TYPE_INT:
            return "int";
        case TSDB_DATA_TYPE_BIGINT:
            return "bigint";
        case TSDB_DATA_TYPE_FLOAT:
            return "float";
        case TSDB_DATA_TYPE_DOUBLE:
            return "double";
        case TSDB_DATA_TYPE_BINARY:
            return "binary";
        case TSDB_DATA_TYPE_TIMESTAMP:
            return "timestamp";
        case TSDB_DATA_TYPE_NCHAR:
            return "nchar";
        case TSDB_DATA_TYPE_UTINYINT:
            return "tinyint unsigned";
        case TSDB_DATA_TYPE_USMALLINT:
            return "smallint unsigned";
        case TSDB_DATA_TYPE_UINT:
            return "int unsigned";
        case TSDB_DATA_TYPE_UBIGINT:
            return "bigint unsigned";
        case TSDB_DATA_TYPE_JSON:
            return "JSON";
        default:
            break;
    }

    return "unknown";
}

static int typeStrToType(const char *type_str) {
    if ((0 == strcasecmp(type_str, "bool"))
            || (0 == strcasecmp(type_str, "boolean"))) {
        return TSDB_DATA_TYPE_BOOL;
    } else if (0 == strcasecmp(type_str, "tinyint")) {
        return TSDB_DATA_TYPE_TINYINT;
    } else if (0 == strcasecmp(type_str, "smallint")) {
        return TSDB_DATA_TYPE_SMALLINT;
    } else if (0 == strcasecmp(type_str, "int")) {
        return TSDB_DATA_TYPE_INT;
    } else if ((0 == strcasecmp(type_str, "bigint"))
            || (0 == strcasecmp(type_str, "long"))) {
        return TSDB_DATA_TYPE_BIGINT;
    } else if (0 == strcasecmp(type_str, "float")) {
        return TSDB_DATA_TYPE_FLOAT;
    } else if (0 == strcasecmp(type_str, "double")) {
        return TSDB_DATA_TYPE_DOUBLE;
    } else if ((0 == strcasecmp(type_str, "binary"))
            || (0 == strcasecmp(type_str, "string"))) {
        return TSDB_DATA_TYPE_BINARY;
    } else if (0 == strcasecmp(type_str, "timestamp")) {
        return TSDB_DATA_TYPE_TIMESTAMP;
    } else if ((0 == strcasecmp(type_str, "nchar"))
            || (0 == strcasecmp(type_str, "bytes"))) {
        return TSDB_DATA_TYPE_NCHAR;
    } else if (0 == strcasecmp(type_str, "tinyint unsigned")) {
        return TSDB_DATA_TYPE_UTINYINT;
    } else if (0 == strcasecmp(type_str, "smallint unsigned")) {
        return TSDB_DATA_TYPE_USMALLINT;
    } else if (0 == strcasecmp(type_str, "int unsigned")) {
        return TSDB_DATA_TYPE_UINT;
    } else if (0 == strcasecmp(type_str, "bigint unsigned")) {
        return TSDB_DATA_TYPE_UBIGINT;
    } else if (0 == strcasecmp(type_str, "JSON")) {
        return TSDB_DATA_TYPE_JSON;
    } else {
        errorPrint("%s() LN%d Unknown type: %s\n",
                __func__, __LINE__, type_str);
    }

    return TSDB_DATA_TYPE_NULL;
}

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    /* Get the input argument from argp_parse, which we
       know is a pointer to our arguments structure. */
    wordexp_t full_path;
    int avroCodec = AVRO_CODEC_START;

    switch (key) {
        // connection option
        case 'a':
            g_args.allow_sys = true;
            break;
        case 'h':
            g_args.host = arg;
            break;
        case 'u':
            g_args.user = arg;
            break;
        case 'p':
            break;
        case 'P':
            if (!isStringNumber(arg)) {
                errorPrintReqArg2("taosdump", "P");
                exit(EXIT_FAILURE);
            }

            uint64_t port = atoi((const char *)arg);
            if (port > 65535) {
                errorWrongValue("taosdump", "-P or --port", arg);
                exit(EXIT_FAILURE);
            }
            g_args.port = (uint16_t)port;

            break;
        case 'o':
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }

            if (full_path.we_wordv[0]) {
                snprintf(g_args.outpath, MAX_FILE_NAME_LEN, "%s/",
                        full_path.we_wordv[0]);
                wordfree(&full_path);
            } else {
                errorPrintReqArg3("taosdump", "-o or --outpath");
                exit(EXIT_FAILURE);
            }
            break;

        case 'g':
            g_args.debug_print = true;
            break;

        case 'i':
            g_args.isDumpIn = true;
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }

            if (full_path.we_wordv[0]) {
                tstrncpy(g_args.inpath, full_path.we_wordv[0],
                        MAX_FILE_NAME_LEN);
                wordfree(&full_path);
            } else {
                errorPrintReqArg3("taosdump", "-i or --inpath");
                exit(EXIT_FAILURE);
            }
            break;

        case 'd':
            for (; avroCodec < AVRO_CODEC_INVALID; avroCodec ++) {
                if (0 == strcmp(arg, g_avro_codec[avroCodec])) {
                    g_args.avro_codec = avroCodec;
                    break;
                }
            }

            if (AVRO_CODEC_UNKNOWN == avroCodec) {
                g_args.avro = false;
            } else if (AVRO_CODEC_INVALID == avroCodec) {
                errorPrint("%s", "Invalid AVRO codec inputed. Exit program!\n");
                exit(1);
            }
            break;

        case 'r':
            g_args.resultFile = arg;
            break;

        case 'c':
            if (0 == strlen(arg)) {
                errorPrintReqArg3("taosdump", "-c or --config-dir");
                exit(EXIT_FAILURE);
            }
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                exit(EXIT_FAILURE);
            }
            tstrncpy(g_configDir, full_path.we_wordv[0], MAX_FILE_NAME_LEN);
            wordfree(&full_path);
            break;

        case 'A':
            break;
        case 'D':
            g_args.databases = true;
            break;
            // dump format option
        case 's':
            g_args.schemaonly = true;
            break;
        case 'N':
            g_args.with_property = false;
            break;
        case 'y':
            g_args.answer_yes = true;
            break;
        case 'S':
            // parse time here.
            break;
        case 'E':
            break;
        case 'B':
            g_args.data_batch = atoi((const char *)arg);
            if (g_args.data_batch > MAX_RECORDS_PER_REQ/2) {
                g_args.data_batch = MAX_RECORDS_PER_REQ/2;
            }
            break;
            /*
        case 'L':
            {
                int32_t len = atoi((const char *)arg);
                if (len > TSDB_MAX_ALLOWED_SQL_LEN) {
                    len = TSDB_MAX_ALLOWED_SQL_LEN;
                } else if (len < TSDB_MAX_SQL_LEN) {
                    len = TSDB_MAX_SQL_LEN;
                }
                g_args.max_sql_len = len;
                break;
            }
            */
        case 'T':
            if (!isStringNumber(arg)) {
                errorPrint("%s", "\n\t-T need a number following!\n");
                exit(EXIT_FAILURE);
            }
            g_args.thread_num = atoi((const char *)arg);
            break;
        case OPT_ABORT:
            g_args.abort = 1;
            break;
        case ARGP_KEY_ARG:
            if (strlen(state->argv[state->next - 1])) {
                g_args.arg_list     = &state->argv[state->next - 1];
                g_args.arg_list_len = state->argc - state->next + 1;
            }
            state->next             = state->argc;
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static void freeTbDes(TableDef *tableDes)
{
    for (int i = 0; i < (tableDes->columns+tableDes->tags); i ++) {
        if (tableDes->cols[i].var_value) {
            free(tableDes->cols[i].var_value);
        }
    }

    free(tableDes);
}

static int queryDbImpl(TAOS *taos, char *command) {
    TAOS_RES *res = NULL;
    int32_t   code = -1;

    res = taos_query(taos, command);
    code = taos_errno(res);

    if (code != 0) {
        errorPrint("Failed to run <%s>, reason: %s\n",
                command, taos_errstr(res));
        taos_free_result(res);
        //taos_close(taos);
        return code;
    }

    taos_free_result(res);
    return 0;
}

static void parse_args(
        int argc, char *argv[], SArguments *arguments) {

    for (int i = 1; i < argc; i++) {
        if ((strncmp(argv[i], "-p", 2) == 0)
              || (strncmp(argv[i], "--password", 10) == 0)) {
            if ((strlen(argv[i]) == 2)
                  || (strncmp(argv[i], "--password", 10) == 0)) {
                printf("Enter password: ");
                setConsoleEcho(false);
                if(scanf("%20s", arguments->password) > 1) {
                    errorPrint("%s() LN%d, password read error!\n", __func__, __LINE__);
                }
                setConsoleEcho(true);
            } else {
                tstrncpy(arguments->password, (char *)(argv[i] + 2),
                        SHELL_MAX_PASSWORD_LEN);
                strcpy(argv[i], "-p");
            }
        } else if (strcmp(argv[i], "-gg") == 0) {
            arguments->verbose_print = true;
            strcpy(argv[i], "");
        } else if (strcmp(argv[i], "-PP") == 0) {
            arguments->performance_print = true;
            strcpy(argv[i], "");
        } else if ((strcmp(argv[i], "-A") == 0)
                || (0 == strncmp(
                            argv[i], "--all-database",
                            strlen("--all-database")))) {
            g_args.all_databases = true;
        } else if ((strncmp(argv[i], "-D", strlen("-D")) == 0)
                || (0 == strncmp(
                        argv[i], "--database",
                        strlen("--database")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "D");
                    exit(EXIT_FAILURE);
                }
                arguments->databasesSeq = argv[++i];
            } else if (0 == strncmp(argv[i], "--databases=", strlen("--databases="))) {
                arguments->databasesSeq = (char *)(argv[i] + strlen("--databases="));
            } else if (0 == strncmp(argv[i], "-D", strlen("-D"))) {
                arguments->databasesSeq = (char *)(argv[i] + strlen("-D"));
            } else if (strlen("--databases") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--databases");
                    exit(EXIT_FAILURE);
                }
                arguments->databasesSeq = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
            g_args.databases = true;
        } else if (0 == strncmp(argv[i], "--version", strlen("--version")) ||
            0 == strncmp(argv[i], "-V", strlen("-V"))) {
                printVersion();
                exit(EXIT_SUCCESS);
        } else {
            continue;
        }

    }
}

static void copyHumanTimeToArg(char *timeStr, bool isStartTime)
{
    if (isStartTime)
        tstrncpy(g_args.humanStartTime, timeStr, HUMAN_TIME_LEN);
    else
        tstrncpy(g_args.humanEndTime, timeStr, HUMAN_TIME_LEN);
}

static void copyTimestampToArg(char *timeStr, bool isStartTime)
{
    if (isStartTime) {
        g_args.start_time = atol((const char *)timeStr);
    } else {
        g_args.end_time = atol((const char *)timeStr);
    }
}

static void parse_timestamp(
        int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        char *tmp;
        bool isStartTime = false;
        bool isEndTime = false;

        if (strcmp(argv[i], "-S") == 0) {
            isStartTime = true;
        } else if (strcmp(argv[i], "-E") == 0) {
            isEndTime = true;
        }

        if (isStartTime || isEndTime) {
            if (NULL == argv[i+1]) {
                errorPrint("%s need a valid value following!\n", argv[i]);
                exit(-1);
            }
            tmp = strdup(argv[i+1]);

            if (strchr(tmp, ':') && strchr(tmp, '-')) {
                copyHumanTimeToArg(tmp, isStartTime);
            } else {
                copyTimestampToArg(tmp, isStartTime);
            }

            free(tmp);
        }
    }
}

static int getPrecisionByString(char *precision)
{
    if (0 == strncasecmp(precision,
                "ms", 2)) {
        return TSDB_TIME_PRECISION_MILLI;
    } else if (0 == strncasecmp(precision,
                "us", 2)) {
        return TSDB_TIME_PRECISION_MICRO;
    } else if (0 == strncasecmp(precision,
                "ns", 2)) {
        return TSDB_TIME_PRECISION_NANO;
    } else {
        errorPrint("Invalid time precision: %s",
                precision);
    }

    return -1;
}

static void freeDbInfos() {
    if (g_dbInfos == NULL) return;
    for (int i = 0; i < g_args.dumpDbCount; i++)
        tfree(g_dbInfos[i]);
    tfree(g_dbInfos);
}

// check table is normal table or super table
static int getTableRecordInfo(
        char *dbName,
        char *table, TableRecordInfo *pTableRecordInfo) {
    TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
            dbName, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return -1;
    }

    TAOS_ROW row = NULL;
    bool isSet = false;
    TAOS_RES *result     = NULL;

    memset(pTableRecordInfo, 0, sizeof(TableRecordInfo));

    char command[COMMAND_SIZE];

    sprintf(command, "USE %s", dbName);
    result = taos_query(taos, command);
    int32_t code = taos_errno(result);
    if (code != 0) {
        errorPrint("invalid database %s, reason: %s\n",
                dbName, taos_errstr(result));
        return 0;
    }

    sprintf(command, "SHOW TABLES LIKE \'%s\'", table);

    result = taos_query(taos, command);
    code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        taos_free_result(result);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);

    while ((row = taos_fetch_row(result)) != NULL) {
        isSet = true;
        pTableRecordInfo->isStb = false;
        tstrncpy(pTableRecordInfo->tableRecord.name,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes + 1));
        if (strlen((char *)row[TSDB_SHOW_TABLES_METRIC_INDEX]) > 0) {
            pTableRecordInfo->belongStb = true;
            tstrncpy(pTableRecordInfo->tableRecord.stable,
                    (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
                    min(TSDB_TABLE_NAME_LEN,
                        fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes + 1));
        } else {
            pTableRecordInfo->belongStb = false;
        }
        break;
    }

    taos_free_result(result);
    result = NULL;

    if (isSet) {
        return 0;
    }

    sprintf(command, "SHOW STABLES LIKE \'%s\'", table);

    result = taos_query(taos, command);
    code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        taos_free_result(result);
        return -1;
    }

    while ((row = taos_fetch_row(result)) != NULL) {
        isSet = true;
        pTableRecordInfo->isStb = true;
        tstrncpy(pTableRecordInfo->tableRecord.stable, table,
                TSDB_TABLE_NAME_LEN);
        break;
    }

    taos_free_result(result);
    result = NULL;

    if (isSet) {
        return 0;
    }
    errorPrint("%s() LN%d, invalid table/stable %s\n",
            __func__, __LINE__, table);
    return -1;
}

static int inDatabasesSeq(
        char *name,
        int len)
{
    if (strstr(g_args.databasesSeq, ",") == NULL) {
        if (0 == strncmp(g_args.databasesSeq, name, len)) {
            return 0;
        }
    } else {
        char *dupSeq = strdup(g_args.databasesSeq);
        char *running = dupSeq;
        char *dbname = strsep(&running, ",");
        while (dbname) {
            if (0 == strncmp(dbname, name, len)) {
                tfree(dupSeq);
                return 0;
            }

            dbname = strsep(&running, ",");
        }

        free(dupSeq);
    }

    return -1;
}

static int getDumpDbCount()
{
    int count = 0;

    TAOS     *taos = NULL;
    TAOS_RES *result     = NULL;
    char     *command    = "show databases";
    TAOS_ROW row;

    /* Connect to server */
    taos = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
    if (NULL == taos) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return 0;
    }

    result = taos_query(taos, command);
    int32_t code = taos_errno(result);

    if (0 != code) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        taos_close(taos);
        return 0;
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);

    while ((row = taos_fetch_row(result)) != NULL) {
        // sys database name : 'log', but subsequent version changed to 'log'
        if ((strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log",
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
                && (!g_args.allow_sys)) {
            continue;
        }

        if (g_args.databases) {  // input multi dbs
            if (inDatabasesSeq(
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0)
                continue;
        } else if (!g_args.all_databases) {  // only input one db
            if (strncasecmp(g_args.arg_list[0],
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0)
                continue;
        }

        count++;
    }

    if (count == 0) {
        errorPrint("%d databases valid to dump\n", count);
    }

    taos_close(taos);
    return count;
}

static int dumpCreateMTableClause(
        char* dbName,
        char *stable,
        TableDef *tableDes,
        int numColsAndTags,
        FILE *fp
        ) {
    int counter = 0;
    int count_temp = 0;

    char* tmpBuf = (char *)malloc(COMMAND_SIZE);
    if (tmpBuf == NULL) {
        errorPrint("%s() LN%d, failed to allocate %d memory\n",
               __func__, __LINE__, COMMAND_SIZE);
        return -1;
    }

    char *pstr = NULL;
    pstr = tmpBuf;

    pstr += sprintf(tmpBuf,
            "CREATE TABLE IF NOT EXISTS %s.`%s` USING %s.`%s` TAGS(",
            dbName, tableDes->name, dbName, stable);

    for (; counter < numColsAndTags; counter++) {
        if (tableDes->cols[counter].note[0] != '\0') break;
    }

    assert(counter < numColsAndTags);
    count_temp = counter;

    for (; counter < numColsAndTags; counter++) {
        if (counter != count_temp) {
            if ((TSDB_DATA_TYPE_BINARY == tableDes->cols[counter].type)
                    || (TSDB_DATA_TYPE_NCHAR == tableDes->cols[counter].type)) {
                //pstr += sprintf(pstr, ", \'%s\'", tableDes->cols[counter].note);
                if (tableDes->cols[counter].var_value) {
                    pstr += sprintf(pstr, ",\'%s\'",
                            tableDes->cols[counter].var_value);
                } else {
                    pstr += sprintf(pstr, ",\'%s\'", tableDes->cols[counter].value);
                }
            } else {
                pstr += sprintf(pstr, ",\'%s\'", tableDes->cols[counter].value);
            }
        } else {
            if ((TSDB_DATA_TYPE_BINARY == tableDes->cols[counter].type)
                    || (TSDB_DATA_TYPE_NCHAR == tableDes->cols[counter].type)) {
                //pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].note);
                if (tableDes->cols[counter].var_value) {
                    pstr += sprintf(pstr,"\'%s\'", tableDes->cols[counter].var_value);
                } else {
                    pstr += sprintf(pstr,"\'%s\'", tableDes->cols[counter].value);
                }
            } else {
                pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].value);
            }
            /* pstr += sprintf(pstr, "%s", tableDes->cols[counter].note); */
        }

        /* if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 || strcasecmp(tableDes->cols[counter].type, "nchar")
         * == 0) { */
        /*     pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length); */
        /* } */
    }

    pstr += sprintf(pstr, ");");

    int ret = fprintf(fp, "%s\n", tmpBuf);
    free(tmpBuf);

    return ret;
}

static int64_t getNtbCountOfStb(char *dbName, char *stbName)
{
    TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
            dbName, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return -1;
    }

    int64_t count = 0;

    char command[COMMAND_SIZE];

    sprintf(command, "SELECT COUNT(TBNAME) FROM %s.`%s`", dbName, stbName);

    TAOS_RES *res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        taos_close(taos);
        return -1;
    }

    TAOS_ROW row = NULL;

    if ((row = taos_fetch_row(res)) != NULL) {
        count = *(int64_t*)row[TSDB_SHOW_TABLES_NAME_INDEX];
    }

    debugPrint("%s() LN%d, COUNT(TBNAME): %"PRId64"\n",
            __func__, __LINE__, count);

    taos_close(taos);
    return count;
}

static int getTableDes(
        TAOS *taos,
        char* dbName, char *table,
        TableDef *tableDes, bool colOnly) {
    TAOS_ROW row = NULL;
    TAOS_RES* res = NULL;
    int colCount = 0;

    char sqlstr[COMMAND_SIZE];
    sprintf(sqlstr, "DESCRIBE %s.`%s`", dbName, table);

    res = taos_query(taos, sqlstr);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, taos: %p, reason: %s\n",
                __func__, __LINE__, sqlstr, taos, taos_errstr(res));
        taos_free_result(res);
        return -1;
    } else {
        debugPrint("%s() LN%d, run command <%s> success, taos: %p\n",
                __func__, __LINE__, sqlstr, taos);
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    tstrncpy(tableDes->name, table, TSDB_TABLE_NAME_LEN);
    while ((row = taos_fetch_row(res)) != NULL) {

        tstrncpy(tableDes->cols[colCount].field,
                (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                min(TSDB_COL_NAME_LEN,
                    fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes + 1));
        tableDes->cols[colCount].type =
                typeStrToType((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
        tableDes->cols[colCount].length =
            *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
        tstrncpy(tableDes->cols[colCount].note,
                (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                min(COL_NOTE_LEN,
                    fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes + 1));

        if (strcmp(tableDes->cols[colCount].note, "TAG") != 0) {
            tableDes->columns ++;
        } else {
            tableDes->tags ++;
        }
        colCount++;
    }

    taos_free_result(res);
    res = NULL;

    if (colOnly) {
        return colCount;
    }

    // if child-table have tag, using  select tagName from table to get tagValue
    for (int i = 0 ; i < colCount; i++) {
        if (strcmp(tableDes->cols[i].note, "TAG") != 0) continue;

        sprintf(sqlstr, "SELECT %s FROM %s.`%s`",
                tableDes->cols[i].field, dbName, table);

        res = taos_query(taos, sqlstr);
        code = taos_errno(res);
        if (code != 0) {
            errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                    __func__, __LINE__, sqlstr, taos_errstr(res));
            taos_free_result(res);
            return -1;
        }

        fields = taos_fetch_fields(res);

        row = taos_fetch_row(res);

        if (NULL == row) {
            warnPrint("No data from fetch to run command <%s>, reason:%s\n",
                    sqlstr, taos_errstr(res));
            taos_free_result(res);
            return -1;
        }

        int32_t* length = taos_fetch_lengths(res);

        if (row[TSDB_SHOW_TABLES_NAME_INDEX] == NULL) {
            sprintf(tableDes->cols[i].note, "%s", "NUL");
            sprintf(tableDes->cols[i].value, "%s", "NULL");
            taos_free_result(res);
            res = NULL;
            continue;
        }

        switch (fields[0].type) {
            case TSDB_DATA_TYPE_BOOL:
                sprintf(tableDes->cols[i].value, "%d",
                        ((((int32_t)(*((char *)
                                       row[TSDB_SHOW_TABLES_NAME_INDEX])))==1)
                         ?1:0));
                break;

            case TSDB_DATA_TYPE_TINYINT:
                sprintf(tableDes->cols[i].value, "%d",
                        *((int8_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                sprintf(tableDes->cols[i].value, "%u",
                        *((uint8_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                sprintf(tableDes->cols[i].value, "%d",
                        *((int16_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                sprintf(tableDes->cols[i].value, "%u",
                        *((uint16_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_INT:
                sprintf(tableDes->cols[i].value, "%d",
                        *((int32_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_UINT:
                sprintf(tableDes->cols[i].value, "%u",
                        *((uint32_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_BIGINT:
                sprintf(tableDes->cols[i].value, "%" PRId64 "",
                        *((int64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                sprintf(tableDes->cols[i].value, "%" PRIu64 "",
                        *((uint64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;

            case TSDB_DATA_TYPE_FLOAT:
                {
                    char tmpFloat[512] = {0};
                    sprintf(tmpFloat, "%f",
                        GET_FLOAT_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                    verbosePrint("%s() LN%d, float value: %s\n",
                            __func__, __LINE__, tmpFloat);
                    int bufLenOfFloat = strlen(tmpFloat);

                    if (bufLenOfFloat < (COL_VALUEBUF_LEN -1)) {
                        sprintf(tableDes->cols[i].value, "%f",
                                GET_FLOAT_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                    } else {
                        if (tableDes->cols[i].var_value) {
                            free(tableDes->cols[i].var_value);
                            tableDes->cols[i].var_value = NULL;
                        }
                        tableDes->cols[i].var_value =
                            calloc(1, bufLenOfFloat + 1);

                        if (NULL == tableDes->cols[i].var_value) {
                            errorPrint("%s() LN%d, memory alalocation failed!\n",
                                    __func__, __LINE__);
                            taos_free_result(res);
                            return -1;
                        }
                        sprintf(tableDes->cols[i].var_value, "%f",
                                GET_FLOAT_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                    }
                }
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                {
                    char tmpDouble[512] = {0};
                    sprintf(tmpDouble, "%f",
                            GET_DOUBLE_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                    verbosePrint("%s() LN%d, double value: %s\n",
                            __func__, __LINE__, tmpDouble);
                    int bufLenOfDouble = strlen(tmpDouble);

                    if (bufLenOfDouble < (COL_VALUEBUF_LEN -1)) {
                        sprintf(tableDes->cols[i].value, "%f",
                                GET_DOUBLE_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                    } else {
                        if (tableDes->cols[i].var_value) {
                            free(tableDes->cols[i].var_value);
                            tableDes->cols[i].var_value = NULL;
                        }
                        tableDes->cols[i].var_value =
                            calloc(1, bufLenOfDouble + 1);

                        if (NULL == tableDes->cols[i].var_value) {
                            errorPrint("%s() LN%d, memory alalocation failed!\n",
                                    __func__, __LINE__);
                            taos_free_result(res);
                            return -1;
                        }
                        sprintf(tableDes->cols[i].var_value, "%f",
                                GET_DOUBLE_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                    }
                }
                break;

            case TSDB_DATA_TYPE_BINARY:
                memset(tableDes->cols[i].value, 0,
                        sizeof(tableDes->cols[i].value));

                if (g_args.avro) {
                    if (length[TSDB_SHOW_TABLES_NAME_INDEX] < (COL_VALUEBUF_LEN - 1)) {
                        strcpy(tableDes->cols[i].value,
                                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
                    } else {
                        if (tableDes->cols[i].var_value) {
                            free(tableDes->cols[i].var_value);
                            tableDes->cols[i].var_value = NULL;
                        }
                        tableDes->cols[i].var_value = calloc(1,
                                1 + length[TSDB_SHOW_TABLES_NAME_INDEX]);

                        if (NULL == tableDes->cols[i].var_value) {
                            errorPrint("%s() LN%d, memory alalocation failed!\n",
                                    __func__, __LINE__);
                            taos_free_result(res);
                            return -1;
                        }
                        strncpy(
                                (char *)(tableDes->cols[i].var_value),
                                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                                min(TSDB_TABLE_NAME_LEN,
                                    length[TSDB_SHOW_TABLES_NAME_INDEX]));
                    }
                } else {
                    if (length[TSDB_SHOW_TABLES_NAME_INDEX] < (COL_VALUEBUF_LEN - 2)) {
                        converStringToReadable(
                                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                                length[TSDB_SHOW_TABLES_NAME_INDEX],
                                tableDes->cols[i].value,
                                length[TSDB_SHOW_TABLES_NAME_INDEX]);
                    } else {
                        if (tableDes->cols[i].var_value) {
                            free(tableDes->cols[i].var_value);
                            tableDes->cols[i].var_value = NULL;
                        }
                        tableDes->cols[i].var_value = calloc(1,
                                length[TSDB_SHOW_TABLES_NAME_INDEX] * 2);

                        if (NULL == tableDes->cols[i].var_value) {
                            errorPrint("%s() LN%d, memory alalocation failed!\n",
                                    __func__, __LINE__);
                            taos_free_result(res);
                            return -1;
                        }
                        converStringToReadable((char *)row[0],
                                length[0],
                                (char *)(tableDes->cols[i].var_value),
                                length[TSDB_SHOW_TABLES_NAME_INDEX]);
                    }
                }
                break;

            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
                {
                    int nlen = strlen((char *)row[TSDB_SHOW_TABLES_NAME_INDEX]);

                    if (g_args.avro) {
                        if (nlen < (COL_VALUEBUF_LEN - 1)) {
                            strncpy(tableDes->cols[i].value,
                                    (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                                    nlen);
                        } else {
                            tableDes->cols[i].var_value = calloc(1, nlen + 1);

                            if (NULL == tableDes->cols[i].var_value) {
                                errorPrint("%s() LN%d, memory alalocation failed!\n",
                                        __func__, __LINE__);
                                taos_free_result(res);
                                return -1;
                            }
                            strncpy(
                                    (char *)(tableDes->cols[i].var_value),
                                    (char *)row[TSDB_SHOW_TABLES_NAME_INDEX], nlen);
                        }
                    } else {
                        if (nlen < (COL_VALUEBUF_LEN-2)) {
                            char tbuf[COL_VALUEBUF_LEN-2];    // need reserve 2 bytes for ' '
                            convertNCharToReadable(
                                    (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                                    length[0], tbuf, COL_VALUEBUF_LEN-2);
                            sprintf(tableDes->cols[i].value, "%s", tbuf);
                        } else {
                            if (tableDes->cols[i].var_value) {
                                free(tableDes->cols[i].var_value);
                                tableDes->cols[i].var_value = NULL;
                            }
                            tableDes->cols[i].var_value = calloc(1, nlen * 5);

                            if (NULL == tableDes->cols[i].var_value) {
                                errorPrint("%s() LN%d, memory alalocation failed!\n",
                                        __func__, __LINE__);
                                taos_free_result(res);
                                return -1;
                            }
                            converStringToReadable(
                                    (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                                    length[0],
                                    (char *)(tableDes->cols[i].var_value), nlen);
                        }
                    }
                }
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                sprintf(tableDes->cols[i].value, "%" PRId64 "",
                        *(int64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
                break;

            default:
                errorPrint("%s() LN%d, unknown type: %d\n",
                        __func__, __LINE__, fields[0].type);
                break;
        }

        taos_free_result(res);
    }

    return colCount;
}

static int convertTableDesToSql(char *dbName,
        TableDef *tableDes, char **buffer)
{
    int counter = 0;
    int count_temp = 0;

    char* pstr = *buffer;

    pstr += sprintf(pstr, "CREATE TABLE IF NOT EXISTS %s.`%s`",
            dbName, tableDes->name);

    for (; counter < tableDes->columns; counter++) {
        if (tableDes->cols[counter].note[0] != '\0') break;

        if (counter == 0) {
            pstr += sprintf(pstr, "(`%s` %s",
                    tableDes->cols[counter].field,
                    typeToStr(tableDes->cols[counter].type));
        } else {
            pstr += sprintf(pstr, ",`%s` %s",
                    tableDes->cols[counter].field,
                    typeToStr(tableDes->cols[counter].type));
        }

        if ((TSDB_DATA_TYPE_BINARY == tableDes->cols[counter].type)
                || (TSDB_DATA_TYPE_NCHAR == tableDes->cols[counter].type)) {
            // Note no JSON allowed in column
            pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
        }
    }

    count_temp = counter;

    for (; counter < (tableDes->columns + tableDes->tags); counter++) {
        if (counter == count_temp) {
            pstr += sprintf(pstr, ") TAGS(`%s` %s",
                    tableDes->cols[counter].field,
                    typeToStr(tableDes->cols[counter].type));
        } else {
            pstr += sprintf(pstr, ",`%s` %s",
                    tableDes->cols[counter].field,
                    typeToStr(tableDes->cols[counter].type));
        }

        if ((TSDB_DATA_TYPE_BINARY == tableDes->cols[counter].type)
                || (TSDB_DATA_TYPE_NCHAR == tableDes->cols[counter].type)) {
            // JSON tag don't need to specify length
            pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
        }
    }

    pstr += sprintf(pstr, ");");

    return 0;
}

static void print_json(json_t *root) { print_json_aux(root, 0); }

static json_t *load_json(char *jsonbuf)
{
    json_t *root;
    json_error_t error;

    root = json_loads(jsonbuf, 0, &error);

    if (root) {
        return root;
    } else {
        errorPrint("JSON error on line %d: %s\n", error.line, error.text);
        return NULL;
    }
}

const char *json_plural(size_t count) { return count == 1 ? "" : "s"; }

static void freeRecordSchema(RecordSchema *recordSchema)
{
    if (recordSchema) {
        if (recordSchema->fields) {
            free(recordSchema->fields);
        }
        free(recordSchema);
    }
}

static RecordSchema *parse_json_to_recordschema(json_t *element)
{
    RecordSchema *recordSchema = calloc(1, sizeof(RecordSchema));
    assert(recordSchema);

    if (JSON_OBJECT != json_typeof(element)) {
        errorPrint("%s() LN%d, json passed is not an object\n",
                __func__, __LINE__);
        return NULL;
    }

    const char *key;
    json_t *value;

    json_object_foreach(element, key, value) {
        if (0 == strcmp(key, "name")) {
            tstrncpy(recordSchema->name, json_string_value(value), RECORD_NAME_LEN-1);
        } else if (0 == strcmp(key, "fields")) {
            if (JSON_ARRAY == json_typeof(value)) {

                size_t i;
                size_t size = json_array_size(value);

                verbosePrint("%s() LN%d, JSON Array of %lld element%s:\n",
                        __func__, __LINE__,
                        (long long)size, json_plural(size));

                recordSchema->num_fields = size;
                recordSchema->fields = calloc(1, sizeof(FieldStruct) * size);
                assert(recordSchema->fields);

                for (i = 0; i < size; i++) {
                    FieldStruct *field = (FieldStruct *)
                        (recordSchema->fields + sizeof(FieldStruct) * i);
                    json_t *arr_element = json_array_get(value, i);
                    const char *ele_key;
                    json_t *ele_value;

                    json_object_foreach(arr_element, ele_key, ele_value) {
                        if (0 == strcmp(ele_key, "name")) {
                            tstrncpy(field->name,
                                    json_string_value(ele_value),
                                    FIELD_NAME_LEN-1);
                        } else if (0 == strcmp(ele_key, "type")) {
                            int ele_type = json_typeof(ele_value);

                            if (JSON_STRING == ele_type) {
                                field->type =
                                    typeStrToType(json_string_value(ele_value));
                            } else if (JSON_ARRAY == ele_type) {
                                size_t ele_size = json_array_size(ele_value);

                                for(size_t ele_i = 0; ele_i < ele_size;
                                        ele_i ++) {
                                    json_t *arr_type_ele =
                                        json_array_get(ele_value, ele_i);

                                    if (JSON_STRING == json_typeof(arr_type_ele)) {
                                        const char *arr_type_ele_str =
                                            json_string_value(arr_type_ele);

                                        if(0 == strcmp(arr_type_ele_str,
                                                            "null")) {
                                            field->nullable = true;
                                        } else {
                                            field->type = typeStrToType(arr_type_ele_str);
                                        }
                                    } else if (JSON_OBJECT ==
                                            json_typeof(arr_type_ele)) {
                                        const char *arr_type_ele_key;
                                        json_t *arr_type_ele_value;

                                        json_object_foreach(arr_type_ele,
                                                arr_type_ele_key,
                                                arr_type_ele_value) {
                                            if (JSON_STRING ==
                                                    json_typeof(arr_type_ele_value)) {
                                                const char *arr_type_ele_value_str =
                                                    json_string_value(arr_type_ele_value);
                                                if(0 == strcmp(arr_type_ele_value_str,
                                                            "null")) {
                                                    field->nullable = true;
                                                } else {
                                                    field->type =
                                                        typeStrToType(arr_type_ele_value_str);
                                                }
                                            }
                                        }
                                    } else {
                                        errorPrint("%s", "Error: not supported!\n");
                                    }
                                }
                            } else if (JSON_OBJECT == ele_type) {
                                const char *obj_key;
                                json_t *obj_value;

                                json_object_foreach(ele_value, obj_key, obj_value) {
                                    if (0 == strcmp(obj_key, "type")) {
                                        int obj_value_type = json_typeof(obj_value);
                                        if (JSON_STRING == obj_value_type) {
                                            const char *obj_value_str = json_string_value(obj_value);
                                            if (0 == strcmp(obj_value_str, "array")) {
                                                field->type = TSDB_DATA_TYPE_NULL;
                                                field->is_array = true;
                                            } else {
                                                field->type =
                                                    typeStrToType(obj_value_str);
                                            }
                                        } else if (JSON_OBJECT == obj_value_type) {
                                            const char *field_key;
                                            json_t *field_value;

                                            json_object_foreach(obj_value, field_key, field_value) {
                                                if (JSON_STRING == json_typeof(field_value)) {
                                                    const char *field_value_str =
                                                        json_string_value(field_value);
                                                    field->type =
                                                        typeStrToType(field_value_str);
                                                } else {
                                                    field->nullable = true;
                                                }
                                            }
                                        }
                                    } else if (0 == strcmp(obj_key, "items")) {
                                        int obj_value_items = json_typeof(obj_value);
                                        if (JSON_STRING == obj_value_items) {
                                            field->is_array = true;
                                            const char *obj_value_str =
                                                json_string_value(obj_value);
                                            field->array_type = typeStrToType(obj_value_str);
                                        } else if (JSON_OBJECT == obj_value_items) {
                                            const char *item_key;
                                            json_t *item_value;

                                            json_object_foreach(obj_value, item_key, item_value) {
                                                if (JSON_STRING == json_typeof(item_value)) {
                                                    const char *item_value_str =
                                                        json_string_value(item_value);
                                                    field->array_type =
                                                        typeStrToType(item_value_str);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                errorPrint("%s() LN%d, fields have no array\n",
                        __func__, __LINE__);
                freeRecordSchema(recordSchema);
                return NULL;
            }

            break;
        }
    }

    return recordSchema;
}

static avro_value_iface_t* prepareAvroWface(
        char *avroFilename,
        char *jsonSchema,
        avro_schema_t *schema,
        RecordSchema **recordSchema,
        avro_file_writer_t *db
        )
{
    assert(avroFilename);
    if (avro_schema_from_json_length(jsonSchema, strlen(jsonSchema), schema)) {
        errorPrint("%s() LN%d, Unable to parse:\n%s \nto schema\nerror message: %s\n",
                __func__, __LINE__, jsonSchema, avro_strerror());
        exit(EXIT_FAILURE);
    }

    json_t *json_root = load_json(jsonSchema);
    verbosePrint("\n%s() LN%d\n === Schema parsed:\n", __func__, __LINE__);

    if (json_root) {
        if (g_args.verbose_print) {
            print_json(json_root);
        }

        *recordSchema = parse_json_to_recordschema(json_root);
        if (NULL == *recordSchema) {
            errorPrint("%s", "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
    } else {
        errorPrint("json:\n%s\n can't be parsed by jansson\n", jsonSchema);
        exit(EXIT_FAILURE);
    }

    int rval = avro_file_writer_create_with_codec
        (avroFilename, *schema, db, g_avro_codec[g_args.avro_codec], 0);
    if (rval) {
        errorPrint("There was an error creating %s. reason: %s\n",
                avroFilename, avro_strerror());
        exit(EXIT_FAILURE);
    }

    avro_value_iface_t* wface =
        avro_generic_class_from_schema(*schema);

    return wface;
}

static int dumpCreateTableClauseAvro(
        char *dumpFilename,
        TableDef *tableDes, int numOfCols,
        char* dbName) {
    assert(dumpFilename);
    // {
    // "type": "record",
    // "name": "_ntb",
    // "namespace": "dbname",
    // "fields": [
    //      {
    //      "name": "tbname",
    //      "type": ["null","string"]
    //      },
    //      {
    //      "name": "sql",
    //      "type": ["null","string"]
    //      }]
    //  }
    char *jsonSchema = (char *)calloc(1,
            17 + TSDB_DB_NAME_LEN               /* dbname section */
            + 17                                /* type: record */
            + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
            + 120);                              /* fields section */
    if (jsonSchema == NULL) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    sprintf(jsonSchema,
            "{\"type\":\"record\",\"name\":\"%s.%s\",\"fields\":[{\"name\":\"tbname\",\"type\":[\"null\",\"string\"]},{\"name\":\"sql\",\"type\":[\"null\",\"string\"]}]}",
            dbName, "_ntb");

    char *sqlstr = calloc(1, COMMAND_SIZE);
    assert(sqlstr);

    convertTableDesToSql(dbName, tableDes, &sqlstr);

    debugPrint("%s() LN%d, write string: %s\n", __func__, __LINE__, sqlstr);

    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(
            dumpFilename,
            jsonSchema, &schema, &recordSchema, &db);

    avro_value_t record;
    avro_generic_value_new(wface, &record);

    avro_value_t value, branch;
    if (0 != avro_value_get_by_name(
                &record, "tbname", &value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                __func__, __LINE__, "sql");
    }

    avro_value_set_branch(&value, 1, &branch);
    avro_value_set_string(&branch, tableDes->name);

    if (0 != avro_value_get_by_name(
                &record, "sql", &value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                __func__, __LINE__, "sql");
    }

    avro_value_set_branch(&value, 1, &branch);
    avro_value_set_string(&branch, sqlstr);

    if (0 != avro_file_writer_append_value(db, &record)) {
        errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                __func__, __LINE__,
                avro_strerror());
    }

    avro_value_decref(&record);
    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    free(jsonSchema);
    free(sqlstr);

    return 0;
}

static int dumpCreateTableClause(TableDef *tableDes, int numOfCols,
        FILE *fp, char* dbName) {
    char *sqlstr = calloc(1, COMMAND_SIZE);
    assert(sqlstr);

    convertTableDesToSql(dbName, tableDes, &sqlstr);

    debugPrint("%s() LN%d, write string: %s\n", __func__, __LINE__, sqlstr);
    int ret = fprintf(fp, "%s\n\n", sqlstr);

    free(sqlstr);

    return ret;
}

static int dumpStableClasuse(TAOS *taos, SDbInfo *dbInfo, char *stbName, FILE *fp)
{
    uint64_t sizeOfTableDes =
        (uint64_t)(sizeof(TableDef) + sizeof(ColDes) * TSDB_MAX_COLUMNS);

    TableDef *tableDes = (TableDef *)calloc(1, sizeOfTableDes);
    if (NULL == tableDes) {
        errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                __func__, __LINE__, sizeOfTableDes);
        exit(-1);
    }

    int colCount = getTableDes(taos, dbInfo->name,
            stbName, tableDes, true);

    if (colCount < 0) {
        free(tableDes);
        errorPrint("%s() LN%d, failed to get stable[%s] schema\n",
               __func__, __LINE__, stbName);
        exit(-1);
    }

    dumpCreateTableClause(tableDes, colCount, fp, dbInfo->name);

    free(tableDes);

    return 0;
}

static void dumpCreateDbClause(
        SDbInfo *dbInfo, bool isDumpProperty, FILE *fp) {
    char sqlstr[TSDB_MAX_SQL_LEN] = {0};

    char *pstr = sqlstr;
    pstr += sprintf(pstr, "CREATE DATABASE IF NOT EXISTS %s ", dbInfo->name);
    if (isDumpProperty) {
        pstr += sprintf(pstr,
                "REPLICA %d QUORUM %d DAYS %d KEEP %s CACHE %d BLOCKS %d MINROWS %d MAXROWS %d FSYNC %d CACHELAST %d COMP %d PRECISION '%s' UPDATE %d",
                dbInfo->replica, dbInfo->quorum, dbInfo->days,
                dbInfo->keeplist,
                dbInfo->cache,
                dbInfo->blocks, dbInfo->minrows, dbInfo->maxrows,
                dbInfo->fsync,
                dbInfo->cachelast,
                dbInfo->comp, dbInfo->precision, dbInfo->update);
    }

    pstr += sprintf(pstr, ";");
    fprintf(fp, "%s\n\n", sqlstr);
}

static FILE* openDumpInFile(char *fptr) {
    wordexp_t full_path;

    if (wordexp(fptr, &full_path, 0) != 0) {
        errorPrint("illegal file name: %s\n", fptr);
        return NULL;
    }

    char *fname = full_path.we_wordv[0];

    FILE *f = NULL;
    if ((fname) && (strlen(fname) > 0)) {
        f = fopen(fname, "r");
        if (f == NULL) {
            errorPrint("Failed to open file %s\n", fname);
        }
    }

    wordfree(&full_path);
    return f;
}

static uint64_t getFilesNum(char *ext)
{
    uint64_t count = 0;

    int namelen, extlen;
    struct dirent *pDirent;
    DIR *pDir;

    extlen = strlen(ext);

    bool isSql = (0 == strcmp(ext, "sql"));

    pDir = opendir(g_args.inpath);
    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen (pDirent->d_name);

            if (namelen > extlen) {
                if (strcmp (ext, &(pDirent->d_name[namelen - extlen])) == 0) {
                    if (isSql) {
                        if (0 == strcmp(pDirent->d_name, "dbs.sql")) {
                            continue;
                        }
                    }
                    verbosePrint("%s found\n", pDirent->d_name);
                    count ++;
                }
            }
        }
        closedir (pDir);
    }

    debugPrint("%"PRId64" .%s files found!\n", count, ext);
    return count;
}

static void freeFileList(enum enWHICH which, int64_t count)
{
    char **fileList = NULL;

    switch(which) {
        case WHICH_AVRO_DATA:
            fileList = g_tsDumpInAvroFiles;
            break;

        case WHICH_AVRO_TBTAGS:
            fileList = g_tsDumpInAvroTagsTbs;
            break;

        case WHICH_AVRO_NTB:
            fileList = g_tsDumpInAvroNtbs;
            break;

        case WHICH_SQL:
            fileList = g_tsDumpInSqlFiles;
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, which);
            return;
    }

    for (int64_t i = 0; i < count; i++) {
        tfree(fileList[i]);
    }
    tfree(fileList);
}

static enum enWHICH createDumpinList(char *ext, int64_t count)
{
    enum enWHICH which = WHICH_UNKNOWN;
    if (0 == strcmp(ext, "sql")) {
        which = WHICH_SQL;
    } else if (0 == strncmp(ext, "avro-ntb",
                strlen("avro-ntb"))) {
        which = WHICH_AVRO_NTB;
    } else if (0 == strncmp(ext, "avro-tbtags",
                strlen("avro-tbtags"))) {
        which = WHICH_AVRO_TBTAGS;
    } else if (0 == strncmp(ext, "avro", strlen("avro"))) {
        which = WHICH_AVRO_DATA;
    }

    switch (which) {
        case WHICH_SQL:
            g_tsDumpInSqlFiles = (char **)calloc(count, sizeof(char *));
            assert(g_tsDumpInSqlFiles);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInSqlFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
                assert(g_tsDumpInSqlFiles[i]);
            }
            break;

        case WHICH_AVRO_NTB:
            g_tsDumpInAvroNtbs = (char **)calloc(count, sizeof(char *));
            assert(g_tsDumpInAvroNtbs);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInAvroNtbs[i] = calloc(1, MAX_FILE_NAME_LEN);
                assert(g_tsDumpInAvroNtbs[i]);
            }
            break;

        case WHICH_AVRO_TBTAGS:
            g_tsDumpInAvroTagsTbs = (char **)calloc(count, sizeof(char *));
            assert(g_tsDumpInAvroTagsTbs);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInAvroTagsTbs[i] = calloc(1, MAX_FILE_NAME_LEN);
                assert(g_tsDumpInAvroTagsTbs[i]);
            }
            break;

        case WHICH_AVRO_DATA:
            g_tsDumpInAvroFiles = (char **)calloc(count, sizeof(char *));
            assert(g_tsDumpInAvroFiles);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInAvroFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
                assert(g_tsDumpInAvroFiles[i]);
            }
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, which);
            return which;
    }

    int namelen, extlen;
    struct dirent *pDirent;
    DIR *pDir;

    extlen = strlen(ext);

    int64_t nCount = 0;
    pDir = opendir(g_args.inpath);
    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen (pDirent->d_name);

            if (namelen > extlen) {
                if (strcmp (ext, &(pDirent->d_name[namelen - extlen])) == 0) {
                    verbosePrint("%s found\n", pDirent->d_name);
                    switch (which) {
                        case WHICH_SQL:
                            if (0 == strcmp(pDirent->d_name, "dbs.sql")) {
                                continue;
                            }
                            tstrncpy(g_tsDumpInSqlFiles[nCount],
                                    pDirent->d_name,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        case WHICH_AVRO_NTB:
                            tstrncpy(g_tsDumpInAvroNtbs[nCount],
                                    pDirent->d_name,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        case WHICH_AVRO_TBTAGS:
                            tstrncpy(g_tsDumpInAvroTagsTbs[nCount],
                                    pDirent->d_name,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        case WHICH_AVRO_DATA:
                            tstrncpy(g_tsDumpInAvroFiles[nCount],
                                    pDirent->d_name,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        default:
                            errorPrint("%s() LN%d input mistake list: %d\n",
                                    __func__, __LINE__, which);
                            break;

                    }
                    nCount++;
                }
            }
        }
        closedir (pDir);
    }

    debugPrint("%"PRId64" .%s files filled to list!\n", nCount, ext);
    return which;
}

static int convertTbDesToJsonImpl(
        char *namespace, TableDef *tableDes,
        char **jsonSchema, bool isColumn)
{
    char *pstr = *jsonSchema;
    pstr += sprintf(pstr,
            "{\"type\":\"record\",\"name\":\"%s.%s\",\"fields\":[",
            namespace, (isColumn)?"_record":"_stb");

    // isCol: add one iterates for tbnmae
    // isTag: add two iterates for stbname and tbnmae
    int iterate = (isColumn)?(tableDes->columns+1):(tableDes->tags+2);

    char *colOrTag = (isColumn)?"col":"tag";

    for (int i = 0; i < iterate; i ++) {
        if (0 == i) {
            pstr += sprintf(pstr,
                    "{\"name\":\"%s\",\"type\":%s",
                    isColumn?"tbname":"stbname",
                    "[\"null\",\"string\"]");
        } else if (1 == i) {
            pstr += sprintf(pstr,
                    "{\"name\":\"%s\",\"type\":%s",
                    isColumn?"ts":"tbname",
                    isColumn?"\"long\"":"[\"null\",\"string\"]");
        } else {
            // isTag: pos is i-2 for stbname and tbnmae
            int pos = i +
                ((isColumn)?(-1):
                 (tableDes->columns-2));

            switch(tableDes->cols[pos].type) {
                case TSDB_DATA_TYPE_BINARY:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                            colOrTag, i-2, "string");
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                case TSDB_DATA_TYPE_JSON:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                            colOrTag, i-2, "bytes");
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                            colOrTag, i-2, "boolean");
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":\"%s\"",
                            colOrTag, i-2, "int");
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\", \"type\":\"%s\"",
                            colOrTag, i-2, "int");
                    break;

                case TSDB_DATA_TYPE_INT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\", \"type\":\"%s\"",
                            colOrTag, i-2, "int");
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":\"%s\"",
                            colOrTag, i-2, "long");
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":\"%s\"",
                            colOrTag, i-2, "float");
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":\"%s\"",
                            colOrTag, i-2, "double");
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":\"%s\"",
                            colOrTag, i-2, "long");
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":{\"type\":\"array\",\"items\":\"%s\"}",
                            colOrTag, i-2, "int");
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":{\"type\":\"array\",\"items\":\"%s\"}",
                            colOrTag, i-2, "int");
                    break;

                case TSDB_DATA_TYPE_UINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":{\"type\":\"array\",\"items\":\"%s\"}",
                            colOrTag, i-2, "int");
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    pstr += sprintf(pstr,
                            "{\"name\":\"%s%d\",\"type\":{\"type\":\"array\",\"items\":\"%s\"}",
                            colOrTag, i-2, "long");
                    break;

                default:
                    errorPrint("%s() LN%d, wrong type: %d\n",
                            __func__, __LINE__, tableDes->cols[pos].type);
                    break;
            }
        }

        if (i != (iterate-1)) {
            pstr += sprintf(pstr, "},");
        } else {
            pstr += sprintf(pstr, "}");
            break;
        }
    }

    pstr += sprintf(pstr, "]}");

    debugPrint("%s() LN%d, jsonSchema:\n %s\n",
            __func__, __LINE__, *jsonSchema);

    return 0;
}

static int convertTbTagsDesToJson(
        char *dbName, char *stbName, TableDef *tableDes,
        char **jsonSchema)
{
    // {
    // "type": "record",
    // "name": "_stb",
    // "namespace": "dbname",
    // "fields": [
    //      {
    //      "name": "stbname",
    //      "type": "string"
    //      },
    //      {
    //      "name": "tbname",
    //      "type": "string"
    //      },
    //      {
    //      "name": "tag0 name",
    //      "type": "long"
    //      },
    //      {
    //      "name": "tag1 name",
    //      "type": "int"
    //      },
    //      {
    //      "name": "tag2 name",
    //      "type": "float"
    //      },
    //      {
    //      "name": "tag3 name",
    //      "type": "boolean"
    //      },
    //      ...
    //      {
    //      "name": "tagl name",
    //      "type": {"type": "array", "items":"int"}
    //      },
    //      {
    //      "name": "tagm name",
    //      "type": ["null", "string"]
    //      },
    //      {
    //      "name": "tagn name",
    //      "type": ["null", "bytes"]}
    //      }
    // ]
    // }
    *jsonSchema = (char *)calloc(1,
            17 + TSDB_DB_NAME_LEN               /* dbname section */
            + 17                                /* type: record */
            + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
            + 10                                /* fields section */
            + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
            + 11 + TSDB_TABLE_NAME_LEN          /* tbname section */
            + (TSDB_COL_NAME_LEN + 50) * tableDes->tags + 4);    /* fields section */
    if (*jsonSchema == NULL) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    return convertTbDesToJsonImpl(dbName, tableDes, jsonSchema, false);
}

static int convertTbDesToJson(
        char *dbName, char *tbName, TableDef *tableDes, int colCount,
        char **jsonSchema)
{
    // {
    // "type": "record",
    // "name": "tbname",
    // "namespace": "dbname",
    // "fields": [
    //      {
    //      "name": "col0 name",
    //      "type": "long"
    //      },
    //      {
    //      "name": "col1 name",
    //      "type": "int"
    //      },
    //      {
    //      "name": "col2 name",
    //      "type": "float"
    //      },
    //      {
    //      "name": "col3 name",
    //      "type": ["null",boolean"]
    //      },
    //      ...
    //      {
    //      "name": "coll name",
    //      "type": {"type": "array", "items":"int"}
    //      },
    //      {
    //      "name": "colm name",
    //      "type": ["null","string"]
    //      },
    //      {
    //      "name": "coln name",
    //      "type": ["null","bytes"]}
    //      }
    // ]
    // }
    *jsonSchema = (char *)calloc(1,
            17 + TSDB_DB_NAME_LEN               /* dbname section */
            + 17                                /* type: record */
            + 11 + TSDB_TABLE_NAME_LEN          /* tbname section */
            + 10                                /* fields section */
            + (TSDB_COL_NAME_LEN + 50) * colCount + 4);    /* fields section */
    if (*jsonSchema == NULL) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    return convertTbDesToJsonImpl(dbName, tableDes, jsonSchema, true);
}

static void print_json_indent(int indent) {
    int i;
    for (i = 0; i < indent; i++) {
        putchar(' ');
    }
}

static void print_json_object(json_t *element, int indent) {
    size_t size;
    const char *key;
    json_t *value;

    print_json_indent(indent);
    size = json_object_size(element);

    printf("JSON Object of %lld pair%s:\n", (long long)size, json_plural(size));
    json_object_foreach(element, key, value) {
        print_json_indent(indent + 2);
        printf("JSON Key: \"%s\"\n", key);
        print_json_aux(value, indent + 2);
    }
}

static void print_json_array(json_t *element, int indent) {
    size_t i;
    size_t size = json_array_size(element);
    print_json_indent(indent);

    printf("JSON Array of %lld element%s:\n", (long long)size, json_plural(size));
    for (i = 0; i < size; i++) {
        print_json_aux(json_array_get(element, i), indent + 2);
    }
}

static void print_json_string(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON String: \"%s\"\n", json_string_value(element));
}

static void print_json_integer(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON Integer: \"%" JSON_INTEGER_FORMAT "\"\n", json_integer_value(element));
}

static void print_json_real(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON Real: %f\n", json_real_value(element));
}

static void print_json_true(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON True\n");
}

static void print_json_false(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON False\n");
}

static void print_json_null(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON Null\n");
}

static void print_json_aux(json_t *element, int indent)
{
    switch(json_typeof(element)) {
        case JSON_OBJECT:
            print_json_object(element, indent);
            break;

        case JSON_ARRAY:
            print_json_array(element, indent);
            break;

        case JSON_STRING:
            print_json_string(element, indent);
            break;

        case JSON_INTEGER:
            print_json_integer(element, indent);
            break;

        case JSON_REAL:
            print_json_real(element, indent);
            break;

        case JSON_TRUE:
            print_json_true(element, indent);
            break;

        case JSON_FALSE:
            print_json_false(element, indent);
            break;

        case JSON_NULL:
            print_json_null(element, indent);
            break;

        default:
            errorPrint("Unrecongnized JSON type %d\n", json_typeof(element));
    }
}

static void printDotOrX(int64_t count, bool *printDot)
{
    if (0 == (count % g_args.data_batch)) {
        if (*printDot) {
            putchar('.');
            *printDot = false;
        } else {
            putchar('x');
            *printDot = true;
        }
    }

    fflush(stdout);
}

static int64_t writeResultToAvro(
        char *avroFilename,
        char *tbName,
        char *jsonSchema,
        TAOS_RES *res)
{
    assert(avroFilename);
    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(
            avroFilename,
            jsonSchema, &schema, &recordSchema, &db);

    TAOS_ROW row = NULL;

    int numFields = taos_field_count(res);
    assert(numFields > 0);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    int64_t success = 0;
    int64_t failed = 0;
    int64_t count = 0;

    bool printDot = true;
    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t *length = taos_fetch_lengths(res);

        printDotOrX(count, &printDot);
        count++;

        avro_value_t record;
        avro_generic_value_new(wface, &record);

        avro_value_t value, branch;

        if (0 != avro_value_get_by_name(
                    &record, "tbname", &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(tbname) failed\n",
                    __func__, __LINE__);
            continue;
        }
        avro_value_set_branch(&value, 1, &branch);
        avro_value_set_string(&branch, tbName);

        for (int col = 0; col < numFields; col++) {
            char tmpBuf[65] = {0};

            if (0 == col) {
                sprintf(tmpBuf, "%s", fields[col].name);
            } else {
                sprintf(tmpBuf, "col%d", col-1);
            }

            if (0 != avro_value_get_by_name(
                        &record,
                        tmpBuf,
                        &value, NULL)) {
                errorPrint("%s() LN%d, avro_value_get_by_name(%s) failed\n",
                        __func__, __LINE__, fields[col].name);
                continue;
            }

            avro_value_t firsthalf, secondhalf;
            uint8_t u8Temp = 0;
            uint16_t u16Temp = 0;
            uint32_t u32Temp = 0;
            uint64_t u64Temp = 0;

            switch (fields[col].type) {
                case TSDB_DATA_TYPE_BOOL:
                    if (NULL == row[col]) {
                        avro_value_set_branch(&value, 0, &branch);
                        verbosePrint("%s() LN%d, before set_bool() null\n",
                                __func__, __LINE__);
                        avro_value_set_null(&branch);
                    } else {
                        avro_value_set_branch(&value, 1, &branch);
                        char tmp = *(char*)row[col];
                        verbosePrint("%s() LN%d, before set_bool() tmp=%d\n",
                                __func__, __LINE__, (int)tmp);
                        avro_value_set_boolean(&branch, (tmp)?1:0);
                    }
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_TINYINT_NULL);
                    } else {
                        avro_value_set_int(&value, *((int8_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_SMALLINT_NULL);
                    } else {
                        avro_value_set_int(&value, *((int16_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_INT:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_INT_NULL);
                    } else {
                        avro_value_set_int(&value, *((int32_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    if (NULL == row[col]) {
                        u8Temp = TSDB_DATA_UTINYINT_NULL;
                    } else {
                        u8Temp = *((uint8_t *)row[col]);
                    }

                    int8_t n8tmp = (int8_t)(u8Temp - SCHAR_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n8tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, (int32_t)n8tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SCHAR_MAX);
                    debugPrint("second half is: %d\n", (int32_t)SCHAR_MAX);

                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    if (NULL == row[col]) {
                        u16Temp = TSDB_DATA_USMALLINT_NULL;
                    } else {
                        u16Temp = *((uint16_t *)row[col]);
                    }

                    int16_t n16tmp = (int16_t)(u16Temp - SHRT_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n16tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, (int32_t)n16tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SHRT_MAX);
                    debugPrint("second half is: %d\n", (int32_t)SHRT_MAX);

                    break;

                case TSDB_DATA_TYPE_UINT:
                    if (NULL == row[col]) {
                        u32Temp = TSDB_DATA_UINT_NULL;
                    } else {
                        u32Temp = *((uint32_t *)row[col]);
                    }

                    int32_t n32tmp = (int32_t)(u32Temp - INT_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n32tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n32tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)INT_MAX);
                    debugPrint("second half is: %d\n", INT_MAX);

                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    if (NULL == row[col]) {
                        u64Temp = TSDB_DATA_UBIGINT_NULL;
                    } else {
                        u64Temp = *((uint64_t *)row[col]);
                    }

                    int64_t n64tmp = (int64_t)(u64Temp - LONG_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_long(&firsthalf, n64tmp);
                    debugPrint("%s() LN%d, first half is: %"PRId64", ",
                            __func__, __LINE__, n64tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_long(&secondhalf, LONG_MAX);
                    debugPrint("second half is: %"PRId64"\n", (int64_t) LONG_MAX);

                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    if (NULL == row[col]) {
                        avro_value_set_long(&value, TSDB_DATA_BIGINT_NULL);
                    } else {
                        avro_value_set_long(&value, *((int64_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (NULL == row[col]) {
                        avro_value_set_float(&value, TSDB_DATA_FLOAT_NULL);
                    } else {
                        avro_value_set_float(&value, GET_FLOAT_VAL(row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    if (NULL == row[col]) {
                        avro_value_set_double(&value, TSDB_DATA_DOUBLE_NULL);
                    } else {
                        avro_value_set_double(&value, GET_DOUBLE_VAL(row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_BINARY:
                    if (NULL == row[col]) {
                        avro_value_set_branch(&value, 0, &branch);
                        avro_value_set_null(&branch);
                    } else {
                        avro_value_set_branch(&value, 1, &branch);
                        char *binTemp = calloc(1, 1+fields[col].bytes);
                        assert(binTemp);
                        strncpy(binTemp, (char*)row[col],
                                min(fields[col].bytes, length[col]));
                        *((char*)row[col] + fields[col].bytes) = '\0';
                        avro_value_set_string(&branch, binTemp);
                        free(binTemp);
                    }
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                case TSDB_DATA_TYPE_JSON:
                    if (NULL == row[col]) {
                        avro_value_set_branch(&value, 0, &branch);
                        avro_value_set_null(&branch);
                    } else {
                        avro_value_set_branch(&value, 1, &branch);
                        avro_value_set_bytes(&branch, (void*)(row[col]),
                                length[col]);
                    }
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    if (NULL == row[col]) {
                        avro_value_set_long(&value, TSDB_DATA_BIGINT_NULL);
                    } else {
                        avro_value_set_long(&value, *((int64_t *)row[col]));
                    }
                    break;

                default:
                    break;
            }
        }

        if (0 != avro_file_writer_append_value(db, &record)) {
            errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                    __func__, __LINE__,
                    avro_strerror());
            failed --;
        } else {
            success ++;
        }
        avro_value_decref(&record);
    }

    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    return success;
}

void freeBindArray(char *bindArray, int elements)
{
    TAOS_BIND *bind;

    for (int j = 0; j < elements; j++) {
        bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * j));
        if ((TSDB_DATA_TYPE_BINARY != bind->buffer_type)
                && (TSDB_DATA_TYPE_NCHAR != bind->buffer_type)
                && (TSDB_DATA_TYPE_JSON != bind->buffer_type)) {
            tfree(bind->buffer);
        }
    }
}

static int dumpInAvroTbTagsImpl(
        TAOS *taos,
        char *namespace,
        avro_schema_t schema,
        avro_file_reader_t reader,
        RecordSchema *recordSchema)
{
    int64_t success = 0;
    int64_t failed = 0;

    char tbuf[COMMAND_SIZE] = {0};

    TableDef *tableDes = (TableDef *)calloc(1, sizeof(TableDef)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    assert(tableDes);

    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    while(!avro_file_reader_read_value(reader, &value)) {
        TAOS_STMT *stmt = taos_stmt_init(taos);
        if (NULL == stmt) {
            errorPrint("%s() LN%d, Failed to execute taos_stmt_init(). reason: %s\n",
                    __func__, __LINE__,
                    taos_stmt_errstr(NULL));
        }
        char *bindArray =
            calloc(1, sizeof(TAOS_BIND)*(recordSchema->num_fields-1));
        assert(bindArray);

        TAOS_BIND *bind;
        int is_null = 1;

        char *stbName = NULL;
        char *tbName = NULL;
        for (int i = 0; i < recordSchema->num_fields-1; i++) {
            avro_value_t field_value, field_branch;
            size_t size;

            int32_t  curr_sqlstr_len = 0;
            if (0 == i) {
                avro_value_get_by_name(&value, "stbname", &field_value, NULL);

                avro_value_get_current_branch(
                                        &field_value, &field_branch);
                avro_value_get_string(&field_branch,
                        (const char **)&stbName, &size);

                if ((NULL == tableDes->name)
                        || (0 != strcmp(tableDes->name, stbName))) {
                    getTableDes(taos, namespace,
                            stbName, tableDes, false);
                }

                avro_value_get_by_name(&value, "tbname", &field_value, NULL);
                avro_value_get_current_branch(
                                        &field_value, &field_branch);
                avro_value_get_string(&field_branch,
                        (const char **)&tbName, &size);

                curr_sqlstr_len = sprintf(tbuf,
                        "CREATE TABLE %s.`%s` USING %s.`%s` TAGS(?",
                        namespace, tbName, namespace, stbName);
                for (int j = 0; j < recordSchema->num_fields-3; j ++) {
                    curr_sqlstr_len += sprintf(tbuf + curr_sqlstr_len, ",?");
                }

                curr_sqlstr_len += sprintf(tbuf + curr_sqlstr_len, ")");

                debugPrint("%s() LN%d, command buffer: %s\n",
                        __func__, __LINE__, tbuf);

                if (0 != taos_stmt_prepare(stmt, tbuf, 0)) {
                    errorPrint("Failed to execute taos_stmt_prepare(). reason: %s\n",
                            taos_stmt_errstr(stmt));
                    failed ++;
                    continue;
                }
            } else {
                FieldStruct *field = (FieldStruct *)
                    (recordSchema->fields + sizeof(FieldStruct)*(i+1));
                if (0 == avro_value_get_by_name(
                            &value, field->name, &field_value, NULL)) {
                    bind = (TAOS_BIND *)((char *)bindArray
                            + (sizeof(TAOS_BIND)*(i-1)));
                    bind->is_null = NULL;
                    switch(tableDes->cols[tableDes->columns -1 + i].type) {
                        case TSDB_DATA_TYPE_BOOL:
                            {
                                avro_value_t bool_branch;
                                avro_value_get_current_branch(&field_value, &bool_branch);

                                if (0 == avro_value_get_null(&bool_branch)) {
                                    bind->is_null = &is_null;
                                    debugPrint2("%s | ", "null");
                                } else {
                                    int32_t *bl = malloc(sizeof(int32_t));
                                    assert(bl);

                                    avro_value_get_boolean(&bool_branch, bl);
                                    verbosePrint("%s() LN%d, *bl=%d\n",
                                            __func__, __LINE__, *bl);
                                    bind->buffer = (int8_t*)bl;
                                    debugPrint2("%s | ", (*bl)?"true":"false");
                                    bind->buffer_length = sizeof(int8_t);
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_TINYINT:
                            {
                                int32_t *n8 = malloc(sizeof(int32_t));
                                assert(n8);

                                avro_value_get_int(&field_value, n8);

                                verbosePrint("%s() LN%d: *n8=%d null=%d\n",
                                        __func__, __LINE__, (int8_t)*n8,
                                        (int8_t)TSDB_DATA_TINYINT_NULL);

                                if ((int8_t)TSDB_DATA_TINYINT_NULL == (int8_t)*n8) {
                                    debugPrint2("%s | ", "null");
                                    bind->is_null = &is_null;
                                    free(n8);
                                } else {
                                    debugPrint2("%d | ", *n8);
                                    bind->buffer_length = sizeof(int8_t);
                                    bind->buffer = (int8_t *)n8;
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_SMALLINT:
                            {
                                int32_t *n16 = malloc(sizeof(int32_t));
                                assert(n16);

                                avro_value_get_int(&field_value, n16);

                                verbosePrint("%s() LN%d: *n16=%d null=%d\n",
                                        __func__, __LINE__, (int16_t)*n16,
                                        (int16_t)TSDB_DATA_SMALLINT_NULL);

                                if ((int16_t)TSDB_DATA_SMALLINT_NULL == (int16_t)*n16) {
                                    debugPrint2("%s | ", "null");
                                    bind->is_null = &is_null;
                                    free(n16);
                                } else {
                                    debugPrint2("%d | ", *n16);
                                    bind->buffer_length = sizeof(int16_t);
                                    bind->buffer = (int32_t*)n16;
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_INT:
                            {
                                int32_t *n32 = malloc(sizeof(int32_t));
                                assert(n32);

                                avro_value_get_int(&field_value, n32);

                                verbosePrint("%s() LN%d: *n32=%d null=%d\n",
                                        __func__, __LINE__, *n32,
                                        (int32_t)TSDB_DATA_INT_NULL);

                                if ((int32_t)TSDB_DATA_INT_NULL == *n32) {
                                    debugPrint2("%s | ", "null");
                                    bind->is_null = &is_null;
                                    free(n32);
                                } else {
                                    debugPrint2("%d | ", *n32);
                                    bind->buffer_length = sizeof(int32_t);
                                    bind->buffer = n32;
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_BIGINT:
                            {
                                int64_t *n64 = malloc(sizeof(int64_t));
                                assert(n64);

                                avro_value_get_long(&field_value, n64);

                                verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
                                        __func__, __LINE__, *n64,
                                        (int64_t)TSDB_DATA_BIGINT_NULL);

                                if ((int64_t)TSDB_DATA_BIGINT_NULL == *n64) {
                                    debugPrint2("%s | ", "null");
                                    bind->is_null = &is_null;
                                    free(n64);
                                } else {
                                    debugPrint2("%"PRId64" | ", *n64);
                                    bind->buffer_length = sizeof(int64_t);
                                    bind->buffer = n64;
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_FLOAT:
                            {
                                float *f = malloc(sizeof(float));
                                assert(f);

                                avro_value_get_float(&field_value, f);
                                if (TSDB_DATA_FLOAT_NULL == *f) {
                                    debugPrint2("%s | ", "NULL");
                                    bind->is_null = &is_null;
                                } else {
                                    debugPrint2("%f | ", *f);
                                }
                                bind->buffer = f;
                                bind->buffer_length = sizeof(float);
                            }
                            break;

                        case TSDB_DATA_TYPE_DOUBLE:
                            {
                                double *dbl = malloc(sizeof(double));
                                assert(dbl);

                                avro_value_get_double(&field_value, dbl);
                                if (TSDB_DATA_DOUBLE_NULL == *dbl) {
                                    debugPrint2("%s | ", "NULL");
                                    bind->is_null = &is_null;
                                } else {
                                    debugPrint2("%f | ", *dbl);
                                }
                                bind->buffer = dbl;
                                bind->buffer_length = sizeof(double);
                            }
                            break;

                        case TSDB_DATA_TYPE_BINARY:
                            {
                                avro_value_t branch;
                                avro_value_get_current_branch(
                                        &field_value, &branch);

                                char *buf = NULL;
                                size_t bin_size;

                                avro_value_get_string(&branch,
                                        (const char **)&buf, &bin_size);

                                if (NULL == buf) {
                                    debugPrint2("%s | ", "NULL");
                                    bind->is_null = &is_null;
                                    bind->buffer_length = 0;
                                } else {
                                    debugPrint2("%s | ", (char *)buf);
                                    bind->buffer_length = strlen(buf);
                                }
                                bind->buffer = buf;
                            }
                            break;

                        case TSDB_DATA_TYPE_NCHAR:
                        case TSDB_DATA_TYPE_JSON:
                            {
                                size_t bytessize;
                                void *bytesbuf= NULL;

                                avro_value_t nchar_branch;
                                avro_value_get_current_branch(
                                        &field_value, &nchar_branch);

                                avro_value_get_bytes(&nchar_branch,
                                        (const void **)&bytesbuf, &bytessize);

                                if (NULL == bytesbuf) {
                                    debugPrint2("%s | ", "NULL");
                                    bind->is_null = &is_null;
                                    bind->buffer_length = 0;
                                } else {
                                    debugPrint2("%s | ", (char *)bytesbuf);
                                    bind->buffer_length = strlen((char*)bytesbuf);
                                }
                                bind->buffer = bytesbuf;
                            }
                            break;

                        case TSDB_DATA_TYPE_TIMESTAMP:
                            {
                                int64_t *n64 = malloc(sizeof(int64_t));
                                assert(n64);

                                avro_value_get_long(&field_value, n64);

                                verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
                                        __func__, __LINE__, *n64,
                                        (int64_t)TSDB_DATA_BIGINT_NULL);

                                if ((int64_t)TSDB_DATA_BIGINT_NULL == *n64) {
                                    debugPrint2("%s | ", "null");
                                    bind->is_null = &is_null;
                                    free(n64);
                                } else {
                                    debugPrint2("%"PRId64" | ", *n64);
                                    bind->buffer_length = sizeof(int64_t);
                                    bind->buffer = n64;
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_UTINYINT:
                            {
                                if (TSDB_DATA_TYPE_INT == field->array_type) {
                                    uint8_t *array_u8 = malloc(sizeof(uint8_t));
                                    assert(array_u8);
                                    *array_u8 = 0;

                                    size_t array_size;
                                    int32_t n32tmp;
                                    avro_value_get_size(&field_value, &array_size);

                                    debugPrint("%s() LN%d, array_size: %d\n",
                                            __func__, __LINE__, (int)array_size);
                                    for (size_t item = 0; item < array_size; item ++) {
                                        avro_value_t item_value;
                                        avro_value_get_by_index(&field_value, item,
                                                &item_value, NULL);
                                        avro_value_get_int(&item_value, &n32tmp);
                                        *array_u8 += (int8_t)n32tmp;
                                    }

                                    if (TSDB_DATA_UTINYINT_NULL == *array_u8) {
                                        debugPrint2("%s |", "null");
                                        bind->is_null = &is_null;
                                    } else {
                                        debugPrint2("%u | ", (uint32_t)*array_u8);
                                        bind->buffer_length = sizeof(uint8_t);
                                        bind->buffer = array_u8;
                                    }
                                } else {
                                    errorPrint("%s() LN%d mix type %s with int array",
                                            __func__, __LINE__,
                                            typeToStr(field->array_type));
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_USMALLINT:
                            {
                                if (TSDB_DATA_TYPE_INT == field->array_type) {
                                    uint16_t *array_u16 = malloc(sizeof(uint16_t));
                                    assert(array_u16);
                                    *array_u16 = 0;

                                    size_t array_size;
                                    int32_t n32tmp;
                                    avro_value_get_size(&field_value, &array_size);

                                    debugPrint("%s() LN%d, array_size: %d\n",
                                            __func__, __LINE__, (int)array_size);
                                    for (size_t item = 0; item < array_size; item ++) {
                                        avro_value_t item_value;
                                        avro_value_get_by_index(&field_value, item,
                                                &item_value, NULL);
                                        avro_value_get_int(&item_value, &n32tmp);
                                        *array_u16 += (int16_t)n32tmp;
                                    }

                                    if (TSDB_DATA_USMALLINT_NULL == *array_u16) {
                                        debugPrint2("%s |", "null");
                                        bind->is_null = &is_null;
                                    } else {
                                        debugPrint2("%u | ", (uint32_t)*array_u16);
                                        bind->buffer_length = sizeof(uint16_t);
                                        bind->buffer = array_u16;
                                    }
                                } else {
                                    errorPrint("%s() LN%d mix type %s with int array",
                                            __func__, __LINE__,
                                            typeToStr(field->array_type));
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_UINT:
                            {
                                if (TSDB_DATA_TYPE_INT == field->array_type) {
                                    uint32_t *array_u32 = malloc(sizeof(uint32_t));
                                    assert(array_u32);
                                    *array_u32 = 0;

                                    size_t array_size;
                                    int32_t n32tmp;
                                    avro_value_get_size(&field_value, &array_size);

                                    debugPrint("%s() LN%d, array_size: %d\n",
                                            __func__, __LINE__, (int)array_size);
                                    for (size_t item = 0; item < array_size; item ++) {
                                        avro_value_t item_value;
                                        avro_value_get_by_index(&field_value, item,
                                                &item_value, NULL);
                                        avro_value_get_int(&item_value, &n32tmp);
                                        *array_u32 += n32tmp;
                                        debugPrint("%s() LN%d, array index: %d, n32tmp: %d, array_u32: %u\n",
                                                __func__, __LINE__,
                                                (int)item, n32tmp,
                                                (uint32_t)*array_u32);
                                    }

                                    if (TSDB_DATA_UINT_NULL == *array_u32) {
                                        debugPrint2("%s |", "null");
                                        bind->is_null = &is_null;
                                    } else {
                                        debugPrint2("%u | ", (uint32_t)*array_u32);
                                        bind->buffer_length = sizeof(uint32_t);
                                        bind->buffer = array_u32;
                                    }
                                } else {
                                    errorPrint("%s() LN%d mix type %s with int array",
                                            __func__, __LINE__,
                                            typeToStr(field->array_type));
                                }
                            }
                            break;

                        case TSDB_DATA_TYPE_UBIGINT:
                            {
                                if (TSDB_DATA_TYPE_BIGINT == field->array_type) {
                                    uint64_t *array_u64 = malloc(sizeof(uint64_t));
                                    assert(array_u64);
                                    *array_u64 = 0;

                                    size_t array_size;
                                    int64_t n64tmp;
                                    avro_value_get_size(&field_value, &array_size);

                                    debugPrint("%s() LN%d, array_size: %d\n",
                                            __func__, __LINE__, (int)array_size);
                                    for (size_t item = 0; item < array_size; item ++) {
                                        avro_value_t item_value;
                                        avro_value_get_by_index(&field_value, item,
                                                &item_value, NULL);
                                        avro_value_get_long(&item_value, &n64tmp);
                                        *array_u64 += n64tmp;
                                        debugPrint("%s() LN%d, array index: %d, n64tmp: %"PRId64", array_u64: %"PRIu64"\n",
                                                __func__, __LINE__,
                                                (int)item, n64tmp,
                                                (uint64_t)*array_u64);
                                    }

                                    if (TSDB_DATA_UBIGINT_NULL == *array_u64) {
                                        debugPrint2("%s |", "null");
                                        bind->is_null = &is_null;
                                    } else {
                                        debugPrint2("%"PRIu64" | ", (uint64_t)*array_u64);
                                        bind->buffer_length = sizeof(uint64_t);
                                        bind->buffer = array_u64;
                                    }
                                } else {
                                    errorPrint("%s() LN%d mix type %s with int array",
                                            __func__, __LINE__,
                                            typeToStr(field->array_type));
                                }
                            }
                            break;

                        default:
                            errorPrint("%s() LN%d Unknown type: %d\n",
                                    __func__, __LINE__, field->type);
                            break;
                    }

                    if (TSDB_DATA_TYPE_JSON == tableDes->cols[tableDes->columns -1 + i].type) {
                        bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
                    } else {
                        bind->buffer_type = tableDes->cols[tableDes->columns -1 + i].type;
                    }
                    debugPrint("%s() LN%d, bind->buffer_type=%d\n",
                            __func__, __LINE__, bind->buffer_type);

                    bind->length = &bind->buffer_length;
                } else {
                    errorPrint("Failed to get value by name: %s\n",
                            field->name);
                }
            }
        }
        debugPrint2("%s", "\n");

        if (0 != taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray)) {
            errorPrint("%s() LN%d stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            freeBindArray(bindArray, recordSchema->num_fields-1);
            tfree(bindArray);
            failed++;
            taos_stmt_close(stmt);
            continue;
        }

        if (0 != taos_stmt_execute(stmt)) {
            errorPrint("%s() LN%d taos_stmt_execute() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            failed ++;
        } else {
            success ++;
        }

        freeBindArray(bindArray, recordSchema->num_fields-1);
        tfree(bindArray);
        taos_stmt_close(stmt);
    }

    avro_value_decref(&value);
    avro_value_iface_decref(value_class);

    freeTbDes(tableDes);

    if (failed)
        return failed;
    return success;

}

static int dumpInAvroNtbImpl(TAOS *taos,
        TAOS_STMT *stmt,
        char *namespace,
        avro_schema_t schema,
        avro_file_reader_t reader,
        RecordSchema *recordSchema)
{
    int64_t success = 0;
    int64_t failed = 0;

    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    while(!avro_file_reader_read_value(reader, &value)) {
        for (int i = 0; i < recordSchema->num_fields-1; i++) {
            avro_value_t field_value, field_branch;
            avro_value_get_by_name(&value, "sql", &field_value, NULL);

            size_t size;
            char *buf = NULL;

            avro_value_get_current_branch(
                    &field_value, &field_branch);
            avro_value_get_string(&field_branch, (const char **)&buf, &size);

            if (0 != taos_stmt_prepare(stmt, buf, 0)) {
                errorPrint("%s() LN%d, Failed to execute taos_stmt_prepare(%s). reason: %s\n",
                        __func__, __LINE__, buf,
                        taos_stmt_errstr(stmt));
                failed ++;
            } else if (0 != taos_stmt_execute(stmt)) {
                errorPrint("%s() LN%d taos_stmt_execute() failed! reason: %s\n",
                        __func__, __LINE__, taos_stmt_errstr(stmt));
                failed ++;
            }

            success ++;
        }
    }

    avro_value_decref(&value);
    avro_value_iface_decref(value_class);

    if (failed)
        return failed;
    return success;
}

static int dumpInAvroDataImpl(
        TAOS *taos,
        TAOS_STMT *stmt,
        char *namespace,
        avro_schema_t schema,
        avro_file_reader_t reader,
        RecordSchema *recordSchema)
{
    TableDef *tableDes = (TableDef *)calloc(1, sizeof(TableDef)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    assert(tableDes);

    char *stmtBuffer = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    assert(stmtBuffer);

    char *pstr = stmtBuffer;
    pstr += sprintf(pstr, "INSERT INTO ? VALUES(?");

    int onlyCol = 1; // at least timestamp
    for (int col = 1; col < recordSchema->num_fields-1; col++) {
        pstr += sprintf(pstr, ",?");
        onlyCol ++;
    }
    pstr += sprintf(pstr, ")");
    debugPrint("%s() LN%d, stmt buffer: %s\n",
            __func__, __LINE__, stmtBuffer);

    if (0 != taos_stmt_prepare(stmt, stmtBuffer, 0)) {
        errorPrint("Failed to execute taos_stmt_prepare(). reason: %s\n",
                taos_stmt_errstr(stmt));

        free(stmtBuffer);
        return -1;
    }

    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    char *bindArray =
            malloc(sizeof(TAOS_BIND) * onlyCol);
    assert(bindArray);

    int64_t success = 0;
    int64_t failed = 0;
    int64_t count = 0;

    int stmt_count = 0;
    bool printDot = true;
    while(!avro_file_reader_read_value(reader, &value)) {
        avro_value_t tbname_value, tbname_branch;

        avro_value_get_by_name(&value, "tbname", &tbname_value, NULL);
        avro_value_get_current_branch(&tbname_value, &tbname_branch);

        char *tbName= NULL;
        size_t tbname_size;
        avro_value_get_string(&tbname_branch,
                (const char **)&tbName, &tbname_size);

        char *escapedTbName = calloc(1, strlen(tbName) + 3);
        assert(escapedTbName);
        sprintf(escapedTbName, "`%s`", tbName);

        if (0 != taos_stmt_set_tbname(stmt, escapedTbName)) {
            errorPrint("Failed to execute taos_stmt_set_tbname(%s). reason: %s\n",
                    escapedTbName, taos_stmt_errstr(stmt));
            free(escapedTbName);
            continue;
        }
        free(escapedTbName);

        if ((NULL == tableDes->name)
                || (0 != strcmp(tableDes->name, tbName))) {
            getTableDes(taos, namespace,
                tbName, tableDes, true);
        }

        printDotOrX(count, &printDot);
        count++;

        memset(bindArray, 0, sizeof(TAOS_BIND) * onlyCol);
        TAOS_BIND *bind;

        int is_null = 1;
        for (int i = 0; i < recordSchema->num_fields-1; i++) {
            bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * i));

            avro_value_t field_value;

            FieldStruct *field =
                (FieldStruct *)(recordSchema->fields
                        + sizeof(FieldStruct)*(i+1));

            bind->is_null = NULL;
            if (0 == i) {
                int64_t *ts = malloc(sizeof(int64_t));
                assert(ts);

                avro_value_get_by_name(&value,
                        field->name, &field_value, NULL);
                avro_value_get_long(&field_value, ts);

                debugPrint2("%"PRId64" | ", *ts);
                bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                bind->buffer_length = sizeof(int64_t);
                bind->buffer = ts;
                bind->length = &bind->buffer_length;
            } else if (0 == avro_value_get_by_name(
                        &value, field->name, &field_value, NULL)) {
                switch(tableDes->cols[i].type) {
                    case TSDB_DATA_TYPE_INT:
                        {
                            int32_t *n32 = malloc(sizeof(int32_t));
                            assert(n32);

                            avro_value_get_int(&field_value, n32);

                            if ((int32_t)TSDB_DATA_INT_NULL == *n32) {
                                debugPrint2("%s | ", "null");
                                bind->is_null = &is_null;
                                free(n32);
                            } else {
                                debugPrint2("%d | ", *n32);
                                bind->buffer_length = sizeof(int32_t);
                                bind->buffer = n32;
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_TINYINT:
                        {
                            int32_t *n8 = malloc(sizeof(int32_t));
                            assert(n8);

                            avro_value_get_int(&field_value, n8);

                            verbosePrint("%s() LN%d: *n8=%d null=%d\n",
                                    __func__, __LINE__, *n8,
                                    (int8_t)TSDB_DATA_TINYINT_NULL);

                            if ((int8_t)TSDB_DATA_TINYINT_NULL == *n8) {
                                debugPrint2("%s | ", "null");
                                bind->is_null = &is_null;
                                free(n8);
                            } else {
                                debugPrint2("%d | ", *n8);
                                bind->buffer_length = sizeof(int8_t);
                                bind->buffer = (int8_t *)n8;
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_SMALLINT:
                        {
                            int32_t *n16 = malloc(sizeof(int32_t));
                            assert(n16);

                            avro_value_get_int(&field_value, n16);

                            verbosePrint("%s() LN%d: *n16=%d null=%d\n",
                                    __func__, __LINE__, *n16,
                                    (int16_t)TSDB_DATA_SMALLINT_NULL);

                            if ((int16_t)TSDB_DATA_SMALLINT_NULL == *n16) {
                                debugPrint2("%s | ", "null");
                                bind->is_null = &is_null;
                                free(n16);
                            } else {
                                debugPrint2("%d | ", *n16);
                                bind->buffer_length = sizeof(int16_t);
                                bind->buffer = (int32_t*)n16;
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_BIGINT:
                        {
                            int64_t *n64 = malloc(sizeof(int64_t));
                            assert(n64);

                            avro_value_get_long(&field_value, n64);

                            verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
                                    __func__, __LINE__, *n64,
                                    (int64_t)TSDB_DATA_BIGINT_NULL);

                            if ((int64_t)TSDB_DATA_BIGINT_NULL == *n64) {
                                debugPrint2("%s | ", "null");
                                bind->is_null = &is_null;
                                free(n64);
                            } else {
                                debugPrint2("%"PRId64" | ", *n64);
                                bind->buffer_length = sizeof(int64_t);
                                bind->buffer = n64;
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_TIMESTAMP:
                        {
                            int64_t *n64 = malloc(sizeof(int64_t));
                            assert(n64);

                            avro_value_get_long(&field_value, n64);
                            debugPrint2("%"PRId64" | ", *n64);
                            bind->buffer_length = sizeof(int64_t);
                            bind->buffer = n64;
                        }
                        break;

                    case TSDB_DATA_TYPE_FLOAT:
                        {
                            float *f = malloc(sizeof(float));
                            assert(f);

                            avro_value_get_float(&field_value, f);
                            if (TSDB_DATA_FLOAT_NULL == *f) {
                                debugPrint2("%s | ", "NULL");
                                bind->is_null = &is_null;
                            } else {
                                debugPrint2("%f | ", *f);
                            }
                            bind->buffer = f;
                            bind->buffer_length = sizeof(float);
                        }
                        break;

                    case TSDB_DATA_TYPE_DOUBLE:
                        {
                            double *dbl = malloc(sizeof(double));
                            assert(dbl);

                            avro_value_get_double(&field_value, dbl);
                            if (TSDB_DATA_DOUBLE_NULL == *dbl) {
                                debugPrint2("%s | ", "NULL");
                                bind->is_null = &is_null;
                            } else {
                                debugPrint2("%f | ", *dbl);
                            }
                            bind->buffer = dbl;
                            bind->buffer_length = sizeof(double);
                        }
                        break;

                    case TSDB_DATA_TYPE_BINARY:
                        {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value, &branch);

                            char *buf = NULL;
                            size_t size;
                            avro_value_get_string(&branch, (const char **)&buf, &size);

                            if (NULL == buf) {
                                debugPrint2("%s | ", "NULL");
                                bind->is_null = &is_null;
                            } else {
                                debugPrint2("%s | ", (char *)buf);
                                bind->buffer_length = strlen(buf);
                            }
                            bind->buffer = buf;
                        }
                        break;

                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_NCHAR:
                        {
                            size_t bytessize;
                            void *bytesbuf = NULL;

                            avro_value_t nchar_branch;
                            avro_value_get_current_branch(&field_value, &nchar_branch);

                            avro_value_get_bytes(&nchar_branch,
                                    (const void **)&bytesbuf, &bytessize);
                            if (NULL == bytesbuf) {
                                debugPrint2("%s | ", "NULL");
                                bind->is_null = &is_null;
                            } else {
                                debugPrint2("%s | ", (char*)bytesbuf);
                                bind->buffer_length = strlen((char*)bytesbuf);
                            }
                            bind->buffer = bytesbuf;
                        }
                        break;

                    case TSDB_DATA_TYPE_BOOL:
                        {
                            avro_value_t bool_branch;
                            avro_value_get_current_branch(&field_value, &bool_branch);
                            if (0 == avro_value_get_null(&bool_branch)) {
                                bind->is_null = &is_null;
                                debugPrint2("%s | ", "null");
                            } else {
                                int32_t *bl = malloc(sizeof(int32_t));
                                assert(bl);

                                avro_value_get_boolean(&bool_branch, bl);
                                verbosePrint("%s() LN%d, *bl=%d\n",
                                        __func__, __LINE__, *bl);
                                debugPrint2("%s | ", (*bl)?"true":"false");
                                bind->buffer_length = sizeof(int8_t);
                                bind->buffer = (int8_t*)bl;
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_UINT:
                        {
                            if (TSDB_DATA_TYPE_INT == field->array_type) {
                                uint32_t *array_u32 = malloc(sizeof(uint32_t));
                                assert(array_u32);
                                *array_u32 = 0;

                                size_t array_size;
                                int32_t n32tmp;
                                avro_value_get_size(&field_value, &array_size);

                                debugPrint("%s() LN%d, array_size: %d\n",
                                        __func__, __LINE__, (int)array_size);
                                for (size_t item = 0; item < array_size; item ++) {
                                    avro_value_t item_value;
                                    avro_value_get_by_index(&field_value, item,
                                            &item_value, NULL);
                                    avro_value_get_int(&item_value, &n32tmp);
                                    *array_u32 += n32tmp;
                                    debugPrint("%s() LN%d, array index: %d, n32tmp: %d, array_u32: %u\n",
                                            __func__, __LINE__,
                                            (int)item, n32tmp,
                                            (uint32_t)*array_u32);
                                }
                                debugPrint2("%u | ", (uint32_t)*array_u32);
                                bind->buffer_length = sizeof(uint32_t);
                                bind->buffer = array_u32;
                            } else {
                                errorPrint("%s() LN%d mix type %s with int array",
                                        __func__, __LINE__,
                                        typeToStr(field->array_type));
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_UTINYINT:
                        {
                            if (TSDB_DATA_TYPE_INT == field->array_type) {
                                uint8_t *array_u8 = malloc(sizeof(uint8_t));
                                assert(array_u8);
                                *array_u8 = 0;

                                size_t array_size;
                                int32_t n32tmp;
                                avro_value_get_size(&field_value, &array_size);

                                debugPrint("%s() LN%d, array_size: %d\n",
                                        __func__, __LINE__, (int)array_size);
                                for (size_t item = 0; item < array_size; item ++) {
                                    avro_value_t item_value;
                                    avro_value_get_by_index(&field_value, item,
                                            &item_value, NULL);
                                    avro_value_get_int(&item_value, &n32tmp);
                                    *array_u8 += (int8_t)n32tmp;
                                }
                                debugPrint2("%u | ", (uint32_t)*array_u8);
                                bind->buffer_length = sizeof(uint8_t);
                                bind->buffer = array_u8;
                            } else {
                                errorPrint("%s() LN%d mix type %s with int array",
                                        __func__, __LINE__,
                                        typeToStr(field->array_type));
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_USMALLINT:
                        {
                            if (TSDB_DATA_TYPE_INT == field->array_type) {
                                uint16_t *array_u16 = malloc(sizeof(uint16_t));
                                assert(array_u16);
                                *array_u16 = 0;

                                size_t array_size;
                                int32_t n32tmp;
                                avro_value_get_size(&field_value, &array_size);

                                debugPrint("%s() LN%d, array_size: %d\n",
                                        __func__, __LINE__, (int)array_size);
                                for (size_t item = 0; item < array_size; item ++) {
                                    avro_value_t item_value;
                                    avro_value_get_by_index(&field_value, item,
                                            &item_value, NULL);
                                    avro_value_get_int(&item_value, &n32tmp);
                                    *array_u16 += (int16_t)n32tmp;
                                }
                                debugPrint2("%u | ", (uint32_t)*array_u16);
                                bind->buffer_length = sizeof(uint16_t);
                                bind->buffer = array_u16;
                            } else {
                                errorPrint("%s() LN%d mix type %s with int array",
                                        __func__, __LINE__,
                                        typeToStr(field->array_type));
                            }
                        }
                        break;

                    case TSDB_DATA_TYPE_UBIGINT:
                        {
                            if (TSDB_DATA_TYPE_BIGINT == field->array_type) {
                                uint64_t *array_u64 = malloc(sizeof(uint64_t));
                                assert(array_u64);
                                *array_u64 = 0;

                                size_t array_size;
                                int64_t n64tmp;
                                avro_value_get_size(&field_value, &array_size);

                                debugPrint("%s() LN%d, array_size: %d\n",
                                        __func__, __LINE__, (int)array_size);
                                for (size_t item = 0; item < array_size; item ++) {
                                    avro_value_t item_value;
                                    avro_value_get_by_index(&field_value, item,
                                            &item_value, NULL);
                                    avro_value_get_long(&item_value, &n64tmp);
                                    *array_u64 += n64tmp;
                                }
                                debugPrint2("%"PRIu64" | ", *array_u64);
                                bind->buffer_length = sizeof(uint64_t);
                                bind->buffer = array_u64;
                            } else {
                                errorPrint("%s() LN%d mix type %s with int array",
                                        __func__, __LINE__,
                                        typeToStr(field->array_type));
                            }
                        }
                        break;

                    default:
                        errorPrint("%s() LN%d, %s's %s is not supported!\n",
                                __func__, __LINE__,
                                tbName,
                                typeToStr(field->type));
                        break;
                }

                bind->buffer_type = tableDes->cols[i].type;
                bind->length = &bind->buffer_length;
            }
        }
        debugPrint2("%s", "\n");

        if (0 != taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray)) {
            errorPrint("%s() LN%d stmt_bind_param() to %s failed! reason: %s\n",
                    __func__, __LINE__, tbName, taos_stmt_errstr(stmt));
            freeBindArray(bindArray, onlyCol);
            failed++;
            continue;
        }

        if (0 != taos_stmt_add_batch(stmt)) {
            errorPrint("%s() LN%d stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            freeBindArray(bindArray, onlyCol);
            failed++;
            continue;
        }

        stmt_count ++;
        if (stmt_count == g_args.data_batch) {
            if (0 != taos_stmt_execute(stmt)) {
                errorPrint("%s() LN%d taos_stmt_execute() failed! reason: %s\n",
                        __func__, __LINE__, taos_stmt_errstr(stmt));
                failed -= stmt_count;
            }
            stmt_count = 0;
        }
        freeBindArray(bindArray, onlyCol);
        success ++;
    }

    if (stmt_count) {
        if (0 != taos_stmt_execute(stmt)) {
            errorPrint("%s() LN%d taos_stmt_execute() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            failed -= stmt_count;
        }
    }

    avro_value_decref(&value);
    avro_value_iface_decref(value_class);

    tfree(bindArray);

    tfree(stmtBuffer);
    freeTbDes(tableDes);

    if (failed)
        return failed;
    return success;
}

static RecordSchema *getSchemaAndReaderFromFile(
        enum enWHICH which, char *avroFile,
        avro_schema_t *schema,
        avro_file_reader_t *reader)
{
    if(avro_file_reader(avroFile, reader)) {
        errorPrint("%s() LN%d, Unable to open avro file %s: %s\n",
                __func__, __LINE__,
                avroFile, avro_strerror());
        return NULL;
    }

    int buf_len = 0;
    switch (which) {
        case WHICH_AVRO_TBTAGS:
            buf_len = 17 + TSDB_DB_NAME_LEN               /* dbname section */
                    + 17                                /* type: record */
                    + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
                    + 10                                /* fields section */
                    + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
                    + (TSDB_COL_NAME_LEN + 40) * TSDB_MAX_TAGS + 4;    /* fields section */
            break;

        case WHICH_AVRO_DATA:
            buf_len = TSDB_MAX_COLUMNS * (TSDB_COL_NAME_LEN + 11 + 16) + 4;
            break;

        case WHICH_AVRO_NTB:
            buf_len = 17 + TSDB_DB_NAME_LEN               /* dbname section */
                + 17                                /* type: record */
                + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
                + 50;                              /* fields section */
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, which);
            return NULL;
    }

    char *jsonbuf = calloc(1, buf_len);
    assert(jsonbuf);

    avro_writer_t jsonwriter = avro_writer_memory(jsonbuf, buf_len);

    *schema = avro_file_reader_get_writer_schema(*reader);
    avro_schema_to_json(*schema, jsonwriter);

    if (0 == strlen(jsonbuf)) {
        errorPrint("Failed to parse avro file: %s schema. reason: %s\n",
                avroFile, avro_strerror());
        avro_writer_free(jsonwriter);
        return NULL;
    }
    verbosePrint("Schema:\n  %s\n", jsonbuf);

    json_t *json_root = load_json(jsonbuf);
    verbosePrint("\n%s() LN%d\n === Schema parsed:\n", __func__, __LINE__);
    if (g_args.verbose_print) {
        print_json(json_root);
    }

    avro_writer_free(jsonwriter);
    tfree(jsonbuf);

    if (NULL == json_root) {
        errorPrint("%s() LN%d, cannot read valid schema from %s\n",
                __func__, __LINE__, avroFile);
        return NULL;
    }

    RecordSchema *recordSchema = parse_json_to_recordschema(json_root);
    if (NULL == recordSchema) {
        errorPrint("Failed to parse json to recordschema. reason: %s\n",
                avro_strerror());
        return NULL;
    }
    json_decref(json_root);

    return recordSchema;
}

static int64_t dumpInOneAvroFile(
        enum enWHICH which,
        char* fcharset,
        char *avroFilepath)
{
    debugPrint("avroFilepath: %s\n", avroFilepath);

    avro_file_reader_t reader;
    avro_schema_t schema;

    RecordSchema *recordSchema = getSchemaAndReaderFromFile(
            which, avroFilepath, &schema, &reader);
    if (NULL == recordSchema) {
        if (schema)
            avro_schema_decref(schema);
        if (reader)
            avro_file_reader_close(reader);
        return -1;
    }

    const char *namespace = avro_schema_namespace((const avro_schema_t)schema);
    debugPrint("%s() LN%d, Namespace: %s\n",
            __func__, __LINE__, namespace);

    TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
            namespace, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return -1;
    }

    TAOS_STMT *stmt = taos_stmt_init(taos);
    if (NULL == stmt) {
        taos_close(taos);
        errorPrint("%s() LN%d, stmt init failed! reason: %s\n",
                __func__, __LINE__, taos_errstr(NULL));
        return -1;
    }

    int retExec = 0;
    switch (which) {
        case WHICH_AVRO_DATA:
            debugPrint("%s() LN%d will dump %s's data\n",
                    __func__, __LINE__, namespace);
            retExec = dumpInAvroDataImpl(taos, stmt,
                    (char *)namespace,
                    schema, reader, recordSchema);
            break;

        case WHICH_AVRO_TBTAGS:
            debugPrint("%s() LN%d will dump %s's normal table with tags\n",
                    __func__, __LINE__, namespace);
            retExec = dumpInAvroTbTagsImpl(
                    taos,
                    (char *)namespace,
                    schema, reader, recordSchema);
            break;

        case WHICH_AVRO_NTB:
            debugPrint("%s() LN%d will dump %s's normal tables\n",
                    __func__, __LINE__, namespace);
            retExec = dumpInAvroNtbImpl(taos, stmt,
                    (char *)namespace,
                    schema, reader, recordSchema);
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, which);
            retExec = -1;
    }

    taos_stmt_close(stmt);
    taos_close(taos);

    freeRecordSchema(recordSchema);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);

    return retExec;
}

static void* dumpInAvroWorkThreadFp(void *arg)
{
    threadInfo *pThreadInfo = (threadInfo*)arg;
    SET_THREAD_NAME("dumpInAvroWorkThrd");
    verbosePrint("[%d] process %"PRId64" files from %"PRId64"\n",
                    pThreadInfo->threadIndex, pThreadInfo->count,
                    pThreadInfo->from);

    char **fileList = NULL;
    switch(pThreadInfo->which) {
        case WHICH_AVRO_DATA:
            fileList = g_tsDumpInAvroFiles;
            break;

        case WHICH_AVRO_TBTAGS:
            fileList = g_tsDumpInAvroTagsTbs;
            break;

        case WHICH_AVRO_NTB:
            fileList = g_tsDumpInAvroNtbs;
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, pThreadInfo->which);
            return NULL;
    }

    int currentPercent = 0;
    int percentComplete = 0;
    for (int64_t i = 0; i < pThreadInfo->count; i++) {
        char avroFile[MAX_PATH_LEN];
        sprintf(avroFile, "%s/%s", g_args.inpath,
                fileList[pThreadInfo->from + i]);

        if (0 == currentPercent) {
            printf("[%d]: Restoring from %s \n",
                    pThreadInfo->threadIndex, avroFile);
        }

        int64_t rows = dumpInOneAvroFile(
                pThreadInfo->which,
                g_tsCharset,
                avroFile);
        switch (pThreadInfo->which) {
            case WHICH_AVRO_DATA:
                if (rows >= 0) {
                    g_totalDumpInRecSuccess += rows;
                    okPrint("[%d] %"PRId64" row(s) of file(%s) be successfully dumped in!\n",
                            pThreadInfo->threadIndex, rows, avroFile);
                } else {
                    g_totalDumpInRecFailed += rows;
                    errorPrint("[%d] %"PRId64" row(s) of file(%s) failed to dumped in!\n",
                            pThreadInfo->threadIndex, rows, avroFile);
                }
                break;

            case WHICH_AVRO_TBTAGS:
                if (rows >= 0) {
                    g_totalDumpInStbSuccess += rows;
                    okPrint("[%d] %"PRId64" table(s) belong stb from the file(%s) be successfully dumped in!\n",
                            pThreadInfo->threadIndex, rows, avroFile);
                } else {
                    g_totalDumpInStbFailed += rows;
                    errorPrint("[%d] %"PRId64" table(s) belong stb from the file(%s) failed to dumped in!\n",
                            pThreadInfo->threadIndex, rows, avroFile);
                }
                break;

            case WHICH_AVRO_NTB:
                if (rows >= 0) {
                    g_totalDumpInNtbSuccess += rows;
                    okPrint("[%d] %"PRId64" normal table(s) from (%s) be successfully dumped in!\n",
                            pThreadInfo->threadIndex, rows, avroFile);
                } else {
                    g_totalDumpInRecFailed += rows;
                    errorPrint("[%d] %"PRId64" normal tables from (%s) failed to dumped in!\n",
                            pThreadInfo->threadIndex, rows, avroFile);
                }
                break;

            default:
                errorPrint("%s() LN%d input mistake list: %d\n",
                        __func__, __LINE__, pThreadInfo->which);
                return NULL;
        }

        currentPercent = ((i+1) * 100 / pThreadInfo->count);
        if (currentPercent > percentComplete) {
            printf("[%d]:%d%%\n", pThreadInfo->threadIndex, currentPercent);
            percentComplete = currentPercent;
        }

    }

    if (percentComplete < 100) {
        printf("[%d]:%d%%\n", pThreadInfo->threadIndex, 100);
    }

    return NULL;
}

static int dumpInAvroWorkThreads(char *whichExt)
{
    int64_t fileCount = getFilesNum(whichExt);

    if (0 == fileCount) {
        debugPrint("No .%s file found in %s\n", whichExt, g_args.inpath);
        return 0;
    }

    int32_t threads = g_args.thread_num;

    int64_t a = fileCount / threads;
    if (a < 1) {
        threads = fileCount;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = fileCount % threads;
    }

    enum enWHICH which = createDumpinList(whichExt, fileCount);

    threadInfo *pThread;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = (threadInfo *)calloc(
            threads, sizeof(threadInfo));
    assert(pids);
    assert(infos);

    int64_t from = 0;

    for (int32_t t = 0; t < threads; ++t) {
        pThread = infos + t;
        pThread->threadIndex = t;
        pThread->which = which;

        pThread->from = from;
        pThread->count = t<b?a+1:a;
        from += pThread->count;
        verbosePrint(
                "Thread[%d] takes care avro files total %"PRId64" files from %"PRId64"\n",
                t, pThread->count, pThread->from);

        if (pthread_create(pids + t, NULL,
                    dumpInAvroWorkThreadFp, (void*)pThread) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start\n",
                    __func__, __LINE__, pThread->threadIndex);
            exit(EXIT_FAILURE);
        }
    }

    for (int t = 0; t < threads; ++t) {
        pthread_join(pids[t], NULL);
    }

    free(infos);
    free(pids);

    freeFileList(which, fileCount);

    return 0;
}

static int64_t writeResultToSql(TAOS_RES *res, FILE *fp,
        char *dbName, char *tbName)
{
    int64_t    totalRows     = 0;

    int32_t  sql_buf_len = g_args.max_sql_len;
    char* tmpBuffer = (char *)calloc(1, sql_buf_len + 128);
    assert(tmpBuffer);

    char *pstr = tmpBuffer;

    TAOS_ROW row = NULL;
    int64_t lastRowsPrint = 5000000;
    int count = 0;

    int numFields = taos_field_count(res);
    assert(numFields > 0);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    int32_t  total_sqlstr_len = 0;

    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t* length = taos_fetch_lengths(res);   // act len

        int32_t curr_sqlstr_len = 0;

        if (count == 0) {
            total_sqlstr_len = 0;
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                    "INSERT INTO %s.%s VALUES (", dbName, tbName);
        } else {
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "(");
        }

        for (int col = 0; col < numFields-1; col++) {
            if (col != 0) curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ", ");

            if (row[col] == NULL) {
                curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "NULL");
                continue;
            }

            char tbuf[COMMAND_SIZE] = {0};
            switch (fields[col].type) {
                case TSDB_DATA_TYPE_BOOL:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            ((((int32_t)(*((char *)row[col])))==1)?1:0));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            *((int8_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            *((int16_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_INT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            *((int32_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                            "%" PRId64 "",
                            *((int64_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f",
                            GET_FLOAT_VAL(row[col]));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f",
                            GET_DOUBLE_VAL(row[col]));
                    break;

                case TSDB_DATA_TYPE_BINARY:
                    converStringToReadable((char *)row[col], length[col],
                            tbuf, COMMAND_SIZE);
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                            "\'%s\'", tbuf);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    convertNCharToReadable((char *)row[col], length[col],
                            tbuf, COMMAND_SIZE);
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                            "\'%s\'", tbuf);
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                            "%" PRId64 "",
                            *(int64_t *)row[col]);
                    break;

                default:
                    break;
            }
        }

        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ")");

        totalRows++;
        count++;
        fprintf(fp, "%s", tmpBuffer);

        if (totalRows >= lastRowsPrint) {
            printf(" %"PRId64 " rows already be dumpout from %s.%s\n",
                    totalRows, dbName, tbName);
            lastRowsPrint += 5000000;
        }

        total_sqlstr_len += curr_sqlstr_len;

        if ((count >= g_args.data_batch)
                || (sql_buf_len - total_sqlstr_len < TSDB_MAX_BYTES_PER_ROW)) {
            fprintf(fp, ";\n");
            count = 0;
        }
    }

    debugPrint("total_sqlstr_len: %d\n", total_sqlstr_len);

    fprintf(fp, "\n");
    free(tmpBuffer);

    return totalRows;
}

TAOS_RES *queryDbForDumpOut(TAOS *taos,
        char *dbName, char *tbName, int precision)
{
    char sqlstr[COMMAND_SIZE] = {0};

    int64_t start_time, end_time;
    if (strlen(g_args.humanStartTime)) {
        if (TSDB_CODE_SUCCESS != taos_parse_time(
                g_args.humanStartTime, &start_time,
                strlen(g_args.humanStartTime),
                precision, 0)) {
            errorPrint("Input %s, time format error!\n",
                    g_args.humanStartTime);
            return NULL;
        }
    } else {
        start_time = g_args.start_time;
    }

    if (strlen(g_args.humanEndTime)) {
        if (TSDB_CODE_SUCCESS != taos_parse_time(
                g_args.humanEndTime, &end_time, strlen(g_args.humanEndTime),
                precision, 0)) {
            errorPrint("Input %s, time format error!\n", g_args.humanEndTime);
            return NULL;
        }
    } else {
        end_time = g_args.end_time;
    }

    sprintf(sqlstr,
            "SELECT * FROM %s.`%s` WHERE _c0 >= %" PRId64 " AND _c0 <= %" PRId64 " ORDER BY _c0 ASC;",
            dbName, tbName, start_time, end_time);

    TAOS_RES* res = taos_query(taos, sqlstr);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("failed to run command %s, reason: %s\n",
                sqlstr, taos_errstr(res));
        taos_free_result(res);
        return NULL;
    }

    return res;
}

static int64_t dumpTableData(
        int64_t index,
        FILE *fp, char *tbName,
        char* dbName, int precision,
        int colCount,
        TableDef *tableDes
        ) {
    char *jsonSchema = NULL;
    if (g_args.avro) {
        if (0 != convertTbDesToJson(
                    dbName, tbName, tableDes, colCount, &jsonSchema)) {
            errorPrint("%s() LN%d, convertTbDesToJson failed\n",
                    __func__,
                    __LINE__);
            freeTbDes(tableDes);
            return -1;
        }
    }

    TAOS *taos = taos_connect(g_args.host,
            g_args.user, g_args.password, dbName, g_args.port);
    if (NULL == taos) {
        errorPrint(
                "Failed to connect to TDengine server %s by specified database %s\n",
                g_args.host, dbName);
        tfree(jsonSchema);
        return -1;
    }

    TAOS_RES *res = queryDbForDumpOut(taos, dbName, tbName, precision);
    if (NULL == res) {
        tfree(jsonSchema);
        return -1;
    }

    int64_t    totalRows     = 0;

    if (g_args.avro) {
        char avroFilename[MAX_PATH_LEN] = {0};

        sprintf(avroFilename, "%s%s.%"PRId64".%"PRIu64".avro",
                g_args.outpath, dbName, index, getUniqueIDFromEpoch());

        totalRows = writeResultToAvro(avroFilename, tbName, jsonSchema, res);
    } else {
        totalRows = writeResultToSql(res, fp, dbName, tbName);
    }

    taos_free_result(res);
    taos_close(taos);
    tfree(jsonSchema);

    return totalRows;
}

static int64_t dumpNormalTable(
        int64_t index,
        TAOS *taos,
        char *dbName,
        char *stable,
        char *tbName,
        int precision,
        char *dumpFilename,
        FILE *fp
        ) {
    TableDef *tableDes = (TableDef *)calloc(1, sizeof(TableDef)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    int numColsAndTags = getTableDes(taos, dbName, tbName, tableDes, false);

    if (stable != NULL && stable[0] != '\0') {  // dump table schema which is created by using super table

        if (numColsAndTags < 0) {
            errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                    __func__,
                    __LINE__,
                    tbName);
            free(tableDes);
            return -1;
        }

        // create child-table using super-table
        if (!g_args.avro) {
            dumpCreateMTableClause(dbName, stable, tableDes, numColsAndTags, fp);
        }
    } else {  // dump table definition
        if (numColsAndTags < 0) {
            errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                    __func__,
                    __LINE__,
                    tbName);
            free(tableDes);
            return -1;
        }

        // create normal-table
        if (g_args.avro) {
            assert(dumpFilename);
            dumpCreateTableClauseAvro(dumpFilename, tableDes, numColsAndTags, dbName);
        } else {
            dumpCreateTableClause(tableDes, numColsAndTags, fp, dbName);
        }
    }

    int64_t totalRows = 0;
    if (!g_args.schemaonly) {
        totalRows = dumpTableData(
                index,
                fp, tbName, dbName, precision,
            numColsAndTags, tableDes);
    }

    freeTbDes(tableDes);
    return totalRows;
}

static int64_t dumpNormalTableWithoutStb(
        int64_t index,
        TAOS *taos, SDbInfo *dbInfo, char *ntbName)
{
    int64_t count = 0;

    char dumpFilename[MAX_PATH_LEN] = {0};
    FILE *fp = NULL;

    if (g_args.avro) {
        sprintf(dumpFilename, "%s%s.%"PRId64".%"PRIu64".avro-ntb",
                g_args.outpath, dbInfo->name, index, getUniqueIDFromEpoch());
        count = dumpNormalTable(
                index,
                taos,
                dbInfo->name,
                NULL,
                ntbName,
                getPrecisionByString(dbInfo->precision),
                dumpFilename,
                NULL);
    } else {
        sprintf(dumpFilename, "%s%s.%s.sql",
                g_args.outpath, dbInfo->name, ntbName);

        fp = fopen(dumpFilename, "w");
        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return -1;
        }

        count = dumpNormalTable(
                index,
                taos,
                dbInfo->name,
                NULL,
                ntbName,
                getPrecisionByString(dbInfo->precision),
                NULL,
                fp);
        fclose(fp);
    }
    if (count > 0) {
        atomic_add_fetch_64(&g_totalDumpOutRows, count);
    }

    return count;
}

static int createMTableAvroHead(
        TAOS *taos,
        char *dumpFilename,
        char *dbName, char *stable,
        int64_t limit, int64_t offset,
        char *specifiedTb)
{
    TableDef *tableDes = (TableDef *)calloc(1, sizeof(TableDef)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    assert(tableDes);

    int colCount = getTableDes(taos, dbName, stable, tableDes, false);

    char *jsonTagsSchema = NULL;
    if (0 != convertTbTagsDesToJson(
                dbName, stable, tableDes, &jsonTagsSchema)) {
        errorPrint("%s() LN%d, convertTbTagsDesToJson failed\n",
                __func__,
                __LINE__);
        tfree(jsonTagsSchema);
        freeTbDes(tableDes);
        return -1;
    }

    debugPrint("tagsJson:\n%s\n", jsonTagsSchema);

    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(
            dumpFilename,
            jsonTagsSchema, &schema, &recordSchema, &db);

    char command[COMMAND_SIZE];

    sprintf(command, "SELECT TBNAME FROM %s.`%s` LIMIT %"PRId64" OFFSET %"PRId64"",
            dbName, stable,
            limit, offset);

    debugPrint("%s() LN%d, failed to run command <%s>.\n",
                __func__, __LINE__, command);
    TAOS_RES *res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        tfree(jsonTagsSchema);
        freeTbDes(tableDes);
        return -1;
    }

    TAOS_ROW row = NULL;
    int64_t ntbCount = 0;

    while((row = taos_fetch_row(res)) != NULL) {
        int32_t *length = taos_fetch_lengths(res);
        char tbName[TSDB_TABLE_NAME_LEN+1] = {0};

        strncpy(tbName,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN, length[TSDB_SHOW_TABLES_NAME_INDEX]));
        if (specifiedTb) {
            if(0 != strcmp(specifiedTb, tbName)) {
                continue;
            }
        }
        debugPrint("sub table %"PRId64": name: %s\n", ++ntbCount, tbName);
        avro_value_t record;
        avro_generic_value_new(wface, &record);

        avro_value_t value, branch;

        if (0 != avro_value_get_by_name(
                    &record, "stbname", &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                    __func__, __LINE__, "stbname");
            continue;
        }

        avro_value_set_branch(&value, 1, &branch);
        avro_value_set_string(&branch, stable);

        if (0 != avro_value_get_by_name(
                    &record, "tbname", &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                    __func__, __LINE__, "tbname");
            continue;
        }

        avro_value_set_branch(&value, 1, &branch);
        avro_value_set_string(&branch,
                tbName);

        TableDef *subTableDes = (TableDef *) calloc(1, sizeof(TableDef)
                + sizeof(ColDes) * colCount);
        assert(subTableDes);

        getTableDes(taos, dbName,
                    tbName,
                    subTableDes, false);

        for (int tag = 0; tag < subTableDes->tags; tag ++) {
            debugPrint("sub table %s no. %d tags is %s, type is %d, value is %s\n",
                tbName,
                tag,
                subTableDes->cols[subTableDes->columns + tag].field,
                subTableDes->cols[subTableDes->columns + tag].type,
                subTableDes->cols[subTableDes->columns + tag].value
                );

            char tmpBuf[20] = {0};
            sprintf(tmpBuf, "tag%d", tag);

            if (0 != avro_value_get_by_name(
                        &record,
                        tmpBuf,
                        &value, NULL)) {
                errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed\n",
                        __func__, __LINE__,
                        subTableDes->cols[subTableDes->columns + tag].field);
            }

            avro_value_t firsthalf, secondhalf;
            uint8_t u8Temp = 0;
            uint16_t u16Temp = 0;
            uint32_t u32Temp = 0;
            uint64_t u64Temp = 0;

            int type = subTableDes->cols[subTableDes->columns + tag].type;
            switch (type) {
                case TSDB_DATA_TYPE_BOOL:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_branch(&value, 0, &branch);
                        avro_value_set_null(&branch);
                    } else {
                        avro_value_set_branch(&value, 1, &branch);
                        int tmp = atoi((const char *)
                                subTableDes->cols[subTableDes->columns+tag].value);
                        verbosePrint("%s() LN%d, before set_bool() tmp=%d\n",
                                __func__, __LINE__, (int)tmp);
                        avro_value_set_boolean(&branch, (tmp)?1:0);
                    }
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_int(&value, TSDB_DATA_TINYINT_NULL);
                    } else {
                        avro_value_set_int(&value,
                                (int8_t)atoi((const char *)
                                    subTableDes->cols[subTableDes->columns + tag].value));
                    }
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_int(&value, TSDB_DATA_SMALLINT_NULL);
                    } else {
                        avro_value_set_int(&value,
                                (int16_t)atoi((const char *)
                                    subTableDes->cols[subTableDes->columns + tag].value));
                    }
                    break;

                case TSDB_DATA_TYPE_INT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_int(&value, TSDB_DATA_INT_NULL);
                    } else {
                        avro_value_set_int(&value,
                                (int32_t)atoi((const char *)
                                    subTableDes->cols[subTableDes->columns + tag].value));

                    }
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_long(&value, TSDB_DATA_BIGINT_NULL);
                    } else {
                        avro_value_set_long(&value,
                                (int64_t)atoll((const char *)
                                    subTableDes->cols[subTableDes->columns + tag].value));
                    }
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_float(&value, TSDB_DATA_FLOAT_NULL);
                    } else {
                        if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                            avro_value_set_float(&value,
                                    atof(subTableDes->cols[subTableDes->columns + tag].var_value));
                        } else {
                            avro_value_set_float(&value,
                                    atof(subTableDes->cols[subTableDes->columns + tag].value));
                        }
                    }
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_double(&value, TSDB_DATA_DOUBLE_NULL);
                    } else {
                        if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                            avro_value_set_double(&value,
                                    atof(subTableDes->cols[subTableDes->columns + tag].var_value));
                        } else {
                            avro_value_set_double(&value,
                                    atof(subTableDes->cols[subTableDes->columns + tag].value));
                        }
                    }
                    break;

                case TSDB_DATA_TYPE_BINARY:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_branch(&value, 0, &branch);
                        avro_value_set_null(&branch);
                    } else {
                        avro_value_set_branch(&value, 1, &branch);
                        if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                            avro_value_set_string(&branch,
                                    subTableDes->cols[subTableDes->columns + tag].var_value);
                        } else {
                            avro_value_set_string(&branch,
                                    subTableDes->cols[subTableDes->columns + tag].value);
                        }
                    }
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                case TSDB_DATA_TYPE_JSON:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_branch(&value, 0, &branch);
                        avro_value_set_null(&branch);
                    } else {
                        avro_value_set_branch(&value, 1, &branch);
                        if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                            size_t nlen = strlen(subTableDes->cols[subTableDes->columns + tag].var_value);
                            char *bytes = malloc(nlen+1);
                            assert(bytes);

                            strncpy(bytes,
                                    subTableDes->cols[subTableDes->columns + tag].var_value,
                                    nlen);
                            avro_value_set_bytes(&branch, bytes, nlen);
                            free(bytes);
                        } else {
                            avro_value_set_bytes(&branch,
                                    subTableDes->cols[subTableDes->columns + tag].value,
                                    strlen(subTableDes->cols[subTableDes->columns + tag].value)
                                    );
                        }
                    }
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        avro_value_set_long(&value, TSDB_DATA_BIGINT_NULL);
                    } else {
                        avro_value_set_long(&value,
                                (int64_t)atoll((const char *)
                                    subTableDes->cols[subTableDes->columns + tag].value));
                    }
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        u8Temp = TSDB_DATA_UTINYINT_NULL;
                    } else {
                        u8Temp = (int8_t)atoi((const char *)
                                subTableDes->cols[subTableDes->columns + tag].value);
                    }

                    int8_t n8tmp = (int8_t)(u8Temp - SCHAR_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n8tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n8tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SCHAR_MAX);
                    debugPrint("second half is: %d\n", SCHAR_MAX);

                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        u16Temp = TSDB_DATA_USMALLINT_NULL;
                    } else {
                        u16Temp = (int16_t)atoi((const char *)
                                subTableDes->cols[subTableDes->columns + tag].value);
                    }

                    int16_t n16tmp = (int16_t)(u16Temp - SHRT_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n16tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n16tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SHRT_MAX);
                    debugPrint("second half is: %d\n", SHRT_MAX);

                    break;

                case TSDB_DATA_TYPE_UINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        u32Temp = TSDB_DATA_UINT_NULL;
                    } else {
                        u32Temp = (int32_t)atoi((const char *)
                                subTableDes->cols[subTableDes->columns + tag].value);
                    }

                    int32_t n32tmp = (int32_t)(u32Temp - INT_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n32tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n32tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)INT_MAX);
                    debugPrint("second half is: %d\n", INT_MAX);

                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    if (0 == strncmp(
                                subTableDes->cols[subTableDes->columns+tag].note,
                                "NUL", 3)) {
                        u64Temp = TSDB_DATA_UBIGINT_NULL;
                    } else {
                        char *eptr;
                        u64Temp = strtoull((const char *)
                                subTableDes->cols[subTableDes->columns + tag].value,
                                &eptr, 10);
                    }

                    int64_t n64tmp = (int64_t)(u64Temp - LONG_MAX);
                    avro_value_append(&value, &firsthalf, NULL);
                    avro_value_set_long(&firsthalf, n64tmp);
                    debugPrint("%s() LN%d, first half is: %"PRId64", ",
                            __func__, __LINE__, n64tmp);
                    avro_value_append(&value, &secondhalf, NULL);
                    avro_value_set_long(&secondhalf, (int64_t)LONG_MAX);
                    debugPrint("second half is: %"PRId64"\n", LONG_MAX);


                default:
                    errorPrint("Unknown type: %d\n", type);
                    break;
            }
        }

        if (0 != avro_file_writer_append_value(db, &record)) {
                errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                        __func__, __LINE__,
                        avro_strerror());
        }
        avro_value_decref(&record);

        freeTbDes(subTableDes);

        if (specifiedTb) {
            break;
        }
    }

    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    tfree(jsonTagsSchema);
    freeTbDes(tableDes);

    return 0;
}

static int64_t dumpNormalTableBelongStb(
        int64_t index,
        TAOS *taos,
        SDbInfo *dbInfo, char *stbName, char *ntbName)
{
    int64_t count = 0;

    char dumpFilename[MAX_PATH_LEN] = {0};
    FILE *fp = NULL;

    if (g_args.avro) {
        sprintf(dumpFilename, "%s%s.%s.avro-tagstb",
                g_args.outpath, dbInfo->name, ntbName);
    } else {
        sprintf(dumpFilename, "%s%s.%s.sql",
                g_args.outpath, dbInfo->name, ntbName);
    }

    if (g_args.avro) {
        int ret = createMTableAvroHead(
                taos,
                dumpFilename,
                dbInfo->name,
                stbName, -1, 0,
                NULL);
        if (-1 == ret) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return -1;
        }
    } else {
        fp = fopen(dumpFilename, "w");

        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return -1;
        }
    }

    count = dumpNormalTable(
            index,
            taos,
            dbInfo->name,
            stbName,
            ntbName,
            getPrecisionByString(dbInfo->precision),
            dumpFilename,
            fp);
    if (count > 0) {
        atomic_add_fetch_64(&g_totalDumpOutRows, count);
    }

    if (!g_args.avro) {
        fclose(fp);
    }

    return count;
}

static void *dumpNtbOfDb(void *arg) {
    threadInfo *pThreadInfo = (threadInfo *)arg;

    debugPrint("%s() LN%d, dump table from = \t%"PRId64"\n",
            __func__, __LINE__,
            pThreadInfo->from);
    debugPrint("%s() LN%d, dump table count = \t%"PRId64"\n",
            __func__, __LINE__,
            pThreadInfo->count);

    FILE *fp = NULL;
    char dumpFilename[MAX_PATH_LEN] = {0};

    if (!g_args.avro) {
        sprintf(dumpFilename, "%s%s.%d.sql",
                g_args.outpath, pThreadInfo->dbName,
                pThreadInfo->threadIndex);

        fp = fopen(dumpFilename, "w");
        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return NULL;
        }
    }

    int64_t count;
    int currentPercent = 0;
    int percentComplete = 0;

    for (int64_t i = 0; i < pThreadInfo->count; i++) {
        debugPrint("[%d] No.\t%"PRId64" %s() LN%d, table name: %s\n",
                pThreadInfo->threadIndex, i,
                __func__, __LINE__,
                ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->name);

        if (g_args.avro) {
            sprintf(dumpFilename, "%s%s.%d.%"PRIu64".%d.avro-ntb",
                    g_args.outpath, pThreadInfo->dbName,
                    pThreadInfo->threadIndex,
                    getUniqueIDFromEpoch(),
                    pThreadInfo->threadIndex);

            if (0 == currentPercent) {
                printf("[%d]: Dumping to %s \n",
                        pThreadInfo->threadIndex, dumpFilename);
            }

            count = dumpNormalTable(
                    pThreadInfo->from+i,
                    pThreadInfo->taos,
                    pThreadInfo->dbName,
                    ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->stable,
                    ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->name,
                    pThreadInfo->precision,
                    dumpFilename,
                    NULL);
        } else {
            if (0 == currentPercent) {
                printf("[%d]: Dumping to %s \n",
                        pThreadInfo->threadIndex, dumpFilename);
            }

            count = dumpNormalTable(
                    pThreadInfo->from+i,
                    pThreadInfo->taos,
                    pThreadInfo->dbName,
                    ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->stable,
                    ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->name,
                    pThreadInfo->precision,
                    NULL,
                    fp);
        }

        if (count < 0) {
            break;
        } else {
            atomic_add_fetch_64(&g_totalDumpOutRows, count);
        }

        currentPercent = (int)((i+1) * 100 / pThreadInfo->count);
        if (currentPercent > percentComplete) {
            printf("[%d]:%d%%\n", pThreadInfo->threadIndex, currentPercent);
            percentComplete = currentPercent;
        }
    }

    if (percentComplete < 100) {
        printf("[%d]:%d%%\n", pThreadInfo->threadIndex, 100);
    }

    if (!g_args.avro) {
        fclose(fp);
    }

    return NULL;
}

static int checkParam() {
    if (g_args.all_databases && g_args.databases) {
        errorPrint("%s", "conflict option --all-databases and --databases\n");
        return -1;
    }

    if (g_args.start_time > g_args.end_time) {
        errorPrint("%s", "start time is larger than end time\n");
        return -1;
    }

    if (g_args.arg_list_len == 0) {
        if ((!g_args.all_databases) && (!g_args.databases) && (!g_args.isDumpIn)) {
            errorPrint("%s", "taosdump requires parameters\n");
            return -1;
        }
    }

    if ((!g_args.isDumpIn)
            && (!g_args.databases)
            && (!g_args.all_databases)
            && (0 == g_args.arg_list_len)) {
        errorPrint("%s", "Invalid option in dump out\n");
        return -1;
    }

    return 0;
}

/*
static bool isEmptyCommand(char *cmd) {
  char *pchar = cmd;

  while (*pchar != '\0') {
    if (*pchar != ' ') return false;
    pchar++;
  }

  return true;
}

static void taosReplaceCtrlChar(char *str) {
  bool ctrlOn = false;
  char *pstr = NULL;

  for (pstr = str; *str != '\0'; ++str) {
    if (ctrlOn) {
      switch (*str) {
        case 'n':
          *pstr = '\n';
          pstr++;
          break;
        case 'r':
          *pstr = '\r';
          pstr++;
          break;
        case 't':
          *pstr = '\t';
          pstr++;
          break;
        case '\\':
          *pstr = '\\';
          pstr++;
          break;
        case '\'':
          *pstr = '\'';
          pstr++;
          break;
        default:
          break;
      }
      ctrlOn = false;
    } else {
      if (*str == '\\') {
        ctrlOn = true;
      } else {
        *pstr = *str;
        pstr++;
      }
    }
  }

  *pstr = '\0';
}
*/

char *ascii_literal_list[] = {
    "\\x00", "\\x01", "\\x02", "\\x03", "\\x04", "\\x05", "\\x06", "\\x07", "\\x08", "\\t",   "\\n",   "\\x0b", "\\x0c",
    "\\r",   "\\x0e", "\\x0f", "\\x10", "\\x11", "\\x12", "\\x13", "\\x14", "\\x15", "\\x16", "\\x17", "\\x18", "\\x19",
    "\\x1a", "\\x1b", "\\x1c", "\\x1d", "\\x1e", "\\x1f", " ",     "!",     "\\\"",  "#",     "$",     "%",     "&",
    "\\'",   "(",     ")",     "*",     "+",     ",",     "-",     ".",     "/",     "0",     "1",     "2",     "3",
    "4",     "5",     "6",     "7",     "8",     "9",     ":",     ";",     "<",     "=",     ">",     "?",     "@",
    "A",     "B",     "C",     "D",     "E",     "F",     "G",     "H",     "I",     "J",     "K",     "L",     "M",
    "N",     "O",     "P",     "Q",     "R",     "S",     "T",     "U",     "V",     "W",     "X",     "Y",     "Z",
    "[",     "\\\\",  "]",     "^",     "_",     "`",     "a",     "b",     "c",     "d",     "e",     "f",     "g",
    "h",     "i",     "j",     "k",     "l",     "m",     "n",     "o",     "p",     "q",     "r",     "s",     "t",
    "u",     "v",     "w",     "x",     "y",     "z",     "{",     "|",     "}",     "~",     "\\x7f", "\\x80", "\\x81",
    "\\x82", "\\x83", "\\x84", "\\x85", "\\x86", "\\x87", "\\x88", "\\x89", "\\x8a", "\\x8b", "\\x8c", "\\x8d", "\\x8e",
    "\\x8f", "\\x90", "\\x91", "\\x92", "\\x93", "\\x94", "\\x95", "\\x96", "\\x97", "\\x98", "\\x99", "\\x9a", "\\x9b",
    "\\x9c", "\\x9d", "\\x9e", "\\x9f", "\\xa0", "\\xa1", "\\xa2", "\\xa3", "\\xa4", "\\xa5", "\\xa6", "\\xa7", "\\xa8",
    "\\xa9", "\\xaa", "\\xab", "\\xac", "\\xad", "\\xae", "\\xaf", "\\xb0", "\\xb1", "\\xb2", "\\xb3", "\\xb4", "\\xb5",
    "\\xb6", "\\xb7", "\\xb8", "\\xb9", "\\xba", "\\xbb", "\\xbc", "\\xbd", "\\xbe", "\\xbf", "\\xc0", "\\xc1", "\\xc2",
    "\\xc3", "\\xc4", "\\xc5", "\\xc6", "\\xc7", "\\xc8", "\\xc9", "\\xca", "\\xcb", "\\xcc", "\\xcd", "\\xce", "\\xcf",
    "\\xd0", "\\xd1", "\\xd2", "\\xd3", "\\xd4", "\\xd5", "\\xd6", "\\xd7", "\\xd8", "\\xd9", "\\xda", "\\xdb", "\\xdc",
    "\\xdd", "\\xde", "\\xdf", "\\xe0", "\\xe1", "\\xe2", "\\xe3", "\\xe4", "\\xe5", "\\xe6", "\\xe7", "\\xe8", "\\xe9",
    "\\xea", "\\xeb", "\\xec", "\\xed", "\\xee", "\\xef", "\\xf0", "\\xf1", "\\xf2", "\\xf3", "\\xf4", "\\xf5", "\\xf6",
    "\\xf7", "\\xf8", "\\xf9", "\\xfa", "\\xfb", "\\xfc", "\\xfd", "\\xfe", "\\xff"};

static int converStringToReadable(char *str, int size, char *buf, int bufsize) {
    char *pstr = str;
    char *pbuf = buf;
    while (size > 0) {
        if (*pstr == '\0') break;
        pbuf = stpcpy(pbuf, ascii_literal_list[((uint8_t)(*pstr))]);
        pstr++;
        size--;
    }
    *pbuf = '\0';
    return 0;
}

static int convertNCharToReadable(char *str, int size, char *buf, int bufsize) {
    char *pstr = str;
    char *pbuf = buf;
    wchar_t wc;
    while (size > 0) {
        if (*pstr == '\0') break;
        int byte_width = mbtowc(&wc, pstr, MB_CUR_MAX);
        if (byte_width < 0) {
            errorPrint("%s() LN%d, mbtowc() return fail.\n", __func__, __LINE__);
            exit(-1);
        }

        if ((int)wc < 256) {
            pbuf = stpcpy(pbuf, ascii_literal_list[(int)wc]);
        } else {
            memcpy(pbuf, pstr, byte_width);
            pbuf += byte_width;
        }
        pstr += byte_width;
    }

    *pbuf = '\0';

    return 0;
}

static void dumpCharset(FILE *fp) {
    char charsetline[256];

    (void)fseek(fp, 0, SEEK_SET);
    sprintf(charsetline, "#!%s\n", g_tsCharset);
    (void)fwrite(charsetline, strlen(charsetline), 1, fp);
}

static void loadFileCharset(FILE *fp, char *fcharset) {
    char * line = NULL;
    size_t line_size = 0;

    (void)fseek(fp, 0, SEEK_SET);
    ssize_t size = getline(&line, &line_size, fp);
    if (size <= 2) {
        goto _exit_no_charset;
    }

    if (strncmp(line, "#!", 2) != 0) {
        goto _exit_no_charset;
    }
    if (line[size - 1] == '\n') {
        line[size - 1] = '\0';
        size--;
    }
    strcpy(fcharset, line + 2);

    tfree(line);
    return;

_exit_no_charset:
    (void)fseek(fp, 0, SEEK_SET);
    *fcharset = '\0';
    tfree(line);
    return;
}

// ========  dumpIn support multi threads functions ================================//
static int64_t dumpInOneSqlFile(
        TAOS* taos, FILE* fp, char* fcharset,
        char* fileName) {
    int       read_len = 0;
    char *    cmd      = NULL;
    size_t    cmd_len  = 0;
    char *    line     = NULL;
    size_t    line_len = 0;

    cmd  = (char *)malloc(TSDB_MAX_ALLOWED_SQL_LEN);
    if (cmd == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        return -1;
    }

    int lastRowsPrint = 5000000;
    int64_t lineNo = 0;
    int64_t success = 0;
    int64_t failed = 0;
    while ((read_len = getline(&line, &line_len, fp)) != -1) {
        ++lineNo;
        if (read_len >= TSDB_MAX_ALLOWED_SQL_LEN) continue;
        line[--read_len] = '\0';

        //if (read_len == 0 || isCommentLine(line)) {  // line starts with #
        if (read_len == 0 ) {
            continue;
        }

        if (line[read_len - 1] == '\\') {
            line[read_len - 1] = ' ';
            memcpy(cmd + cmd_len, line, read_len);
            cmd_len += read_len;
            continue;
        }

        memcpy(cmd + cmd_len, line, read_len);
        cmd[read_len + cmd_len]= '\0';
        bool isRows = (0 == strncmp(cmd, "INSERT ", strlen("INSERT ")));

        if (queryDbImpl(taos, cmd)) {
            errorPrint("%s() LN%d, error sql: lineno:%"PRId64", file:%s\n",
                    __func__, __LINE__, lineNo, fileName);
            fprintf(g_fpOfResult, "error sql: lineno:%"PRId64", file:%s\n",
                    lineNo, fileName);
            if (isRows)
                failed ++;
        } else {
            if (isRows)
                success ++;
        }

        memset(cmd, 0, TSDB_MAX_ALLOWED_SQL_LEN);
        cmd_len = 0;

        if (lineNo >= lastRowsPrint) {
            printf(" %"PRId64" lines already be executed from file %s\n", lineNo, fileName);
            lastRowsPrint += 5000000;
        }
    }

    tfree(cmd);
    tfree(line);

    if (success > 0)
        return success;
    return failed;
}

static void* dumpInSqlWorkThreadFp(void *arg)
{
    threadInfo *pThread = (threadInfo*)arg;
    SET_THREAD_NAME("dumpInSqlWorkThrd");
    errorPrint("[%d] Start to process %"PRId64" files from %"PRId64"\n",
                    pThread->threadIndex, pThread->count, pThread->from);

    for (int64_t i = 0; i < pThread->count; i++) {
        char sqlFile[MAX_PATH_LEN];
        sprintf(sqlFile, "%s/%s", g_args.inpath, g_tsDumpInSqlFiles[pThread->from + i]);

        FILE* fp = openDumpInFile(sqlFile);
        if (NULL == fp) {
            errorPrint("[%d] Failed to open input file: %s\n",
                    pThread->threadIndex, sqlFile);
            continue;
        }

        int64_t rows = dumpInOneSqlFile(
                pThread->taos, fp, g_tsCharset,
                sqlFile);
        if (rows > 0) {
            pThread->recSuccess += rows;
            okPrint("[%d] Total %"PRId64" row(s) be successfully dumped in file: %s\n",
                    pThread->threadIndex, rows, sqlFile);
        } else if (rows < 0) {
            pThread->recFailed += rows;
            errorPrint("[%d] Total %"PRId64" row(s) failed to dump in file: %s\n",
                    pThread->threadIndex, rows, sqlFile);
        }
        fclose(fp);
    }

    return NULL;
}

static int dumpInSqlWorkThreads()
{
    int ret = 0;
    int32_t threads = g_args.thread_num;

    uint64_t sqlFileCount = getFilesNum("sql");
    if (0 == sqlFileCount) {
        debugPrint("No .sql file found in %s\n", g_args.inpath);
        return 0;
    }

    enum enWHICH which = createDumpinList("sql", sqlFileCount);

    threadInfo *pThread;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = (threadInfo *)calloc(
            threads, sizeof(threadInfo));
    assert(pids);
    assert(infos);

    int64_t a = sqlFileCount / threads;
    if (a < 1) {
        threads = sqlFileCount;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = sqlFileCount % threads;
    }

    int64_t from = 0;

    for (int32_t t = 0; t < threads; ++t) {
        pThread = infos + t;
        pThread->threadIndex = t;
        pThread->which = which;

        pThread->stbSuccess = 0;
        pThread->stbFailed = 0;
        pThread->ntbSuccess = 0;
        pThread->ntbFailed = 0;
        pThread->recSuccess = 0;
        pThread->recFailed = 0;

        pThread->from = from;
        pThread->count = t<b?a+1:a;
        from += pThread->count;
        verbosePrint(
                "Thread[%d] takes care sql files total %"PRId64" files from %"PRId64"\n",
                t, pThread->count, pThread->from);

        pThread->taos = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
        if (pThread->taos == NULL) {
            errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
            free(infos);
            free(pids);
            return -1;
        }

        if (pthread_create(pids + t, NULL,
                    dumpInSqlWorkThreadFp, (void*)pThread) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start\n",
                    __func__, __LINE__, pThread->threadIndex);
            exit(EXIT_FAILURE);
        }
    }

    for (int t = 0; t < threads; ++t) {
        pthread_join(pids[t], NULL);
    }

    for (int t = 0; t < threads; ++t) {
        switch (infos[t].which) {
            case WHICH_AVRO_DATA:
                g_totalDumpInRecSuccess += infos[t].recSuccess;
                g_totalDumpInRecFailed += infos[t].recFailed;
                break;

            case WHICH_AVRO_TBTAGS:
                g_totalDumpInStbSuccess += infos[t].stbSuccess;
                g_totalDumpInStbFailed += infos[t].stbFailed;
                break;

            case WHICH_AVRO_NTB:
                g_totalDumpInNtbSuccess += infos[t].ntbSuccess;
                g_totalDumpInNtbFailed += infos[t].ntbFailed;
                break;

            default:
                errorPrint("%s() LN%d input mistake list: %d\n",
                        __func__, __LINE__, infos[t].which);
                ret = -1;
        }
        taos_close(infos[t].taos);
    }

    free(infos);
    free(pids);

    freeFileList(which, sqlFileCount);

    return ret;
}

static int dumpInDbs()
{
    TAOS *taos = taos_connect(
            g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);

    if (taos == NULL) {
        errorPrint("%s() LN%d, failed to connect to TDengine server\n",
                __func__, __LINE__);
        return -1;
    }

    char dbsSql[MAX_PATH_LEN];
    sprintf(dbsSql, "%s/%s", g_args.inpath, "dbs.sql");

    FILE *fp = openDumpInFile(dbsSql);
    if (NULL == fp) {
        debugPrint("%s() LN%d, failed to open input file %s\n",
                __func__, __LINE__, dbsSql);
        return -1;
    }
    debugPrint("Success Open input file: %s\n", dbsSql);
    loadFileCharset(fp, g_tsCharset);

    int64_t rows = dumpInOneSqlFile(
            taos, fp, g_tsCharset, dbsSql);
    if(rows > 0) {
        okPrint("Total %"PRId64" line(s) SQL be successfully dumped in file: %s!\n",
                rows, dbsSql);
    } else if (rows < 0) {
        errorPrint("Total %"PRId64" line(s) SQL failed to dump in file: %s!\n",
                rows, dbsSql);
    }

    fclose(fp);
    taos_close(taos);

    return (rows < 0)?rows:0;
}

static int dumpIn() {
    assert(g_args.isDumpIn);

    int ret = 0;
    if (dumpInDbs()) {
        errorPrint("%s", "Failed to dump database(s) in!\n");
        exit(EXIT_FAILURE);
    }

    if (g_args.avro) {
        ret = dumpInAvroWorkThreads("avro-tbtags");

        if (0 == ret) {
            ret = dumpInAvroWorkThreads("avro-ntb");

            if (0 == ret) {
                ret = dumpInAvroWorkThreads("avro");
            }
        }
    } else {
        ret = dumpInSqlWorkThreads();
    }

    return ret;
}

static void *dumpNormalTablesOfStb(void *arg) {
    threadInfo *pThreadInfo = (threadInfo *)arg;

    debugPrint("dump table from = \t%"PRId64"\n", pThreadInfo->from);
    debugPrint("dump table count = \t%"PRId64"\n", pThreadInfo->count);

    FILE *fp = NULL;
    char dumpFilename[MAX_PATH_LEN] = {0};

    if (g_args.avro) {
        sprintf(dumpFilename, "%s%s.%d.%"PRIu64".%d.avro-tbtags",
                g_args.outpath,
                pThreadInfo->dbName,
                pThreadInfo->threadIndex,
                getUniqueIDFromEpoch(),
                pThreadInfo->threadIndex);
    } else {
        sprintf(dumpFilename, "%s%s.%s.%d.sql",
                g_args.outpath,
                pThreadInfo->dbName,
                pThreadInfo->stbName,
                pThreadInfo->threadIndex);
    }

    if (g_args.avro) {
        int ret = createMTableAvroHead(
                pThreadInfo->taos,
                dumpFilename,
                pThreadInfo->dbName,
                pThreadInfo->stbName,
                pThreadInfo->count,
                pThreadInfo->from,
                NULL);
        if (-1 == ret) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return NULL;
        }
    } else {
        fp = fopen(dumpFilename, "w");

        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return NULL;
        }
    }

    char command[COMMAND_SIZE];

    sprintf(command, "SELECT TBNAME FROM %s.`%s` LIMIT %"PRId64" OFFSET %"PRId64"",
            pThreadInfo->dbName, pThreadInfo->stbName,
            pThreadInfo->count, pThreadInfo->from);

    TAOS_RES *res = taos_query(pThreadInfo->taos, command);
    int32_t code = taos_errno(res);
    if (code) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        return NULL;
    }

    TAOS_ROW row = NULL;
    int64_t i = 0;
    int64_t count;
    while((row = taos_fetch_row(res)) != NULL) {
        int32_t *length = taos_fetch_lengths(res);
        char tbName[TSDB_TABLE_NAME_LEN+1] = {0};
        strncpy(tbName,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN, length[TSDB_SHOW_TABLES_NAME_INDEX]));
        debugPrint("[%d] sub table %"PRId64": name: %s\n",
                pThreadInfo->threadIndex, i++, tbName);

        if (g_args.avro) {
            count = dumpNormalTable(
                    i,
                    pThreadInfo->taos,
                    pThreadInfo->dbName,
                    pThreadInfo->stbName,
                    tbName,
                    pThreadInfo->precision,
                    dumpFilename,
                    NULL);
        } else {
            count = dumpNormalTable(
                    i,
                    pThreadInfo->taos,
                    pThreadInfo->dbName,
                    pThreadInfo->stbName,
                    tbName,
                    pThreadInfo->precision,
                    NULL,
                    fp);
        }

        if (count < 0) {
            break;
        } else {
            atomic_add_fetch_64(&g_totalDumpOutRows, count);
        }
    }

    if (!g_args.avro) {
        fclose(fp);
    }

    return NULL;
}

static int64_t dumpNtbOfDbByThreads(
        SDbInfo *dbInfo,
        int64_t ntbCount)
{
    if (ntbCount <= 0) {
        return 0;
    }

    int threads = g_args.thread_num;

    int64_t a = ntbCount / threads;
    if (a < 1) {
        threads = ntbCount;
        a = 1;
    }

    assert(threads);
    int64_t b = ntbCount % threads;

    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    assert(pids);
    assert(infos);

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->taos = taos_connect(
                g_args.host,
                g_args.user,
                g_args.password,
                dbInfo->name,
                g_args.port
                );
        if (NULL == pThreadInfo->taos) {
            errorPrint("%s() LN%d, Failed to connect to TDengine, reason: %s\n",
                    __func__,
                    __LINE__,
                    taos_errstr(NULL));
            free(pids);
            free(infos);

            return -1;
        }

        pThreadInfo->threadIndex = i;
        pThreadInfo->count = (i<b)?a+1:a;
        pThreadInfo->from = (i==0)?0:
            ((threadInfo *)(infos + i - 1))->from +
            ((threadInfo *)(infos + i - 1))->count;
        strcpy(pThreadInfo->dbName, dbInfo->name);
        pThreadInfo->precision = getPrecisionByString(dbInfo->precision);

        pthread_create(pids + i, NULL, dumpNtbOfDb, pThreadInfo);
    }

    for (int64_t i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);
    }

    free(pids);
    free(infos);

    return 0;
}

static int64_t dumpNTablesOfDb(SDbInfo *dbInfo)
{
    TAOS *taos = taos_connect(g_args.host,
            g_args.user, g_args.password, dbInfo->name, g_args.port);
    if (NULL == taos) {
        errorPrint(
                "Failed to connect to TDengine server %s by specified database %s\n",
                g_args.host, dbInfo->name);
        return 0;
    }

    char command[COMMAND_SIZE];
    TAOS_RES *result;
    int32_t code;

    sprintf(command, "USE %s", dbInfo->name);
    result = taos_query(taos, command);
    code = taos_errno(result);
    if (code != 0) {
        errorPrint("invalid database %s, reason: %s\n",
                dbInfo->name, taos_errstr(result));
        taos_close(taos);
        return 0;
    }

    sprintf(command, "SHOW TABLES");
    result = taos_query(taos, command);
    code = taos_errno(result);
    if (code != 0) {
        errorPrint("Failed to show %s\'s tables, reason: %s\n",
                dbInfo->name, taos_errstr(result));
        taos_close(taos);
        return 0;
    }

    g_tablesList = calloc(1, dbInfo->ntables * sizeof(TableInfo));
    assert(g_tablesList);

    TAOS_ROW row;
    int64_t count = 0;
    while(NULL != (row = taos_fetch_row(result))) {
        int32_t *length = taos_fetch_lengths(result);
        char tbName[TSDB_TABLE_NAME_LEN+1];
        char stbName[TSDB_TABLE_NAME_LEN+1];
        strncpy(tbName,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN, length[TSDB_SHOW_TABLES_NAME_INDEX]));

        debugPrint("%s() LN%d, No.\t%"PRId64" table name: %s\n",
                __func__, __LINE__,
                count, tbName);
        strncpy(((TableInfo *)(g_tablesList + count))->name,
                tbName,
                min(TSDB_TABLE_NAME_LEN, length[TSDB_SHOW_TABLES_NAME_INDEX]));
        strncpy(stbName,
                (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
                min(TSDB_TABLE_NAME_LEN, length[TSDB_SHOW_TABLES_METRIC_INDEX]));
        if (strlen(stbName)) {
            strncpy(((TableInfo *)(g_tablesList + count))->stable,
                stbName,
                min(TSDB_TABLE_NAME_LEN, length[TSDB_SHOW_TABLES_METRIC_INDEX]));
            ((TableInfo *)(g_tablesList + count))->belongStb = true;
        } else {
            ((TableInfo *)(g_tablesList + count))->belongStb = false;
        }
        count ++;
    }
    taos_close(taos);

    int64_t records = dumpNtbOfDbByThreads(dbInfo, count);

    free(g_tablesList);
    g_tablesList = NULL;

    return records;
}

static int64_t dumpNtbOfStbByThreads(
        SDbInfo *dbInfo, char *stbName)
{
    int64_t ntbCount = getNtbCountOfStb(dbInfo->name, stbName);

    if (ntbCount <= 0) {
        return 0;
    }

    int threads = g_args.thread_num;

    int64_t a = ntbCount / threads;
    if (a < 1) {
        threads = ntbCount;
        a = 1;
    }

    assert(threads);
    int64_t b = ntbCount % threads;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    assert(pids);
    assert(infos);

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->taos = taos_connect(
                g_args.host,
                g_args.user,
                g_args.password,
                dbInfo->name,
                g_args.port
                );
        if (NULL == pThreadInfo->taos) {
            errorPrint("%s() LN%d, Failed to connect to TDengine, reason: %s\n",
                    __func__,
                    __LINE__,
                    taos_errstr(NULL));
            free(pids);
            free(infos);

            return -1;
        }

        pThreadInfo->threadIndex = i;
        pThreadInfo->count = (i<b)?a+1:a;
        pThreadInfo->from = (i==0)?0:
            ((threadInfo *)(infos + i - 1))->from +
            ((threadInfo *)(infos + i - 1))->count;
        strcpy(pThreadInfo->dbName, dbInfo->name);
        pThreadInfo->precision = getPrecisionByString(dbInfo->precision);

        strcpy(pThreadInfo->stbName, stbName);
        pthread_create(pids + i, NULL, dumpNormalTablesOfStb, pThreadInfo);
    }

    for (int64_t i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t records = 0;
    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        records += pThreadInfo->rowsOfDumpOut;
        taos_close(pThreadInfo->taos);
    }

    free(pids);
    free(infos);

    return records;
}

static int dumpTbTagsToAvro(
        int64_t index,
        TAOS *taos, SDbInfo *dbInfo, char *stable,
        char *specifiedTb)
{
    debugPrint("%s() LN%d dbName: %s, stable: %s\n",
            __func__, __LINE__,
            dbInfo->name,
            stable);

    char dumpFilename[MAX_PATH_LEN] = {0};
    sprintf(dumpFilename, "%s%s.%"PRId64".%"PRId64".avro-tbtags",
            g_args.outpath, dbInfo->name,
            index,
            getUniqueIDFromEpoch());
    int ret = createMTableAvroHead(
            taos,
            dumpFilename,
            dbInfo->name,
            stable,
            -1, 0,
            specifiedTb);
    if (-1 == ret) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, dumpFilename);
    }

    return ret;
}

static int64_t dumpCreateSTableClauseOfDb(
        SDbInfo *dbInfo, FILE *fp)
{
    TAOS *taos = taos_connect(g_args.host,
            g_args.user, g_args.password, dbInfo->name, g_args.port);
    if (NULL == taos) {
        errorPrint(
                "Failed to connect to TDengine server %s by specified database %s\n",
                g_args.host, dbInfo->name);
        return 0;
    }

    TAOS_ROW row;
    char command[COMMAND_SIZE] = {0};

    sprintf(command, "SHOW %s.STABLES", dbInfo->name);

    TAOS_RES* res = taos_query(taos, command);
    int32_t  code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        taos_close(taos);
        exit(-1);
    }

    int64_t superTblCnt = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == dumpStableClasuse(taos, dbInfo,
                    row[TSDB_SHOW_TABLES_NAME_INDEX],
                    fp)) {
            superTblCnt ++;
        }

        if (g_args.avro) {
            dumpTbTagsToAvro(
                    superTblCnt,
                    taos,
                    dbInfo,
                    row[TSDB_SHOW_TABLES_NAME_INDEX],
                    NULL
                    );
        }
    }

    taos_free_result(res);

    fprintf(g_fpOfResult,
            "# super table counter:               %"PRId64"\n",
            superTblCnt);
    g_resultStatistics.totalSuperTblsOfDumpOut += superTblCnt;

    taos_close(taos);

    return superTblCnt;
}

static int64_t dumpWholeDatabase(SDbInfo *dbInfo, FILE *fp)
{
    printf("Start to dump out database: %s\n", dbInfo->name);

    dumpCreateDbClause(dbInfo, g_args.with_property, fp);

    fprintf(g_fpOfResult, "\n#### database:                       %s\n",
            dbInfo->name);
    g_resultStatistics.totalDatabasesOfDumpOut++;

    dumpCreateSTableClauseOfDb(dbInfo, fp);

    return dumpNTablesOfDb(dbInfo);
}

static bool checkFileExists(char *path, char *filename)
{
    char filePath[MAX_PATH_LEN] = {0};
    if (strlen(path)) {
        sprintf(filePath, "%s/%s", path, filename);
    } else {
        sprintf(filePath, "%s/%s", ".", filename);
    }

    if( access(filePath, F_OK ) == 0 ) {
        return true;
    }

    return false;
}

static bool checkFileExistsExt(char *path, char *ext)
{
    bool bRet;

    int namelen, extlen;
    struct dirent *pDirent;
    DIR *pDir;

    extlen = strlen(ext);
    pDir = opendir(path);

    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen (pDirent->d_name);
            if (namelen > extlen) {
                if (strcmp (ext, &(pDirent->d_name[namelen - extlen])) == 0) {
                    bRet = true;
                }
            }
        }
        closedir (pDir);
    }

    return bRet;
}

static void checkOutDirAndWarn(char *outpath)
{
    DIR *pDir = NULL;

    if (strlen(outpath)) {
        if (NULL == (pDir= opendir(outpath))) {
            errorPrint("%s is not exist!\n", outpath);
            return;
        }
    } else {
        outpath = ".";
    }

    if ((true == checkFileExists(outpath, "dbs.sql"))
                || (0 != checkFileExistsExt(outpath, "avro-tbstb"))
                || (0 != checkFileExistsExt(outpath, "avro-ntb"))
                || (0 != checkFileExistsExt(outpath, "avro"))) {
        if (strlen(outpath)) {
            warnPrint("Found data file(s) exists in %s! Continue to dump out will overwrite exist file(s)!\n", outpath);
        } else {
            warnPrint("Found data file(s) exists in %s! Continue to dump out will overwrite exist file(s)!\n", "current path");
        }
        prompt();
    }

    if (pDir) {
        closedir(pDir);
    }

    return;
}

static int dumpOut() {
    TAOS     *taos       = NULL;
    TAOS_RES *result     = NULL;

    TAOS_ROW row;
    FILE *fp = NULL;
    int32_t count = 0;

    checkOutDirAndWarn(g_args.outpath);

    char dumpFilename[MAX_PATH_LEN] = {0};
    sprintf(dumpFilename, "%sdbs.sql", g_args.outpath);

    fp = fopen(dumpFilename, "w");
    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, dumpFilename);
        return -1;
    }

    g_args.dumpDbCount = getDumpDbCount();
    debugPrint("%s() LN%d, dump db count: %d\n",
            __func__, __LINE__, g_args.dumpDbCount);

    if (0 == g_args.dumpDbCount) {
        errorPrint("%d databases valid to dump\n", g_args.dumpDbCount);
        fclose(fp);
        return -1;
    }

    g_dbInfos = (SDbInfo **)calloc(g_args.dumpDbCount, sizeof(SDbInfo *));
    if (g_dbInfos == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        goto _exit_failure;
    }

    char command[COMMAND_SIZE];

    /* Connect to server */
    taos = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        goto _exit_failure;
    }

    /* --------------------------------- Main Code -------------------------------- */
    dumpCharset(fp);

    sprintf(command, "show databases");
    result = taos_query(taos, command);
    int32_t code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        goto _exit_failure;
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);

    while ((row = taos_fetch_row(result)) != NULL) {
        if ((strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log",
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
                && (!g_args.allow_sys)) {
            continue;
        }

        if (g_args.databases) {
            if (inDatabasesSeq(
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0) {
                continue;
            }
        } else if (!g_args.all_databases) {
            if (strncasecmp(g_args.arg_list[0],
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0)
                continue;
        }

        g_dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
        if (g_dbInfos[count] == NULL) {
            errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                    __func__, __LINE__, (uint64_t)sizeof(SDbInfo));
            goto _exit_failure;
        }

        *(((char*)row[TSDB_SHOW_DB_NAME_INDEX])+fields[TSDB_SHOW_DB_NAME_INDEX].bytes) = '\0';
        okPrint("Database:%s exists\n", (char *)row[TSDB_SHOW_DB_NAME_INDEX]);
        tstrncpy(g_dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                min(TSDB_DB_NAME_LEN,
                    fields[TSDB_SHOW_DB_NAME_INDEX].bytes + 1));
        g_dbInfos[count]->ntables =
            *((int32_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
        g_dbInfos[count]->vgroups =
            *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);
        g_dbInfos[count]->replica =
            *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
        g_dbInfos[count]->quorum =
            *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
        g_dbInfos[count]->days =
            *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);

        tstrncpy(g_dbInfos[count]->keeplist,
                (char *)row[TSDB_SHOW_DB_KEEP_INDEX],
                min(32, fields[TSDB_SHOW_DB_KEEP_INDEX].bytes + 1));

        g_dbInfos[count]->cache =
            *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
        g_dbInfos[count]->blocks =
            *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
        g_dbInfos[count]->minrows =
            *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
        g_dbInfos[count]->maxrows =
            *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
        g_dbInfos[count]->wallevel =
            *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
        g_dbInfos[count]->fsync =
            *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
        g_dbInfos[count]->comp =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
        g_dbInfos[count]->cachelast =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_CACHELAST_INDEX]));

        tstrncpy(g_dbInfos[count]->precision,
                (char *)row[TSDB_SHOW_DB_PRECISION_INDEX],
                DB_PRECISION_LEN);
        g_dbInfos[count]->update =
            *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
        count++;

        if (g_args.databases) {
            if (count > g_args.dumpDbCount)
                break;
        } else if (!g_args.all_databases) {
            if (count >= 1)
                break;
        }
    }

    if (count == 0) {
        errorPrint("%d databases valid to dump\n", count);
        goto _exit_failure;
    }

    // case: taosdump --databases dbx,dby ...   OR  taosdump --all-databases
    if (g_args.databases || g_args.all_databases) {
        for (int i = 0; i < count; i++) {
            int64_t records = 0;
            records = dumpWholeDatabase(g_dbInfos[i], fp);
            if (records >= 0) {
                okPrint("Database %s dumped\n", g_dbInfos[i]->name);
                g_totalDumpOutRows += records;
            }
        }
    } else {
        if (1 == g_args.arg_list_len) {
            int64_t records = dumpWholeDatabase(g_dbInfos[0], fp);
            if (records >= 0) {
                okPrint("Database %s dumped\n", g_dbInfos[0]->name);
                g_totalDumpOutRows += records;
            }
        } else {
            dumpCreateDbClause(g_dbInfos[0], g_args.with_property, fp);
        }

        int superTblCnt = 0 ;
        for (int64_t i = 1; g_args.arg_list[i]; i++) {
            if (0 == strlen(g_args.arg_list[i])) {
                continue;
            }
            TableRecordInfo tableRecordInfo;

            if (getTableRecordInfo(g_dbInfos[0]->name,
                        g_args.arg_list[i],
                        &tableRecordInfo) < 0) {
                errorPrint("input the invalid table %s\n",
                        g_args.arg_list[i]);
                continue;
            }

            int64_t records = 0;
            if (tableRecordInfo.isStb) {  // dump all table of this stable
                int ret = dumpStableClasuse(
                        taos,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        fp);
                if (ret >= 0) {
                    superTblCnt++;
                    records = dumpNtbOfStbByThreads(g_dbInfos[0], g_args.arg_list[i]);
                }
            } else if (tableRecordInfo.belongStb){
                dumpStableClasuse(
                        taos,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        fp);

                if (g_args.avro) {
                    dumpTbTagsToAvro(
                            i,
                            taos,
                            g_dbInfos[0],
                            tableRecordInfo.tableRecord.stable,
                            g_args.arg_list[i]);
                }
                records = dumpNormalTableBelongStb(
                        i,
                        taos,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        g_args.arg_list[i]);
            } else {
                records = dumpNormalTableWithoutStb(
                        i,
                        taos, g_dbInfos[0], g_args.arg_list[i]);
            }

            if (records >= 0) {
                okPrint("table: %s dumped\n", g_args.arg_list[i]);
                g_totalDumpOutRows += records;
            }
        }
    }

    taos_close(taos);

    /* Close the handle and return */
    fclose(fp);
    taos_free_result(result);
    freeDbInfos();
    okPrint("%" PRId64 " row(s) dumped out!\n", g_totalDumpOutRows);
    return 0;

_exit_failure:
    fclose(fp);
    taos_close(taos);
    taos_free_result(result);
    freeDbInfos();
    errorPrint("%" PRId64 " row(s) dumped out!\n", g_totalDumpOutRows);
    return -1;
}

int main(int argc, char *argv[]) {
    static char verType[32] = {0};
    sprintf(verType, "version: %s\n", version);
    argp_program_version = verType;

    g_uniqueID = getUniqueIDFromEpoch();

    int ret = 0;
    /* Parse our arguments; every option seen by parse_opt will be
       reflected in arguments. */
    if (argc > 1) {
        parse_timestamp(argc, argv, &g_args);
        parse_args(argc, argv, &g_args);
    }

    argp_parse(&argp, argc, argv, 0, 0, &g_args);

    if (g_args.abort) {
        abort();
    }

    printf("====== arguments config ======\n");

    printf("host: %s\n", g_args.host);
    printf("user: %s\n", g_args.user);
    printf("password: %s\n", g_args.password);
    printf("port: %u\n", g_args.port);
    printf("outpath: %s\n", g_args.outpath);
    printf("inpath: %s\n", g_args.inpath);
    printf("resultFile: %s\n", g_args.resultFile);
    printf("all_databases: %s\n", g_args.all_databases?"true":"false");
    printf("databases: %d\n", g_args.databases);
    printf("databasesSeq: %s\n", g_args.databasesSeq);
    printf("schemaonly: %s\n", g_args.schemaonly?"true":"false");
    printf("with_property: %s\n", g_args.with_property?"true":"false");
    printf("answer_yes: %s\n", g_args.answer_yes?"true":"false");
    printf("avro codec: %s\n", g_avro_codec[g_args.avro_codec]);
    printf("start_time: %" PRId64 "\n", g_args.start_time);
    printf("human readable start time: %s \n", g_args.humanStartTime);
    printf("end_time: %" PRId64 "\n", g_args.end_time);
    printf("human readable end time: %s \n", g_args.humanEndTime);
    printf("precision: %s\n", g_args.precision);
    printf("data_batch: %d\n", g_args.data_batch);
    printf("max_sql_len: %d\n", g_args.max_sql_len);
    printf("thread_num: %d\n", g_args.thread_num);
    printf("allow_sys: %d\n", g_args.allow_sys);
    printf("abort: %d\n", g_args.abort);
    printf("isDumpIn: %d\n", g_args.isDumpIn);
    printf("arg_list_len: %d\n", g_args.arg_list_len);
    printf("debug_print: %d\n", g_args.debug_print);
    fflush(stdout);

    for (int32_t i = 0; i < g_args.arg_list_len; i++) {
        if (g_args.databases || g_args.all_databases) {
            errorPrint("%s is an invalid input if database(s) be already specified.\n",
                    g_args.arg_list[i]);
            exit(EXIT_FAILURE);
        } else {
            printf("arg_list[%d]: %s\n", i, g_args.arg_list[i]);
        }
    }

    printf("==============================\n");
    if (checkParam(&g_args) < 0) {
        exit(EXIT_FAILURE);
    }

    g_fpOfResult = fopen(g_args.resultFile, "a");
    if (NULL == g_fpOfResult) {
        errorPrint("Failed to open %s for save result\n", g_args.resultFile);
        exit(-1);
    };

    fprintf(g_fpOfResult, "#############################################################################\n");
    fprintf(g_fpOfResult, "============================== arguments config =============================\n");

    fprintf(g_fpOfResult, "host: %s\n", g_args.host);
    fprintf(g_fpOfResult, "user: %s\n", g_args.user);
    fprintf(g_fpOfResult, "password: %s\n", g_args.password);
    fprintf(g_fpOfResult, "port: %u\n", g_args.port);
    fprintf(g_fpOfResult, "outpath: %s\n", g_args.outpath);
    fprintf(g_fpOfResult, "inpath: %s\n", g_args.inpath);
    fprintf(g_fpOfResult, "resultFile: %s\n", g_args.resultFile);
    fprintf(g_fpOfResult, "all_databases: %s\n", g_args.all_databases?"true":"false");
    fprintf(g_fpOfResult, "databases: %d\n", g_args.databases);
    fprintf(g_fpOfResult, "databasesSeq: %s\n", g_args.databasesSeq);
    fprintf(g_fpOfResult, "schemaonly: %s\n", g_args.schemaonly?"true":"false");
    fprintf(g_fpOfResult, "with_property: %s\n", g_args.with_property?"true":"false");
    fprintf(g_fpOfResult, "answer_yes: %s\n", g_args.answer_yes?"true":"false");
    fprintf(g_fpOfResult, "avro codec: %s\n", g_avro_codec[g_args.avro_codec]);
    fprintf(g_fpOfResult, "start_time: %" PRId64 "\n", g_args.start_time);
    fprintf(g_fpOfResult, "human readable start time: %s \n", g_args.humanStartTime);
    fprintf(g_fpOfResult, "end_time: %" PRId64 "\n", g_args.end_time);
    fprintf(g_fpOfResult, "human readable end time: %s \n", g_args.humanEndTime);
    fprintf(g_fpOfResult, "precision: %s\n", g_args.precision);
    fprintf(g_fpOfResult, "data_batch: %d\n", g_args.data_batch);
    fprintf(g_fpOfResult, "max_sql_len: %d\n", g_args.max_sql_len);
    fprintf(g_fpOfResult, "thread_num: %d\n", g_args.thread_num);
    fprintf(g_fpOfResult, "allow_sys: %d\n", g_args.allow_sys);
    fprintf(g_fpOfResult, "abort: %d\n", g_args.abort);
    fprintf(g_fpOfResult, "isDumpIn: %d\n", g_args.isDumpIn);
    fprintf(g_fpOfResult, "arg_list_len: %d\n", g_args.arg_list_len);

    for (int32_t i = 0; i < g_args.arg_list_len; i++) {
        fprintf(g_fpOfResult, "arg_list[%d]: %s\n", i, g_args.arg_list[i]);
    }
    fprintf(g_fpOfResult, "debug_print: %d\n", g_args.debug_print);

    g_numOfCores = (int32_t)sysconf(_SC_NPROCESSORS_ONLN);

    time_t tTime = time(NULL);
    struct tm tm = *localtime(&tTime);

    taos_options(TSDB_OPTION_CONFIGDIR, g_configDir);

    if (g_args.isDumpIn) {
        fprintf(g_fpOfResult, "============================== DUMP IN ============================== \n");
        fprintf(g_fpOfResult, "# DumpIn start time:                   %d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        int dumpInRet = dumpIn();
        if (dumpInRet) {
            errorPrint("%s\n", "dumpIn() failed!");
            okPrint("%"PRId64" row(s) dumped in!\n", g_totalDumpInRecSuccess);
            errorPrint("%"PRId64" rows(s) failed to dump in!\n", g_totalDumpInRecFailed);
            ret = -1;
        } else {
            if (g_totalDumpInRecFailed < 0) {
                if (g_totalDumpInRecSuccess > 0) {
                    okPrint("%"PRId64" row(s) dumped in!\n", g_totalDumpInRecSuccess);
                }
                errorPrint("%"PRId64" rows(s) failed to dump in!\n", g_totalDumpInRecFailed);
            } else {
                okPrint("%"PRId64" row(s) dumped in!\n", g_totalDumpInRecSuccess);
            }
            ret = 0;
        }
    } else {
        fprintf(g_fpOfResult, "============================== DUMP OUT ============================== \n");
        fprintf(g_fpOfResult, "# DumpOut start time:                   %d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

        if (dumpOut() < 0) {
            ret = -1;
        }

        fprintf(g_fpOfResult, "\n============================== TOTAL STATISTICS ============================== \n");
        fprintf(g_fpOfResult, "# total database count:     %d\n",
                g_resultStatistics.totalDatabasesOfDumpOut);
        fprintf(g_fpOfResult, "# total super table count:  %d\n",
                g_resultStatistics.totalSuperTblsOfDumpOut);
        fprintf(g_fpOfResult, "# total child table count:  %"PRId64"\n",
                g_resultStatistics.totalChildTblsOfDumpOut);
        fprintf(g_fpOfResult, "# total row count:          %"PRId64"\n",
                g_resultStatistics.totalRowsOfDumpOut);
    }

    fprintf(g_fpOfResult, "\n");
    fclose(g_fpOfResult);

    if (g_tablesList) {
        free(g_tablesList);
    }

    return ret;
}

