#ifndef INC_DUMP_H_
#define INC_DUMP_H_


#ifdef WINDOWS
#include <argp.h>
#include <time.h>
#include <WinSock2.h>
#elif defined(DARWIN)
#include <ctype.h>
#include <unistd.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <wordexp.h>
#else
#include <argp.h>
#include <unistd.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <wordexp.h>
#include <dirent.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <iconv.h>
#include <sys/stat.h>

#include <inttypes.h>
#include <limits.h>

#include <avro.h>
#include <jansson.h>

#include <taos.h>
#include <taoserror.h>
#include <toolsdef.h>

#ifdef WEBSOCKET
#include <taosws.h>
#endif


//
// ---------------- define ----------------
//

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include <cus_name.h>
#else
#ifndef CUS_NAME
    #define CUS_NAME      "TDengine"
#endif

#ifndef CUS_PROMPT
    #define CUS_PROMPT    "taos"
#endif

#ifndef CUS_EMAIL
    #define CUS_EMAIL     "<support@taosdata.com>"
#endif
#endif

// get taosdump commit number version
#ifndef TAOSDUMP_COMMIT_SHA1
#define TAOSDUMP_COMMIT_SHA1 "unknown"
#endif

#ifndef TAOSDUMP_TAG
#define TAOSDUMP_TAG "0.1.0"
#endif

#ifndef TAOSDUMP_STATUS
#define TAOSDUMP_STATUS "unknown"
#endif

// use 256 as normal buffer length
#define BUFFER_LEN              256

#define VALUE_BUF_LEN           4096
#define MAX_RECORDS_PER_REQ     32766
#define NEED_CALC_COUNT         UINT64_MAX
#define HUMAN_TIME_LEN      28
#define DUMP_DIR_LEN        (MAX_DIR_LEN - (TSDB_DB_NAME_LEN + 10))


#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) { \
      fprintf(stdout, "DEBG: "fmt, __VA_ARGS__); } } while (0)

#define debugPrint2(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) { \
      fprintf(stdout, ""fmt, __VA_ARGS__); } } while (0)

#define verbosePrint(fmt, ...) \
    do { if (g_args.verbose_print) { \
        fprintf(stdout, "VERB: "fmt, __VA_ARGS__); } } while (0)

#define perfPrint(fmt, ...) \
    do { if (g_args.performance_print) { \
        fprintf(stdout, "PERF: "fmt, __VA_ARGS__); } } while (0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while (0)

#define infoPrint(fmt, ...) \
    do { \
        fprintf(stdout, "INFO: "fmt, __VA_ARGS__); \
    } while (0)

#define freeTbNameIfLooseMode(tbName) \
    do { \
        if (g_dumpInLooseModeFlag) tfree(tbName);   \
    } while (0)


//
// ------------- struct  ---------------
//


// rename db 
typedef struct SRenameDB {
    char* old;
    char* new;
    void* next;
}SRenameDB;

/* Used by main to communicate with parse_opt. */
typedef struct arguments {
    // connection option
    char    *host;
    char    *user;
    char     password[SHELL_MAX_PASSWORD_LEN];
    uint16_t port;
    // strlen(taosdump.) +1 is 10
    char     outpath[DUMP_DIR_LEN];
    char     inpath[DUMP_DIR_LEN];
    // result file
    char    *resultFile;
    // dump unit option
    bool     all_databases;
    bool     databases;
    char    *databasesSeq;
    // dump format option
    bool     schemaonly;
    bool     with_property;
    bool     avro;
    int      avro_codec;
    int64_t  start_time;
    char     humanStartTime[HUMAN_TIME_LEN];
    int64_t  end_time;
    char     humanEndTime[HUMAN_TIME_LEN];
    char     precision[8];

    int32_t  data_batch;
    bool     data_batch_input;
    int32_t  max_sql_len;
    bool     allow_sys;
    bool     escape_char;
    bool     db_escape_char;
    bool     loose_mode;
    bool     inspect;
    // other options
    int32_t  thread_num;
    int      abort;
    char   **arg_list;
    int      arg_list_len;
    bool     isDumpIn;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;
    bool     dotReplace;
    int      dumpDbCount;
#ifdef WEBSOCKET
    bool     restful;
    bool     cloud;
    int      ws_timeout;
    char    *dsn;
    char    *cloudToken;
    int      cloudPort;
    char     cloudHost[MAX_HOSTNAME_LEN];
#endif

    // put rename db string
    char      * renameBuf;
    SRenameDB * renameHead;
    // retry for call engine api
    int32_t     retryCount;
    int32_t     retrySleepMs;      
} SArguments;

extern struct arguments g_args;

#endif  // INC_DUMP_H_