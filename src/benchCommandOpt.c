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
 */

#include "bench.h"

#ifdef LINUX
#include <argp.h>
#else
#ifndef ARGP_ERR_UNKNOWN
    #define ARGP_ERR_UNKNOWN E2BIG
#endif
#endif

extern char version[];

// get taosBenchmark commit number version
#ifndef TAOSBENCHMARK_COMMIT_SHA1
#define TAOSBENCHMARK_COMMIT_SHA1 "unknown"
#endif

#ifndef TAOSBENCHMARK_TAG
#define TAOSBENCHMARK_TAG "0.1.0"
#endif

#ifndef TAOSBENCHMARK_STATUS
#define TAOSBENCHMARK_STATUS "unknown"
#endif

#ifdef WINDOWS
char      g_configDir[MAX_PATH_LEN] = {0};  // "C:\\TDengine\\cfg"};
#else
char      g_configDir[MAX_PATH_LEN] = {0};  // "/etc/taos"};
#endif

char *g_aggreFuncDemo[] = {"*",
                           "count(*)",
                           "avg(current)",
                           "sum(current)",
                           "max(current)",
                           "min(current)",
                           "first(current)",
                           "last(current)"};
char *g_aggreFunc[] = {"*",       "count(*)", "avg(C0)",   "sum(C0)",
                       "max(C0)", "min(C0)",  "first(C0)", "last(C0)"};

void printVersion() {
    char taosBenchmark_ver[] = TAOSBENCHMARK_TAG;
    char taosBenchmark_commit[] = TAOSBENCHMARK_COMMIT_SHA1;
    char taosBenchmark_status[] = TAOSBENCHMARK_STATUS;
    if (0 == strlen(taosBenchmark_status)) {
        printf("taosBenchmark version: %s\ngitinfo: %s\n",
                taosBenchmark_ver, taosBenchmark_commit);
    } else {
        printf("taosBenchmark version: %s\ngitinfo: %s\nstatus: %s\n",
                taosBenchmark_ver, taosBenchmark_commit, taosBenchmark_status);
    }
}

void parseFieldDatatype(char *dataType, BArray *fields, bool isTag) {
    char *dup_str;
    benchArrayClear(fields);
    if (strstr(dataType, ",") == NULL) {
        Field * field = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(fields, field);
        field = benchArrayGet(fields, 0);
        if (1 == regexMatch(dataType,
                    "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$",
                    REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            field->type = convertStringToDatatype(type, 0);
            field->length = atoi(length);
        } else {
            field->type = convertStringToDatatype(dataType, 0);
            field->length = convertTypeToLength(field->type);
        }
        field->min = convertDatatypeToDefaultMin(field->type);
        field->max = convertDatatypeToDefaultMax(field->type);
        tstrncpy(field->name, isTag?"t0":"c0", TSDB_COL_NAME_LEN);
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        int   index = 0;
        while (token != NULL) {
            Field * field = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(fields, field);
            field = benchArrayGet(fields, index);
            if (1 == regexMatch(token,
                        "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$",
                        REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                field->type = convertStringToDatatype(type, 0);
                field->length = atoi(length);
            } else {
                field->type = convertStringToDatatype(token, 0);
                field->length = convertTypeToLength(field->type);
            }
            field->max = convertDatatypeToDefaultMax(field->type);
            field->min = convertDatatypeToDefaultMin(field->type);
            snprintf(field->name, TSDB_COL_NAME_LEN, isTag?"t%d":"c%d", index);
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
}

int32_t benchParseSingleOpt(int32_t key, char* arg) {
    SDataBase *database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    switch (key) {
        case 'F':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "F");
            }

            g_arguments->prepared_rand = atol(arg);
            if (g_arguments->prepared_rand <= 0) {
                errorPrint(
                           "Invalid -F: %s, will auto set to default(10000)\n",
                           arg);
                g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
            }
            break;

        case 'f':
            g_arguments->demo_mode = false;
            g_arguments->metaFile = arg;
            g_arguments->nthreads_auto = false;
            break;

        case 'h':
            g_arguments->host = arg;
            g_arguments->host_auto = false;
            break;

        case 'P':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "P");
            }
            g_arguments->port = atoi(arg);
            if (g_arguments->port <= 0) {
                errorPrint(
                           "Invalid -P: %s, will auto set to default(6030)\n",
                           arg);
                if (REST_IFACE == g_arguments->iface) {
                    g_arguments->port = DEFAULT_REST_PORT;
                } else {
                    g_arguments->port = DEFAULT_PORT;
                }
            } else {
                g_arguments->port_auto = false;
            }
            g_arguments->port_inputed = true;
            break;

        case 'I':
            if (0 == strcasecmp(arg, "taosc")) {
                stbInfo->iface = TAOSC_IFACE;
            } else if (0 == strcasecmp(arg, "stmt")) {
                stbInfo->iface = STMT_IFACE;
            } else if (0 == strcasecmp(arg, "rest")) {
                stbInfo->iface = REST_IFACE;
                g_arguments->nthreads_auto = false;
                if (false == g_arguments->port_inputed) {
                    g_arguments->port = DEFAULT_REST_PORT;
                }
            } else if (0 == strcasecmp(arg, "sml")) {
                stbInfo->iface = SML_IFACE;
            } else {
                errorPrint(
                           "Invalid -I: %s, will auto set to default (taosc)\n",
                           arg);
                stbInfo->iface = TAOSC_IFACE;
            }
            g_arguments->iface = stbInfo->iface;
            break;

        case 'p':
            g_arguments->password = arg;
            break;

        case 'u':
            g_arguments->user = arg;
            break;

        case 'c':
            tstrncpy(g_configDir, arg, TSDB_FILENAME_LEN);
            g_arguments->cfg_inputed = true;
            break;

        case 'o':
            g_arguments->output_file = arg;
            break;

        case 'T':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "T");
            }

            g_arguments->nthreads = atoi(arg);
            if (g_arguments->nthreads <= 0) {
                errorPrint(
                           "Invalid -T: %s, will auto set to default(8)\n",
                           arg);
                g_arguments->nthreads = DEFAULT_NTHREADS;
            } else {
                g_arguments->nthreads_auto = false;
            }
            break;

        case 'i':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "i");
            }

            stbInfo->insert_interval = atoi(arg);
            if (stbInfo->insert_interval <= 0) {
                errorPrint(
                           "Invalid -i: %s, will auto set to default(0)\n",
                           arg);
                stbInfo->insert_interval = 0;
            }
            break;

        case 'S':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "S");
            }

            stbInfo->timestamp_step = atol(arg);
            if (stbInfo->timestamp_step <= 0) {
                errorPrint(
                           "Invalid -S: %s, will auto set to default(1)\n",
                           arg);
                stbInfo->timestamp_step = 1;
            }
            break;

        case 'B':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "B");
            }

            stbInfo->interlaceRows = atoi(arg);
            if (stbInfo->interlaceRows <= 0) {
                errorPrint(
                           "Invalid -B: %s, will auto set to default(0)\n",
                           arg);
                stbInfo->interlaceRows = 0;
            }
            break;

        case 'r':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "r");
            }

            g_arguments->reqPerReq = atoi(arg);
            if (g_arguments->reqPerReq <= 0 ||
                g_arguments->reqPerReq > MAX_RECORDS_PER_REQ) {
                errorPrint(
                           "Invalid -r: %s, will auto set to default(30000)\n",
                           arg);
                g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
            }
            break;

        case 's':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "s");
            }

            g_arguments->startTimestamp = atol(arg);
            break;

        case 'U':
            g_arguments->supplementInsert = true;
            g_arguments->nthreads_auto = false;
            break;

        case 't':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "t");
            }

            stbInfo->childTblCount = atoi(arg);
            if (stbInfo->childTblCount <= 0) {
                errorPrint(
                           "Invalid -t: %s, will auto set to default(10000)\n",
                           arg);
                stbInfo->childTblCount = DEFAULT_CHILDTABLES;
            }
            g_arguments->totalChildTables = stbInfo->childTblCount;
            break;

        case 'n':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "n");
            }

            stbInfo->insertRows = atol(arg);
            if (stbInfo->insertRows <= 0) {
                errorPrint(
                           "Invalid -n: %s, will auto set to default(10000)\n",
                           arg);
                stbInfo->insertRows = DEFAULT_INSERT_ROWS;
            }
            break;

        case 'd':
            database->dbName = arg;
            break;

        case 'l':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "l");
            }

            g_arguments->demo_mode = false;
            g_arguments->intColumnCount = atoi(arg);
            if (g_arguments->intColumnCount <= 0) {
                errorPrint(
                           "Invalid -l: %s, will auto set to default(0)\n",
                           arg);
                g_arguments->intColumnCount = 0;
            }
            break;

        case 'L':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "L");
            }

            g_arguments->demo_mode = false;
            g_arguments->partialColNum = atoi(arg);
            break;

        case 'A':
            g_arguments->demo_mode = false;
            parseFieldDatatype(arg, stbInfo->tags, true);
            break;

        case 'b':
            g_arguments->demo_mode = false;
            parseFieldDatatype(arg, stbInfo->cols, false);
            break;

        case 'k':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "k");
            }

            g_arguments->keep_trying = atoi(arg);
            debugPrint("keep_trying: %d\n", g_arguments->keep_trying);
            break;

        case 'z':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "z");
            }

            g_arguments->trying_interval = atoi(arg);
            if (g_arguments->trying_interval < 0) {
                errorPrint(
                        "Invalid value for z: %s, will auto set to default(0)\n",
                        arg);
                g_arguments->trying_interval = 0;
            }
            debugPrint("trying_interval: %d\n", g_arguments->trying_interval);
            break;

        case 'w':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "w");
            }

            g_arguments->binwidth = atoi(arg);
            if (g_arguments->binwidth <= 0) {
                errorPrint(
                        "Invalid value for w: %s, will auto set to default(64)\n",
                        arg);
                g_arguments->binwidth = DEFAULT_BINWIDTH;
            } else if (g_arguments->binwidth > TSDB_MAX_BINARY_LEN) {
                errorPrint(
                           "-w(%d) > TSDB_MAX_BINARY_LEN(%" PRIu64
                                   "), will auto set to default(64)\n",
                           g_arguments->binwidth, (uint64_t)TSDB_MAX_BINARY_LEN);
                g_arguments->binwidth = DEFAULT_BINWIDTH;
            }
            break;

        case 'm':
            stbInfo->childTblPrefix = arg;
            break;

        case 'E':
            stbInfo->escape_character = true;
            break;

        case 'C':
            g_arguments->chinese = true;
            break;

        case 'N':
            g_arguments->demo_mode = false;
            stbInfo->use_metric = false;
            benchArrayClear(stbInfo->tags);
            break;

        case 'M':
            g_arguments->demo_mode = false;
            break;

        case 'x':
            g_arguments->aggr_func = true;
            break;

        case 'y':
            g_arguments->answer_yes = true;
            break;

        case 'R':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "R");
            }

            stbInfo->disorderRange = atoi(arg);
            if (stbInfo->disorderRange <= 0) {
                errorPrint(
                           "Invalid value for -R: %s, will auto set to "
                           "default(1000)\n",
                           arg);
                stbInfo->disorderRange =
                        DEFAULT_DISORDER_RANGE;
            }
            break;

        case 'O':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "O");
            }

            stbInfo->disorderRatio = atoi(arg);
            if (stbInfo->disorderRatio <= 0) {
                errorPrint(
                        "Invalid value for -O: %s, will auto set to default(0)\n",
                        arg);
                stbInfo->disorderRatio = 0;
            }
            break;

        case 'a':{
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "a");
            }

            int replica = atoi(arg);
            if (replica <= 0) {
                errorPrint(
                        "Invalid value for -a: %s, will auto set to default(1)\n",
                        arg);
                replica = 1;
            }
            SDbCfg* cfg = benchCalloc(1, sizeof(SDbCfg), true);
            cfg->name = benchCalloc(1, 10, true);
            sprintf(cfg->name, "replica");
            cfg->valuestring = NULL;
            cfg->valueint = replica;
            benchArrayPush(database->cfgs, cfg);
            break;
        }
        case 'g':
            g_arguments->debug_print = true;
            break;
        case 'G':
            g_arguments->performance_print = true;
            break;

#ifdef WEBSOCKET
        case 'W':
            g_arguments->dsn = arg;
            break;
        case 'D':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2("taosBenchmark", "D");
            }

            g_arguments->timeout = atoi(arg);
            break;
#endif

        case 'V':
            printVersion();
            exit(0);

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

#ifdef LINUX

const char *              argp_program_version = version;
const char *              argp_program_bug_address = BENCH_EMAIL;

static struct argp_option bench_options[] = {
    {"file", 'f', "FILE", 0, BENCH_FILE, 0},
    {"config-dir", 'c', "CONFIG_DIR", 0, BENCH_CFG_DIR, 1},
    {"host", 'h', "HOST", 0, BENCH_HOST},
    {"port", 'P', "PORT", 0, BENCH_PORT},
    {"interface", 'I', "IFACE", 0, BENCH_MODE},
    {"user", 'u', "USER", 0, BENCH_USER},
    {"password", 'p', "PASSWORD", 0, BENCH_PASS},
    {"output", 'o', "FILE", 0, BENCH_OUTPUT},
    {"threads", 'T', "NUMBER", 0, BENCH_THREAD},
    {"insert-interval", 'i', "NUMBER", 0, BENCH_INTERVAL},
    {"time-step", 'S', "NUMBER", 0, BENCH_STEP},
    {"start-timestamp", 's', "NUMBER", 0, BENCH_START_TIMESTAMP},
    {"supplement-insert", 'U', 0, 0, BENCH_SUPPLEMENT},
    {"interlace-rows", 'B', "NUMBER", 0, BENCH_INTERLACE},
    {"rec-per-req", 'r', "NUMBER", 0, BENCH_BATCH},
    {"tables", 't', "NUMBER", 0, BENCH_TABLE},
    {"records", 'n', "NUMBER", 0, BENCH_ROWS},
    {"database", 'd', "DATABASE", 0, BENCH_DATABASE},
    {"columns", 'l', "NUMBER", 0, BENCH_COLS_NUM},
    {"partial-col-num", 'L', "NUMBER", 0, BENCH_PARTIAL_COL_NUM},
    {"tag-type", 'A', "TAG_TYPE", 0, BENCH_TAGS},
    {"data-type", 'b', "COL_TYPE", 0, BENCH_COLS},
    {"binwidth", 'w', "NUMBER", 0, BENCH_WIDTH},
    {"table-prefix", 'm', "TABLE_PREFIX", 0, BENCH_PREFIX},
    {"escape-character", 'E', 0, 0, BENCH_ESCAPE},
    {"chinese", 'C', 0, 0, BENCH_CHINESE},
    {"normal-table", 'N', 0, 0, BENCH_NORMAL},
    {"random", 'M', 0, 0, BENCH_RANDOM},
    {"aggr-func", 'x', 0, 0, BENCH_AGGR},
    {"answer-yes", 'y', 0, 0, BENCH_YES},
    {"disorder-range", 'R', "NUMBER", 0, BENCH_RANGE},
    {"disorder", 'O', "NUMBER", 0, BENCH_DISORDER},
    {"replia", 'a', "NUMBER", 0, BENCH_REPLICA},
    {"debug", 'g', 0, 0, BENCH_DEBUG},
    {"performance", 'G', 0, 0, BENCH_PERFORMANCE},
    {"prepared_rand", 'F', "NUMBER", 0, BENCH_PREPARE},
#ifdef WEBSOCKET
    {"cloud_dsn", 'W', "DSN", 0, BENCH_DSN},
    {"timeout", 'D', "NUMBER", 0, BENCH_TIMEOUT},
#endif
    {"keep-trying", 'k', "NUMBER", 0, BENCH_KEEPTRYING},
    {"trying-interval", 'z', "NUMBER", 0, BENCH_TRYING_INTERVAL},
    {"version", 'V', 0, 0, BENCH_VERSION},
    {0}
};

static error_t benchParseOpt(int key, char *arg, struct argp_state *state) {
    return benchParseSingleOpt(key, arg);
}

static struct argp bench_argp = {bench_options, benchParseOpt, "", ""};

void benchParseArgsByArgp(int argc, char *argv[]) {
    argp_parse(&bench_argp, argc, argv, 0, 0, g_arguments);
}

#endif

int32_t benchParseArgs(int32_t argc, char* argv[]) {
#ifdef LINUX
    benchParseArgsByArgp(argc, argv);
    return 0;
#else
    return benchParseArgsNoArgp(argc, argv);
#endif
}

static void init_stable() {
    SDataBase *database = benchArrayGet(g_arguments->databases, 0);
    database->superTbls = benchArrayInit(1, sizeof(SSuperTable));
    SSuperTable * stbInfo = benchCalloc(1, sizeof(SSuperTable), true);
    benchArrayPush(database->superTbls, stbInfo);
    stbInfo = benchArrayGet(database->superTbls, 0);
    stbInfo->iface = TAOSC_IFACE;
    stbInfo->stbName = "meters";
    stbInfo->childTblPrefix = DEFAULT_TB_PREFIX;
    stbInfo->escape_character = 0;
    stbInfo->use_metric = 1;
    stbInfo->max_sql_len = MAX_SQL_LEN;
    stbInfo->cols = benchArrayInit(3, sizeof(Field));
    for (int i = 0; i < 3; ++i) {
        Field *col = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(stbInfo->cols, col);
    }
    Field * c1 = benchArrayGet(stbInfo->cols, 0);
    Field * c2 = benchArrayGet(stbInfo->cols, 1);
    Field * c3 = benchArrayGet(stbInfo->cols, 2);

    c1->type = TSDB_DATA_TYPE_FLOAT;
    c2->type = TSDB_DATA_TYPE_INT;
    c3->type = TSDB_DATA_TYPE_FLOAT;

    c1->length = sizeof(float);
    c2->length = sizeof(int32_t);
    c3->length = sizeof(float);

    tstrncpy(c1->name, "current", TSDB_COL_NAME_LEN + 1);
    tstrncpy(c2->name, "voltage", TSDB_COL_NAME_LEN + 1);
    tstrncpy(c3->name, "phase", TSDB_COL_NAME_LEN + 1);

    c1->min = 9;
    c1->max = 10;
    c2->min = 110;
    c2->max = 119;
    c3->min = 115;
    c3->max = 125;

    stbInfo->tags = benchArrayInit(2, sizeof(Field));
    for (int i = 0; i < 2; ++i) {
        Field * tag = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(stbInfo->tags, tag);
    }
    Field * t1 = benchArrayGet(stbInfo->tags, 0);
    Field * t2 = benchArrayGet(stbInfo->tags, 1);

    t1->type = TSDB_DATA_TYPE_INT;
    t2->type = TSDB_DATA_TYPE_BINARY;

    t1->length = sizeof(int32_t);
    t2->length = 24;

    tstrncpy(t1->name, "groupid", TSDB_COL_NAME_LEN + 1);
    tstrncpy(t2->name, "location", TSDB_COL_NAME_LEN + 1);

    t1->min = 1;
    t1->max = 10;


    stbInfo->insert_interval = 0;
    stbInfo->timestamp_step = 1;
    stbInfo->interlaceRows = 0;
    stbInfo->childTblCount = DEFAULT_CHILDTABLES;
    stbInfo->childTblLimit = 0;
    stbInfo->childTblOffset = 0;
    stbInfo->autoCreateTable = false;
    stbInfo->childTblExists = false;
    stbInfo->random_data_source = true;
    stbInfo->lineProtocol = TSDB_SML_LINE_PROTOCOL;

    stbInfo->insertRows = DEFAULT_INSERT_ROWS;
    stbInfo->disorderRange = DEFAULT_DISORDER_RANGE;
    stbInfo->disorderRatio = 0;
    stbInfo->file_factor = -1;
    stbInfo->delay = -1;
    stbInfo->keep_trying = 0;
    stbInfo->trying_interval = 0;
}

static void init_database() {
    g_arguments->databases = benchArrayInit(1, sizeof(SDataBase));
    SDataBase *database = benchCalloc(1, sizeof(SDataBase), true);
    benchArrayPush(g_arguments->databases, database);
    database = benchArrayGet(g_arguments->databases, 0);
    database->dbName = DEFAULT_DATABASE;
    database->drop = 1;
    database->precision = TSDB_TIME_PRECISION_MILLI;
    database->sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    database->cfgs = benchArrayInit(1, sizeof(SDbCfg));
}

void init_argument() {
    g_arguments = benchCalloc(1, sizeof(SArguments), true);
    if (taos_get_client_info()[0] == '3') {
        g_arguments->taosc_version = 3;
    } else {
        g_arguments->taosc_version = 2;
    }
    g_arguments->test_mode = INSERT_TEST;
    g_arguments->demo_mode = 1;
    g_arguments->host = NULL;
    g_arguments->host_auto = true;
    g_arguments->port = DEFAULT_PORT;
    g_arguments->port_inputed = false;
    g_arguments->port_auto = true;
    g_arguments->telnet_tcp_port = TELNET_TCP_PORT;
    g_arguments->user = TSDB_DEFAULT_USER;
    g_arguments->password = TSDB_DEFAULT_PASS;
    g_arguments->answer_yes = 0;
    g_arguments->debug_print = 0;
    g_arguments->binwidth = DEFAULT_BINWIDTH;
    g_arguments->performance_print = 0;
    g_arguments->output_file = DEFAULT_OUTPUT;
    g_arguments->nthreads = DEFAULT_NTHREADS;
    g_arguments->nthreads_auto = true;
    g_arguments->table_threads = DEFAULT_NTHREADS;
    g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    g_arguments->totalChildTables = DEFAULT_CHILDTABLES;
    g_arguments->actualChildTables = 0;
    g_arguments->autoCreatedChildTables = 0;
    g_arguments->existedChildTables = 0;
    g_arguments->chinese = false;
    g_arguments->aggr_func = 0;
    g_arguments->terminate = false;
#ifdef WEBSOCKET
    g_arguments->timeout = 10;
#endif

    g_arguments->supplementInsert = false;
    g_arguments->startTimestamp = DEFAULT_START_TIME;
    g_arguments->partialColNum = 0;

    g_arguments->keep_trying = 0;
    g_arguments->trying_interval = 0;
    g_arguments->iface = TAOSC_IFACE;

    init_database();
    init_stable();
    g_arguments->streams = benchArrayInit(1, sizeof(SSTREAM));
}

void modify_argument() {
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable *superTable = benchArrayGet(database->superTbls, 0);
#ifdef WEBSOCKET
    if (!g_arguments->websocket) {
#endif
        if (strlen(g_configDir)
                && g_arguments->host_auto
                && g_arguments->port_auto) {
#ifdef LINUX
            wordexp_t full_path;
            if (wordexp(g_configDir, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", g_configDir);
                exit(EXIT_FAILURE);
            }
            taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
            wordfree(&full_path);
#else
            taos_options(TSDB_OPTION_CONFIGDIR, g_configDir);
#endif
            g_arguments->host = NULL;
            g_arguments->port = 0;
        }
#ifdef WEBSOCKET
    }
#endif

    superTable->startTimestamp = g_arguments->startTimestamp;

    if (0 != g_arguments->partialColNum) {
        superTable->partialColNum = g_arguments->partialColNum;
    }

    if (superTable->iface == STMT_IFACE) {
        if (g_arguments->reqPerReq > INT16_MAX) {
            g_arguments->reqPerReq = INT16_MAX;
        }
        if (g_arguments->prepared_rand > g_arguments->reqPerReq) {
            g_arguments->prepared_rand = g_arguments->reqPerReq;
        }
    }

    for (int i = 0; i < superTable->cols->size; ++i) {
        Field * col = benchArrayGet(superTable->cols, i);
        if (!g_arguments->demo_mode) {
            snprintf(col->name, TSDB_COL_NAME_LEN, "c%d", i);
            col->min = convertDatatypeToDefaultMin(col->type);
            col->max = convertDatatypeToDefaultMax(col->type);
        }
        if (col->length == 0) {
            col->length = g_arguments->binwidth;
        }
    }

    for (int i = 0; i < superTable->tags->size; ++i) {
        Field* tag = benchArrayGet(superTable->tags, i);
        if (!g_arguments->demo_mode) {
            snprintf(tag->name, TSDB_COL_NAME_LEN, "t%d", i);
        }
        if (tag->length == 0) {
            tag->length = g_arguments->binwidth;
        }
    }

    if (g_arguments->intColumnCount > superTable->cols->size) {
        for (int i = superTable->cols->size;
                i < g_arguments->intColumnCount; ++i) {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(superTable->cols, col);
            col = benchArrayGet(superTable->cols, i);
            col->type = TSDB_DATA_TYPE_INT;
            col->length = sizeof(int32_t);
            snprintf(col->name, TSDB_COL_NAME_LEN, "c%d", i);
            col->min = convertDatatypeToDefaultMin(col->type);
            col->max = convertDatatypeToDefaultMax(col->type);
        }
    }

    if (g_arguments->keep_trying) {
        superTable->keep_trying = g_arguments->keep_trying;
        superTable->trying_interval = g_arguments->trying_interval;
    }
}

static void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->conn->taos;
#ifdef LINUX
    prctl(PR_SET_NAME, "queryStableAggrFunc");
#endif
    char *command = benchCalloc(1, BUFFER_SIZE, false);
    FILE *  fp = g_arguments->fpOfInsertResult;
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    int64_t totalData = stbInfo->insertRows * stbInfo->childTblCount;
    char **aggreFunc;
    int    n;

    if (g_arguments->demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]);
    } else {
        aggreFunc = g_aggreFunc;
        n = sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0]);
    }

    infoPrint("total Data: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp, "Querying On %" PRId64 " records:\n", totalData);
    }
    for (int j = 0; j < n; j++) {
        char condition[COND_BUF_LEN] = "\0";
        char tempS[64] = "\0";

        int64_t m = 10 < stbInfo->childTblCount ? 10 : stbInfo->childTblCount;

        for (int64_t i = 1; i <= m; i++) {
            if (i == 1) {
                if (g_arguments->demo_mode) {
                    sprintf(tempS, "groupid = %" PRId64 "", i);
                } else {
                    sprintf(tempS, "t0 = %" PRId64 "", i);
                }
            } else {
                if (g_arguments->demo_mode) {
                    sprintf(tempS, " or groupid = %" PRId64 " ", i);
                } else {
                    sprintf(tempS, " or t0 = %" PRId64 " ", i);
                }
            }
            strncat(condition, tempS, COND_BUF_LEN - 1);

            sprintf(command, "SELECT %s FROM %s.meters WHERE %s", aggreFunc[j], database->dbName,
                    condition);
            if (fp) {
                fprintf(fp, "%s\n", command);
            }

            double t = (double)toolsGetTimestampUs();

            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint("Failed to query:%s\n", taos_errstr(pSql));
                taos_free_result(pSql);
                free(command);
                return NULL;
            }
            int count = 0;
            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }
            t = toolsGetTimestampUs() - t;
            if (fp) {
                fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
                        totalData / (t / 1000), t);
            }
            infoPrint("%s took %.6f second(s)\n\n", command,
                      t / 1000000);

            taos_free_result(pSql);
        }
    }
    free(command);
    return NULL;
}

static void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->conn->taos;
#ifdef LINUX
    prctl(PR_SET_NAME, "queryNtableAggrFunc");
#endif
    char *  command = benchCalloc(1, BUFFER_SIZE, false);
    FILE *  fp = g_arguments->fpOfInsertResult;
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    int64_t totalData = stbInfo->childTblCount * stbInfo->insertRows;
    char **aggreFunc;
    int    n;

    if (g_arguments->demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]);
    } else {
        aggreFunc = g_aggreFunc;
        n = sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0]);
    }

    infoPrint("totalData: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp,
                "| QFunctions |    QRecords    |   QSpeed(R/s)   |  "
                "QLatency(ms) |\n");
    }

    for (int j = 0; j < n; j++) {
        double   totalT = 0;
        uint64_t count = 0;
        for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
            if (stbInfo->escape_character) {
                sprintf(command,
                        "SELECT %s FROM %s.`%s%" PRId64 "` WHERE ts>= %" PRIu64,
                        aggreFunc[j],
                        database->dbName,
                        stbInfo->childTblPrefix, i,
                        (uint64_t) DEFAULT_START_TIME);
            } else {
                sprintf(
                    command, "SELECT %s FROM %s.%s%" PRId64 " WHERE ts>= %" PRIu64,
                    aggreFunc[j], database->dbName, stbInfo->childTblPrefix, i,
                    (uint64_t)DEFAULT_START_TIME);
            }

            double    t = (double)toolsGetTimestampUs();
            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint("Failed to query <%s>, reason:%s\n", command,
                           taos_errstr(pSql));
                taos_free_result(pSql);
                free(command);
                return NULL;
            }

            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }

            t = toolsGetTimestampUs() - t;
            totalT += t;

            taos_free_result(pSql);
        }
        if (fp) {
            fprintf(fp, "|%10s  |   %" PRId64 "   |  %12.2f   |   %10.2f  |\n",
                    aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
                    (double)(stbInfo->childTblCount * stbInfo->insertRows) / totalT,
                    totalT / 1000000);
        }
        infoPrint("<%s> took %.6f second(s)\n", command,
                  totalT / 1000000);
    }
    free(command);
    return NULL;
}

void queryAggrFunc() {
    pthread_t   read_id;
    threadInfo *pThreadInfo = benchCalloc(1, sizeof(threadInfo), false);
    if (NULL == pThreadInfo) {
        errorPrint("%s() failed to allocate memory\n", __func__);
        return;
    }
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    if (NULL == database) {
        errorPrint("%s() failed to get database\n", __func__);
        free(pThreadInfo);
        return;
    }
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    if (NULL == stbInfo) {
        errorPrint("%s() failed to get super table\n", __func__);
        free(pThreadInfo);
        return;
    }
    pThreadInfo->conn = init_bench_conn();
    if (pThreadInfo->conn == NULL) {
        errorPrint("%s() failed to init connection\n", __func__);
        free(pThreadInfo);
        return;
    }
    if (stbInfo->use_metric) {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    close_bench_conn(pThreadInfo->conn);
    free(pThreadInfo);
}
