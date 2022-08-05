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

#include "bench.h"

#define BENCH_FILE  "(**IMPORTANT**) Set JSON configuration file(all options are going to read from this JSON file), which is mutually exclusive with other commandline options, examples are under /usr/local/taos/examples"
#define BENCH_CFG_DIR "Configuration directory."
#define BENCH_HOST  "TDengine server FQDN to connect, default is localhost."
#define BENCH_PORT  "The TCP/IP port number to use for the connection, default is 6030."
#define BENCH_MODE  "insert mode, default is taosc, options: taosc|rest|stmt|sml"
#define BENCH_USER  "The user name to use when connecting to the server, default is root."
#define BENCH_PASS  "The password to use when connecting to the server, default is taosdata."
#define BENCH_OUTPUT  "The path of result output file, default is ./output.txt."
#define BENCH_THREAD  "The number of thread when insert data, default is 8."
#define BENCH_INTERVAL  "Insert interval for interlace mode in milliseconds, default is 0."
#define BENCH_STEP  "Timestamp step in milliseconds, default is 1."
#define BENCH_INTERLACE "The number of interlace rows insert into tables, default is 0."
#define BENCH_BATCH "Number of records in each insert request, default is 30000."
#define BENCH_TABLE "Number of child tables, default is 10000."
#define BENCH_ROWS  "Number of records for each table, default is 10000."
#define BENCH_DATABASE  "Name of database, default is test."
#define BENCH_COLS_NUM  "Number of INT data type columns in table, default is 0."
#define BENCH_TAGS  "Data type of tables' tags, default is INT,BINARY(16)."
#define BENCH_COLS  "Data type of tables' cols, default is FLOAT,INT,FLOAT."
#define BENCH_WIDTH "The default length of nchar and binary if not specified, default is 64."
#define BENCH_PREFIX  "Prefix of child table name, default is d."
#define BENCH_ESCAPTE "Use escape character in stable and child table name, optional."
#define BENCH_CHINESE "Nchar and binary are basic unicode chinese characters, optional."
#define BENCH_NORMAL  "Only create normal table without super table, optional."
#define BENCH_RANDOM  "Data source is randomly generated, optional."
#define BENCH_AGGR  "Query aggregation function after insertion, optional."
#define BENCH_YES "Pass confirmation prompt to continue, optional."
#define BENCH_RANGE "Range of disordered timestamp, default is 1000."
#define BENCH_DISORDER "Ratio of inserting data with disorder timestamp, default is 0."
#define BENCH_REPLICA "The number of replica when create database, default is 1."
#define BENCH_DEBUG "Debug mode, optional."
#define BENCH_PERFORMANCE "Performance mode, optional."
#define BENCH_PREPARE "Random data source size, default is 10000."
#define BENCH_POOL  "The connection pool size(deprecated)."
#define BENCH_EMAIL   "<support@taosdata.com>"
#define BENCH_VERSION "Print program version."
#ifdef WEBSOCKET
#define BENCH_DSN "The dsn to connect TDengine cloud service."
#define BENCH_TIMEOUT "The timeout wait on websocket query in seconds, default is 10."
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

void bench_print_help() {
    char indent[] = "  ";
    printf("Usage: taosBenchmark [OPTION ...] \r\n\r\n");
    printf("%s%s%s%s\r\n", indent, "-f,", indent, BENCH_FILE);
    printf("%s%s%s%s\r\n", indent, "-c,", indent, BENCH_CFG_DIR);
    printf("%s%s%s%s\r\n", indent, "-h,", indent, BENCH_HOST);
    printf("%s%s%s%s\r\n", indent, "-P,", indent, BENCH_PORT);
    printf("%s%s%s%s\r\n", indent, "-I,", indent, BENCH_MODE);
    printf("%s%s%s%s\r\n", indent, "-u,", indent, BENCH_USER);
    printf("%s%s%s%s\r\n", indent, "-p,", indent, BENCH_PASS);
    printf("%s%s%s%s\r\n", indent, "-o,", indent, BENCH_OUTPUT);
    printf("%s%s%s%s\r\n", indent, "-T,", indent, BENCH_THREAD);
    printf("%s%s%s%s\r\n", indent, "-i,", indent, BENCH_INTERVAL);
    printf("%s%s%s%s\r\n", indent, "-S,", indent, BENCH_STEP);
    printf("%s%s%s%s\r\n", indent, "-B,", indent, BENCH_INTERLACE);
    printf("%s%s%s%s\r\n", indent, "-r,", indent, BENCH_BATCH);
    printf("%s%s%s%s\r\n", indent, "-t,", indent, BENCH_TABLE);
    printf("%s%s%s%s\r\n", indent, "-n,", indent, BENCH_ROWS);
    printf("%s%s%s%s\r\n", indent, "-d,", indent, BENCH_DATABASE);
    printf("%s%s%s%s\r\n", indent, "-l,", indent, BENCH_COLS_NUM);
    printf("%s%s%s%s\r\n", indent, "-A,", indent, BENCH_TAGS);
    printf("%s%s%s%s\r\n", indent, "-b,", indent, BENCH_COLS);
    printf("%s%s%s%s\r\n", indent, "-w,", indent, BENCH_WIDTH);
    printf("%s%s%s%s\r\n", indent, "-m,", indent, BENCH_PREFIX);
    printf("%s%s%s%s\r\n", indent, "-E,", indent, BENCH_ESCAPTE);
    printf("%s%s%s%s\r\n", indent, "-C,", indent, BENCH_CHINESE);
    printf("%s%s%s%s\r\n", indent, "-N,", indent, BENCH_NORMAL);
    printf("%s%s%s%s\r\n", indent, "-M,", indent, BENCH_RANDOM);
    printf("%s%s%s%s\r\n", indent, "-x,", indent, BENCH_AGGR);
    printf("%s%s%s%s\r\n", indent, "-y,", indent, BENCH_YES);
    printf("%s%s%s%s\r\n", indent, "-R,", indent, BENCH_RANGE);
    printf("%s%s%s%s\r\n", indent, "-O,", indent, BENCH_DISORDER);
    printf("%s%s%s%s\r\n", indent, "-a,", indent, BENCH_REPLICA);
    printf("%s%s%s%s\r\n", indent, "-g,", indent, BENCH_DEBUG);
    printf("%s%s%s%s\r\n", indent, "-G,", indent, BENCH_PERFORMANCE);
    printf("%s%s%s%s\r\n", indent, "-F,", indent, BENCH_PREPARE);
#ifdef WEBSOCKET
    printf("%s%s%s%s\r\n", indent, "-W,", indent, BENCH_DSN);
    printf("%s%s%s%s\r\n", indent, "-D,", indent, BENCH_TIMEOUT);
#endif
    printf("%s%s%s%s\r\n", indent, "-V,", indent, BENCH_VERSION);
    printf("\r\n\r\nReport bugs to %s.\r\n", BENCH_EMAIL);
}

#ifdef LINUX

#include <argp.h>

const char *              argp_program_version;
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
    {"interlace-rows", 'B', "NUMBER", 0, BENCH_INTERLACE},
    {"rec-per-req", 'r', "NUMBER", 0, BENCH_BATCH},
    {"tables", 't', "NUMBER", 0, BENCH_TABLE},
    {"records", 'n', "NUMBER", 0, BENCH_ROWS},
    {"database", 'd', "DATABASE", 0, BENCH_DATABASE},
    {"columns", 'l', "NUMBER", 0, BENCH_COLS_NUM},
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
    {"performance", 'G', 0, 0, BENCH_PERFORMACE},
    {"prepared_rand", 'F', "NUMBER", 0, BENCH_PREPARE},
    {"connection_pool_size", 'H', "NUMBER", 0, BENCH_POOL},
#ifdef WEBSOCKET
    {"cloud_dsn", 'W', "DSN", 0, BENCH_DSN},
    {"timeout", 'D', "NUMBER", 0, BENCH_TIMEOUT},
#endif
    {0}
};

static error_t bench_parse_opt(int key, char *arg, struct argp_state *state) {
    return bench_parse_single_opt(key, arg);
}

static struct argp bench_argp = {bench_options, bench_parse_opt, "", ""};

void bench_parse_args_in_argp(int argc, char *argv[]) {
  argp_program_version = taos_get_client_info();
  argp_parse(&bench_argp, argc, argv, 0, 0, g_arguments);
}

#endif

#ifndef ARGP_ERR_UNKNOWN
  #define ARGP_ERR_UNKNOWN E2BIG
#endif

void parse_field_datatype(char *dataType, BArray *fields, bool isTag) {
    char *dup_str;
    benchArrayClear(fields);
    if (strstr(dataType, ",") == NULL) {
        Field * field = benchCalloc(1, sizeof(Field), true);
        benchArrayPush(fields, field);
        field = benchArrayGet(fields, 0);
        if (1 == regexMatch(dataType, "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$", REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            field->type = taos_convert_string_to_datatype(type, 0);
            field->length = atoi(length);
        } else {
            field->type = taos_convert_string_to_datatype(dataType, 0);
            field->length = taos_convert_type_to_length(field->type);
        }
        field->min = taos_convert_datatype_to_default_min(field->type);
        field->max = taos_convert_datatype_to_default_max(field->type);
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
            if (1 == regexMatch(token, "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$", REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                field->type = taos_convert_string_to_datatype(type, 0);
                field->length = atoi(length);
            } else {
                field->type = taos_convert_string_to_datatype(token, 0);
                field->length = taos_convert_type_to_length(field->type);
            }
            field->max = taos_convert_datatype_to_default_max(field->type);
            field->min = taos_convert_datatype_to_default_min(field->type);
            snprintf(field->name, TSDB_COL_NAME_LEN, isTag?"t%d":"c%d", index);
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
}

static int32_t bench_parse_single_opt(int32_t key, char* arg) {
    SDataBase *database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    switch (key) {
        case 'F':
            g_arguments->prepared_rand = atol(arg);
            if (g_arguments->prepared_rand <= 0) {
                errorPrint(stderr,
                           "Invalid -F: %s, will auto set to default(10000)\n",
                           arg);
                g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
            }
            break;
        case 'f':
            g_arguments->demo_mode = false;
            g_arguments->metaFile = arg;
            break;
        case 'h':
            g_arguments->host = arg;
            break;
        case 'P':
            g_arguments->port = atoi(arg);
            if (g_arguments->port <= 0) {
                errorPrint(stderr,
                           "Invalid -P: %s, will auto set to default(6030)\n",
                           arg);
                g_arguments->port = DEFAULT_PORT;
            }
            break;
        case 'I':
            if (0 == strcasecmp(arg, "taosc")) {
                stbInfo->iface = TAOSC_IFACE;
            } else if (0 == strcasecmp(arg, "stmt")) {
                stbInfo->iface = STMT_IFACE;
            } else if (0 == strcasecmp(arg, "rest")) {
                stbInfo->iface = REST_IFACE;
            } else if (0 == strcasecmp(arg, "sml")) {
                stbInfo->iface = SML_IFACE;
            } else {
                errorPrint(stderr,
                           "Invalid -I: %s, will auto set to default (taosc)\n",
                           arg);
                stbInfo->iface = TAOSC_IFACE;
            }
            break;
        case 'p':
            g_arguments->password = arg;
            break;
        case 'u':
            g_arguments->user = arg;
            break;
        case 'c':
            tstrncpy(configDir, arg, TSDB_FILENAME_LEN);
            break;
        case 'o':
            g_arguments->output_file = arg;
            break;
        case 'T':
            g_arguments->nthreads = atoi(arg);
            if (g_arguments->nthreads <= 0) {
                errorPrint(stderr,
                           "Invalid -T: %s, will auto set to default(8)\n",
                           arg);
                g_arguments->nthreads = DEFAULT_NTHREADS;
            }
            break;
        case 'H':
            break;
        case 'i':
            stbInfo->insert_interval = atoi(arg);
            if (stbInfo->insert_interval <= 0) {
                errorPrint(stderr,
                           "Invalid -i: %s, will auto set to default(0)\n",
                           arg);
                stbInfo->insert_interval = 0;
            }
            break;
        case 'S':
            stbInfo->timestamp_step = atol(arg);
            if (stbInfo->timestamp_step <= 0) {
                errorPrint(stderr,
                           "Invalid -S: %s, will auto set to default(1)\n",
                           arg);
                stbInfo->timestamp_step = 1;
            }
            break;
        case 'B':
            stbInfo->interlaceRows = atoi(arg);
            if (stbInfo->interlaceRows <= 0) {
                errorPrint(stderr,
                           "Invalid -B: %s, will auto set to default(0)\n",
                           arg);
                stbInfo->interlaceRows = 0;
            }
            break;
        case 'r':
            g_arguments->reqPerReq = atoi(arg);
            if (g_arguments->reqPerReq <= 0 ||
                g_arguments->reqPerReq > MAX_RECORDS_PER_REQ) {
                errorPrint(stderr,
                           "Invalid -r: %s, will auto set to default(30000)\n",
                           arg);
                g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
            }
            break;
        case 't':
            stbInfo->childTblCount = atoi(arg);
            if (stbInfo->childTblCount <= 0) {
                errorPrint(stderr,
                           "Invalid -t: %s, will auto set to default(10000)\n",
                           arg);
                stbInfo->childTblCount = DEFAULT_CHILDTABLES;
            }
            g_arguments->g_totalChildTables = stbInfo->childTblCount;
            break;
        case 'n':
            stbInfo->insertRows = atol(arg);
            if (stbInfo->insertRows <= 0) {
                errorPrint(stderr,
                           "Invalid -n: %s, will auto set to default(10000)\n",
                           arg);
                stbInfo->insertRows = DEFAULT_INSERT_ROWS;
            }
            break;
        case 'd':
            database->dbName = arg;
            break;
        case 'l':
            g_arguments->demo_mode = false;
            g_arguments->intColumnCount = atoi(arg);
            if (g_arguments->intColumnCount <= 0) {
                errorPrint(stderr,
                           "Invalid -l: %s, will auto set to default(0)\n",
                           arg);
                g_arguments->intColumnCount = 0;
            }
            break;
        case 'A':
            g_arguments->demo_mode = false;
            parse_field_datatype(arg, stbInfo->tags, true);
            break;
        case 'b':
            g_arguments->demo_mode = false;
            parse_field_datatype(arg, stbInfo->cols, false);
            break;
        case 'w':
            g_arguments->binwidth = atoi(arg);
            if (g_arguments->binwidth <= 0) {
                errorPrint(
                        stderr,
                        "Invalid value for w: %s, will auto set to default(64)\n",
                        arg);
                g_arguments->binwidth = DEFAULT_BINWIDTH;
            } else if (g_arguments->binwidth > TSDB_MAX_BINARY_LEN) {
                errorPrint(stderr,
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
            stbInfo->disorderRange = atoi(arg);
            if (stbInfo->disorderRange <= 0) {
                errorPrint(stderr,
                           "Invalid value for -R: %s, will auto set to "
                           "default(1000)\n",
                           arg);
                stbInfo->disorderRange =
                        DEFAULT_DISORDER_RANGE;
            }
            break;
        case 'O':
            stbInfo->disorderRatio = atoi(arg);
            if (stbInfo->disorderRatio <= 0) {
                errorPrint(
                        stderr,
                        "Invalid value for -O: %s, will auto set to default(0)\n",
                        arg);
                stbInfo->disorderRatio = 0;
            }
            break;
        case 'a':{
            int replica = atoi(arg);
            if (replica <= 0) {
                errorPrint(
                        stderr,
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
          g_g_arguments->dsn = arg;
          break;
        case 'D':
          g_g_arguments->timeout = atoi(arg);
          break;
#endif
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

int32_t bench_parse_args_no_argp(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if(strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0 || strcmp(argv[i], "-?") == 0) {
            bench_print_help();
            return 0;
        }

        char* key = argv[i];
        int32_t key_len = strlen(key);
        if (key_len != 2) {
            errorPrint(stderr, "Invalid option %s\r\n", key);
            return -1;
        }
        if (key[0] != '-') {
            errorPrint(stderr, "Invalid option %s\r\n", key);
            return -1;
        }

        if (key[1] == 'f' || key[1] == 'c' || key[1] == 'h' || key[1] == 'P' ||
            key[1] == 'I' || key[1] == 'u' || key[1] == 'p' || key[1] == 'o' ||
            key[1] == 'T' || key[1] == 'i' || key[1] == 'S' || key[1] == 'B' ||
            key[1] == 'r' || key[1] == 't' || key[1] == 'n' || key[1] == 'd' ||
            key[1] == 'd' || key[1] == 'l' || key[1] == 'A' || key[1] == 'b' ||
            key[1] == 'w' || key[1] == 'm' || key[1] == 'R' || key[1] == 'O' ||
            key[1] == 'a' || key[1] == 'F'
#ifdef WEBSOCKET
            || key[1] == 'D' || key[1] == 'W'
#endif
            ) {
            if (i + 1 >= argc) {
                errorPrint(stderr, "option %s requires an argument\r\n", key);
                return -1;
            }
            char* val = argv[i+1];
            if (val[0] == '-') {
                errorPrint(stderr, "option %s requires an argument\r\n", key);
                return -1;
            }
            bench_parse_single_opt(key[1], val);
            i++;
        } else if (key[1] == 'E' || key[1] == 'C' || key[1] == 'N' || key[1] == 'M' ||
            key[1] == 'x' || key[1] == 'y' || key[1] == 'g' || key[1] == 'G') {
            bench_parse_single_opt(key[1], NULL);
        } else {
            errorPrint(stderr, "Invalid option %s\r\n", key);
            return -1;
        }
    }
    return 0;
}

int32_t bench_parse_args(int32_t argc, char* argv[]) {
#ifdef LINUX
    return bench_parse_args_in_argp(argc, argv);
#else
    return bench_parse_args_no_argp(argc, argv);
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
    t2->length = 16;

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
    stbInfo->startTimestamp = DEFAULT_START_TIME;
    stbInfo->insertRows = DEFAULT_INSERT_ROWS;
    stbInfo->disorderRange = DEFAULT_DISORDER_RANGE;
    stbInfo->disorderRatio = 0;
    stbInfo->file_factor = -1;
    stbInfo->delay = -1;
}

static void init_database() {
    g_arguments->databases = benchArrayInit(1, sizeof(SDataBase));
    SDataBase *database = benchCalloc(1, sizeof(SDataBase), true);
    benchArrayPush(g_arguments->databases, database);
    database = benchArrayGet(g_arguments->databases, 0);
    database->streams = benchArrayInit(1, sizeof(SSTREAM));
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
    g_arguments->port = DEFAULT_PORT;
    g_arguments->telnet_tcp_port = TELNET_TCP_PORT;
    g_arguments->user = TSDB_DEFAULT_USER;
    g_arguments->password = TSDB_DEFAULT_PASS;
    g_arguments->answer_yes = 0;
    g_arguments->debug_print = 0;
    g_arguments->binwidth = DEFAULT_BINWIDTH;
    g_arguments->performance_print = 0;
    g_arguments->output_file = DEFAULT_OUTPUT;
    g_arguments->nthreads = DEFAULT_NTHREADS;
    g_arguments->table_threads = DEFAULT_NTHREADS;
    g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    g_arguments->g_totalChildTables = DEFAULT_CHILDTABLES;
    g_arguments->g_actualChildTables = 0;
    g_arguments->g_autoCreatedChildTables = 0;
    g_arguments->g_existedChildTables = 0;
    g_arguments->chinese = 0;
    g_arguments->aggr_func = 0;
    g_arguments->terminate = false;
#ifdef WEBSOCKET
	g_arguments->timeout = 10;
#endif
    init_database();
    init_stable();
}

void modify_argument() {
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable *superTable = benchArrayGet(database->superTbls, 0);
#ifdef WEBSOCKET
    if (!g_arguments->websocket) {
#endif
#ifdef LINUX
    if (strlen(configDir)) {
        wordexp_t full_path;
        if (wordexp(configDir, &full_path, 0) != 0) {
            errorPrint(stderr, "Invalid path %s\n", configDir);
            exit(EXIT_FAILURE);
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
    }
#endif
#ifdef WEBSOCKET
    }
#endif

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
        for (int i = superTable->cols->size; i < g_arguments->intColumnCount; ++i) {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(superTable->cols, col);
            col = benchArrayGet(superTable->cols, i);
            col->type = TSDB_DATA_TYPE_INT;
            col->length = sizeof(int32_t);
            snprintf(col->name, TSDB_COL_NAME_LEN, "c%d", i);
            col->min = taos_convert_datatype_to_default_min(col->type);
            col->max = taos_convert_datatype_to_default_max(col->type);
        }
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

    infoPrint(stdout, "total Data: %" PRId64 "\n", totalData);
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
                errorPrint(stderr, "Failed to query:%s\n", taos_errstr(pSql));
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
            infoPrint(stdout, "%s took %.6f second(s)\n\n", command,
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

    infoPrint(stdout, "totalData: %" PRId64 "\n", totalData);
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
                errorPrint(stderr, "Failed to query <%s>, reason:%s\n", command,
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
        infoPrint(stdout, "<%s> took %.6f second(s)\n", command,
                  totalT / 1000000);
    }
    free(command);
    return NULL;
}

void queryAggrFunc() {
    pthread_t   read_id;
    threadInfo *pThreadInfo = benchCalloc(1, sizeof(threadInfo), false);
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    pThreadInfo->conn = init_bench_conn();
    if (pThreadInfo->conn == NULL) {
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
