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
#include "benchData.h"

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

const char *              argp_program_version;
const char *              argp_program_bug_address = "<support@taosdata.com>";
static char               doc[] = "";
static char               args_doc[] = "";
static struct argp_option options[] = {
    {"file", 'f', "FILE", 0, "Json configuration file."},
    {"config-dir", 'c', "CONFIG_DIR", 0, "Configuration directory."},
    {"host", 'h', "HOST", 0,
     "TDengine server FQDN to connect, default is localhost."},
    {"port", 'P', "PORT", 0,
     "The TCP/IP port number to use for the connection, default is 6030."},
    {"interface", 'I', "IFACE", 0,
     "insert mode, default is taosc, options: taosc|rest|stmt|sml"},
    {"user", 'u', "USER", 0,
     "The user name to use when connecting to the server, default is root."},
    {"password", 'p', 0, 0,
     "The password to use when connecting to the server, default is taosdata."},
    {"output", 'o', "FILE", 0,
     "The path of result output file, default is ./output.txt."},
    {"threads", 'T', "NUMBER", 0,
     "The number of thread when insert data, default is 8."},
    {"insert-interval", 'i', "NUMBER", 0,
     "Insert interval for interlace mode in milliseconds, default is 0."},
    {"time-step", 'S', "NUMBER", 0,
     "Timestamp step in milliseconds, default is 1."},
    {"interlace-rows", 'B', "NUMBER", 0,
     "The number of interlace rows insert into tables, default is 0"},
    {"rec-per-req", 'r', "NUMBER", 0,
     "Number of records in each insert request, default is 30000."},
    {"tables", 't', "NUMBER", 0, "Number of child tables, default is 10000."},
    {"records", 'n', "NUMBER", 0,
     "Number of records for each table, default is 10000."},
    {"database", 'd', "DATABASE", 0, "Name of database, default is test."},
    {"columns", 'l', "NUMBER", 0,
     "Number of INT data type columns in table, default is 0. "},
    {"tag-type", 'A', "TAG_TYPE", 0,
     "Data type of tables' tags, default is INT,BINARY(16)."},
    {"data-type", 'b', "COL_TYPE", 0,
     "Data type of tables' cols, default is FLOAT,INT,FLOAT."},
    {"binwidth", 'w', "NUMBEER", 0,
     "The default length of nchar and binary if not specified, default is "
     "64."},
    {"table-prefix", 'm', "TABLE_PREFIX", 0,
     "Prefix of child table name, default is d."},
    {"escape-character", 'E', 0, 0,
     "Use escape character in stable and child table name, optional."},
    {"chinese", 'C', 0, 0,
     "Nchar and binary are basic unicode chinese characters, optional."},
    {"normal-table", 'N', 0, 0,
     "Only create normal table without super table, optional."},
    {"random", 'M', 0, 0, "Data source is randomly generated, optional."},
    {"aggr-func", 'x', 0, 0,
     "Query aggregation function after insertion, optional."},
    {"answer-yes", 'y', 0, 0,
     "Pass confirmation prompt to continue, optional."},
    {"disorder-range", 'R', "NUMBER", 0,
     "Range of disordered timestamp, default is 1000."},
    {"disorder", 'O', "NUMBER", 0,
     "Ratio of inserting data with disorder timestamp, default is 0."},
    {"replia", 'a', "NUMBER", 0,
     "The number of replica when create database, default is 1."},
    {"debug", 'g', 0, 0, "Debug mode"},
    {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    SArguments *arguments = state->input;
    switch (key) {
        case 'f':
            arguments->metaFile = arg;
            break;
        case 'h':
            arguments->host = arg;
            break;
        case 'P':
            arguments->port = atoi(arg);
            break;
        case 'I':
            if (0 == strcasecmp(arg, "taosc")) {
                arguments->iface = TAOSC_IFACE;
            } else if (0 == strcasecmp(arg, "stmt")) {
                arguments->iface = STMT_IFACE;
            } else if (0 == strcasecmp(arg, "rest")) {
                arguments->iface = REST_IFACE;
            } else if (0 == strcasecmp(arg, "sml")) {
                arguments->iface = SML_IFACE;
            } else {
                errorPrint("Invalid -I: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'p':
            break;
        case 'u':
            arguments->user = arg;
            break;
        case 'c':
            tstrncpy(configDir, arg, TSDB_FILENAME_LEN);
            break;
        case 'o':
            arguments->output_file = arg;
            break;
        case 'T':
            arguments->nthreads = atoi(arg);
            if (arguments->nthreads == 0) {
                errorPrint("Invalid -T: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'i':
            arguments->insert_interval = atol(arg);
            if (arguments->insert_interval == 0) {
                errorPrint("Invalid -i: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'S':
            arguments->timestamp_step = atol(arg);
            if (arguments->timestamp_step == 0) {
                errorPrint("Invalid -S: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'B':
            arguments->interlaceRows = atoi(arg);
            if (arguments->interlaceRows <= 0) {
                errorPrint("Invalid -B: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'r':
            arguments->reqPerReq = atoi(arg);
            break;
        case 't':
            arguments->ntables = atoi(arg);
            break;
        case 'n':
            arguments->insertRows = atol(arg);
            break;
        case 'd':
            arguments->database = arg;
            break;
        case 'l':
            arguments->intColumnCount = atoi(arg);
            if (arguments->intColumnCount <= 0) {
                errorPrint("Invalid -l: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'A':
            tmfree(arguments->tag_type);
            tmfree(arguments->tag_length);
            if (count_datatype(arg, &(arguments->tagCount))) {
                exit(EXIT_FAILURE);
            }
            arguments->tag_type = calloc(arguments->tagCount, sizeof(char));
            arguments->tag_length =
                calloc(arguments->tagCount, sizeof(int32_t));
            if (parse_datatype(arg, arguments->tag_type, arguments->tag_length,
                               true)) {
                exit(EXIT_FAILURE);
            }
            break;
        case 'b':
            tmfree(arguments->col_type);
            tmfree(arguments->col_length);
            if (count_datatype(arg, &(arguments->columnCount))) {
                exit(EXIT_FAILURE);
            }
            arguments->col_type = calloc(arguments->columnCount, sizeof(char));
            arguments->col_length =
                calloc(arguments->columnCount, sizeof(int32_t));
            if (parse_datatype(arg, arguments->col_type, arguments->col_length,
                               false)) {
                exit(EXIT_FAILURE);
            }
            break;
        case 'w':
            arguments->binwidth = atoi(arg);
            if (arguments->binwidth == 0) {
                errorPrint("Invalid value for w: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'm':
            arguments->tb_prefix = arg;
            break;
        case 'E':
            arguments->escapeChar = true;
            break;
        case 'C':
            arguments->chinese = true;
            break;
        case 'N':
            arguments->use_metric = false;
            break;
        case 'M':
            arguments->demo_mode = false;
            break;
        case 'x':
            arguments->aggr_func = true;
            break;
        case 'y':
            arguments->answer_yes = false;
            break;
        case 'R':
            arguments->disorderRange = atoi(arg);
            if (arguments->disorderRange == 0) {
                errorPrint("Invalid value for -R: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'O':
            arguments->disorderRatio = atoi(arg);
            if (arguments->disorderRatio == 0) {
                errorPrint("Invalid value for -O: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'a':
            arguments->replica = atoi(arg);
            if (arguments->replica == 0) {
                errorPrint("Invalid value for -a: %s\n", arg);
                exit(EXIT_FAILURE);
            }
            break;
        case 'g':
            arguments->debug_print = true;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = {options, parse_opt, args_doc, doc};

void init_g_args(SArguments *pg_args) {
    pg_args->dbCount = 1;
    pg_args->test_mode = DEFAULT_TEST_MODE;
    pg_args->host = DEFAULT_HOST;
    pg_args->port = DEFAULT_PORT;
    pg_args->iface = DEFAULT_IFACE;
    pg_args->output_file = DEFAULT_OUTPUT;
    pg_args->user = TSDB_DEFAULT_USER;
    pg_args->password = TSDB_DEFAULT_PASS;
    pg_args->database = DEFAULT_DATABASE;
    pg_args->replica = DEFAULT_REPLICA;
    pg_args->tb_prefix = DEFAULT_TB_PREFIX;
    pg_args->escapeChar = DEFAULT_ESCAPE_CHAR;
    pg_args->use_metric = DEFAULT_USE_METRIC;
    pg_args->drop_database = DEFAULT_DROP_DB;
    pg_args->aggr_func = DEFAULT_AGGR_FUNC;
    pg_args->answer_yes = DEFAULT_ANS_YES;
    pg_args->debug_print = DEFAULT_DEBUG;
    pg_args->verbose_print = DEFAULT_VERBOSE;
    pg_args->performance_print = DEFAULT_PERF_STAT;
    pg_args->col_type = (char *)calloc(3, sizeof(char));
    pg_args->col_type[0] = TSDB_DATA_TYPE_FLOAT;
    pg_args->col_type[1] = TSDB_DATA_TYPE_INT;
    pg_args->col_type[2] = TSDB_DATA_TYPE_FLOAT;
    pg_args->col_length = (int32_t *)calloc(3, sizeof(int32_t));
    pg_args->col_length[0] = sizeof(float);
    pg_args->col_length[1] = sizeof(int32_t);
    pg_args->col_length[2] = sizeof(float);
    pg_args->tag_type = (char *)calloc(2, sizeof(char));
    pg_args->tag_type[0] = TSDB_DATA_TYPE_INT;
    pg_args->tag_type[1] = TSDB_DATA_TYPE_BINARY;
    pg_args->tag_length = (int32_t *)calloc(2, sizeof(int32_t));
    pg_args->tag_length[0] = sizeof(int32_t);
    pg_args->tag_length[1] = 16;
    pg_args->response_buffer = RESP_BUF_LEN;
    pg_args->columnCount = 3;
    pg_args->tagCount = 2;
    pg_args->binwidth = DEFAULT_BINWIDTH;
    pg_args->nthreads = DEFAULT_NTHREADS;
    pg_args->nthreads_pool = DEFAULT_NTHREADS + 5;
    pg_args->insert_interval = DEFAULT_INSERT_INTERVAL;
    pg_args->timestamp_step = DEFAULT_TIMESTAMP_STEP;
    pg_args->query_times = DEFAULT_QUERY_TIME;
    pg_args->prepared_rand = DEFAULT_PREPARED_RAND;
    pg_args->interlaceRows = DEFAULT_INTERLACE_ROWS;
    pg_args->reqPerReq = DEFAULT_REQ_PER_REQ;
    pg_args->ntables = DEFAULT_CHILDTABLES;
    pg_args->insertRows = DEFAULT_INSERT_ROWS;
    pg_args->disorderRange = DEFAULT_DISORDER_RANGE;
    pg_args->disorderRatio = DEFAULT_RATIO;
    pg_args->demo_mode = DEFAULT_DEMO_MODE;
    pg_args->chinese = DEFAULT_CHINESE_OPT;
}

int count_datatype(char *dataType, int32_t *number) {
    char *dup_str;
    *number = 0;
    if (strstr(dataType, ",") == NULL) {
        *number = 1;
        return 0;
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        while (token != NULL) {
            (*number)++;
            token = strsep(&running, ",");
        }
    }
    tmfree(dup_str);
    return 0;
}
int parse_datatype(char *dataType, char *data_type, int32_t *data_length,
                   bool is_tag) {
    char *dup_str;
    if (strstr(dataType, ",") == NULL) {
        if (0 == strcasecmp(dataType, "int")) {
            data_type[0] = TSDB_DATA_TYPE_INT;
            data_length[0] = sizeof(int32_t);
        } else if (0 == strcasecmp(dataType, "float")) {
            data_type[0] = TSDB_DATA_TYPE_FLOAT;
            data_length[0] = sizeof(float);
        } else if (0 == strcasecmp(dataType, "double")) {
            data_type[0] = TSDB_DATA_TYPE_DOUBLE;
            data_length[0] = sizeof(double);
        } else if (0 == strcasecmp(dataType, "tinyint")) {
            data_type[0] = TSDB_DATA_TYPE_TINYINT;
            data_length[0] = sizeof(int8_t);
        } else if (0 == strcasecmp(dataType, "bool")) {
            data_type[0] = TSDB_DATA_TYPE_BOOL;
            data_length[0] = sizeof(char);
        } else if (0 == strcasecmp(dataType, "smallint")) {
            data_type[0] = TSDB_DATA_TYPE_SMALLINT;
            data_length[0] = sizeof(int16_t);
        } else if (0 == strcasecmp(dataType, "bigint")) {
            data_type[0] = TSDB_DATA_TYPE_BIGINT;
            data_length[0] = sizeof(int64_t);
        } else if (0 == strcasecmp(dataType, "timestamp")) {
            data_type[0] = TSDB_DATA_TYPE_TIMESTAMP;
            data_length[0] = sizeof(int64_t);
        } else if (0 == strcasecmp(dataType, "utinyint")) {
            data_type[0] = TSDB_DATA_TYPE_UTINYINT;
            data_length[0] = sizeof(uint8_t);
        } else if (0 == strcasecmp(dataType, "usmallint")) {
            data_type[0] = TSDB_DATA_TYPE_USMALLINT;
            data_length[0] = sizeof(uint16_t);
        } else if (0 == strcasecmp(dataType, "uint")) {
            data_type[0] = TSDB_DATA_TYPE_UINT;
            data_length[0] = sizeof(uint32_t);
        } else if (0 == strcasecmp(dataType, "ubigint")) {
            data_type[0] = TSDB_DATA_TYPE_UBIGINT;
            data_length[0] = sizeof(uint64_t);
        } else if (0 == strcasecmp(dataType, "nchar")) {
            data_type[0] = TSDB_DATA_TYPE_NCHAR;
            data_length[0] = DEFAULT_BINWIDTH;
        } else if (0 == strcasecmp(dataType, "binary")) {
            data_type[0] = TSDB_DATA_TYPE_BINARY;
            data_length[0] = DEFAULT_BINWIDTH;
        } else if (is_tag && 0 == strcasecmp(dataType, "json")) {
            data_type[0] = TSDB_DATA_TYPE_JSON;
            data_length[0] = DEFAULT_BINWIDTH;
        } else if (1 == regexMatch(dataType, "^(BINARY)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            data_type[0] = TSDB_DATA_TYPE_BINARY;
            data_length[0] = atoi(length);
        } else if (1 == regexMatch(dataType, "^(NCHAR)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            data_type[0] = TSDB_DATA_TYPE_NCHAR;
            data_length[0] = atoi(length);
        } else if (is_tag &&
                   1 == regexMatch(dataType, "^(json)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            data_type[0] = TSDB_DATA_TYPE_JSON;
            data_length[0] = atoi(length);
        } else {
            errorPrint("Invalid data type: %s\n", dataType);
            return -1;
        }
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        int   index = 0;
        while (token != NULL) {
            if (0 == strcasecmp(token, "int")) {
                data_type[index] = TSDB_DATA_TYPE_INT;
                data_length[index] = sizeof(int32_t);
            } else if (0 == strcasecmp(token, "float")) {
                data_type[index] = TSDB_DATA_TYPE_FLOAT;
                data_length[index] = sizeof(float);
            } else if (0 == strcasecmp(token, "double")) {
                data_type[index] = TSDB_DATA_TYPE_DOUBLE;
                data_length[index] = sizeof(double);
            } else if (0 == strcasecmp(token, "tinyint")) {
                data_type[index] = TSDB_DATA_TYPE_TINYINT;
                data_length[index] = sizeof(int8_t);
            } else if (0 == strcasecmp(token, "bool")) {
                data_type[index] = TSDB_DATA_TYPE_BOOL;
                data_length[index] = sizeof(char);
            } else if (0 == strcasecmp(token, "smallint")) {
                data_type[index] = TSDB_DATA_TYPE_SMALLINT;
                data_length[index] = sizeof(int16_t);
            } else if (0 == strcasecmp(token, "bigint")) {
                data_type[index] = TSDB_DATA_TYPE_BIGINT;
                data_length[index] = sizeof(int64_t);
            } else if (0 == strcasecmp(token, "timestamp")) {
                data_type[index] = TSDB_DATA_TYPE_TIMESTAMP;
                data_length[index] = sizeof(int64_t);
            } else if (0 == strcasecmp(token, "utinyint")) {
                data_type[index] = TSDB_DATA_TYPE_UTINYINT;
                data_length[index] = sizeof(uint8_t);
            } else if (0 == strcasecmp(token, "usmallint")) {
                data_type[index] = TSDB_DATA_TYPE_USMALLINT;
                data_length[index] = sizeof(uint16_t);
            } else if (0 == strcasecmp(token, "uint")) {
                data_type[index] = TSDB_DATA_TYPE_UINT;
                data_length[index] = sizeof(uint32_t);
            } else if (0 == strcasecmp(token, "ubigint")) {
                data_type[index] = TSDB_DATA_TYPE_UBIGINT;
                data_length[index] = sizeof(uint64_t);
            } else if (0 == strcasecmp(token, "nchar")) {
                data_type[index] = TSDB_DATA_TYPE_NCHAR;
                data_length[index] = DEFAULT_BINWIDTH;
            } else if (0 == strcasecmp(token, "binary")) {
                data_type[index] = TSDB_DATA_TYPE_BINARY;
                data_length[index] = DEFAULT_BINWIDTH;
            } else if (1 == regexMatch(token, "^(BINARY)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                data_type[index] = TSDB_DATA_TYPE_BINARY;
                data_length[index] = atoi(length);
            } else if (1 == regexMatch(token, "^(NCHAR)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                data_type[index] = TSDB_DATA_TYPE_NCHAR;
                data_length[index] = atoi(length);
            } else if (is_tag &&
                       1 == regexMatch(token, "^(JSON)(\\([1-9][0-9]*\\))?$",
                                       REG_ICASE | REG_EXTENDED)) {
                errorPrint("%s",
                           "Json tag type cannot use with other type tags\n");
                tmfree(dup_str);
                return -1;
            } else {
                errorPrint("Invalid data type <%s>\n", token);
                tmfree(dup_str);
                return -1;
            }
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
    return 0;
}

void commandLineParseArgument(int argc, char *argv[], SArguments *arguments) {
    argp_program_version = taos_get_client_info();
    argp_parse(&argp, argc, argv, 0, 0, arguments);
}

void setParaFromArg(SArguments *pg_args, SDataBase *pdb) {
    pg_args->test_mode = INSERT_TEST;
    pdb->drop = true;
    tstrncpy(pdb->dbName, pg_args->database, TSDB_DB_NAME_LEN);
    pdb->dbCfg.replica = pg_args->replica;
    tstrncpy(pdb->dbCfg.precision, "ms", SMALL_BUFF_LEN);
    pg_args->prepared_rand = min(pg_args->insertRows, MAX_PREPARED_RAND);

    if (pg_args->intColumnCount > pg_args->columnCount) {
        char *tmp_type = (char *)realloc(
            pg_args->col_type, pg_args->intColumnCount * sizeof(char));
        int32_t *tmp_length = (int32_t *)realloc(
            pg_args->col_length, pg_args->intColumnCount * sizeof(int32_t));
        if (tmp_type != NULL && tmp_length != NULL) {
            pg_args->col_type = tmp_type;
            pg_args->col_length = tmp_length;
            for (int i = pg_args->columnCount; i < pg_args->intColumnCount;
                 ++i) {
                pg_args->col_type[i] = TSDB_DATA_TYPE_INT;
                pg_args->col_length[i] = sizeof(int32_t);
            }
        }
        pg_args->columnCount = pg_args->intColumnCount;
    }

    if ((pg_args->col_type[0] == TSDB_DATA_TYPE_BINARY) ||
        (pg_args->col_type[0] == TSDB_DATA_TYPE_BOOL) ||
        (pg_args->col_type[0] == TSDB_DATA_TYPE_NCHAR)) {
        pg_args->aggr_func = false;
    }
    if (pg_args->use_metric) {
        pdb->superTblCount = 1;
        tstrncpy(pdb->superTbls[0].stbName, "meters", TSDB_TABLE_NAME_LEN);
        pdb->superTbls[0].childTblCount = pg_args->ntables;
        pdb->superTbls[0].childTblLimit = pg_args->ntables;
        pdb->superTbls[0].childTblOffset = 0;
        pdb->superTbls[0].escapeChar = pg_args->escapeChar;

        pdb->superTbls[0].autoCreateTable = PRE_CREATE_SUBTBL;
        pdb->superTbls[0].childTblExists = TBL_NO_EXISTS;
        pdb->superTbls[0].disorderRange = pg_args->disorderRange;
        pdb->superTbls[0].disorderRatio = pg_args->disorderRatio;
        tstrncpy(pdb->superTbls[0].childTblPrefix, pg_args->tb_prefix,
                 TBNAME_PREFIX_LEN);
        tstrncpy(pdb->superTbls[0].dataSource, "rand", SMALL_BUFF_LEN);

        pdb->superTbls[0].iface = pg_args->iface;
        pdb->superTbls[0].lineProtocol = TSDB_SML_LINE_PROTOCOL;
        pdb->superTbls[0].tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        tstrncpy(pdb->superTbls[0].startTimestamp, "2017-07-14 10:40:00.000",
                 MAX_TB_NAME_SIZE);
        pdb->superTbls[0].timeStampStep = pg_args->timestamp_step;

        pdb->superTbls[0].insertRows = pg_args->insertRows;
        pdb->superTbls[0].interlaceRows = pg_args->interlaceRows;
        pdb->superTbls[0].columnCount = pg_args->columnCount;
        pdb->superTbls[0].col_type = pg_args->col_type;
        pdb->superTbls[0].col_length = pg_args->col_length;
        pdb->superTbls[0].tagCount = pg_args->tagCount;
        pdb->superTbls[0].tag_type = pg_args->tag_type;
        pdb->superTbls[0].tag_length = pg_args->tag_length;
    }
}

void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
    prctl(PR_SET_NAME, "queryStableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);

    FILE *fp = fopen(pThreadInfo->filePath, "a");
    if (NULL == fp) {
        errorPrint("fopen %s fail, reason:%s.\n", pThreadInfo->filePath,
                   strerror(errno));
    }

    int64_t insertRows = pThreadInfo->stbInfo->insertRows;
    int64_t ntables =
        pThreadInfo->ntables;  // pThreadInfo->end_table_to -
                               // pThreadInfo->start_table_from + 1;
    int64_t totalData = insertRows * ntables;
    bool    aggr_func = g_args.aggr_func;

    char **aggreFunc;
    int    n;

    if (g_args.demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = aggr_func ? (sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]))
                      : 2;
    } else {
        aggreFunc = g_aggreFunc;
        n = aggr_func ? (sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0])) : 2;
    }

    if (!aggr_func) {
        printf(
            "\nThe first field is either Binary or Bool. Aggregation functions "
            "are not supported.\n");
    }

    infoPrint("total Data: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp, "Querying On %" PRId64 " records:\n", totalData);
    }
    for (int j = 0; j < n; j++) {
        char condition[COND_BUF_LEN] = "\0";
        char tempS[64] = "\0";

        int64_t m = 10 < ntables ? 10 : ntables;

        for (int64_t i = 1; i <= m; i++) {
            if (i == 1) {
                if (g_args.demo_mode) {
                    sprintf(tempS, "groupid = %" PRId64 "", i);
                } else {
                    sprintf(tempS, "t0 = %" PRId64 "", i);
                }
            } else {
                if (g_args.demo_mode) {
                    sprintf(tempS, " or groupid = %" PRId64 " ", i);
                } else {
                    sprintf(tempS, " or t0 = %" PRId64 " ", i);
                }
            }
            strncat(condition, tempS, COND_BUF_LEN - 1);

            sprintf(command, "SELECT %s FROM meters WHERE %s", aggreFunc[j],
                    condition);
            if (fp) {
                fprintf(fp, "%s\n", command);
            }

            double t = (double)taosGetTimestampUs();

            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint("Failed to query:%s\n", taos_errstr(pSql));
                taos_free_result(pSql);
                fclose(fp);
                free(command);
                return NULL;
            }
            int count = 0;
            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }
            t = taosGetTimestampUs() - t;
            if (fp) {
                fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
                        ntables * insertRows / (t / 1000), t);
            }
            infoPrint("%s took %.6f second(s)\n\n", command, t / 1000000);

            taos_free_result(pSql);
        }
    }
    fclose(fp);
    free(command);
    return NULL;
}

void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
    prctl(PR_SET_NAME, "queryNtableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);

    uint64_t startTime = pThreadInfo->start_time;
    char *   tb_prefix = pThreadInfo->tb_prefix;
    FILE *   fp = fopen(pThreadInfo->filePath, "a");
    if (NULL == fp) {
        errorPrint("fopen %s fail, reason:%s.\n", pThreadInfo->filePath,
                   strerror(errno));
    }

    int64_t insertRows;
    insertRows = g_args.insertRows;

    int64_t ntables =
        pThreadInfo->ntables;  // pThreadInfo->end_table_to -
                               // pThreadInfo->start_table_from + 1;
    int64_t totalData = insertRows * ntables;
    bool    aggr_func = g_args.aggr_func;

    char **aggreFunc;
    int    n;

    if (g_args.demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = aggr_func ? (sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]))
                      : 2;
    } else {
        aggreFunc = g_aggreFunc;
        n = aggr_func ? (sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0])) : 2;
    }

    if (!aggr_func) {
        printf(
            "\nThe first field is either Binary or Bool. Aggregation functions "
            "are not supported.\n");
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
        for (int64_t i = 0; i < ntables; i++) {
            if (g_args.escapeChar) {
                sprintf(command,
                        "SELECT %s FROM `%s%" PRId64 "` WHERE ts>= %" PRIu64,
                        aggreFunc[j], tb_prefix, i, startTime);
            } else {
                sprintf(command,
                        "SELECT %s FROM %s%" PRId64 " WHERE ts>= %" PRIu64,
                        aggreFunc[j], tb_prefix, i, startTime);
            }

            double    t = (double)taosGetTimestampUs();
            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint("Failed to query <%s>, reason:%s\n", command,
                           taos_errstr(pSql));
                taos_free_result(pSql);
                fclose(fp);
                free(command);
                return NULL;
            }

            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }

            t = taosGetTimestampUs() - t;
            totalT += t;

            taos_free_result(pSql);
        }
        if (fp) {
            fprintf(fp, "|%10s  |   %" PRId64 "   |  %12.2f   |   %10.2f  |\n",
                    aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j],
                    totalData, (double)(ntables * insertRows) / totalT,
                    totalT / 1000000);
        }
        infoPrint("<%s> took %.6f second(s)\n", command, totalT / 1000000);
    }
    fclose(fp);
    free(command);
    return NULL;
}

void queryAggrFunc(SArguments *pg_args) {
    // query data

    pthread_t   read_id;
    threadInfo *pThreadInfo = calloc(1, sizeof(threadInfo));

    pThreadInfo->start_time = DEFAULT_START_TIME;  // 2017-07-14 10:40:00.000
    pThreadInfo->start_table_from = 0;

    if (pg_args->use_metric) {
        pThreadInfo->ntables = db[0].superTbls[0].childTblCount;
        pThreadInfo->end_table_to = db[0].superTbls[0].childTblCount - 1;
        pThreadInfo->stbInfo = &db[0].superTbls[0];
        tstrncpy(pThreadInfo->tb_prefix, db[0].superTbls[0].childTblPrefix,
                 TBNAME_PREFIX_LEN);
    } else {
        pThreadInfo->ntables = pg_args->ntables;
        pThreadInfo->end_table_to = pg_args->ntables - 1;
        tstrncpy(pThreadInfo->tb_prefix, pg_args->tb_prefix,
                 TSDB_TABLE_NAME_LEN);
    }

    pThreadInfo->taos = select_one_from_pool(&g_taos_pool, db[0].dbName);

    if (pThreadInfo->taos == NULL) {
        free(pThreadInfo);
        exit(EXIT_FAILURE);
    }

    tstrncpy(pThreadInfo->filePath, g_args.output_file, MAX_FILE_NAME_LEN);

    if (!g_args.use_metric) {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    free(pThreadInfo);
}

int test(SArguments *pg_args) {
    if (strlen(configDir)) {
        wordexp_t full_path;
        if (wordexp(configDir, &full_path, 0) != 0) {
            errorPrint("Invalid path %s\n", configDir);
            return -1;
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
    }

    if (pg_args->test_mode == INSERT_TEST) {
        if (insertTestProcess()) {
            return -1;
        }
    } else if (pg_args->test_mode == QUERY_TEST) {
        if (queryTestProcess()) {
            return -1;
        }
        for (int64_t i = 0; i < g_queryInfo.superQueryInfo.childTblCount; ++i) {
            tmfree(g_queryInfo.superQueryInfo.childTblName[i]);
        }
        tmfree(g_queryInfo.superQueryInfo.childTblName);
    } else if (pg_args->test_mode == SUBSCRIBE_TEST) {
        if (subscribeTestProcess()) {
            return -1;
        }
    } else {
        errorPrint("unknown test mode: %d\n", pg_args->test_mode);
        return -1;
    }
    if (g_args.aggr_func) {
        queryAggrFunc(pg_args);
    }
    return 0;
}