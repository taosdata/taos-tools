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
    {"file", 'f', "FILE", 0,
     "(**IMPORTANT**) Set JSON configuration file(all options are going to "
     "read from this JSON file), which is mutually exclusive with other "
     "commandline options",
     0},
    {"config-dir", 'c', "CONFIG_DIR", 0, "Configuration directory.", 1},
    {"host", 'h', "HOST", 0,
     "TDengine server FQDN to connect, default is localhost."},
    {"port", 'P', "PORT", 0,
     "The TCP/IP port number to use for the connection, default is 6030."},
    {"interface", 'I', "IFACE", 0,
     "insert mode, default is taosc, options: taosc|rest|stmt|sml"},
    {"user", 'u', "USER", 0,
     "The user name to use when connecting to the server, default is root."},
    {"password", 'p', "PASSWORD", 0,
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
    {"binwidth", 'w', "NUMBER", 0,
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
    {"debug", 'g', 0, 0, "Debug mode, optional."},
    {"performace", 'G', 0, 0, "Performance mode, optional."},
    {"prepared_rand", 'F', "NUMBER", 0,
     "Random data source size, default is 10000."},
    {0}};

static int count_datatype(char *dataType, int32_t *number) {
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
static int parse_datatype(char *dataType, char *data_type, int32_t *data_length,
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
            data_length[0] = 0;
        } else if (0 == strcasecmp(dataType, "binary")) {
            data_type[0] = TSDB_DATA_TYPE_BINARY;
            data_length[0] = 0;
        } else if (is_tag && 0 == strcasecmp(dataType, "json")) {
            data_type[0] = TSDB_DATA_TYPE_JSON;
            data_length[0] = 0;
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
                data_length[index] = 0;
            } else if (0 == strcasecmp(token, "binary")) {
                data_type[index] = TSDB_DATA_TYPE_BINARY;
                data_length[index] = 0;
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

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    SArguments *arguments = state->input;
    switch (key) {
        case 'F':
            arguments->prepared_rand = atol(arg);
            if (arguments->prepared_rand <= 0) {
                errorPrint("Invalid -F: %s, will auto set to default(10000)\n",
                           arg);
                arguments->prepared_rand = DEFAULT_PREPARED_RAND;
            }
            break;
        case 'f':
            arguments->demo_mode = false;
            arguments->metaFile = arg;
            break;
        case 'h':
            arguments->host = arg;
            break;
        case 'P':
            arguments->port = atoi(arg);
            if (arguments->port <= 0) {
                errorPrint("Invalid -P: %s, will auto set to default(6030)\n",
                           arg);
                arguments->port = DEFAULT_PORT;
            }
            break;
        case 'I':
            if (0 == strcasecmp(arg, "taosc")) {
                arguments->db->superTbls->iface = TAOSC_IFACE;
            } else if (0 == strcasecmp(arg, "stmt")) {
                arguments->db->superTbls->iface = STMT_IFACE;
            } else if (0 == strcasecmp(arg, "rest")) {
                arguments->db->superTbls->iface = REST_IFACE;
            } else if (0 == strcasecmp(arg, "sml")) {
                arguments->db->superTbls->iface = SML_IFACE;
            } else {
                errorPrint("Invalid -I: %s, will auto set to default (taosc)\n",
                           arg);
                arguments->db->superTbls->iface = TAOSC_IFACE;
            }
            break;
        case 'p':
            arguments->password = arg;
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
            if (arguments->nthreads <= 0) {
                errorPrint("Invalid -T: %s, will auto set to default(8)\n",
                           arg);
                arguments->nthreads = DEFAULT_NTHREADS;
            }
            arguments->nthreads_pool = arguments->nthreads + 5;
            break;
        case 'i':
            arguments->db->superTbls->insert_interval = atoi(arg);
            if (arguments->db->superTbls->insert_interval <= 0) {
                errorPrint("Invalid -i: %s, will auto set to default(0)\n",
                           arg);
                arguments->db->superTbls->insert_interval = 0;
            }
            break;
        case 'S':
            arguments->db->superTbls->timestamp_step = atol(arg);
            if (arguments->db->superTbls->timestamp_step <= 0) {
                errorPrint("Invalid -S: %s, will auto set to default(1)\n",
                           arg);
                arguments->db->superTbls->timestamp_step = 1;
            }
            break;
        case 'B':
            arguments->db->superTbls->interlaceRows = atoi(arg);
            if (arguments->db->superTbls->interlaceRows <= 0) {
                errorPrint("Invalid -B: %s, will auto set to default(0)\n",
                           arg);
                arguments->db->superTbls->interlaceRows = 0;
            }
            break;
        case 'r':
            arguments->reqPerReq = atoi(arg);
            if (arguments->reqPerReq <= 0 ||
                arguments->reqPerReq > MAX_RECORDS_PER_REQ) {
                errorPrint("Invalid -r: %s, will auto set to default(30000)\n",
                           arg);
                arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
            }
            break;
        case 't':
            arguments->db->superTbls->childTblCount = atoi(arg);
            if (arguments->db->superTbls->childTblCount <= 0) {
                errorPrint("Invalid -t: %s, will auto set to default(10000)\n",
                           arg);
                arguments->db->superTbls->childTblCount = DEFAULT_CHILDTABLES;
            }
            g_arguments->g_totalChildTables =
                arguments->db->superTbls->childTblCount;
            break;
        case 'n':
            arguments->db->superTbls->insertRows = atol(arg);
            if (arguments->db->superTbls->insertRows <= 0) {
                errorPrint("Invalid -n: %s, will auto set to default(10000)\n",
                           arg);
                arguments->db->superTbls->insertRows = DEFAULT_INSERT_ROWS;
            }
            break;
        case 'd':
            arguments->db->dbName = arg;
            break;
        case 'l':
            arguments->demo_mode = false;
            arguments->intColumnCount = atoi(arg);
            if (arguments->intColumnCount <= 0) {
                errorPrint("Invalid -l: %s, will auto set to default(0)\n",
                           arg);
                arguments->intColumnCount = 0;
            }
            break;
        case 'A':
            arguments->demo_mode = false;
            count_datatype(arg, &(arguments->db->superTbls->tagCount));
            tmfree(arguments->db->superTbls->tag_type);
            tmfree(arguments->db->superTbls->tag_length);
            tmfree(arguments->db->superTbls->tag_null);
            arguments->db->superTbls->tag_type =
                calloc(arguments->db->superTbls->tagCount, sizeof(char));
            arguments->db->superTbls->tag_null =
                calloc(arguments->db->superTbls->tagCount, sizeof(bool));
            arguments->db->superTbls->tag_length =
                calloc(arguments->db->superTbls->tagCount, sizeof(int32_t));
            if (parse_datatype(arg, arguments->db->superTbls->tag_type,
                               arguments->db->superTbls->tag_length, true)) {
                tmfree(arguments->db->superTbls->tag_type);
                tmfree(arguments->db->superTbls->tag_length);
                tmfree(arguments->db->superTbls->tag_null);
                exit(EXIT_FAILURE);
            }
            break;
        case 'b':
            arguments->demo_mode = false;
            tmfree(arguments->db->superTbls->col_type);
            tmfree(arguments->db->superTbls->col_length);
            tmfree(arguments->db->superTbls->col_null);
            count_datatype(arg, &(arguments->db->superTbls->columnCount));
            arguments->db->superTbls->col_type =
                calloc(arguments->db->superTbls->columnCount, sizeof(char));
            arguments->db->superTbls->col_null =
                calloc(arguments->db->superTbls->columnCount, sizeof(bool));
            arguments->db->superTbls->col_length =
                calloc(arguments->db->superTbls->columnCount, sizeof(int32_t));
            if (parse_datatype(arg, arguments->db->superTbls->col_type,
                               arguments->db->superTbls->col_length, false)) {
                tmfree(arguments->db->superTbls->col_type);
                tmfree(arguments->db->superTbls->col_null);
                tmfree(arguments->db->superTbls->col_length);
                exit(EXIT_FAILURE);
            }
            break;
        case 'w':
            arguments->binwidth = atoi(arg);
            if (arguments->binwidth <= 0) {
                errorPrint(
                    "Invalid value for w: %s, will auto set to default(64)\n",
                    arg);
                arguments->binwidth = DEFAULT_BINWIDTH;
            } else if (arguments->binwidth > TSDB_MAX_BINARY_LEN) {
                errorPrint("-w(%d) > TSDB_MAX_BINARY_LEN(%" PRIu64
                           "), will auto set to default(64)\n",
                           arguments->binwidth, (uint64_t)TSDB_MAX_BINARY_LEN);
                arguments->binwidth = DEFAULT_BINWIDTH;
            }
            break;
        case 'm':
            arguments->db->superTbls->childTblPrefix = arg;
            break;
        case 'E':
            arguments->db->superTbls->escape_character = true;
            break;
        case 'C':
            arguments->chinese = true;
            break;
        case 'N':
            arguments->db->superTbls->use_metric = false;
            arguments->db->superTbls->tagCount = 0;
            break;
        case 'M':
            arguments->demo_mode = false;
            break;
        case 'x':
            arguments->aggr_func = true;
            break;
        case 'y':
            arguments->answer_yes = true;
            break;
        case 'R':
            arguments->db->superTbls->disorderRange = atoi(arg);
            if (arguments->db->superTbls->disorderRange <= 0) {
                errorPrint(
                    "Invalid value for -R: %s, will auto set to "
                    "default(1000)\n",
                    arg);
                arguments->db->superTbls->disorderRange =
                    DEFAULT_DISORDER_RANGE;
            }
            break;
        case 'O':
            arguments->db->superTbls->disorderRatio = atoi(arg);
            if (arguments->db->superTbls->disorderRatio <= 0) {
                errorPrint(
                    "Invalid value for -O: %s, will auto set to default(0)\n",
                    arg);
                arguments->db->superTbls->disorderRatio = 0;
            }
            break;
        case 'a':
            arguments->db->dbCfg.replica = atoi(arg);
            if (arguments->db->dbCfg.replica <= 0) {
                errorPrint(
                    "Invalid value for -a: %s, will auto set to default(1)\n",
                    arg);
                arguments->db->dbCfg.replica = 1;
            }
            break;
        case 'g':
            arguments->debug_print = true;
            break;
        case 'G':
            arguments->performance_print = true;
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = {options, parse_opt, args_doc, doc};

static SSuperTable *init_stable() {
    SDataBase *database = g_arguments->db;
    database->superTbls = calloc(1, sizeof(SSuperTable));
    SSuperTable *stbInfo = database->superTbls;
    stbInfo->iface = TAOSC_IFACE;
    stbInfo->stbName = "meters";
    stbInfo->childTblPrefix = DEFAULT_TB_PREFIX;
    stbInfo->escape_character = 0;
    stbInfo->use_metric = 1;
    stbInfo->col_type = (char *)calloc(3, sizeof(char));
    stbInfo->col_null = (bool *)calloc(3, sizeof(bool));
    stbInfo->col_type[0] = TSDB_DATA_TYPE_FLOAT;
    stbInfo->col_type[1] = TSDB_DATA_TYPE_INT;
    stbInfo->col_type[2] = TSDB_DATA_TYPE_FLOAT;
    stbInfo->col_length = (int32_t *)calloc(3, sizeof(int32_t));
    stbInfo->col_length[0] = sizeof(float);
    stbInfo->col_length[1] = sizeof(int32_t);
    stbInfo->col_length[2] = sizeof(float);
    stbInfo->tag_type = (char *)calloc(2, sizeof(char));
    stbInfo->tag_null = (bool *)calloc(2, sizeof(bool));
    stbInfo->tag_type[0] = TSDB_DATA_TYPE_INT;
    stbInfo->tag_type[1] = TSDB_DATA_TYPE_BINARY;
    stbInfo->tag_length = (int32_t *)calloc(2, sizeof(int32_t));
    stbInfo->tag_length[0] = sizeof(int32_t);
    stbInfo->tag_length[1] = 16;
    stbInfo->columnCount = 3;
    stbInfo->tagCount = 2;
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
    stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    stbInfo->startTimestamp = DEFAULT_START_TIME;
    stbInfo->insertRows = DEFAULT_INSERT_ROWS;
    stbInfo->disorderRange = DEFAULT_DISORDER_RANGE;
    stbInfo->disorderRatio = 0;
    return stbInfo;
}

static SDataBase *init_database() {
    g_arguments->db = calloc(1, sizeof(SDataBase));
    SDataBase *database = g_arguments->db;
    database->dbName = DEFAULT_DATABASE;
    database->drop = 1;
    database->superTblCount = 1;
    database->dbCfg.minRows = -1;
    database->dbCfg.maxRows = -1;
    database->dbCfg.comp = -1;
    database->dbCfg.walLevel = -1;
    database->dbCfg.cacheLast = -1;
    database->dbCfg.fsync = -1;
    database->dbCfg.replica = -1;
    database->dbCfg.update = -1;
    database->dbCfg.keep = -1;
    database->dbCfg.days = -1;
    database->dbCfg.cache = -1;
    database->dbCfg.blocks = -1;
    database->dbCfg.quorum = -1;
    database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
    database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    return database;
}
void init_argument() {
    g_arguments = calloc(1, sizeof(SArguments));
    g_memoryUsage += sizeof(SArguments);
    g_arguments->pool = calloc(1, sizeof(TAOS_POOL));
    g_memoryUsage += sizeof(TAOS_POOL);
    g_arguments->test_mode = INSERT_TEST;
    g_arguments->demo_mode = 1;
    g_arguments->dbCount = 1;
    g_arguments->host = NULL;
    g_arguments->port = DEFAULT_PORT;
    g_arguments->telnet_tcp_port = TELNET_TCP_PORT;
    g_arguments->user = TSDB_DEFAULT_USER;
    g_arguments->password = TSDB_DEFAULT_PASS;
    g_arguments->answer_yes = 0;
    g_arguments->debug_print = 0;
    g_arguments->performance_print = 0;
    g_arguments->output_file = DEFAULT_OUTPUT;
    g_arguments->nthreads = DEFAULT_NTHREADS;
    g_arguments->nthreads_pool = DEFAULT_NTHREADS + 5;
    g_arguments->binwidth = DEFAULT_BINWIDTH;
    g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    g_arguments->g_totalChildTables = DEFAULT_CHILDTABLES;
    g_arguments->g_actualChildTables = 0;
    g_arguments->g_autoCreatedChildTables = 0;
    g_arguments->g_existedChildTables = 0;
    g_arguments->chinese = 0;
    g_arguments->aggr_func = 0;
    g_arguments->db = init_database();
    g_arguments->db->superTbls = init_stable();
}

void commandLineParseArgument(int argc, char *argv[]) {
    argp_program_version = taos_get_client_info();
    argp_parse(&argp, argc, argv, 0, 0, g_arguments);
}

void modify_argument() {
    SSuperTable *superTable = g_arguments->db->superTbls;
    if (init_taos_list()) exit(EXIT_FAILURE);

    for (int i = 0; i < superTable->columnCount; ++i) {
        if (superTable->col_length[i] == 0) {
            superTable->col_length[i] = g_arguments->binwidth;
        }
    }

    superTable->tag_names = calloc(superTable->tagCount, sizeof(char *));
    superTable->col_names = calloc(superTable->columnCount, sizeof(char *));
    if (g_arguments->demo_mode) {
        superTable->tag_names[0] = calloc(1, TSDB_COL_NAME_LEN);
        sprintf(superTable->tag_names[0], "groupid");
        superTable->tag_names[1] = calloc(1, TSDB_COL_NAME_LEN);
        sprintf(superTable->tag_names[1], "location");
        superTable->col_names[0] = calloc(1, TSDB_COL_NAME_LEN);
        sprintf(superTable->col_names[0], "current");
        superTable->col_names[1] = calloc(1, TSDB_COL_NAME_LEN);
        sprintf(superTable->col_names[1], "voltage");
        superTable->col_names[2] = calloc(1, TSDB_COL_NAME_LEN);
        sprintf(superTable->col_names[2], "phase");
    } else {
        for (int i = 0; i < superTable->tagCount; ++i) {
            superTable->tag_names[i] = calloc(1, TSDB_COL_NAME_LEN);
            sprintf(superTable->tag_names[i], "t%d", i);
        }
        for (int i = 0; i < superTable->columnCount; ++i) {
            superTable->col_names[i] = calloc(1, TSDB_COL_NAME_LEN);
            sprintf(superTable->col_names[i], "c%d", i);
        }
    }

    for (int i = 0; i < superTable->tagCount; ++i) {
        if (superTable->tag_length[i] == 0) {
            superTable->tag_length[i] = g_arguments->binwidth;
        }
    }

    if (g_arguments->intColumnCount > superTable->columnCount) {
        char *tmp_type = (char *)realloc(
            superTable->col_type, g_arguments->intColumnCount * sizeof(char));
        int32_t *tmp_length =
            (int32_t *)realloc(superTable->col_length,
                               g_arguments->intColumnCount * sizeof(int32_t));
        if (tmp_type != NULL && tmp_length != NULL) {
            superTable->col_type = tmp_type;
            superTable->col_length = tmp_length;
            for (int i = superTable->columnCount;
                 i < g_arguments->intColumnCount; ++i) {
                superTable->col_type[i] = TSDB_DATA_TYPE_INT;
                superTable->col_length[i] = sizeof(int32_t);
            }
        }
        superTable->columnCount = g_arguments->intColumnCount;
        tmfree(superTable->col_null);
        superTable->col_null = calloc(superTable->columnCount, sizeof(bool));
    }
}

static void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
    prctl(PR_SET_NAME, "queryStableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);

    FILE *  fp = g_arguments->fpOfInsertResult;
    int64_t totalData = g_arguments->db->superTbls->insertRows *
                        g_arguments->db->superTbls->childTblCount;
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

        int64_t m = 10 < g_arguments->db->superTbls->childTblCount
                        ? 10
                        : g_arguments->db->superTbls->childTblCount;

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

            sprintf(command, "SELECT %s FROM meters WHERE %s", aggreFunc[j],
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
            infoPrint("%s took %.6f second(s)\n\n", command, t / 1000000);

            taos_free_result(pSql);
        }
    }
    free(command);
    return NULL;
}

static void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
    prctl(PR_SET_NAME, "queryNtableAggrFunc");
    char *  command = calloc(1, BUFFER_SIZE);
    FILE *  fp = g_arguments->fpOfInsertResult;
    int64_t totalData = g_arguments->db->superTbls->childTblCount *
                        g_arguments->db->superTbls->insertRows;
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
        for (int64_t i = 0; i < g_arguments->db->superTbls->childTblCount;
             i++) {
            if (g_arguments->db->superTbls->escape_character) {
                sprintf(command,
                        "SELECT %s FROM `%s%" PRId64 "` WHERE ts>= %" PRIu64,
                        aggreFunc[j],
                        g_arguments->db->superTbls->childTblPrefix, i,
                        DEFAULT_START_TIME);
            } else {
                sprintf(
                    command, "SELECT %s FROM %s%" PRId64 " WHERE ts>= %" PRIu64,
                    aggreFunc[j], g_arguments->db->superTbls->childTblPrefix, i,
                    DEFAULT_START_TIME);
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
                    aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j],
                    totalData,
                    (double)(g_arguments->db->superTbls->childTblCount *
                             g_arguments->db->superTbls->insertRows) /
                        totalT,
                    totalT / 1000000);
        }
        infoPrint("<%s> took %.6f second(s)\n", command, totalT / 1000000);
    }
    free(command);
    return NULL;
}

void queryAggrFunc() {
    pthread_t   read_id;
    threadInfo *pThreadInfo = calloc(1, sizeof(threadInfo));
    pThreadInfo->taos = select_one_from_pool(g_arguments->db->dbName);
    if (g_arguments->db->superTbls->use_metric) {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    free(pThreadInfo);
}
