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

#include "demo.h"
#include "demoData.h"

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
    pg_args->pressure_mode = DEFAULT_PRESSURE_MODE;
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

int parse_args(int argc, char *argv[], SArguments *pg_args) {
    int32_t code = -1;
    for (int i = 1; i < argc; i++) {
        if ((0 == strncmp(argv[i], "-f", strlen("-f"))) ||
            (0 == strncmp(argv[i], "--file", strlen("--file")))) {
            pg_args->demo_mode = false;

            if (2 == strlen(argv[i])) {
                if (i + 1 == argc) {
                    errorPrintReqArg(argv[0], "f");
                    goto end_parse_command;
                }
                pg_args->metaFile = argv[++i];
            } else if (0 == strncmp(argv[i], "-f", strlen("-f"))) {
                pg_args->metaFile = (char *)(argv[i] + strlen("-f"));
            } else if (strlen("--file") == strlen(argv[i])) {
                if (i + 1 == argc) {
                    errorPrintReqArg3(argv[0], "--file");
                    goto end_parse_command;
                }
                pg_args->metaFile = argv[++i];
            } else if (0 == strncmp(argv[i], "--file=", strlen("--file="))) {
                pg_args->metaFile = (char *)(argv[i] + strlen("--file="));
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-c", strlen("-c"))) ||
                   (0 ==
                    strncmp(argv[i], "--config-dir", strlen("--config-dir")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "c");
                    goto end_parse_command;
                }
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else if (0 == strncmp(argv[i], "-c", strlen("-c"))) {
                tstrncpy(configDir, (char *)(argv[i] + strlen("-c")),
                         TSDB_FILENAME_LEN);
            } else if (strlen("--config-dir") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--config-dir");
                    goto end_parse_command;
                }
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else if (0 == strncmp(argv[i],
                                    "--config-dir=", strlen("--config-dir="))) {
                tstrncpy(configDir, (char *)(argv[i] + strlen("--config-dir=")),
                         TSDB_FILENAME_LEN);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-h", strlen("-h"))) ||
                   (0 == strncmp(argv[i], "--host", strlen("--host")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "h");
                    goto end_parse_command;
                }
                pg_args->host = argv[++i];
            } else if (0 == strncmp(argv[i], "-h", strlen("-h"))) {
                pg_args->host = (char *)(argv[i] + strlen("-h"));
            } else if (strlen("--host") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--host");
                    goto end_parse_command;
                }
                pg_args->host = argv[++i];
            } else if (0 == strncmp(argv[i], "--host=", strlen("--host="))) {
                pg_args->host = (char *)(argv[i] + strlen("--host="));
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if (strcmp(argv[i], "-PP") == 0) {
            pg_args->performance_print = true;
        } else if ((0 == strncmp(argv[i], "-P", strlen("-P"))) ||
                   (0 == strncmp(argv[i], "--port", strlen("--port")))) {
            uint64_t port;
            char     strPort[BIGINT_BUFF_LEN];

            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "P");
                    goto end_parse_command;
                } else if (isStringNumber(argv[i + 1])) {
                    tstrncpy(strPort, argv[++i], BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "P");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "--port=", strlen("--port="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--port=")))) {
                    tstrncpy(strPort, (char *)(argv[i] + strlen("--port=")),
                             BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-P", strlen("-P"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-P")))) {
                    tstrncpy(strPort, (char *)(argv[i] + strlen("-P")),
                             BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    goto end_parse_command;
                }
            } else if (strlen("--port") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--port");
                    goto end_parse_command;
                } else if (isStringNumber(argv[i + 1])) {
                    tstrncpy(strPort, argv[++i], BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    goto end_parse_command;
                }
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            port = atoi(strPort);
            if (port > 65535) {
                errorWrongValue("taosdump", "-P or --port", strPort);
                goto end_parse_command;
            }
            pg_args->port = (uint16_t)port;

        } else if ((0 == strncmp(argv[i], "-I", strlen("-I"))) ||
                   (0 ==
                    strncmp(argv[i], "--interface", strlen("--interface")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "I");
                    goto end_parse_command;
                }
                if (0 == strcasecmp(argv[i + 1], "taosc")) {
                    pg_args->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "rest")) {
                    pg_args->iface = REST_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "stmt")) {
                    pg_args->iface = STMT_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "sml")) {
                    pg_args->iface = SML_IFACE;
                } else {
                    errorWrongValue(argv[0], "-I", argv[i + 1]);
                    goto end_parse_command;
                }
                i++;
            } else if (0 == strncmp(argv[i],
                                    "--interface=", strlen("--interface="))) {
                if (0 == strcasecmp((char *)(argv[i] + strlen("--interface=")),
                                    "taosc")) {
                    pg_args->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(
                                    (char *)(argv[i] + strlen("--interface=")),
                                    "rest")) {
                    pg_args->iface = REST_IFACE;
                } else if (0 == strcasecmp(
                                    (char *)(argv[i] + strlen("--interface=")),
                                    "stmt")) {
                    pg_args->iface = STMT_IFACE;
                } else if (0 == strcasecmp(
                                    (char *)(argv[i] + strlen("--interface=")),
                                    "sml")) {
                    pg_args->iface = SML_IFACE;
                } else {
                    errorPrintReqArg3(argv[0], "--interface");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-I", strlen("-I"))) {
                if (0 ==
                    strcasecmp((char *)(argv[i] + strlen("-I")), "taosc")) {
                    pg_args->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")),
                                           "rest")) {
                    pg_args->iface = REST_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")),
                                           "stmt")) {
                    pg_args->iface = STMT_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")),
                                           "sml")) {
                    pg_args->iface = SML_IFACE;
                } else {
                    errorWrongValue(argv[0], "-I",
                                    (char *)(argv[i] + strlen("-I")));
                    goto end_parse_command;
                }
            } else if (strlen("--interface") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--interface");
                    goto end_parse_command;
                }
                if (0 == strcasecmp(argv[i + 1], "taosc")) {
                    pg_args->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "rest")) {
                    pg_args->iface = REST_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "stmt")) {
                    pg_args->iface = STMT_IFACE;
                } else if (0 == strcasecmp(argv[i + 1], "sml")) {
                    pg_args->iface = SML_IFACE;
                } else {
                    errorWrongValue(argv[0], "--interface", argv[i + 1]);
                    goto end_parse_command;
                }
                i++;
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-u", strlen("-u"))) ||
                   (0 == strncmp(argv[i], "--user", strlen("--user")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "u");
                    goto end_parse_command;
                }
                pg_args->user = argv[++i];
            } else if (0 == strncmp(argv[i], "-u", strlen("-u"))) {
                pg_args->user = (char *)(argv[i++] + strlen("-u"));
            } else if (0 == strncmp(argv[i], "--user=", strlen("--user="))) {
                pg_args->user = (char *)(argv[i++] + strlen("--user="));
            } else if (strlen("--user") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--user");
                    goto end_parse_command;
                }
                pg_args->user = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-p", strlen("-p"))) ||
                   (0 == strcmp(argv[i], "--password"))) {
            if ((strlen(argv[i]) == 2) ||
                (0 == strcmp(argv[i], "--password"))) {
                printf("Enter password: ");
                if (scanf("%26s", pg_args->password) > 1) {
                    fprintf(stderr, "password read error!\n");
                }
            } else {
                pg_args->password = (char *)(argv[i] + 2);
            }
        } else if ((0 == strncmp(argv[i], "-o", strlen("-o"))) ||
                   (0 == strncmp(argv[i], "--output", strlen("--output")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--output");
                    goto end_parse_command;
                }
                pg_args->output_file = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--output=", strlen("--output="))) {
                pg_args->output_file =
                    (char *)(argv[i++] + strlen("--output="));
            } else if (0 == strncmp(argv[i], "-o", strlen("-o"))) {
                pg_args->output_file = (char *)(argv[i++] + strlen("-o"));
            } else if (strlen("--output") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--output");
                    goto end_parse_command;
                }
                pg_args->output_file = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-s", strlen("-s"))) ||
                   (0 ==
                    strncmp(argv[i], "--sql-file", strlen("--sql-file")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "s");
                    goto end_parse_command;
                }
                pg_args->sqlFile = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--sql-file=", strlen("--sql-file="))) {
                pg_args->sqlFile = (char *)(argv[i++] + strlen("--sql-file="));
            } else if (0 == strncmp(argv[i], "-s", strlen("-s"))) {
                pg_args->sqlFile = (char *)(argv[i++] + strlen("-s"));
            } else if (strlen("--sql-file") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--sql-file");
                    goto end_parse_command;
                }
                pg_args->sqlFile = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-T", strlen("-T"))) ||
                   (0 == strncmp(argv[i], "--threads", strlen("--threads")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "T");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "T");
                    goto end_parse_command;
                }
                pg_args->nthreads = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--threads=", strlen("--threads="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--threads=")))) {
                    pg_args->nthreads =
                        atoi((char *)(argv[i] + strlen("--threads=")));
                } else {
                    errorPrintReqArg2(argv[0], "--threads");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-T", strlen("-T"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-T")))) {
                    pg_args->nthreads = atoi((char *)(argv[i] + strlen("-T")));
                } else {
                    errorPrintReqArg2(argv[0], "-T");
                    goto end_parse_command;
                }
            } else if (strlen("--threads") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--threads");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--threads");
                    goto end_parse_command;
                }
                pg_args->nthreads = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-i", strlen("-i"))) ||
                   (0 == strncmp(argv[i], "--insert-interval",
                                 strlen("--insert-interval")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "i");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "i");
                    goto end_parse_command;
                }
                pg_args->insert_interval = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--insert-interval=",
                                    strlen("--insert-interval="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--insert-interval=")))) {
                    pg_args->insert_interval =
                        atoi((char *)(argv[i] + strlen("--insert-interval=")));
                } else {
                    errorPrintReqArg3(argv[0], "--insert-innterval");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-i", strlen("-i"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-i")))) {
                    pg_args->insert_interval =
                        atoi((char *)(argv[i] + strlen("-i")));
                } else {
                    errorPrintReqArg3(argv[0], "-i");
                    goto end_parse_command;
                }
            } else if (strlen("--insert-interval") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--insert-interval");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--insert-interval");
                    goto end_parse_command;
                }
                pg_args->insert_interval = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-S", strlen("-S"))) ||
                   (0 ==
                    strncmp(argv[i], "--time-step", strlen("--time-step")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "S");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "S");
                    goto end_parse_command;
                }
                pg_args->timestamp_step = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i],
                                    "--time-step=", strlen("--time-step="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--time-step=")))) {
                    pg_args->timestamp_step =
                        atoi((char *)(argv[i] + strlen("--time-step=")));
                } else {
                    errorPrintReqArg2(argv[0], "--time-step");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-S", strlen("-S"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-S")))) {
                    pg_args->timestamp_step =
                        atoi((char *)(argv[i] + strlen("-S")));
                } else {
                    errorPrintReqArg2(argv[0], "-S");
                    goto end_parse_command;
                }
            } else if (strlen("--time-step") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--time-step");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--time-step");
                    goto end_parse_command;
                }
                pg_args->timestamp_step = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if (strcmp(argv[i], "-qt") == 0) {
            if ((argc == i + 1) || (!isStringNumber(argv[i + 1]))) {
                printHelp();
                errorPrint("%s", "\n\t-qt need a number following!\n");
                goto end_parse_command;
            }
            pg_args->query_times = atoi(argv[++i]);
        } else if ((0 == strncmp(argv[i], "-B", strlen("-B"))) ||
                   (0 == strncmp(argv[i], "--interlace-rows",
                                 strlen("--interlace-rows")))) {
            if (strlen("-B") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "B");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "B");
                    goto end_parse_command;
                }
                pg_args->interlaceRows = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--interlace-rows=",
                                    strlen("--interlace-rows="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--interlace-rows=")))) {
                    pg_args->interlaceRows =
                        atoi((char *)(argv[i] + strlen("--interlace-rows=")));
                } else {
                    errorPrintReqArg2(argv[0], "--interlace-rows");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-B", strlen("-B"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-B")))) {
                    pg_args->interlaceRows =
                        atoi((char *)(argv[i] + strlen("-B")));
                } else {
                    errorPrintReqArg2(argv[0], "-B");
                    goto end_parse_command;
                }
            } else if (strlen("--interlace-rows") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--interlace-rows");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--interlace-rows");
                    goto end_parse_command;
                }
                pg_args->interlaceRows = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-r", strlen("-r"))) ||
                   (0 == strncmp(argv[i], "--rec-per-req", 13))) {
            if (strlen("-r") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "r");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "r");
                    goto end_parse_command;
                }
                pg_args->reqPerReq = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--rec-per-req=",
                                    strlen("--rec-per-req="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--rec-per-req=")))) {
                    pg_args->reqPerReq =
                        atoi((char *)(argv[i] + strlen("--rec-per-req=")));
                } else {
                    errorPrintReqArg2(argv[0], "--rec-per-req");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-r", strlen("-r"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-r")))) {
                    pg_args->reqPerReq = atoi((char *)(argv[i] + strlen("-r")));
                } else {
                    errorPrintReqArg2(argv[0], "-r");
                    goto end_parse_command;
                }
            } else if (strlen("--rec-per-req") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--rec-per-req");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--rec-per-req");
                    goto end_parse_command;
                }
                pg_args->reqPerReq = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-t", strlen("-t"))) ||
                   (0 == strncmp(argv[i], "--tables", strlen("--tables")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "t");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "t");
                    goto end_parse_command;
                }
                pg_args->ntables = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--tables=", strlen("--tables="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--tables=")))) {
                    pg_args->ntables =
                        atoi((char *)(argv[i] + strlen("--tables=")));
                } else {
                    errorPrintReqArg2(argv[0], "--tables");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-t", strlen("-t"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-t")))) {
                    pg_args->ntables = atoi((char *)(argv[i] + strlen("-t")));
                } else {
                    errorPrintReqArg2(argv[0], "-t");
                    goto end_parse_command;
                }
            } else if (strlen("--tables") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--tables");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--tables");
                    goto end_parse_command;
                }
                pg_args->ntables = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            g_totalChildTables = pg_args->ntables;
        } else if ((0 == strncmp(argv[i], "-n", strlen("-n"))) ||
                   (0 == strncmp(argv[i], "--records", strlen("--records")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "n");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "n");
                    goto end_parse_command;
                }
                pg_args->insertRows = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--records=", strlen("--records="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--records=")))) {
                    pg_args->insertRows =
                        atoi((char *)(argv[i] + strlen("--records=")));
                } else {
                    errorPrintReqArg2(argv[0], "--records");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-n", strlen("-n"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-n")))) {
                    pg_args->insertRows =
                        atoi((char *)(argv[i] + strlen("-n")));
                } else {
                    errorPrintReqArg2(argv[0], "-n");
                    goto end_parse_command;
                }
            } else if (strlen("--records") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--records");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--records");
                    goto end_parse_command;
                }
                pg_args->insertRows = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-d", strlen("-d"))) ||
                   (0 ==
                    strncmp(argv[i], "--database", strlen("--database")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "d");
                    goto end_parse_command;
                }
                pg_args->database = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--database=", strlen("--database="))) {
                pg_args->output_file =
                    (char *)(argv[i] + strlen("--database="));
            } else if (0 == strncmp(argv[i], "-d", strlen("-d"))) {
                pg_args->output_file = (char *)(argv[i] + strlen("-d"));
            } else if (strlen("--database") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--database");
                    goto end_parse_command;
                }
                pg_args->database = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-l", strlen("-l"))) ||
                   (0 == strncmp(argv[i], "--columns", strlen("--columns")))) {
            pg_args->demo_mode = false;
            if (custom_col_num) {
                errorPrint(
                    "%s",
                    "-l/columns option cannot work with custom column types\n");
                goto end_parse_command;
            }
            custom_col_num = true;
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "l");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "l");
                    goto end_parse_command;
                }
                pg_args->columnCount = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--columns=", strlen("--columns="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--columns=")))) {
                    pg_args->columnCount =
                        atoi((char *)(argv[i] + strlen("--columns=")));
                } else {
                    errorPrintReqArg2(argv[0], "--columns");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-l", strlen("-l"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-l")))) {
                    pg_args->columnCount =
                        atoi((char *)(argv[i] + strlen("-l")));
                } else {
                    errorPrintReqArg2(argv[0], "-l");
                    goto end_parse_command;
                }
            } else if (strlen("--columns") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--columns");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--columns");
                    goto end_parse_command;
                }
                pg_args->columnCount = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
            tmfree(pg_args->col_type);
            tmfree(pg_args->col_length);
            pg_args->col_type = calloc(pg_args->columnCount, sizeof(char));
            pg_args->col_length = calloc(pg_args->columnCount, sizeof(int32_t));
            for (int j = 0; j < pg_args->columnCount; ++j) {
                pg_args->col_type[j] = TSDB_DATA_TYPE_INT;
                pg_args->col_length[j] = sizeof(int32_t);
            }
        } else if ((0 == strncmp(argv[i], "-A", strlen("-A"))) ||
                   (0 ==
                    strncmp(argv[i], "--tag-type", strlen("--tag-type")))) {
            pg_args->demo_mode = false;

            char *dataType;
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "A");
                    goto end_parse_command;
                }
                dataType = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--tag-type=", strlen("--tag-type="))) {
                dataType = (char *)(argv[i] + strlen("--tag-type="));
            } else if (0 == strncmp(argv[i], "-A", strlen("-A"))) {
                dataType = (char *)(argv[i] + strlen("-A"));
            } else if (strlen("--tag-type") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--tag-type");
                    goto end_parse_command;
                }
                dataType = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
            tmfree(pg_args->tag_type);
            tmfree(pg_args->tag_length);
            count_datatype(dataType, &(pg_args->tagCount));
            pg_args->tag_type = calloc(pg_args->tagCount, sizeof(char));
            pg_args->tag_length = calloc(pg_args->tagCount, sizeof(int32_t));
            if (parse_datatype(dataType, pg_args->tag_type, pg_args->tag_length,
                               true)) {
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-b", strlen("-b"))) ||
                   (0 ==
                    strncmp(argv[i], "--col-type", strlen("--col-type")))) {
            pg_args->demo_mode = false;
            if (custom_col_num) {
                errorPrint(
                    "%s",
                    "-l/columns option cannot work with custom column types\n");
                goto end_parse_command;
            }
            custom_col_num = true;
            char *dataType;
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "b");
                    goto end_parse_command;
                }
                dataType = argv[++i];
            } else if (0 ==
                       strncmp(argv[i], "--col-type=", strlen("--col-type="))) {
                dataType = (char *)(argv[i] + strlen("--col-type="));
            } else if (0 == strncmp(argv[i], "-b", strlen("-b"))) {
                dataType = (char *)(argv[i] + strlen("-b"));
            } else if (strlen("--col-type") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--col-type");
                    goto end_parse_command;
                }
                dataType = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
            tmfree(g_args.col_type);
            tmfree(g_args.col_length);
            count_datatype(dataType, &(g_args.columnCount));
            g_args.col_type = calloc(g_args.columnCount, sizeof(char));
            g_args.col_length = calloc(g_args.columnCount, sizeof(int32_t));
            if (parse_datatype(dataType, g_args.col_type, g_args.col_length,
                               false)) {
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-w", strlen("-w"))) ||
                   (0 ==
                    strncmp(argv[i], "--binwidth", strlen("--binwidth")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "w");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "w");
                    goto end_parse_command;
                }
                pg_args->binwidth = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--binwidth=", strlen("--binwidth="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--binwidth=")))) {
                    pg_args->binwidth =
                        atoi((char *)(argv[i] + strlen("--binwidth=")));
                } else {
                    errorPrintReqArg2(argv[0], "--binwidth");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-w", strlen("-w"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-w")))) {
                    pg_args->binwidth = atoi((char *)(argv[i] + strlen("-w")));
                } else {
                    errorPrintReqArg2(argv[0], "-w");
                    goto end_parse_command;
                }
            } else if (strlen("--binwidth") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--binwidth");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--binwidth");
                    goto end_parse_command;
                }
                pg_args->binwidth = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-m", strlen("-m"))) ||
                   (0 == strncmp(argv[i], "--table-prefix",
                                 strlen("--table-prefix")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "m");
                    goto end_parse_command;
                }
                pg_args->tb_prefix = argv[++i];
            } else if (0 == strncmp(argv[i], "--table-prefix=",
                                    strlen("--table-prefix="))) {
                pg_args->tb_prefix =
                    (char *)(argv[i] + strlen("--table-prefix="));
            } else if (0 == strncmp(argv[i], "-m", strlen("-m"))) {
                pg_args->tb_prefix = (char *)(argv[i] + strlen("-m"));
            } else if (strlen("--table-prefix") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--table-prefix");
                    goto end_parse_command;
                }
                pg_args->tb_prefix = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-E", strlen("-E"))) ||
                   (0 == strncmp(argv[i], "--escape-character",
                                 strlen("--escape-character")))) {
            pg_args->escapeChar = true;
        } else if (0 == strncmp(argv[i], "-pressure", strlen("-pressure"))) {
            pg_args->pressure_mode = true;
        } else if ((0 == strncmp(argv[i], "-C", strlen("-C"))) ||
                   (0 == strncmp(argv[i], "--chinese", strlen("--chinese")))) {
            pg_args->chinese = true;
        } else if ((strcmp(argv[i], "-N") == 0) ||
                   (0 == strcmp(argv[i], "--normal-table"))) {
            pg_args->demo_mode = false;
            pg_args->use_metric = false;
            pg_args->tagCount = 0;
        } else if ((strcmp(argv[i], "-M") == 0) ||
                   (0 == strcmp(argv[i], "--random"))) {
            pg_args->demo_mode = false;
        } else if ((strcmp(argv[i], "-x") == 0) ||
                   (0 == strcmp(argv[i], "--aggr-func"))) {
            pg_args->aggr_func = true;
        } else if ((strcmp(argv[i], "-y") == 0) ||
                   (0 == strcmp(argv[i], "--answer-yes"))) {
            pg_args->answer_yes = true;
        } else if ((strcmp(argv[i], "-g") == 0) ||
                   (0 == strcmp(argv[i], "--debug"))) {
            pg_args->debug_print = true;
        } else if (strcmp(argv[i], "-gg") == 0) {
            pg_args->verbose_print = true;
        } else if ((0 == strncmp(argv[i], "-R", strlen("-R"))) ||
                   (0 == strncmp(argv[i], "--disorder-range",
                                 strlen("--disorder-range")))) {
            if (strlen("-R") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "R");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "R");
                    goto end_parse_command;
                }
                pg_args->disorderRange = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--disorder-range=",
                                    strlen("--disorder-range="))) {
                if (isStringNumber(
                        (char *)(argv[i] + strlen("--disorder-range=")))) {
                    pg_args->disorderRange =
                        atoi((char *)(argv[i] + strlen("--disorder-range=")));
                } else {
                    errorPrintReqArg2(argv[0], "--disorder-range");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-R", strlen("-R"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-R")))) {
                    pg_args->disorderRange =
                        atoi((char *)(argv[i] + strlen("-R")));
                } else {
                    errorPrintReqArg2(argv[0], "-R");
                    goto end_parse_command;
                }

                if (pg_args->disorderRange < 0) {
                    errorPrint("Invalid disorder range %d, will be set to %d\n",
                               pg_args->disorderRange, 1000);
                    pg_args->disorderRange = 1000;
                }
            } else if (strlen("--disorder-range") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--disorder-range");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--disorder-range");
                    goto end_parse_command;
                }
                pg_args->disorderRange = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }
        } else if ((0 == strncmp(argv[i], "-O", strlen("-O"))) ||
                   (0 ==
                    strncmp(argv[i], "--disorder", strlen("--disorder")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "O");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "O");
                    goto end_parse_command;
                }
                pg_args->disorderRatio = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--disorder=", strlen("--disorder="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--disorder=")))) {
                    pg_args->disorderRatio =
                        atoi((char *)(argv[i] + strlen("--disorder=")));
                } else {
                    errorPrintReqArg2(argv[0], "--disorder");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-O", strlen("-O"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-O")))) {
                    pg_args->disorderRatio =
                        atoi((char *)(argv[i] + strlen("-O")));
                } else {
                    errorPrintReqArg2(argv[0], "-O");
                    goto end_parse_command;
                }
            } else if (strlen("--disorder") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--disorder");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--disorder");
                    goto end_parse_command;
                }
                pg_args->disorderRatio = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            if (pg_args->disorderRatio > 50) {
                errorPrint("Invalid disorder ratio %d, will be set to %d\n",
                           pg_args->disorderRatio, 50);
                pg_args->disorderRatio = 50;
            }
        } else if ((0 == strncmp(argv[i], "-a", strlen("-a"))) ||
                   (0 == strncmp(argv[i], "--replica", strlen("--replica")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg(argv[0], "a");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "a");
                    goto end_parse_command;
                }
                pg_args->replica = atoi(argv[++i]);
            } else if (0 ==
                       strncmp(argv[i], "--replica=", strlen("--replica="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--replica=")))) {
                    pg_args->replica =
                        atoi((char *)(argv[i] + strlen("--replica=")));
                } else {
                    errorPrintReqArg2(argv[0], "--replica");
                    goto end_parse_command;
                }
            } else if (0 == strncmp(argv[i], "-a", strlen("-a"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-a")))) {
                    pg_args->replica = atoi((char *)(argv[i] + strlen("-a")));
                } else {
                    errorPrintReqArg2(argv[0], "-a");
                    goto end_parse_command;
                }
            } else if (strlen("--replica") == strlen(argv[i])) {
                if (argc == i + 1) {
                    errorPrintReqArg3(argv[0], "--replica");
                    goto end_parse_command;
                } else if (!isStringNumber(argv[i + 1])) {
                    errorPrintReqArg2(argv[0], "--replica");
                    goto end_parse_command;
                }
                pg_args->replica = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                goto end_parse_command;
            }

            if (pg_args->replica > 3 || pg_args->replica < 1) {
                errorPrint("Invalid replica value %d, will be set to %d\n",
                           pg_args->replica, 1);
                pg_args->replica = 1;
            }
        } else if ((strcmp(argv[i], "--version") == 0) ||
                   (strcmp(argv[i], "-V") == 0)) {
            printVersion();
        } else if ((strcmp(argv[i], "--help") == 0) ||
                   (strcmp(argv[i], "-?") == 0)) {
            printHelp();
        } else if (strcmp(argv[i], "--usage") == 0) {
            printf(
                "    Usage: taosdemo [-f JSONFILE] [-u USER] [-p PASSWORD] [-c CONFIG_DIR]\n\
                    [-h HOST] [-P PORT] [-I INTERFACE] [-d DATABASE] [-a REPLICA]\n\
                    [-m TABLEPREFIX] [-s SQLFILE] [-N] [-o OUTPUTFILE] [-q QUERYMODE]\n\
                    [-b DATATYPES] [-w WIDTH_OF_BINARY] [-l COLUMNS] [-T THREADNUMBER]\n\
                    [-i SLEEPTIME] [-S TIME_STEP] [-B INTERLACE_ROWS] [-t TABLES]\n\
                    [-n RECORDS] [-M] [-x] [-y] [-O ORDERMODE] [-R RANGE] [-a REPLIcA][-g]\n\
                    [--help] [--usage] [--version]\n");
            exit(EXIT_SUCCESS);
        } else {
            // to simulate argp_option output
            if (strlen(argv[i]) > 2) {
                if (0 == strncmp(argv[i], "--", 2)) {
                    fprintf(stderr, "%s: unrecognized options '%s'\n", argv[0],
                            argv[i]);
                } else if (0 == strncmp(argv[i], "-", 1)) {
                    char tmp[2] = {0};
                    tstrncpy(tmp, argv[i] + 1, 2);
                    fprintf(stderr, "%s: invalid options -- '%s'\n", argv[0],
                            tmp);
                } else {
                    fprintf(stderr, "%s: Too many arguments\n", argv[0]);
                }
            } else {
                fprintf(stderr, "%s invalid options -- '%s'\n", argv[0],
                        (char *)((char *)argv[i]) + 1);
            }
            fprintf(stderr,
                    "Try `taosdemo --help' or `taosdemo --usage' for more "
                    "information.\n");
            goto end_parse_command;
        }
    }

    code = 0;
end_parse_command:
    return code;
}
void setParaFromArg(SArguments *pg_args) {
    pg_args->test_mode = INSERT_TEST;
    db[0].drop = true;
    tstrncpy(db[0].dbName, pg_args->database, TSDB_DB_NAME_LEN);
    db[0].dbCfg.replica = pg_args->replica;
    tstrncpy(db[0].dbCfg.precision, "ms", SMALL_BUFF_LEN);
    pg_args->prepared_rand = min(pg_args->insertRows, MAX_PREPARED_RAND);

    if ((pg_args->col_type[0] == TSDB_DATA_TYPE_BINARY) ||
        (pg_args->col_type[0] == TSDB_DATA_TYPE_BOOL) ||
        (pg_args->col_type[0] == TSDB_DATA_TYPE_NCHAR)) {
        g_args.aggr_func = false;
    }
    if (pg_args->use_metric) {
        db[0].superTblCount = 1;
        tstrncpy(db[0].superTbls[0].stbName, "meters", TSDB_TABLE_NAME_LEN);
        db[0].superTbls[0].childTblCount = pg_args->ntables;
        db[0].superTbls[0].escapeChar = pg_args->escapeChar;

        db[0].superTbls[0].autoCreateTable = PRE_CREATE_SUBTBL;
        db[0].superTbls[0].childTblExists = TBL_NO_EXISTS;
        db[0].superTbls[0].disorderRange = pg_args->disorderRange;
        db[0].superTbls[0].disorderRatio = pg_args->disorderRatio;
        tstrncpy(db[0].superTbls[0].childTblPrefix, pg_args->tb_prefix,
                 TBNAME_PREFIX_LEN);
        tstrncpy(db[0].superTbls[0].dataSource, "rand", SMALL_BUFF_LEN);

        db[0].superTbls[0].iface = pg_args->iface;
        db[0].superTbls[0].lineProtocol = TSDB_SML_LINE_PROTOCOL;
        db[0].superTbls[0].tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        tstrncpy(db[0].superTbls[0].startTimestamp, "2017-07-14 10:40:00.000",
                 MAX_TB_NAME_SIZE);
        db[0].superTbls[0].timeStampStep = pg_args->timestamp_step;

        db[0].superTbls[0].insertRows = pg_args->insertRows;
        db[0].superTbls[0].interlaceRows = pg_args->interlaceRows;
        db[0].superTbls[0].columnCount = g_args.columnCount;
        db[0].superTbls[0].col_type = g_args.col_type;
        db[0].superTbls[0].col_length = g_args.col_length;
        db[0].superTbls[0].tagCount = g_args.tagCount;
        db[0].superTbls[0].tag_type = g_args.tag_type;
        db[0].superTbls[0].tag_length = g_args.tag_length;
    }
}

int querySqlFile(TAOS *taos, char *sqlFile) {
    int32_t code = -1;
    FILE *  fp = fopen(sqlFile, "r");
    char *  cmd;
    int     read_len = 0;
    size_t  cmd_len = 0;
    char *  line = NULL;
    size_t  line_len = 0;
    if (fp == NULL) {
        errorPrint("failed to open file %s, reason:%s\n", sqlFile,
                   strerror(errno));
        goto free_of_query_sql_file;
    }
    cmd = calloc(1, TSDB_MAX_BYTES_PER_ROW);
    if (cmd == NULL) {
        errorPrint("%s", "failde to allocate memory\n");
        goto free_of_query_sql_file;
    }
    double t = (double)taosGetTimestampMs();
    while ((read_len = getline(&line, &line_len, fp)) != -1) {
        if (read_len >= TSDB_MAX_BYTES_PER_ROW) continue;
        line[--read_len] = '\0';

        if (read_len == 0 || isCommentLine(line)) {  // line starts with #
            continue;
        }

        if (line[read_len - 1] == '\\') {
            line[read_len - 1] = ' ';
            memcpy(cmd + cmd_len, line, read_len);
            cmd_len += read_len;
            continue;
        }

        memcpy(cmd + cmd_len, line, read_len);
        if (0 != queryDbExec(taos, cmd, NO_INSERT_TYPE, false)) {
            errorPrint("queryDbExec %s failed!\n", cmd);
            goto free_of_query_sql_file;
        }
        memset(cmd, 0, TSDB_MAX_BYTES_PER_ROW);
        cmd_len = 0;
    }

    t = taosGetTimestampMs() - t;
    printf("run %s took %.6f second(s)\n\n", sqlFile, t / 1000000);
    code = 0;
free_of_query_sql_file:
    tmfree(cmd);
    tmfree(line);
    tmfclose(fp);
    return code;
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

    pThreadInfo->taos = taos_connect(g_args.host, g_args.user, g_args.password,
                                     db[0].dbName, g_args.port);
    if (pThreadInfo->taos == NULL) {
        free(pThreadInfo);
        errorPrint("Failed to connect to TDengine, reason:%s\n",
                   taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    tstrncpy(pThreadInfo->filePath, g_args.output_file, MAX_FILE_NAME_LEN);

    if (!g_args.use_metric) {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    taos_close(pThreadInfo->taos);
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