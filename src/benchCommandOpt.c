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
    database->dbCfg.buffer = -1;
    database->dbCfg.pages = -1;
    database->dbCfg.page_size = -1;
    database->dbCfg.retentions = NULL;
    database->dbCfg.single_stable = -1;
    database->dbCfg.vgroups = -1;
    database->dbCfg.strict = -1;
    database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
    database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
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

            sprintf(command, "SELECT %s FROM meters WHERE %s", aggreFunc[j],
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
                        "SELECT %s FROM `%s%" PRId64 "` WHERE ts>= %" PRIu64,
                        aggreFunc[j],
                        stbInfo->childTblPrefix, i,
                        (uint64_t) DEFAULT_START_TIME);
            } else {
                sprintf(
                    command, "SELECT %s FROM %s%" PRId64 " WHERE ts>= %" PRIu64,
                    aggreFunc[j], stbInfo->childTblPrefix, i,
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
    free(pThreadInfo);
}
