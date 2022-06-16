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


void parse_datatype(char *dataType, BArray *fields, bool isTag) {
    char *dup_str;
    if (strstr(dataType, ",") == NULL) {
        Field * field = benchCalloc(1, sizeof(Field), true);
        if (1 == regexMatch(dataType, "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            setField(field, taos_convert_string_to_datatype(type, 0), atoi(length), isTag?"t0":"c0", 0,0);
        } else {
            uint8_t type = taos_convert_string_to_datatype(dataType, 0);
            setField(field, type, taos_convert_type_to_length(type), isTag?"t0":"c0",
                     get_min_from_data_type(type), get_max_from_data_type(type));
        }
        benchArrayPush(fields, field);
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        int   index = 0;
        while (token != NULL) {
            Field * field = benchCalloc(1, sizeof(Field), true);
            char name[TSDB_COL_NAME_LEN] = {0};
            snprintf(name, TSDB_COL_NAME_LEN, isTag?"t%d":"c%d", index);
             if (1 == regexMatch(token, "^(BINARY|NCHAR|JSON)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                setField(field, taos_convert_string_to_datatype(type, 0), atoi(length), name, 0,0);
            } else {
                uint8_t type = taos_convert_string_to_datatype(dataType, 0);
                setField(field, type, taos_convert_type_to_length(type), name,
                         get_min_from_data_type(type), get_max_from_data_type(type));
            }
            benchArrayPush(fields, field);
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
}

void init_stable() {
    SDataBase *database = benchArrayGet(g_arguments->db, 0);
    database->superTbls = benchArrayInit(1, sizeof(SSuperTable));
    SSuperTable *stbInfo = benchArrayGet(database->superTbls, 0);
    stbInfo->iface = TAOSC_IFACE;
    tstrncpy(stbInfo->stbName, "meters", TSDB_TABLE_NAME_LEN);
    stbInfo->childTblPrefix = DEFAULT_TB_PREFIX;
    stbInfo->escape_character = 0;
    stbInfo->use_metric = 1;
    stbInfo->columns = benchArrayInit(3, sizeof(Field));
    Field* c1 = benchArrayGet(stbInfo->columns, 0);
    Field* c2 = benchArrayGet(stbInfo->columns, 1);
    Field* c3 = benchArrayGet(stbInfo->columns, 2);
    setField(c1, TSDB_DATA_TYPE_FLOAT, sizeof(float), "current", 9, 10);
    setField(c2, TSDB_DATA_TYPE_INT, sizeof(int32_t), "voltage", 110, 119);
    setField(c3, TSDB_DATA_TYPE_FLOAT, sizeof(float), "phase", 115, 125);
    stbInfo->tags = benchArrayInit(2, sizeof(Field));
    Field* t1 = benchArrayGet(stbInfo->tags, 0);
    Field* t2 = benchArrayGet(stbInfo->tags, 1);
    setField(t1, TSDB_DATA_TYPE_INT, sizeof(int32_t), "groupid", 1, 10);
    setField(t2, TSDB_DATA_TYPE_BINARY, 16, "location", 0, 0);
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
    stbInfo->file_factor = -1;
    stbInfo->delay = -1;
}

static void init_database() {
    g_arguments->db = benchArrayInit(1, sizeof(SDataBase));
    SDataBase *database = benchArrayGet(g_arguments->db, 0);
    tstrncpy(database->dbName, DEFAULT_DATABASE, TSDB_DB_NAME_LEN);
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
    g_arguments->pool = benchArrayInit(DEFAULT_NTHREADS, sizeof(TAOS));
    g_arguments->test_mode = INSERT_TEST;
    g_arguments->demo_mode = 1;
    g_arguments->port = DEFAULT_PORT;
    g_arguments->telnet_tcp_port = TELNET_TCP_PORT;
    tstrncpy(g_arguments->user, TSDB_DEFAULT_USER, NAME_MAX);
    tstrncpy(g_arguments->password, TSDB_DEFAULT_PASS, TSDB_PASS_LEN);
    g_arguments->answer_yes = 1;
    g_arguments->debug_print = 0;
    g_arguments->performance_print = 0;
    g_arguments->nthreads = DEFAULT_NTHREADS;
    g_arguments->table_threads = DEFAULT_NTHREADS;
    g_arguments->connection_pool = DEFAULT_NTHREADS;
    g_arguments->binwidth = DEFAULT_BINWIDTH;
    g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    g_arguments->g_totalChildTables = DEFAULT_CHILDTABLES;
    g_arguments->g_actualChildTables = 0;
    g_arguments->g_autoCreatedChildTables = 0;
    g_arguments->g_existedChildTables = 0;
    g_arguments->chinese = 0;
    g_arguments->aggr_func = 0;
    init_database();
    init_stable();
}

void modify_argument() {
    SDataBase *db = benchArrayGet(g_arguments->db, 0);
    SSuperTable *superTable = benchArrayGet(db->superTbls, 0);
    if (init_taos_list()) exit(EXIT_FAILURE);

    if (superTable->iface == STMT_IFACE) {
        if (g_arguments->reqPerReq > INT16_MAX) {
            g_arguments->reqPerReq = INT16_MAX;
        }
        if (g_arguments->prepared_rand > g_arguments->reqPerReq) {
            g_arguments->prepared_rand = g_arguments->reqPerReq;
        }
    }
    BArray * cols = superTable->columns;
    BArray * tags = superTable->tags;
    for (int i = 0; i < cols->size; ++i) {
        Field * col = benchArrayGet(cols, i);
        if (col->length == 0) {
            col->length = g_arguments->binwidth;
        }
    }
    for (int i = 0; i < tags->size; ++i) {
        Field * tag = benchArrayGet(tags, i);
        if (tag->length == 0) {
            tag->length = g_arguments->binwidth;
        }
    }

    if (g_arguments->intColumnCount > cols->size) {
        for (int i = cols->size;
             i < g_arguments->intColumnCount; ++i) {
            Field* intCol = benchCalloc(1, sizeof(Field), true);
            char tmpName[TSDB_COL_NAME_LEN] = {0};
            snprintf(tmpName, TSDB_COL_NAME_LEN, "c%d", i);
            setField(intCol, TSDB_DATA_TYPE_INT, sizeof(int32_t), tmpName,
                     get_min_from_data_type(TSDB_DATA_TYPE_INT), get_max_from_data_type(TSDB_DATA_TYPE_INT));
            benchArrayPush(cols, intCol);
        }
    }
}

static void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
#ifdef LINUX
    prctl(PR_SET_NAME, "queryStableAggrFunc");
#endif
    char *command = benchCalloc(1, BUFFER_SIZE, false);
    FILE *  fp = g_arguments->fpOfInsertResult;
    SDataBase * db = benchArrayGet(g_arguments->db, 0);
    SSuperTable * stbInfo = benchArrayGet(db->superTbls, 0);
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

        int64_t m = 10 < stbInfo->childTblCount
                        ? 10
                        : stbInfo->childTblCount;

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
    TAOS *      taos = pThreadInfo->taos;
#ifdef LINUX
    prctl(PR_SET_NAME, "queryNtableAggrFunc");
#endif
    char *  command = benchCalloc(1, BUFFER_SIZE, false);
    FILE *  fp = g_arguments->fpOfInsertResult;
    SDataBase * db = benchArrayGet(g_arguments->db, 0);
    SSuperTable * stbInfo = benchArrayGet(db->superTbls, 0);
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
        for (int64_t i = 0; i < stbInfo->childTblCount;
             i++) {
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
                    aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j],
                    totalData,
                    (double)(stbInfo->childTblCount * stbInfo->insertRows) /
                        totalT,
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
    threadInfo *pThreadInfo = benchCalloc(1, sizeof(threadInfo), true);
    SDataBase * db = benchArrayGet(g_arguments->db, 0);
    SSuperTable * stbInfo = benchArrayGet(db->superTbls, 0);
    pThreadInfo->taos = select_one_from_pool(db->dbName);
    if (stbInfo->use_metric) {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    free(pThreadInfo);
}
