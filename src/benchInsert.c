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

static void printStatPerThread(threadInfo *pThreadInfo) {
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;

    infoPrint(stdout,
              "thread[%d] completed total inserted rows: %" PRIu64
              ", total affected rows: %" PRIu64 ". %.2f records/second\n",
              pThreadInfo->threadID, pThreadInfo->totalInsertRows,
              pThreadInfo->totalAffectedRows,
              (double)(pThreadInfo->totalAffectedRows /
                       ((double)pThreadInfo->totalDelay / 1000000.0)));
}

static int getSuperTableFromServer(int db_index, int stb_index) {
    char         command[SQL_BUFF_LEN] = "\0";
    TAOS_RES *   res;
    TAOS_ROW     row = NULL;
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    TAOS *       taos = select_one_from_pool(NULL);
    snprintf(command, SQL_BUFF_LEN, "describe %s.`%s`", database->dbName,
             stbInfo->stbName);
    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        debugPrint(stdout, "failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        infoPrint(stdout, "stable %s does not exist, will create one\n",
                  stbInfo->stbName);
        taos_free_result(res);
        return -1;
    }
    infoPrint(stdout, "find stable<%s>, will get meta data from server\n",
              stbInfo->stbName);
    int tagIndex = 0;
    int columnIndex = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "tag",
                        strlen("tag")) == 0) {
            tagIndex++;
        } else {
            columnIndex++;
        }
    }
    int32_t *tmp_length_list = benchCalloc(columnIndex, sizeof(int32_t), true);
    columnIndex -= 1;
    if (columnIndex == stbInfo->columnCount) {
        for (int i = 0; i < stbInfo->columnCount; ++i) {
            if (stbInfo->columns[i].length == 0) {
                tmp_length_list[i] = 0;
            } else {
                tmp_length_list[i] = 1;
            }
        }
    } else {
        for (int i = 0; i < columnIndex; ++i) {
            tmp_length_list[i] = 1;
        }
    }
    stbInfo->tagCount = tagIndex;
    tmfree(stbInfo->tags);
    stbInfo->tags = benchCalloc(tagIndex, sizeof(Column), true);
    tmfree(stbInfo->columns);
    stbInfo->columnCount = columnIndex;
    stbInfo->columns = benchCalloc(columnIndex, sizeof(Column), true);
    infoPrint(stdout, "stable<%s> with %u columns and %u tags\n",
              stbInfo->stbName, stbInfo->tagCount, stbInfo->columnCount);
    taos_free_result(res);
    res = taos_query(taos, command);
    code = taos_errno(res);
    if (code != 0) {
        errorPrint(stderr, "failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        taos_free_result(res);
        tmfree(tmp_length_list);
        return -1;
    }
    tagIndex = 0;
    columnIndex = 0;
    int count = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (count == 0) {
            count++;
            continue;
        }
        int32_t *lengths = taos_fetch_lengths(res);
        if (lengths == NULL) {
            errorPrint(stderr, "%s", "failed to execute taos_fetch_length\n");
            tmfree(tmp_length_list);
            taos_free_result(res);
            return -1;
        }
        if (strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "tag",
                        strlen("tag")) == 0) {
            stbInfo->tags[tagIndex].type = taos_convert_string_to_datatype(
                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            stbInfo->tags[tagIndex].length =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            stbInfo->tags[tagIndex].max = RAND_MAX >> 1;
            stbInfo->tags[tagIndex].min = (RAND_MAX >> 1) * -1;

            tagIndex++;
        } else {
            stbInfo->columns[columnIndex].type =
                taos_convert_string_to_datatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            if (tmp_length_list[columnIndex] != 0) {
                stbInfo->columns[columnIndex].length =
                    *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            } else {
                stbInfo->columns[columnIndex].length = 0;
            }
            stbInfo->columns[columnIndex].max = RAND_MAX >> 1;
            stbInfo->columns[columnIndex].min = (RAND_MAX >> 1) * -1;
            tstrncpy(stbInfo->columns[columnIndex].name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     strlen((char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX]) + 1);
            columnIndex++;
        }
    }
    tmfree(tmp_length_list);
    taos_free_result(res);
    return 0;
}

static int createSuperTable(int db_index, int stb_index) {
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    TAOS *       taos = select_one_from_pool(NULL);
    uint32_t col_buffer_len = (TSDB_COL_NAME_LEN + 15) * stbInfo->columnCount;
    char         *cols = benchCalloc(1, col_buffer_len, false);
    char*         command = benchCalloc(1, BUFFER_SIZE, false);
    int          len = 0;

    for (int colIndex = 0; colIndex < stbInfo->columnCount; colIndex++) {
        if (stbInfo->columns[colIndex].type == TSDB_DATA_TYPE_BINARY ||
                stbInfo->columns[colIndex].type == TSDB_DATA_TYPE_NCHAR){
            len += snprintf(cols + len, col_buffer_len - len, ",%s %s(%d)",
                            stbInfo->columns[colIndex].name,
                            taos_convert_datatype_to_string(stbInfo->columns[colIndex].type),
                            stbInfo->columns[colIndex].length);
        } else {
            len += snprintf(cols + len, col_buffer_len - len, ",%s %s",
                            stbInfo->columns[colIndex].name,
                            taos_convert_datatype_to_string(stbInfo->columns[colIndex].type));
        }
    }

    // save for creating child table
    stbInfo->colsOfCreateChildTable =
        (char *)benchCalloc(len + TIMESTAMP_BUFF_LEN, 1, true);

    snprintf(stbInfo->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(ts timestamp%s)", cols);

    if (stbInfo->tagCount == 0) {
        free(cols);
        free(command);
        return 0;
    }

    uint32_t tag_buffer_len = (TSDB_COL_NAME_LEN + 15) * stbInfo->tagCount;
    char *tags = benchCalloc(1, tag_buffer_len, false);
    int  tagIndex;
    len = 0;

    len += snprintf(tags + len, tag_buffer_len - len, "(");
    for (tagIndex = 0; tagIndex < stbInfo->tagCount; tagIndex++) {
        if (stbInfo->tags[tagIndex].type == TSDB_DATA_TYPE_BINARY ||
                stbInfo->tags[tagIndex].type == TSDB_DATA_TYPE_NCHAR) {
            len += snprintf(tags + len, tag_buffer_len - len,
                            "%s %s(%d),", stbInfo->tags[tagIndex].name,
                            taos_convert_datatype_to_string(stbInfo->tags[tagIndex].type),
                            stbInfo->tags[tagIndex].length);
        } else if (stbInfo->tags[tagIndex].type == TSDB_DATA_TYPE_JSON) {
            len += snprintf(tags + len, tag_buffer_len - len, "%s json",
                            stbInfo->tags[tagIndex].name);
            goto skip;
        } else {
            len += snprintf(tags + len, tag_buffer_len - len, "%s %s,",
                            stbInfo->tags[tagIndex].name,
                            taos_convert_datatype_to_string(stbInfo->tags[tagIndex].type));
        }
    }
    len -= 1;
skip:
    len += snprintf(tags + len, tag_buffer_len - len, ")");

    int length = snprintf(
        command, BUFFER_SIZE,
        stbInfo->escape_character
            ? "CREATE TABLE IF NOT EXISTS %s.`%s` (ts TIMESTAMP%s) TAGS %s"
            : "CREATE TABLE IF NOT EXISTS %s.%s (ts TIMESTAMP%s) TAGS %s",
        database->dbName, stbInfo->stbName, cols, tags);
    if (stbInfo->comment != NULL) {
        length += snprintf(command + length, BUFFER_SIZE - length,
                           " COMMENT '%s'", stbInfo->comment);
    }
    if (stbInfo->delay >= 0) {
        length += snprintf(command + length, BUFFER_SIZE - length, " DELAY %d",
                           stbInfo->delay);
    }
    if (stbInfo->file_factor >= 0) {
        length +=
            snprintf(command + length, BUFFER_SIZE - length, " FILE_FACTOR %f",
                     (float)stbInfo->file_factor / 100);
    }
    if (stbInfo->rollup != NULL) {
        length += snprintf(command + length, BUFFER_SIZE - length,
                           " ROLLUP(%s)", stbInfo->rollup);
    }
    bool first_sma = true;
    for (int i = 0; i < stbInfo->columnCount; ++i) {
        if (stbInfo->columns[i].sma) {
            if (first_sma) {
                length += snprintf(command + length, BUFFER_SIZE - length,
                                   " SMA(%s", stbInfo->columns[i].name);
                first_sma = false;
            } else {
                length += snprintf(command + length, BUFFER_SIZE - length,
                                   ",%s", stbInfo->columns[i].name);
            }
        }
    }
    if (!first_sma) {
        sprintf(command + length, ")");
    }
    infoPrint(stdout, "create stable: <%s>\n", command);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint(stderr, "create supertable %s failed!\n\n",
                   stbInfo->stbName);
        free(command);
        free(cols);
        free(tags);
        return -1;
    }
    free(command);
    free(cols);
    free(tags);
    return 0;
}

int createDatabase(int db_index) {
    char       command[SQL_BUFF_LEN] = "\0";
    SDataBase *database = &(g_arguments->db[db_index]);
    TAOS *     taos = NULL;
    taos = select_one_from_pool(NULL);
    sprintf(command, "drop database if exists %s;", database->dbName);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        return -1;
    }

    int dataLen = 0;
    dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                        "CREATE DATABASE IF NOT EXISTS %s", database->dbName);

    if (database->dbCfg.blocks >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " BLOCKS %d", database->dbCfg.blocks);
    }
    if (database->dbCfg.cache >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " CACHE %d", database->dbCfg.cache);
    }
    if (database->dbCfg.days >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " DAYS %d", database->dbCfg.days);
    }
    if (database->dbCfg.keep >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " KEEP %d", database->dbCfg.keep);
    }
    if (database->dbCfg.quorum > 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " QUORUM %d", database->dbCfg.quorum);
    }
    if (database->dbCfg.replica > 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " REPLICA %d", database->dbCfg.replica);
    }
    if (database->dbCfg.update >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " UPDATE %d", database->dbCfg.update);
    }
    if (database->dbCfg.minRows >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " MINROWS %d", database->dbCfg.minRows);
    }
    if (database->dbCfg.maxRows >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " MAXROWS %d", database->dbCfg.maxRows);
    }
    if (database->dbCfg.comp >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " COMP %d", database->dbCfg.comp);
    }
    if (database->dbCfg.walLevel >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " wal %d",
                            database->dbCfg.walLevel);
    }
    if (database->dbCfg.cacheLast >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " CACHELAST %d", database->dbCfg.cacheLast);
    }
    if (database->dbCfg.fsync >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " FSYNC %d", database->dbCfg.fsync);
    }
    if (database->dbCfg.buffer >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " BUFFER %d", database->dbCfg.buffer);
    }
    if (database->dbCfg.strict >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " STRICT %d", database->dbCfg.strict);
    }
    if (database->dbCfg.page_size >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " PAGESIZE %d", database->dbCfg.page_size);
    }
    if (database->dbCfg.pages >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " PAGES %d", database->dbCfg.pages);
    }
    if (database->dbCfg.vgroups >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " VGROUPS %d", database->dbCfg.vgroups);
    }
    if (database->dbCfg.single_stable >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " SINGLE_STABLE %d", database->dbCfg.single_stable);
    }
    if (database->dbCfg.retentions != NULL) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " RETENTIONS %s", database->dbCfg.retentions);
    }
    switch (database->dbCfg.precision) {
        case TSDB_TIME_PRECISION_MILLI:
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " precision \'ms\';");
            break;
        case TSDB_TIME_PRECISION_MICRO:
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " precision \'us\';");
            break;
        case TSDB_TIME_PRECISION_NANO:
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " precision \'ns\';");
            break;
    }

    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint(stderr, "\ncreate database %s failed!\n\n",
                   database->dbName);
        return -1;
    }
    infoPrint(stdout, "create database: <%s>\n", command);
#ifdef LINUX
    sleep(2);
#elif defined(DARWIN)
    sleep(2);
#else
    Sleep(2);
#endif
    return 0;
}

static void *createTable(void *sarg) {
    int32_t *code = benchCalloc(1, sizeof(int32_t), false);
    *code = -1;
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
#ifdef LINUX
    prctl(PR_SET_NAME, "createTable");
#endif
    uint64_t lastPrintTime = toolsGetTimestampMs();
    pThreadInfo->buffer = benchCalloc(1, TSDB_MAX_SQL_LEN, false);
    int len = 0;
    int batchNum = 0;
    infoPrint(stdout,
              "thread[%d] start creating table from %" PRIu64 " to %" PRIu64
              "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        if (g_arguments->terminate) {
            goto create_table_end;
        }
        if (!stbInfo->use_metric) {
            snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                     stbInfo->escape_character
                         ? "CREATE TABLE IF NOT EXISTS %s.`%s%" PRIu64 "` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s%" PRIu64 " %s;",
                     database->dbName, stbInfo->childTblPrefix, i,
                     stbInfo->colsOfCreateChildTable);
            batchNum++;
        } else {
            if (0 == len) {
                batchNum = 0;
                memset(pThreadInfo->buffer, 0, TSDB_MAX_SQL_LEN);
                len += snprintf(pThreadInfo->buffer + len,
                                TSDB_MAX_SQL_LEN - len, "CREATE TABLE ");
            }

            len += snprintf(
                pThreadInfo->buffer + len, TSDB_MAX_SQL_LEN - len,
                stbInfo->escape_character ? "if not exists %s.`%s%" PRIu64
                                            "` using %s.`%s` tags (%s) "
                                          : "if not exists %s.%s%" PRIu64
                                            " using %s.%s tags (%s) ",
                database->dbName, stbInfo->childTblPrefix, i, database->dbName,
                stbInfo->stbName, stbInfo->tagDataBuf + i * stbInfo->lenOfTags);
            batchNum++;
            if ((batchNum < stbInfo->batchCreateTableNum) &&
                ((TSDB_MAX_SQL_LEN - len) >=
                 (stbInfo->lenOfTags + EXTRA_SQL_LEN))) {
                continue;
            }
        }

        len = 0;

        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        batchNum = 0;
        uint64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            debugPrint(stdout,
                       "thread[%d] already created %" PRId64 " tables\n",
                       pThreadInfo->threadID, pThreadInfo->tables_created);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        debugPrint(stdout, "thread[%d] already created %" PRId64 " tables\n",
                   pThreadInfo->threadID, pThreadInfo->tables_created);
    }
    *code = 0;
create_table_end:
    tmfree(pThreadInfo->buffer);
    return code;
}

static int startMultiThreadCreateChildTable(int db_index, int stb_index) {
    int          threads = g_arguments->table_threads;
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    int64_t      ntables = stbInfo->childTblCount;
    pthread_t *  pids = benchCalloc(1, threads * sizeof(pthread_t), false);
    threadInfo * infos = benchCalloc(1, threads * sizeof(threadInfo), false);
    uint64_t     tableFrom = 0;
    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }

    int64_t b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
#ifdef LINUX
        pThreadInfo->threadID = (pthread_t)i;
#else
        pThreadInfo->threadID = (uint32_t)i;
#endif

        pThreadInfo->stb_index = stb_index;
        pThreadInfo->db_index = db_index;
        pThreadInfo->taos = select_one_from_pool(database->dbName);
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->tables_created = 0;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
    }

    for (int i = 0; i < threads; i++) {
        void *result;
        pthread_join(pids[i], &result);
        if (*(int32_t *)result) {
            g_fail = true;
        }
        tmfree(result);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        g_arguments->g_actualChildTables += pThreadInfo->tables_created;
    }

    free(pids);
    free(infos);
    if (g_fail) {
        return -1;
    }
    return 0;
}

static int createChildTables() {
    int32_t    code;
    SDataBase *database = g_arguments->db;
    infoPrint(stdout, "start creating %" PRId64 " table(s) with %d thread(s)\n",
              g_arguments->g_totalChildTables, g_arguments->table_threads);
    if (g_arguments->fpOfInsertResult) {
        infoPrint(g_arguments->fpOfInsertResult,
                  "start creating %" PRId64 " table(s) with %d thread(s)\n",
                  g_arguments->g_totalChildTables, g_arguments->table_threads);
    }
    double start = (double)toolsGetTimestampMs();

    for (int i = 0; i < g_arguments->dbCount; i++) {
        for (int j = 0; j < database[i].superTblCount; j++) {
            if (database[i].superTbls[j].autoCreateTable ||
                database[i].superTbls[j].iface == SML_IFACE ||
                database[i].superTbls[j].iface == SML_REST_IFACE) {
                g_arguments->g_autoCreatedChildTables +=
                    database[i].superTbls[j].childTblCount;
                continue;
            }
            if (database[i].superTbls[j].childTblExists) {
                g_arguments->g_existedChildTables +=
                    database[i].superTbls[j].childTblCount;
                continue;
            }
            debugPrint(stdout, "colsOfCreateChildTable: %s\n",
                       database[i].superTbls[j].colsOfCreateChildTable);

            code = startMultiThreadCreateChildTable(i, j);
            if (code && !g_arguments->terminate) {
                errorPrint(stderr,
                           "startMultiThreadCreateChildTable() "
                           "failed for db %d stable %d\n",
                           i, j);
                return code;
            }
        }
    }

    double end = (double)toolsGetTimestampMs();
    infoPrint(stdout,
              "Spent %.4f seconds to create %" PRId64
              " table(s) with %d thread(s), already exist %" PRId64
              " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
              " table(s) will be auto created\n",
              (end - start) / 1000.0, g_arguments->g_totalChildTables,
              g_arguments->table_threads, g_arguments->g_existedChildTables,
              g_arguments->g_actualChildTables,
              g_arguments->g_autoCreatedChildTables);
    if (g_arguments->fpOfInsertResult) {
        fprintf(g_arguments->fpOfInsertResult,
                "Spent %.4f seconds to create %" PRId64
                " table(s) with %d thread(s), already exist %" PRId64
                " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
                " table(s) will be auto created\n",
                (end - start) / 1000.0, g_arguments->g_totalChildTables,
                g_arguments->table_threads, g_arguments->g_existedChildTables,
                g_arguments->g_actualChildTables,
                g_arguments->g_autoCreatedChildTables);
    }
    return 0;
}

void postFreeResource() {
    tmfree(g_arguments->base64_buf);
    tmfclose(g_arguments->fpOfInsertResult);
    SDataBase *database = g_arguments->db;
    for (int i = 0; i < g_arguments->dbCount; i++) {
        for (uint64_t j = 0; j < database[i].superTblCount; j++) {
            tmfree(database[i].superTbls[j].colsOfCreateChildTable);
            tmfree(database[i].superTbls[j].sampleDataBuf);
            tmfree(database[i].superTbls[j].tagDataBuf);
            tmfree(database[i].superTbls[j].partialColumnNameBuf);
            for (int k = 0; k < database[i].superTbls[j].tagCount; ++k) {
                tmfree(database[i].superTbls[j].tags[k].data);
            }
            tmfree(database[i].superTbls[j].tags);

            for (int k = 0; k < database[i].superTbls[j].columnCount; ++k) {
                tmfree(database[i].superTbls[j].columns[k].data);
            }
            tmfree(database[i].superTbls[j].columns);
            if (g_arguments->test_mode == INSERT_TEST &&
                database[i].superTbls[j].insertRows != 0) {
                for (int64_t k = 0; k < database[i].superTbls[j].childTblCount;
                     ++k) {
                    tmfree(database[i].superTbls[j].childTblName[k]);
                }
            }
            tmfree(database[i].superTbls[j].childTblName);
        }
        tmfree(database[i].superTbls);
    }
    tmfree(database);
    cJSON_Delete(root);
    cleanup_taos_list();
    tmfree(g_arguments->pool);
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    int32_t      affectedRows = 0;
    TAOS_RES *   res = NULL;
    int32_t      code;
    uint16_t     iface = stbInfo->iface;

    switch (iface) {
        case TAOSC_IFACE:

            affectedRows = queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                                       INSERT_TYPE, false);
            break;

        case REST_IFACE:

            if (0 != postProceSql(pThreadInfo->buffer, pThreadInfo)) {
                affectedRows = -1;
            } else {
                affectedRows = k;
            }
            break;

        case STMT_IFACE:
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint(stderr,
                           "failed to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->stmt));
                affectedRows = -1;
            } else {
                affectedRows = taos_stmt_affected_rows(pThreadInfo->stmt);
            }
            break;
        case SML_IFACE:
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(pThreadInfo->json_array);
            }
            res = taos_schemaless_insert(
                pThreadInfo->taos, pThreadInfo->lines,
                stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                stbInfo->lineProtocol,
                stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL
                    ? database->dbCfg.sml_precision
                    : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            code = taos_errno(res);
            affectedRows = taos_affected_rows(res);
            if (code != TSDB_CODE_SUCCESS) {
                errorPrint(
                    stderr,
                    "failed to execute schemaless insert. content: %s, reason: "
                    "%s\n",
                    pThreadInfo->lines[0], taos_errstr(res));
                affectedRows = -1;
            }
            break;
        case SML_REST_IFACE: {
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(pThreadInfo->json_array);
                if (0 != postProceSql(pThreadInfo->lines[0], pThreadInfo)) {
                    affectedRows = -1;
                } else {
                    affectedRows = k;
                }
            } else {
                int len = 0;
                for (int i = 0; i < k; ++i) {
                    if (strlen(pThreadInfo->lines[i]) != 0) {
                        if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL &&
                            stbInfo->tcpTransfer) {
                            len += sprintf(pThreadInfo->buffer + len,
                                           "put %s\n", pThreadInfo->lines[i]);
                        } else {
                            len += sprintf(pThreadInfo->buffer + len, "%s\n",
                                           pThreadInfo->lines[i]);
                        }
                    } else {
                        break;
                    }
                }
                if (0 != postProceSql(pThreadInfo->buffer, pThreadInfo)) {
                    affectedRows = -1;
                } else {
                    affectedRows = k;
                }
            }
            break;
        }
    }
    return affectedRows;
}

static void *syncWriteInterlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    infoPrint(stdout,
              "thread[%d] start interlace inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    int64_t pos = 0;

    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;

    uint64_t   lastPrintTime = toolsGetTimestampMs();
    uint64_t   startTs = toolsGetTimestampMs();
    uint64_t   endTs = toolsGetTimestampMs();
    delayNode *current_delay_node;
    int32_t    generated = 0;
    int        len = 0;
    uint64_t   tableSeq = pThreadInfo->start_table_from;
    while (insertRows > 0) {
        generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }
        for (int i = 0; i < batchPerTblTimes; ++i) {
            if (g_arguments->terminate) {
                goto free_of_interlace;
            }
            int64_t timestamp = pThreadInfo->start_time;
            char *  tableName = stbInfo->childTblName[tableSeq];
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE: {
                    if (i == 0) {
                        len = snprintf(pThreadInfo->buffer,
                                       strlen(STR_INSERT_INTO) + 1, "%s",
                                       STR_INSERT_INTO);
                    }
                    if (stbInfo->partialColumnNum == stbInfo->columnCount) {
                        if (stbInfo->autoCreateTable) {
                            len += snprintf(
                                pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s.%s using `%s` tags (%s) values ",
                                database->dbName, tableName, stbInfo->stbName,
                                stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq);
                        } else {
                            len += snprintf(pThreadInfo->buffer + len,
                                            pThreadInfo->max_sql_len - len,
                                            "%s.%s values", database->dbName,
                                            tableName);
                        }
                    } else {
                        if (stbInfo->autoCreateTable) {
                            len += snprintf(
                                pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s.%s (%s) using `%s` tags (%s) values ",
                                database->dbName, tableName,
                                stbInfo->partialColumnNameBuf, stbInfo->stbName,
                                stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq);
                        } else {
                            len += snprintf(pThreadInfo->buffer + len,
                                            pThreadInfo->max_sql_len - len,
                                            "%s.%s (%s) values",
                                            database->dbName, tableName,
                                            stbInfo->partialColumnNameBuf);
                        }
                    }

                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        len += snprintf(
                            pThreadInfo->buffer + len,
                            pThreadInfo->max_sql_len - len, "(%" PRId64 ",%s)",
                            timestamp,
                            stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols);
                        generated++;
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (taos_stmt_set_tbname(pThreadInfo->stmt, tableName)) {
                        errorPrint(
                            stderr,
                            "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                            tableName, taos_stmt_errstr(pThreadInfo->stmt));
                        g_fail = true;
                        goto free_of_interlace;
                    }
                    generated =
                        bindParamBatch(pThreadInfo, interlaceRows, timestamp);
                    break;
                }
                case SML_REST_IFACE:
                case SML_IFACE: {
                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                            cJSON *tag = cJSON_Duplicate(
                                cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->dbCfg.sml_precision, timestamp);
                        } else if (stbInfo->lineProtocol ==
                                   TSDB_SML_LINE_PROTOCOL) {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %s %" PRId64 "",
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                timestamp);
                        } else {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                timestamp,
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                        }
                        generated++;
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                    }
                    break;
                }
            }
            tableSeq++;
            pThreadInfo->totalInsertRows += interlaceRows;
            if (tableSeq > pThreadInfo->end_table_to) {
                tableSeq = pThreadInfo->start_table_from;
                pThreadInfo->start_time +=
                    interlaceRows * stbInfo->timestamp_step;
                if (!stbInfo->non_stop) {
                    insertRows -= interlaceRows;
                }
                if (stbInfo->insert_interval > 0) {
                    performancePrint(stdout, "sleep %" PRIu64 " ms\n",
                                     stbInfo->insert_interval);
                    taosMsleep((int32_t)stbInfo->insert_interval);
                }
                break;
            }
        }

        startTs = toolsGetTimestampUs();

        int64_t affectedRows = execInsert(pThreadInfo, generated);

        endTs = toolsGetTimestampUs();
        switch (stbInfo->iface) {
            case TAOSC_IFACE:
            case REST_IFACE:
                debugPrint(stdout, "pThreadInfo->buffer: %s\n",
                           pThreadInfo->buffer);
                memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                pThreadInfo->totalAffectedRows += affectedRows;
                break;
            case SML_REST_IFACE:
                memset(pThreadInfo->buffer, 0,
                       g_arguments->reqPerReq * (pThreadInfo->max_sql_len + 1));
            case SML_IFACE:
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    debugPrint(stdout, "pThreadInfo->lines[0]: %s\n",
                               pThreadInfo->lines[0]);
                    cJSON_Delete(pThreadInfo->json_array);
                    pThreadInfo->json_array = cJSON_CreateArray();
                    tmfree(pThreadInfo->lines[0]);
                } else {
                    for (int j = 0; j < generated; ++j) {
                        debugPrint(stdout, "pThreadInfo->lines[%d]: %s\n", j,
                                   pThreadInfo->lines[j]);
                        memset(pThreadInfo->lines[j], 0,
                               pThreadInfo->max_sql_len);
                    }
                }
                pThreadInfo->totalAffectedRows += affectedRows;
                break;
            case STMT_IFACE:
                pThreadInfo->totalAffectedRows = affectedRows;
                break;
        }
        if (affectedRows < 0) {
            g_fail = true;
            goto free_of_interlace;
        }
        uint64_t delay = endTs - startTs;
        performancePrint(stdout, "insert execution time is %10.2f ms\n",
                         delay / 1000.0);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        current_delay_node = benchCalloc(1, sizeof(delayNode), false);
        current_delay_node->value = delay;
        if (pThreadInfo->delayList.size == 0) {
            pThreadInfo->delayList.head = current_delay_node;
            pThreadInfo->delayList.tail = current_delay_node;
            pThreadInfo->delayList.size++;
        } else {
            pThreadInfo->delayList.tail->next = current_delay_node;
            pThreadInfo->delayList.tail = current_delay_node;
            pThreadInfo->delayList.size++;
        }
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            infoPrint(stdout,
                      "thread[%d] has currently inserted rows: %" PRIu64
                      ", affected rows: %" PRIu64 "\n",
                      pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                      pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }
        debugPrint(stdout,
                   "thread[%d] has currently inserted rows: %" PRIu64
                   ", affected rows: %" PRIu64 "\n",
                   pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
    }
free_of_interlace:
    printStatPerThread(pThreadInfo);
    return NULL;
}

void *syncWriteProgressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    infoPrint(stdout,
              "thread[%d] start progressive inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    uint64_t   startTs = toolsGetTimestampMs();
    uint64_t   endTs;
    delayNode *current_delay_node;

    char *  pstr = pThreadInfo->buffer;
    int32_t pos = 0;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *   tableName = stbInfo->childTblName[tableSeq];
        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;
        if (stbInfo->iface == STMT_IFACE && stbInfo->autoCreateTable) {
            taos_stmt_close(pThreadInfo->stmt);
            pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
            if (stmt_prepare(stbInfo, pThreadInfo->stmt, tableSeq)) {
                g_fail = true;
                goto free_of_progressive;
            }
        }

        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            int32_t generated = 0;
            switch (stbInfo->iface) {
                case TAOSC_IFACE:
                case REST_IFACE: {
                    if (stbInfo->partialColumnNum == stbInfo->columnCount) {
                        if (stbInfo->autoCreateTable) {
                            len =
                                snprintf(pstr, MAX_SQL_LEN,
                                         "%s %s.%s using %s tags (%s) values ",
                                         STR_INSERT_INTO, database->dbName,
                                         tableName, stbInfo->stbName,
                                         stbInfo->tagDataBuf +
                                             stbInfo->lenOfTags * tableSeq);
                        } else {
                            len = snprintf(pstr, MAX_SQL_LEN,
                                           "%s %s.%s values ", STR_INSERT_INTO,
                                           database->dbName, tableName);
                        }
                    } else {
                        if (stbInfo->autoCreateTable) {
                            len = snprintf(
                                pstr, MAX_SQL_LEN,
                                "%s %s.%s (%s) using %s tags (%s) values ",
                                STR_INSERT_INTO, database->dbName, tableName,
                                stbInfo->partialColumnNameBuf, stbInfo->stbName,
                                stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq);
                        } else {
                            len = snprintf(pstr, MAX_SQL_LEN,
                                           "%s %s.%s (%s) values ",
                                           STR_INSERT_INTO, database->dbName,
                                           tableName,
                                           stbInfo->partialColumnNameBuf);
                        }
                    }

                    for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                        if (stbInfo && stbInfo->useSampleTs &&
                            !stbInfo->random_data_source) {
                            len +=
                                snprintf(pstr + len,
                                         MAX_SQL_LEN - len, "(%s)",
                                         stbInfo->sampleDataBuf +
                                             pos * stbInfo->lenOfCols);
                        } else {
                            len += snprintf(pstr + len,
                                            MAX_SQL_LEN - len,
                                            "(%" PRId64 ",%s)", timestamp,
                                            stbInfo->sampleDataBuf +
                                                pos * stbInfo->lenOfCols);
                        }
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                        generated++;
                        if (len > (MAX_SQL_LEN - stbInfo->lenOfCols)) {
                            break;
                        }
                        if (i + generated >= stbInfo->insertRows) {
                            break;
                        }
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (taos_stmt_set_tbname(pThreadInfo->stmt, tableName)) {
                        errorPrint(
                            stderr,
                            "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                            tableName, taos_stmt_errstr(pThreadInfo->stmt));
                        g_fail = true;
                        goto free_of_progressive;
                    }
                    generated = bindParamBatch(
                        pThreadInfo,
                        (g_arguments->reqPerReq > (stbInfo->insertRows - i))
                            ? (stbInfo->insertRows - i)
                            : g_arguments->reqPerReq,
                        timestamp);
                    timestamp += generated * stbInfo->timestamp_step;
                    break;
                }
                case SML_REST_IFACE:
                case SML_IFACE: {
                    for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                            cJSON *tag = cJSON_Duplicate(
                                cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->dbCfg.sml_precision, timestamp);
                        } else if (stbInfo->lineProtocol ==
                                   TSDB_SML_LINE_PROTOCOL) {
                            snprintf(
                                pThreadInfo->lines[j],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %s %" PRId64 "",
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                timestamp);
                        } else {
                            snprintf(
                                pThreadInfo->lines[j],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                timestamp,
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                        }
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                        generated++;
                        if (i + generated >= stbInfo->insertRows) {
                            break;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            if (!stbInfo->non_stop) {
                i += generated;
            }
            pThreadInfo->totalInsertRows += generated;
            // only measure insert
            startTs = toolsGetTimestampUs();
            int32_t affectedRows = execInsert(pThreadInfo, generated);
            endTs = toolsGetTimestampUs();
            if (affectedRows < 0) {
                g_fail = true;
                goto free_of_progressive;
            }
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE:
                    memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                    pThreadInfo->totalAffectedRows += affectedRows;
                    break;
                case SML_REST_IFACE:
                    memset(pThreadInfo->buffer, 0,
                           g_arguments->reqPerReq *
                               (pThreadInfo->max_sql_len + 1));
                case SML_IFACE:
                    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                        cJSON_Delete(pThreadInfo->json_array);
                        pThreadInfo->json_array = cJSON_CreateArray();
                        tmfree(pThreadInfo->lines[0]);
                    } else {
                        for (int j = 0; j < generated; ++j) {
                            debugPrint(stdout, "pThreadInfo->lines[%d]: %s\n",
                                       j, pThreadInfo->lines[j]);
                            memset(pThreadInfo->lines[j], 0,
                                   pThreadInfo->max_sql_len);
                        }
                    }
                    pThreadInfo->totalAffectedRows += affectedRows;
                    break;
                case STMT_IFACE:
                    pThreadInfo->totalAffectedRows = affectedRows;
                    break;
            }

            uint64_t delay = endTs - startTs;
            performancePrint(stdout, "insert execution time is %10.f ms\n",
                             delay / 1000.0);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            current_delay_node = benchCalloc(1, sizeof(delayNode), false);
            current_delay_node->value = delay;
            if (pThreadInfo->delayList.size == 0) {
                pThreadInfo->delayList.head = current_delay_node;
                pThreadInfo->delayList.tail = current_delay_node;
                pThreadInfo->delayList.size++;
            } else {
                pThreadInfo->delayList.tail->next = current_delay_node;
                pThreadInfo->delayList.tail = current_delay_node;
                pThreadInfo->delayList.size++;
            }
            pThreadInfo->totalDelay += delay;

            int64_t currentPrintTime = toolsGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(stdout,
                          "thread[%d] has currently inserted rows: "
                          "%" PRId64 ", affected rows: %" PRId64 "\n",
                          pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                          pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }
            debugPrint(stdout,
                       "thread[%d] has currently inserted rows: %" PRId64
                       ", affected rows: %" PRId64 "\n",
                       pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                       pThreadInfo->totalAffectedRows);
            if (i >= stbInfo->insertRows) {
                break;
            }
        }  // insertRows
    }      // tableSeq
free_of_progressive:
    printStatPerThread(pThreadInfo);
    return NULL;
}

static int startMultiThreadInsertData(int db_index, int stb_index) {
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    if ((stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) &&
        !stbInfo->use_metric) {
        errorPrint(stderr, "%s", "schemaless cannot work without stable\n");
        return -1;
    }

    if (stbInfo->interlaceRows > g_arguments->reqPerReq) {
        infoPrint(
            stdout,
            "interlaceRows(%d) is larger than record per request(%u), which "
            "will be set to %u\n",
            stbInfo->interlaceRows, g_arguments->reqPerReq,
            g_arguments->reqPerReq);
        stbInfo->interlaceRows = g_arguments->reqPerReq;
    }

    if (stbInfo->interlaceRows > stbInfo->insertRows) {
        infoPrint(stdout,
                  "interlaceRows larger than insertRows %d > %" PRId64 "\n",
                  stbInfo->interlaceRows, stbInfo->insertRows);
        infoPrint(stdout, "%s", "interlaceRows will be set to 0\n");
        stbInfo->interlaceRows = 0;
    }

    if (stbInfo->interlaceRows == 0 && g_arguments->reqPerReq > stbInfo->insertRows) {
        infoPrint(stdout, "record per request (%u) is larger than insert rows (%"PRIu64")"
                  " in progressive mode, which will be set to %"PRIu64"\n",
                  g_arguments->reqPerReq, stbInfo->insertRows, stbInfo->insertRows);
        g_arguments->reqPerReq = stbInfo->insertRows;
    }

    if (stbInfo->interlaceRows > 0 && stbInfo->iface == STMT_IFACE &&
        stbInfo->autoCreateTable) {
        infoPrint(stdout, "%s",
                  "not support autocreate table with interlace row in stmt "
                  "insertion, will change to progressive mode\n");
        stbInfo->interlaceRows = 0;
    }

    uint64_t tableFrom = 0;
    uint64_t ntables = stbInfo->childTblCount;
    stbInfo->childTblName = benchCalloc(stbInfo->childTblCount, sizeof(char *), true);
    for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
        stbInfo->childTblName[i] = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    }

    if ((stbInfo->iface != SML_IFACE && stbInfo->iface != SML_REST_IFACE) &&
        stbInfo->childTblExists) {
        TAOS *taos = select_one_from_pool(database->dbName);
        if (taos == NULL) {
            return -1;
        }
        char cmd[SQL_BUFF_LEN] = "\0";
        if (stbInfo->escape_character) {
            snprintf(cmd, SQL_BUFF_LEN,
                     "select tbname from %s.`%s` limit %" PRId64
                     " offset %" PRIu64 "",
                     database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                     stbInfo->childTblOffset);
        } else {
            snprintf(cmd, SQL_BUFF_LEN,
                     "select tbname from %s.%s limit %" PRId64
                     " offset %" PRIu64 "",
                     database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                     stbInfo->childTblOffset);
        }
        debugPrint(stdout, "cmd: %s\n", cmd);
        TAOS_RES *res = taos_query(taos, cmd);
        int32_t   code = taos_errno(res);
        int64_t   count = 0;
        if (code) {
            errorPrint(stderr, "failed to get child table name: %s. reason: %s",
                       cmd, taos_errstr(res));
            taos_free_result(res);

            return -1;
        }
        TAOS_ROW row = NULL;
        while ((row = taos_fetch_row(res)) != NULL) {
            int *lengths = taos_fetch_lengths(res);
            if (stbInfo->escape_character) {
                stbInfo->childTblName[count][0] = '`';
                strncpy(stbInfo->childTblName[count] + 1, row[0], lengths[0]);
                stbInfo->childTblName[count][lengths[0] + 1] = '`';
                stbInfo->childTblName[count][lengths[0] + 2] = '\0';
            } else {
                tstrncpy(stbInfo->childTblName[count], row[0], lengths[0] + 1);
            }
            debugPrint(stdout, "stbInfo->childTblName[%" PRId64 "]: %s\n",
                       count, stbInfo->childTblName[count]);
            count++;
        }
        ntables = count;
        taos_free_result(res);
    } else {
        for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
            if (stbInfo->escape_character) {
                snprintf(stbInfo->childTblName[i], TSDB_TABLE_NAME_LEN,
                         "`%s%" PRIu64 "`", stbInfo->childTblPrefix, i);
            } else {
                snprintf(stbInfo->childTblName[i], TSDB_TABLE_NAME_LEN,
                         "%s%" PRIu64 "", stbInfo->childTblPrefix, i);
            }
        }
        ntables = stbInfo->childTblCount;
    }
    int     threads = g_arguments->nthreads;
    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }
    int64_t b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }

    pthread_t * pids = benchCalloc(1, threads * sizeof(pthread_t), true);
    threadInfo *infos = benchCalloc(1, threads * sizeof(threadInfo), true);

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->db_index = db_index;
        pThreadInfo->stb_index = stb_index;
        pThreadInfo->start_time = stbInfo->startTimestamp;
        pThreadInfo->totalInsertRows = 0;
        pThreadInfo->totalAffectedRows = 0;
        pThreadInfo->samplePos = 0;
        pThreadInfo->maxDelay = 0;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        delay_list_init(&(pThreadInfo->delayList));
        switch (stbInfo->iface) {
            case REST_IFACE: {
                pThreadInfo->buffer = benchCalloc(1, MAX_SQL_LEN, false);
#ifdef WINDOWS
                WSADATA wsaData;
                WSAStartup(MAKEWORD(2, 2), &wsaData);
                SOCKET sockfd;
#else
                int sockfd;
#endif
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd < 0) {
#ifdef WINDOWS
                    errorPrint(stderr, "Could not create socket : %d",
                               WSAGetLastError());
#endif
                    debugPrint(stdout, "%s() LN%d, sockfd=%d\n", __func__,
                               __LINE__, sockfd);
                    errorPrint(stderr, "%s\n", "failed to create socket");
                    return -1;
                }

                int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
                if (retConn < 0) {
                    errorPrint(stderr, "%s\n", "failed to connect");
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
                break;
            }
            case STMT_IFACE: {
                pThreadInfo->taos = select_one_from_pool(database->dbName);
                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint(stderr, "taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
                    return -1;
                }
                if (!stbInfo->autoCreateTable) {
                    if (stmt_prepare(stbInfo, pThreadInfo->stmt, 0)) {
                        return -1;
                    }
                }

                pThreadInfo->bind_ts = benchCalloc(1, sizeof(int64_t), true);
                pThreadInfo->bind_ts_array =
                        benchCalloc(1, sizeof(int64_t) * g_arguments->reqPerReq, true);
                pThreadInfo->bindParams = benchCalloc(
                    1, sizeof(TAOS_MULTI_BIND) * (stbInfo->columnCount + 1), true);
                pThreadInfo->is_null = benchCalloc(1, g_arguments->reqPerReq, true);

                break;
            }
            case SML_REST_IFACE: {
#ifdef WINDOWS
                WSADATA wsaData;
                WSAStartup(MAKEWORD(2, 2), &wsaData);
                SOCKET sockfd;
#else
                int sockfd;
#endif
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                debugPrint(stdout, "sockfd=%d\n", sockfd);
                if (sockfd < 0) {
#ifdef WINDOWS
                    errorPrint(stderr, "Could not create socket : %d",
                               WSAGetLastError());
#endif

                    errorPrint(stderr, "%s\n", "failed to create socket");
                    return -1;
                }
                int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
                if (retConn < 0) {
                    errorPrint(stderr, "%s\n", "failed to connect");
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                    free(pids);
                    free(infos);
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
            }
            case SML_IFACE: {
                if (stbInfo->iface == SML_IFACE) {
                    pThreadInfo->taos = select_one_from_pool(database->dbName);
                }
                pThreadInfo->max_sql_len =
                    stbInfo->lenOfCols + stbInfo->lenOfTags;
                if (stbInfo->iface == SML_REST_IFACE) {
                    pThreadInfo->buffer =
                            benchCalloc(1, g_arguments->reqPerReq *
                                      (1 + pThreadInfo->max_sql_len), true);
                }
                if (stbInfo->lineProtocol != TSDB_SML_JSON_PROTOCOL) {
                    pThreadInfo->sml_tags =
                        (char **)benchCalloc(pThreadInfo->ntables, sizeof(char *), true);
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        pThreadInfo->sml_tags[t] =
                                benchCalloc(1, stbInfo->lenOfTags, true);
                    }

                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        generateRandData(
                            stbInfo, pThreadInfo->sml_tags[t],
                            stbInfo->lenOfCols + stbInfo->lenOfTags,
                            stbInfo->tags, stbInfo->tagCount, 1, true);
                        debugPrint(stdout, "pThreadInfo->sml_tags[%d]: %s\n", t,
                                   pThreadInfo->sml_tags[t]);
                    }
                    pThreadInfo->lines =
                            benchCalloc(g_arguments->reqPerReq, sizeof(char *), true);

                    for (int j = 0; j < g_arguments->reqPerReq; j++) {
                        pThreadInfo->lines[j] =
                                benchCalloc(1, pThreadInfo->max_sql_len, true);
                    }
                } else {
                    pThreadInfo->json_array = cJSON_CreateArray();
                    pThreadInfo->sml_json_tags = cJSON_CreateArray();
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        if (generateSmlJsonTags(
                                pThreadInfo->sml_json_tags, stbInfo,
                                pThreadInfo->start_table_from, t)) {
                            return -1;
                        }
                    }
                    pThreadInfo->lines = (char **)benchCalloc(1, sizeof(char *), true);
                }
                break;
            }
            case TAOSC_IFACE: {
                pThreadInfo->taos = select_one_from_pool(database->dbName);
                
                if (stbInfo->interlaceRows > 0) {
                    if (stbInfo->autoCreateTable) {
                        pThreadInfo->max_sql_len = g_arguments->reqPerReq * (stbInfo->lenOfCols + stbInfo->lenOfTags) + 1024;
                    } else {
                        pThreadInfo->max_sql_len = g_arguments->reqPerReq * stbInfo->lenOfCols + 1024;
                    }
                    pThreadInfo->buffer = benchCalloc(1, pThreadInfo->max_sql_len, true);
                } else {
                    pThreadInfo->buffer = benchCalloc(1, MAX_SQL_LEN, true);
                }

                break;
            }
            default:
                break;
        }
        
    }

    infoPrint(stdout, "Estimate memory usage: %.2fMB\n",
              (double)g_memoryUsage / 1048576);
    prompt(0);

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        if (stbInfo->interlaceRows > 0) {
            pthread_create(pids + i, NULL, syncWriteInterlace, pThreadInfo);
        } else {
            pthread_create(pids + i, NULL, syncWriteProgressive, pThreadInfo);
        }
    }

    int64_t start = toolsGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t end = toolsGetTimestampUs();

    uint64_t  totalDelay = 0;
    uint64_t  maxDelay = 0;
    uint64_t  minDelay = UINT64_MAX;
    uint64_t  cntDelay = 0;
    uint64_t *total_delay_list;
    double    avgDelay = 0;
    uint64_t  totalInsertRows = 0;
    uint64_t  totalAffectedRows = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        switch (stbInfo->iface) {
            case REST_IFACE:
#ifdef WINDOWS
                closesocket(pThreadInfo->sockfd);
                WSACleanup();
#else
                close(pThreadInfo->sockfd);
#endif
                tmfree(pThreadInfo->buffer);
                break;
            case SML_REST_IFACE:
                tmfree(pThreadInfo->buffer);
            case SML_IFACE:
                if (stbInfo->lineProtocol != TSDB_SML_JSON_PROTOCOL) {
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        tmfree(pThreadInfo->sml_tags[t]);
                    }
                    for (int j = 0; j < g_arguments->reqPerReq; j++) {
                        tmfree(pThreadInfo->lines[j]);
                    }
                    tmfree(pThreadInfo->sml_tags);

                } else {
                    cJSON_Delete(pThreadInfo->sml_json_tags);
                    cJSON_Delete(pThreadInfo->json_array);
                }
                tmfree(pThreadInfo->lines);
                break;
            case STMT_IFACE:
                taos_stmt_close(pThreadInfo->stmt);
                tmfree(pThreadInfo->bind_ts);
                tmfree(pThreadInfo->bind_ts_array);
                tmfree(pThreadInfo->bindParams);
                tmfree(pThreadInfo->is_null);
                break;
            case TAOSC_IFACE:
                tmfree(pThreadInfo->buffer);
                break;
            default:
                break;
        }
        totalAffectedRows += pThreadInfo->totalAffectedRows;
        totalInsertRows += pThreadInfo->totalInsertRows;
        totalDelay += pThreadInfo->totalDelay;
        cntDelay += pThreadInfo->cntDelay;

        if (pThreadInfo->maxDelay > maxDelay) {
            maxDelay = pThreadInfo->maxDelay;
        }

        if (pThreadInfo->minDelay < minDelay) {
            minDelay = pThreadInfo->minDelay;
        }
    }

    total_delay_list = benchCalloc(cntDelay, sizeof(uint64_t), false);
    uint64_t index = 0;
    for (int i = 0; i < threads; ++i) {
        threadInfo *pThreadInfo = infos + i;
        delayNode * node = pThreadInfo->delayList.head;
        for (int j = 0; j < pThreadInfo->delayList.size; ++j) {
            total_delay_list[index] = node->value;
            node = node->next;
            index++;
        }
        delay_list_destroy(&(pThreadInfo->delayList));
    }
    qsort(total_delay_list, cntDelay, sizeof(uint64_t), compare);
    if (g_arguments->debug_print) {
        for (int i = 0; i < cntDelay; ++i) {
            debugPrint(stdout, "total_delay_list[%d]: %" PRIu64 "\n", i,
                       total_delay_list[i]);
        }
    }

    free(pids);
    free(infos);

    if (cntDelay == 0) cntDelay = 1;
    avgDelay = (double)totalDelay / cntDelay;

    int64_t t = end - start;
    if (0 == t) t = 1;

    double tInMs = (double)t / 1000000.0;

    infoPrint(stdout,
              "Spent %.4f seconds to insert rows: %" PRIu64
              ", affected rows: %" PRIu64
              " with %d thread(s) into %s %.2f records/second\n\n",
              tInMs, totalInsertRows, totalAffectedRows, threads,
              database->dbName, (double)(totalInsertRows / tInMs));
    if (g_arguments->fpOfInsertResult) {
        fprintf(g_arguments->fpOfInsertResult,
                "Spent %.4f seconds to insert rows: %" PRIu64
                ", affected rows: %" PRIu64
                " with %d thread(s) into %s %.2f "
                "records/second\n\n",
                tInMs, totalInsertRows, totalAffectedRows, threads,
                database->dbName, (double)(totalInsertRows / tInMs));
    }

    if (minDelay != UINT64_MAX) {
        infoPrint(
            stdout,
            "insert delay, min: %5.2fms, avg: %5.2fms, p90: %5.2fms, p95: "
            "%5.2fms, p99: %5.2fms, max: %5.2fms\n\n",
            (double)minDelay / 1000.0, (double)avgDelay / 1000.0,
            (double)total_delay_list[(int32_t)(cntDelay * 0.9)] / 1000.0,
            (double)total_delay_list[(int32_t)(cntDelay * 0.95)] / 1000.0,
            (double)total_delay_list[(int32_t)(cntDelay * 0.99)] / 1000.0,
            (double)maxDelay / 1000.0);

        if (g_arguments->fpOfInsertResult) {
            fprintf(
                g_arguments->fpOfInsertResult,
                "insert delay, min: %5.2fms, avg: %5.2fms, p90: %5.2fms, p95: "
                "%5.2fms, p99: %5.2fms, max: %5.2fms\n\n",
                (double)minDelay / 1000.0, (double)avgDelay / 1000.0,
                (double)total_delay_list[(int32_t)(cntDelay * 0.9)] / 1000.0,
                (double)total_delay_list[(int32_t)(cntDelay * 0.95)] / 1000.0,
                (double)total_delay_list[(int32_t)(cntDelay * 0.99)] / 1000.0,
                (double)maxDelay / 1000.0);
        }
    }
    tmfree(total_delay_list);
    if (g_fail) {
        return -1;
    }
    return 0;
}

static int createStream(SSTREAM* stream, char* dbName) {
    int code = -1;
    TAOS *       taos = select_one_from_pool(dbName);
    char * command = benchCalloc(1, BUFFER_SIZE, false);
    snprintf(command, BUFFER_SIZE, "drop stream if exists %s", stream->stream_name);
    infoPrint(stderr, "%s\n", command);
    if (queryDbExec(taos, command, NO_INSERT_TYPE, false)){
        goto END;
    }
    memset(command, 0, BUFFER_SIZE);
    int pos = snprintf(command, BUFFER_SIZE, "create stream if not exists %s ", stream->stream_name);
    if (stream->trigger_mode[0] != '\0') {
        pos += snprintf(command + pos, BUFFER_SIZE - pos, "trigger %s ", stream->trigger_mode);
    }
    if (stream->watermark[0] != '\0') {
        pos += snprintf(command + pos, BUFFER_SIZE - pos, "watermark %s ", stream->watermark);
    }
    snprintf(command + pos, BUFFER_SIZE - pos, "into %s as %s", stream->stream_stb, stream->source_sql);
    infoPrint(stderr, "%s\n", command);
    if (queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        goto END;
    }
    code = 0;
    END:
    tmfree(command);
    return code;
}

int insertTestProcess() {
    SDataBase *database = g_arguments->db;

    prompt(0);

    encode_base_64();

    for (int i = 0; i < g_arguments->dbCount; ++i) {
        if (database[i].drop) {
            if (createDatabase(i)) return -1;
        }
    }
    for (int i = 0; i < g_arguments->dbCount; ++i) {
        for (int j = 0; j < database[i].superTblCount; ++j) {
            if (database[i].superTbls[j].iface != SML_IFACE &&
                database[i].superTbls[j].iface != SML_REST_IFACE) {
                if (getSuperTableFromServer(i, j)) {
                    if (createSuperTable(i, j)) return -1;
                }
            }
            if (0 != prepare_sample_data(i, j)) {
                return -1;
            }
        }
    }

    if (createChildTables()) return -1;

    if (g_arguments->taosc_version == 3) {
        for (int i = 0; i < g_arguments->dbCount; ++i) {
            for (int j = 0; j < database[i].streams->size; ++j) {
                SSTREAM * stream = benchArrayGet(database[i].streams, j);
                if (stream->drop) {
                    if (createStream(stream, database[i].dbName)) {
                        return -1;
                    }
                }
            }
        }
    }

    // create sub threads for inserting data
    for (int i = 0; i < g_arguments->dbCount; i++) {
        for (uint64_t j = 0; j < database[i].superTblCount; j++) {
            if (database[i].superTbls[j].insertRows == 0) {
                continue;
            }
            prompt(database[i].superTbls[j].non_stop);
            if (startMultiThreadInsertData(i, j)) {
                return -1;
            }
        }
    }
    return 0;
}
