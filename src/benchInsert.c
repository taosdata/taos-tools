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

static char* get_sml_field_value(const char* val, int32_t length, Field* field) {
    char* ret = benchCalloc(1, 512 , true);
    switch (field->type) {
        case TSDB_DATA_TYPE_BOOL: 
            sprintf(ret, "%s=%s", field->name, ((((int32_t)(*((char *)val))) == 1) ? "true" : "false"));
            break;
        case TSDB_DATA_TYPE_TINYINT:
            sprintf(ret, "%s=%di8", field->name, *((int8_t *)val));
            break;
        case TSDB_DATA_TYPE_UTINYINT:
            sprintf(ret, "%s=%uu8", field->name, *((uint8_t *)val));
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            sprintf(ret, "%s=%di16", field->name, *((int16_t *)val));
            break;
        case TSDB_DATA_TYPE_USMALLINT:
            sprintf(ret, "%s=%uu16", field->name, *((uint16_t *)val));
            break;
        case TSDB_DATA_TYPE_INT:
            sprintf(ret, "%s=%di32", field->name, *((int32_t *)val));
            break;
        case TSDB_DATA_TYPE_UINT:
            sprintf(ret, "%s=%uu32", field->name, *((uint32_t *)val));
            break;
        case TSDB_DATA_TYPE_BIGINT:
            sprintf(ret, "%s=%"PRId64"i64", field->name, *((int64_t *)val));
            break;
        case TSDB_DATA_TYPE_UBIGINT:
            sprintf(ret, "%s=%"PRIu64"u64", field->name, *((uint64_t *)val));
            break;
        case TSDB_DATA_TYPE_FLOAT:
            sprintf(ret, "%s=%ff32", field->name, GET_FLOAT_VAL(val));
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            sprintf(ret, "%s=%ff64", field->name, GET_DOUBLE_VAL(val));
            break;
        case TSDB_DATA_TYPE_BINARY:
            tmfree(ret);
            ret = benchCalloc(1, length, true);
            sprintf(ret, "%s=\"%s\"", field->name, (char *)val);
            break;
        case TSDB_DATA_TYPE_NCHAR:
            tmfree(ret);
            ret = benchCalloc(1, length, true);
            sprintf(ret, "%s=L\"%s\"", field->name, (char *)val);
            break;
        default:
            errorPrint(stderr, "Unsupport data type %d\n", field->type);
            exit(EXIT_FAILURE);
    }
    return ret;
}

static int get_sml_tag_buffer_from_server(SDataBase* database, SSuperTable* stbInfo, SBenchConn* conn) {
    TAOS_RES*   res;
    TAOS_ROW    row = NULL;
    char* command = ds_new(0);
    char** pcommand = &command;
    ds_append(pcommand, "select tbname");
    for (int i = 0; i < stbInfo->tags->size; i++) {
        Field* tag = benchArrayGet(stbInfo->tags, i);
        ds_append(pcommand, ",");
        ds_append(pcommand, tag->name);
    }
    ds_appends(pcommand, 4, " from ", database->dbName, ".", stbInfo->stbName);
    res = taos_query(conn->taos, command);
    int code = taos_errno(res);
    if (code != 0) {
        errorPrint(stderr, "failed to run command %s, reason: %s\n", command, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }
    stbInfo->childTbls = benchArrayInit(1, sizeof(NormalTable));
    while((row = taos_fetch_row(res)) != NULL) {
        int32_t* lengths = taos_fetch_lengths(res);
        if (lengths == NULL) {
            errorPrint(stderr, "%s", "failed to execute taos_fetch_length\n");
            taos_free_result(res);
            return -1;
        }
        NormalTable* ntb = benchCalloc(1, sizeof(NormalTable), true);
        benchArrayPush(stbInfo->childTbls, ntb);
        ntb = benchArrayGet(stbInfo->childTbls, stbInfo->childTbls->size - 1);
        tstrncpy(ntb->name, (char *)row[0], lengths[0] + 1);
        ntb->sml_tag = ds_new(0);
        for (int i = 0; i < stbInfo->tags->size; i++) {
            Field* tag = benchArrayGet(stbInfo->tags, i);
            char* val = get_sml_field_value((const char*)row[i + 1], lengths[i], tag);
            ds_append(&ntb->sml_tag, val);
            if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
                ds_append(&ntb->sml_tag, ",");
            } else if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
                ds_append(&ntb->sml_tag, " ");
            }
        }
    }

    return 0;
}


static int get_info_from_server(SDataBase* database, SSuperTable* stbInfo) {
    char*   command = ds_new(0);
    ds_appends(&command, 4, "describe ", database->dbName, ".", stbInfo->stbName);
    TAOS_RES *   res;
    TAOS_ROW     row = NULL;
    SBenchConn* conn = init_bench_conn();
    res = taos_query(conn->taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint(stdout, "failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        taos_free_result(res);
        close_bench_conn(conn);
        return -1;
    }
    benchArrayClear(stbInfo->tags);
    benchArrayClear(stbInfo->cols);
    int count = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (count == 0) {
            count++;
            continue;
        }
        int32_t *lengths = taos_fetch_lengths(res);
        if (lengths == NULL) {
            errorPrint(stderr, "%s", "failed to execute taos_fetch_length\n");
            taos_free_result(res);
            close_bench_conn(conn);
            return -1;
        }
        if (strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "tag",
                        strlen("tag")) == 0) {
            Field* tag = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->tags, tag);
            tag = benchArrayGet(stbInfo->tags, stbInfo->tags->size - 1);
            tag->type = taos_convert_string_to_datatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            tag->length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tag->min = taos_convert_datatype_to_default_min(tag->type);
            tag->max = taos_convert_datatype_to_default_max(tag->type);
            tstrncpy(tag->name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     strlen((char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX]) + 1);
        } else {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->cols, col);
            col = benchArrayGet(stbInfo->cols, stbInfo->cols->size - 1);
            col->type = taos_convert_string_to_datatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            col->length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            col->min = taos_convert_datatype_to_default_min(col->type);
            col->max = taos_convert_datatype_to_default_max(col->type);
            tstrncpy(col->name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     strlen((char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX]) + 1);
        }
    }
    taos_free_result(res);
    if (stbInfo->iface == SML_IFACE) {
        if (get_sml_tag_buffer_from_server(database, stbInfo, conn)) {
            close_bench_conn(conn);
            return -1;
        }
    }
    close_bench_conn(conn);
    return 0;
}

static int createSuperTable(SDataBase* database, SSuperTable* stbInfo) {
    uint32_t col_buffer_len = (TSDB_COL_NAME_LEN + 15) * stbInfo->cols->size;
    char         *cols = benchCalloc(1, col_buffer_len, false);
    char*         command = benchCalloc(1, BUFFER_SIZE, false);
    int          len = 0;

    for (int colIndex = 0; colIndex < stbInfo->cols->size; colIndex++) {
        Field * col = benchArrayGet(stbInfo->cols, colIndex);
        if (col->type == TSDB_DATA_TYPE_BINARY ||
                col->type == TSDB_DATA_TYPE_NCHAR){
            len += snprintf(cols + len, col_buffer_len - len, ",%s %s(%d)", col->name,
                            taos_convert_datatype_to_string(col->type), col->length);
        } else {
            len += snprintf(cols + len, col_buffer_len - len, ",%s %s",col->name,
                            taos_convert_datatype_to_string(col->type));
        }
    }

    // save for creating child table
    stbInfo->colsOfCreateChildTable =
        (char *)benchCalloc(len + TIMESTAMP_BUFF_LEN, 1, true);

    snprintf(stbInfo->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(ts timestamp%s)", cols);

    if (stbInfo->tags->size == 0) {
        free(cols);
        free(command);
        return 0;
    }

    uint32_t tag_buffer_len = (TSDB_COL_NAME_LEN + 15) * stbInfo->tags->size;
    char *tags = benchCalloc(1, tag_buffer_len, false);
    int  tagIndex;
    len = 0;

    len += snprintf(tags + len, tag_buffer_len - len, "(");
    for (tagIndex = 0; tagIndex < stbInfo->tags->size; tagIndex++) {
        Field * tag = benchArrayGet(stbInfo->tags, tagIndex);
        if (tag->type == TSDB_DATA_TYPE_BINARY ||
                tag->type == TSDB_DATA_TYPE_NCHAR) {
            len += snprintf(tags + len, tag_buffer_len - len, "%s %s(%d),", tag->name,
                            taos_convert_datatype_to_string(tag->type), tag->length);
        } else if (tag->type == TSDB_DATA_TYPE_JSON) {
            len += snprintf(tags + len, tag_buffer_len - len, "%s json", tag->name);
            goto skip;
        } else {
            len += snprintf(tags + len, tag_buffer_len - len, "%s %s,", tag->name,
                            taos_convert_datatype_to_string(tag->type));
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
    tmfree(cols);
    tmfree(tags);
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
    for (int i = 0; i < stbInfo->cols->size; ++i) {
        Field * col = benchArrayGet(stbInfo->cols, i);
        if (col->sma) {
            if (first_sma) {
                length += snprintf(command + length, BUFFER_SIZE - length, " SMA(%s", col->name);
                first_sma = false;
            } else {
                length += snprintf(command + length, BUFFER_SIZE - length, ",%s", col->name);
            }
        }
    }
    if (!first_sma) {
        sprintf(command + length, ")");
    }
    infoPrint(stdout, "create stable: <%s>\n", command);
    SBenchConn* conn = init_bench_conn();
    if (conn == NULL) {
        free(command);
        return -1;
    }
    if (0 != queryDbExec(conn, command)) {
        errorPrint(stderr, "create supertable %s failed!\n\n",
                   stbInfo->stbName);
        close_bench_conn(conn);
        free(command);
        return -1;
    }
    close_bench_conn(conn);
    free(command);
    return 0;
}

int createDatabase(SDataBase* database) {
    char       command[SQL_BUFF_LEN] = "\0";
    SBenchConn* conn = init_bench_conn();
    if (conn == NULL) {
        return -1;
    }
    sprintf(command, "drop database if exists %s;", database->dbName);
    if (0 != queryDbExec(conn, command)) {
        close_bench_conn(conn);
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

    if (0 != queryDbExec(conn, command)) {
        close_bench_conn(conn);
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
    close_bench_conn(conn);
    return 0;
}

static void *createTable(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
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
        if (!stbInfo->use_metric || stbInfo->tags->size == 0) {
            if (stbInfo->childTblCount == 1) {
                snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                         stbInfo->escape_character
                         ? "CREATE TABLE IF NOT EXISTS %s.`%s` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s %s;",
                         pThreadInfo->dbName, stbInfo->stbName,
                         stbInfo->colsOfCreateChildTable);
            } else {
                snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                         stbInfo->escape_character
                         ? "CREATE TABLE IF NOT EXISTS %s.`%s%" PRIu64 "` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s%" PRIu64 " %s;",
                         pThreadInfo->dbName, stbInfo->childTblPrefix, i,
                         stbInfo->colsOfCreateChildTable);
            }
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
                pThreadInfo->dbName, stbInfo->childTblPrefix, i, pThreadInfo->dbName,
                stbInfo->stbName, stbInfo->tagDataBuf + i * stbInfo->lenOfTags);
            batchNum++;
            if ((batchNum < stbInfo->batchCreateTableNum) &&
                ((TSDB_MAX_SQL_LEN - len) >=
                 (stbInfo->lenOfTags + EXTRA_SQL_LEN))) {
                continue;
            }
        }

        len = 0;

        if (0 != queryDbExec(pThreadInfo->conn, pThreadInfo->buffer)) {
            g_fail = true;
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        batchNum = 0;
        uint64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            infoPrint(stdout,
                       "thread[%d] already created %" PRId64 " tables\n",
                       pThreadInfo->threadID, pThreadInfo->tables_created);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        if (0 != queryDbExec(pThreadInfo->conn, pThreadInfo->buffer)) {
            g_fail = true;
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        debugPrint(stdout, "thread[%d] already created %" PRId64 " tables\n",
                   pThreadInfo->threadID, pThreadInfo->tables_created);
    }
create_table_end:
    tmfree(pThreadInfo->buffer);
    return NULL;
}

static int startMultiThreadCreateChildTable(SDataBase* database, SSuperTable* stbInfo) {
    int code = -1;
    infoPrint(stdout, "start creating %" PRId64 " table(s) with %d thread(s)\n",
              stbInfo->childTblCount, g_arguments->table_threads);
    int          threads = g_arguments->table_threads;
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

    if (ntables == 0) {
        errorPrint(stderr, "failed to create child table, childTblCount: %"PRId64"\n", ntables);
        goto over;
    }
    int64_t b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
#ifdef LINUX
        pThreadInfo->threadID = (pthread_t)i;
#else
        pThreadInfo->threadID = (uint32_t)i;
#endif

        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->dbName = database->dbName;
        pThreadInfo->conn = init_bench_conn();
        if (pThreadInfo->conn == NULL) {
            goto over;
        }
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->tables_created = 0;
    }


    double start = (double)toolsGetTimestampMs();
    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
    }

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }
    double end = (double)toolsGetTimestampMs();

    int64_t actual_created_tables = 0;
    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        close_bench_conn(pThreadInfo->conn);
        actual_created_tables += pThreadInfo->tables_created;
    }
    infoPrint(stdout, "Spent %.4f seconds to create %" PRId64" table(s) with %d thread(s)", 
              (end - start) / 1000.0, actual_created_tables, threads);
    if (g_arguments->fpOfInsertResult) {
        infoPrint(g_arguments->fpOfInsertResult,
                  "Spent %.4f seconds to create %" PRId64" table(s) with %d thread(s)", 
                  (end - start) / 1000.0, actual_created_tables, threads);
    }

    if (g_fail) {
        goto over;
    }

    code = 0;

over:
    free(pids);
    free(infos);
    return code;
}

void postFreeResource() {
    tmfree(g_arguments->base64_buf);
    tmfclose(g_arguments->fpOfInsertResult);
    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        benchArrayDestroy(database[i].streams);
        for (uint64_t j = 0; j < database->superTbls->size; j++) {
            SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
            tmfree(stbInfo->colsOfCreateChildTable);
            tmfree(stbInfo->sampleDataBuf);
            tmfree(stbInfo->tagDataBuf);
            tmfree(stbInfo->partialColumnNameBuf);
            for (int k = 0; k < stbInfo->tags->size; ++k) {
                Field * tag = benchArrayGet(stbInfo->tags, k);
                tmfree(tag->data);
            }
            benchArrayDestroy(stbInfo->tags);

            for (int k = 0; k < stbInfo->cols->size; ++k) {
                Field * col = benchArrayGet(stbInfo->cols, k);
                tmfree(col->data);
            }
            benchArrayDestroy(stbInfo->cols);
            if (g_arguments->test_mode == INSERT_TEST &&
                    stbInfo->insertRows != 0) {
                for (int64_t k = 0; k < stbInfo->childTblCount;
                     ++k) {
                    tmfree(stbInfo->childTblName[k]);
                }
            }
            tmfree(stbInfo->childTblName);
        }
        benchArrayDestroy(database->superTbls);
    }
    benchArrayDestroy(g_arguments->databases);
    tools_cJSON_Delete(root);
}

inline int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_RES *   res = NULL;
    int32_t      code;
    uint16_t     iface = stbInfo->iface;

    switch (iface) {
        case TAOSC_IFACE:
            code = queryDbExec(pThreadInfo->conn, pThreadInfo->buffer);
            break;

        case REST_IFACE:
            code =  postProceSql(pThreadInfo->buffer,
                                  pThreadInfo->dbName,
                                  pThreadInfo->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd,
                                  pThreadInfo->filePath);
            break;

        case STMT_IFACE:
            if (taos_stmt_execute(pThreadInfo->conn->stmt)) {
                errorPrint(stderr,
                           "failed to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->conn->stmt));
                code = -1;
                break;
            }
            code = 0;
            break;
        case SML_IFACE:
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = tools_cJSON_Print(pThreadInfo->json_array);
            }
            res = taos_schemaless_insert(
                pThreadInfo->conn->taos, pThreadInfo->lines,
                stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                stbInfo->lineProtocol,
                stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL
                    ? pThreadInfo->sml_precision
                    : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            code = taos_errno(res);
            if (code != TSDB_CODE_SUCCESS) {
                errorPrint(
                    stderr,
                    "failed to execute schemaless insert. content: %s, reason: "
                    "%s\n",
                    pThreadInfo->lines[0], taos_errstr(res));
            }
            break;
        case SML_REST_IFACE: {
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = tools_cJSON_Print(pThreadInfo->json_array);
                code = postProceSql(pThreadInfo->lines[0], pThreadInfo->dbName,
                                      pThreadInfo->precision, stbInfo->iface,
                                      stbInfo->lineProtocol, stbInfo->tcpTransfer,
                                      pThreadInfo->sockfd, pThreadInfo->filePath);
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
                code = postProceSql(pThreadInfo->buffer, pThreadInfo->dbName, pThreadInfo->precision,
                                      stbInfo->iface, stbInfo->lineProtocol, stbInfo->tcpTransfer,
                                      pThreadInfo->sockfd, pThreadInfo->filePath);
            }
            break;
        }
    }
    return code;
}

static int startMultiThreadInsertData(SDataBase* database, SSuperTable* stbInfo) {
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
        SBenchConn* conn = init_bench_conn();
        if (conn == NULL) {
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
        TAOS_RES *res = taos_query(conn->taos, cmd);
        int32_t   code = taos_errno(res);
        int64_t   count = 0;
        if (code) {
            errorPrint(stderr, "failed to get child table name: %s. reason: %s",
                       cmd, taos_errstr(res));
            taos_free_result(res);
            close_bench_conn(conn);
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
        close_bench_conn(conn);
    }
    else if (stbInfo->childTblCount == 1 && stbInfo->tags->size == 0) {
        if (stbInfo->escape_character) {
            snprintf(stbInfo->childTblName[0], TSDB_TABLE_NAME_LEN,
                     "`%s`", stbInfo->stbName);
        } else {
            snprintf(stbInfo->childTblName[0], TSDB_TABLE_NAME_LEN,
                     "%s", stbInfo->stbName);
        }
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
    if (ntables == 0) {
        return 0;
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
        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->dbName = database->dbName;
        pThreadInfo->sml_precision = database->dbCfg.sml_precision;
        pThreadInfo->precision = database->dbCfg.precision;
        pThreadInfo->start_time = stbInfo->startTimestamp;
        pThreadInfo->totalInsertRows = 0;
        pThreadInfo->samplePos = 0;
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->delayList = benchArrayInit(1, sizeof(int64_t));
        switch (stbInfo->iface) {
            case REST_IFACE:
                if (prepare_rest(pThreadInfo)) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                break;
            case STMT_IFACE: 
                if (prepare_stmt(pThreadInfo, database->dbName, stbInfo)) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                break;
            case SML_REST_IFACE:
                if (prepare_rest(pThreadInfo)) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
            case SML_IFACE: 
                if (prepare_sml(pThreadInfo, database->dbName, stbInfo)) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                break;
            case TAOSC_IFACE: 
                if (prepare_taosc(pThreadInfo, database->dbName)) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                break;
            
            default:
                break;
        }

    }

    infoPrint(stdout, "Estimate memory usage: %.2fMB\n",
              (double)g_memoryUsage / 1048576);
    prompt(0);

    int64_t start = toolsGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        if (stbInfo->interlaceRows > 0) {
            if (stbInfo->iface == STMT_IFACE) {
                pthread_create(pids + i, NULL, sync_write_stmt_interlace, pThreadInfo);
            } else if (stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) {
                pthread_create(pids + i, NULL, sync_write_sml_interlace, pThreadInfo);
            } else {
                pthread_create(pids + i, NULL, sync_write_taosc_interlace, pThreadInfo);
            }
        } else {
            if (stbInfo->iface == STMT_IFACE) {
                pthread_create(pids + i, NULL, sync_write_stmt_progressive, pThreadInfo);
            } else if (stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) {
                pthread_create(pids + i, NULL, sync_write_sml_progressive, pThreadInfo);
            } else {
                pthread_create(pids + i, NULL, sync_write_taosc_progressive, pThreadInfo);
            }
        }
    }

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t end = toolsGetTimestampUs();

    BArray *  total_delay_list = benchArrayInit(1, sizeof(int64_t));
    int64_t   totalDelay = 0;
    uint64_t  totalInsertRows = 0;

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
                    tools_cJSON_Delete(pThreadInfo->sml_json_tags);
                    tools_cJSON_Delete(pThreadInfo->json_array);
                }
                close_bench_conn(pThreadInfo->conn);
                tmfree(pThreadInfo->lines);
                break;
            case STMT_IFACE:
                taos_stmt_close(pThreadInfo->conn->stmt);
                close_bench_conn(pThreadInfo->conn);
                tmfree(pThreadInfo->bind_ts);
                tmfree(pThreadInfo->bind_ts_array);
                tmfree(pThreadInfo->bindParams);
                tmfree(pThreadInfo->is_null);
                break;
            case TAOSC_IFACE:
                tmfree(pThreadInfo->buffer);
                close_bench_conn(pThreadInfo->conn);
                break;
            default:
                break;
        }
        totalInsertRows += pThreadInfo->totalInsertRows;
        totalDelay += pThreadInfo->totalDelay;
        benchArrayAddBatch(total_delay_list, pThreadInfo->delayList->pData, pThreadInfo->delayList->size);
        tmfree(pThreadInfo->delayList);
    }
    qsort(total_delay_list->pData, total_delay_list->size, total_delay_list->elemSize, compare);

    free(pids);
    free(infos);

    infoPrint(stdout, "Spent %.2f seconds to insert rows: %" PRIu64
                          " with %d thread(s) into %s %.2f records/second\n",
                  (end - start)/1E6, totalInsertRows, threads,
                  database->dbName, (double)(totalInsertRows / ((end - start)/1E6)));

    if (g_arguments->fpOfInsertResult) {
            infoPrint(g_arguments->fpOfInsertResult,
                      "Spent %.6f seconds to insert rows: %" PRIu64
                              " with %d thread(s) into %s %.2f records/second\n",
                      (end - start)/1E6, totalInsertRows, threads,
                      database->dbName, (double)(totalInsertRows / ((end - start)/1E6)));
    }
	if (!total_delay_list->size) {
		benchArrayDestroy(total_delay_list);
		return -1;
	}

    infoPrint(stdout, "insert delay, "
                      "min: %.2fms, "
                      "avg: %.2fms, "
                      "p90: %.2fms, "
                      "p95: %.2fms, "
                      "p99: %.2fms, "
                      "max: %.2fms\n",
                      *(int64_t *)(benchArrayGet(total_delay_list, 0))/1E3,
                      (double)totalDelay/total_delay_list->size/1E3,
                      *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size * 0.9)))/1E3,
                      *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size * 0.95)))/1E3,
                      *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size * 0.99)))/1E3,
                      *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size - 1)))/1E3);

    if (g_arguments->fpOfInsertResult) {
        infoPrint(g_arguments->fpOfInsertResult,
                          "insert delay, "
                          "min: %.2fms, "
                          "avg: %.2fms, "
                          "p90: %.2fms, "
                          "p95: %.2fms, "
                          "p99: %.2fms, "
                          "max: %.2fms\n",
                  *(int64_t *)(benchArrayGet(total_delay_list, 0))/1E3,
                  (double)totalDelay/total_delay_list->size/1E3,
                  *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size * 0.9)))/1E3,
                  *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size * 0.95)))/1E3,
                  *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size * 0.99)))/1E3,
                  *(int64_t *)(benchArrayGet(total_delay_list, (int32_t)(total_delay_list->size - 1)))/1E3);
    }
    benchArrayDestroy(total_delay_list);
    if (g_fail) {
        return -1;
    }
    return 0;
}

static int createStream(SSTREAM* stream, char* dbName) {
    int code = -1;
    char * command = benchCalloc(1, BUFFER_SIZE, false);
    snprintf(command, BUFFER_SIZE, "drop stream if exists %s", stream->stream_name);
    infoPrint(stderr, "%s\n", command);
    SBenchConn* conn = init_bench_conn();
    if (conn == NULL) {
        goto END;
    }
    if (queryDbExec(conn, command)){
        close_bench_conn(conn);
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
    if (queryDbExec(conn, command)) {
        close_bench_conn(conn);
        goto END;
    }
    code = 0;
    close_bench_conn(conn);
END:
    tmfree(command);
    return code;
}

int insertTestProcess() {

    prompt(0);

    encode_base_64();

    for (int i = 0; i < g_arguments->databases->size; ++i) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->drop) {
            // create database
            if (createDatabase(database)){
                return -1;
            }
        }
        for (int j = 0; j < database->superTbls->size; ++j) {
            SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);

            // check if childtable already exists
            if (stbInfo->childTblExists) {
                if (get_info_from_server(database, stbInfo)) {
                    return -1;
                }
            } else {
                // create super table
                if (createSuperTable(database, stbInfo)) {
                    return -1;
                }
            }

            // prepare data for both cols and tags
            if (prepare_sample_data(database, stbInfo)) {
                return -1;
            }

            // create child tables
            if (!stbInfo->childTblExists) {
                if (startMultiThreadCreateChildTable(database, stbInfo)) {
                    return -1;
                }
            }
        }

        // create stream if only taosc version is 3
        if (g_arguments->taosc_version == 3) {
            for (int j = 0; j < database->streams->size; ++j) {
                SSTREAM* stream = benchArrayGet(database->streams, j);
                if (stream->drop) {
                    if (createStream(stream, database->dbName)) {
                        return -1;
                    }
                }
            }
        }

        // create sub threads for inserting data
        for (int j = 0; j < database->superTbls->size; j++) {
            SSuperTable* stbInfo = benchArrayGet(database->superTbls, j);
            prompt(stbInfo->non_stop);
            if (startMultiThreadInsertData(database, stbInfo)) {
                return -1;
            }
        }
    }

    return 0;
}
