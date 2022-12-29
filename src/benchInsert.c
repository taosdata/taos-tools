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
#include "benchData.h"

static int getSuperTableFromServerRest(
    SDataBase* database, SSuperTable* stbInfo, char *command) {

    return -1;
    // TODO: finish full implementation
#if 0
    int sockfd = createSockFd();
    if (sockfd < 0) {
        return -1;
    }

    int code = postProceSql(command,
                         database->dbName,
                         database->precision,
                         REST_IFACE,
                         0,
                         false,
                         sockfd,
                         NULL);

    destroySockFd(sockfd);
#endif   // 0
}

static int getSuperTableFromServerTaosc(
    SDataBase* database, SSuperTable* stbInfo, char *command) {
#ifdef WEBSOCKET
    if (g_arguments->websocket) {
        return -1;
    }
#endif
    TAOS_RES *   res;
    TAOS_ROW     row = NULL;
    SBenchConn* conn = init_bench_conn();
    if (NULL == conn) {
        return -1;
    }

    res = taos_query(conn->taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        debugPrint("failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        infoPrint("stable %s does not exist, will create one\n",
                  stbInfo->stbName);
        taos_free_result(res);
        close_bench_conn(conn);
        return -1;
    }
    infoPrint("find stable<%s>, will get meta data from server\n",
              stbInfo->stbName);
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
            errorPrint("%s", "failed to execute taos_fetch_length\n");
            taos_free_result(res);
            return -1;
        }
        if (strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "tag",
                        strlen("tag")) == 0) {
            Field* tag = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->tags, tag);
            tag = benchArrayGet(stbInfo->tags, stbInfo->tags->size - 1);
            tag->type = convertStringToDatatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            tag->length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tag->min = convertDatatypeToDefaultMin(tag->type);
            tag->max = convertDatatypeToDefaultMax(tag->type);
            tstrncpy(tag->name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX] + 1);
        } else {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->cols, col);
            col = benchArrayGet(stbInfo->cols, stbInfo->cols->size - 1);
            col->type = convertStringToDatatype(
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]);
            col->length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            col->min = convertDatatypeToDefaultMin(col->type);
            col->max = convertDatatypeToDefaultMax(col->type);
            tstrncpy(col->name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX] + 1);
        }
    }
    taos_free_result(res);
    close_bench_conn(conn);
    return 0;
}

static int getSuperTableFromServer(SDataBase* database, SSuperTable* stbInfo) {
    int ret = 0;

    char         command[SQL_BUFF_LEN] = "\0";
    snprintf(command, SQL_BUFF_LEN, "describe %s.`%s`", database->dbName,
             stbInfo->stbName);

    if (REST_IFACE == stbInfo->iface) {
        ret = getSuperTableFromServerRest(database, stbInfo, command);
    } else {
        ret = getSuperTableFromServerTaosc(database, stbInfo, command);
    }

    return ret;
}

static int createSuperTable(SDataBase* database, SSuperTable* stbInfo) {
    if (g_arguments->supplementInsert) {
        return 0;
    }

    uint32_t col_buffer_len = (TSDB_COL_NAME_LEN + 15) * stbInfo->cols->size;
    char         *cols = benchCalloc(1, col_buffer_len, false);
    char*         command = benchCalloc(1, BUFFER_SIZE, false);
    int          len = 0;

    for (int colIndex = 0; colIndex < stbInfo->cols->size; colIndex++) {
        Field * col = benchArrayGet(stbInfo->cols, colIndex);
        if (col->type == TSDB_DATA_TYPE_BINARY ||
                col->type == TSDB_DATA_TYPE_NCHAR){
            len += snprintf(cols + len, col_buffer_len - len,
                    ",%s %s(%d)", col->name,
                    convertDatatypeToString(col->type), col->length);
        } else {
            len += snprintf(cols + len, col_buffer_len - len,
                    ",%s %s",col->name,
                    convertDatatypeToString(col->type));
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
            len += snprintf(tags + len, tag_buffer_len - len,
                    "%s %s(%d),", tag->name,
                    convertDatatypeToString(tag->type), tag->length);
        } else if (tag->type == TSDB_DATA_TYPE_JSON) {
            len += snprintf(tags + len, tag_buffer_len - len,
                    "%s json", tag->name);
            goto skip;
        } else {
            len += snprintf(tags + len, tag_buffer_len - len,
                    "%s %s,", tag->name,
                    convertDatatypeToString(tag->type));
        }
    }
    len -= 1;
skip:
    snprintf(tags + len, tag_buffer_len - len, ")");

    int length = snprintf(
        command, BUFFER_SIZE,
        stbInfo->escape_character
            ? "CREATE TABLE %s.`%s` (ts TIMESTAMP%s) TAGS %s"
            : "CREATE TABLE %s.%s (ts TIMESTAMP%s) TAGS %s",
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

    if (stbInfo->max_delay != NULL) {
        length += snprintf(command + length, BUFFER_SIZE - length,
                " MAX_DELAY %s", stbInfo->max_delay);
    }

    if (stbInfo->watermark != NULL) {
        length += snprintf(command + length, BUFFER_SIZE - length,
                " WATERMARK %s", stbInfo->watermark);
    }

    if (stbInfo->ttl != 0) {
        length += snprintf(command + length, BUFFER_SIZE - length,
                " TTL %d", stbInfo->ttl);
    }

    bool first_sma = true;
    for (int i = 0; i < stbInfo->cols->size; ++i) {
        Field * col = benchArrayGet(stbInfo->cols, i);
        if (col->sma) {
            if (first_sma) {
                length += snprintf(command + length, BUFFER_SIZE - length,
                        " SMA(%s", col->name);
                first_sma = false;
            } else {
                length += snprintf(command + length, BUFFER_SIZE - length,
                        ",%s", col->name);
            }
        }
    }
    if (!first_sma) {
        sprintf(command + length, ")");
    }
    infoPrint("create stable: <%s>\n", command);

    int ret = 0;
    if (REST_IFACE == stbInfo->iface) {
        int sockfd = createSockFd();
        if (sockfd < 0) {
            ret = -1;
        } else {
            ret = queryDbExecRest(command,
                              database->dbName,
                              database->precision,
                              stbInfo->iface,
                              stbInfo->lineProtocol,
                              stbInfo->tcpTransfer,
                              sockfd);
            destroySockFd(sockfd);
        }
    } else {
        SBenchConn* conn = init_bench_conn();
        if (NULL == conn) {
            ret = -1;
        } else {
            ret = queryDbExec(conn, command);
            int32_t trying = g_arguments->keep_trying;
            while (ret && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-create "
                          "supertable %s\n",
                          g_arguments->trying_interval, stbInfo->stbName);
                toolsMsleep(g_arguments->trying_interval);
                ret = queryDbExec(conn, command);
                if (trying != -1) {
                    trying --;
                }
            }
            if (0 != ret) {
                errorPrint("create supertable %s failed!\n\n",
                       stbInfo->stbName);
                ret = -1;
            } 
            close_bench_conn(conn);
        }
    }
    free(command);
    return ret;
}

#ifdef TD_VER_COMPATIBLE_3_0_0_0
int32_t getVgroupsOfDb(SBenchConn *conn, SDataBase *database) {
    int     vgroups = 0;
    char    cmd[SQL_BUFF_LEN] = "\0";

    sprintf(cmd, "USE %s", database->dbName);

    int32_t   code;
    TAOS_RES *res = NULL;

    res = taos_query(conn->taos, cmd);
    code = taos_errno(res);
    if (code) {
        errorPrint("failed to execute: %s. code: 0x%08x reason: %s\n",
                    cmd, code, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    sprintf(cmd, "SHOW vgroups");
    res = taos_query(conn->taos, cmd);
    code = taos_errno(res);
    if (code) {
        errorPrint("failed to execute: %s. code: 0x%08x reason: %s\n",
                    cmd, code, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != NULL) {
        vgroups ++;
    }
    debugPrint("%s() LN%d, vgroups: %d\n", __func__, __LINE__, vgroups);
    taos_free_result(res);

    database->vgroups = vgroups;
    database->vgArray = benchArrayInit(vgroups, sizeof(SVGroup));
    for (int32_t v = 0; v < vgroups; v ++) {
        SVGroup *vg = benchCalloc(1, sizeof(SVGroup), true);
        benchArrayPush(database->vgArray, vg);
    }

    res = taos_query(conn->taos, cmd);
    code = taos_errno(res);
    if (code) {
        errorPrint("failed to execute: %s. code: 0x%08x reason: %s\n",
                    cmd, code, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    int32_t vgItem = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        SVGroup *vg = benchArrayGet(database->vgArray, vgItem);
        vg->vgId = *(int32_t*)row[0];
        vgItem ++;
    }
    taos_free_result(res);

    return vgroups;
}
#endif  // TD_VER_COMPATIBLE_3_0_0_0

int geneDbCreateCmd(SDataBase *database, char *command) {
    int dataLen = 0;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (g_arguments->nthreads_auto) {
        dataLen += snprintf(command + dataLen, SQL_BUFF_LEN - dataLen,
                            "CREATE DATABASE IF NOT EXISTS %s VGROUPS %d", database->dbName, toolsGetNumberOfCores());
    } else {
        dataLen += snprintf(command + dataLen, SQL_BUFF_LEN - dataLen,
                            "CREATE DATABASE IF NOT EXISTS %s", database->dbName);
    }
#else
    dataLen += snprintf(command + dataLen, SQL_BUFF_LEN - dataLen,
                        "CREATE DATABASE IF NOT EXISTS %s", database->dbName);
#endif  // TD_VER_COMPATIBLE_3_0_0_0

    if (database->cfgs) {
        for (int i = 0; i < database->cfgs->size; i++) {
                SDbCfg* cfg = benchArrayGet(database->cfgs, i);
                if (cfg->valuestring) {
                    dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " %s %s", cfg->name, cfg->valuestring);
                } else {
                    dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " %s %d", cfg->name, cfg->valueint);
                }
        }
    }

    switch (database->precision) {
        case TSDB_TIME_PRECISION_MILLI:
            snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " PRECISION \'ms\';");
            break;
        case TSDB_TIME_PRECISION_MICRO:
            snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " PRECISION \'us\';");
            break;
        case TSDB_TIME_PRECISION_NANO:
            snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " PRECISION \'ns\';");
            break;
    }

    return dataLen;
}

int createDatabaseRest(SDataBase* database) {
    int32_t code = 0;
    char       command[SQL_BUFF_LEN] = "\0";

    int sockfd = createSockFd();
    if (sockfd < 0) {
        return -1;
    }

    sprintf(command, "DROP DATABASE IF EXISTS %s;", database->dbName);
    code = postProceSql(command,
                         database->dbName,
                         database->precision,
                         REST_IFACE,
                         0,
                         false,
                         sockfd,
                         NULL);

    if (code != 0) {
        errorPrint("Failed to drop database %s\n", database->dbName);
    } else {
        geneDbCreateCmd(database, command);
        code = postProceSql(command,
                             database->dbName,
                             database->precision,
                             REST_IFACE,
                             0,
                             false,
                             sockfd,
                             NULL);
    }
    destroySockFd(sockfd);
    return code;
}

int createDatabaseTaosc(SDataBase* database) {
    char       command[SQL_BUFF_LEN] = "\0";
    SBenchConn* conn = init_bench_conn();
    if (NULL == conn) {
        return -1;
    }
    if (g_arguments->taosc_version == 3) {
        for (int i = 0; i < g_arguments->streams->size; i++) {
            SSTREAM* stream = benchArrayGet(g_arguments->streams, i);
            if (stream->drop) {
                sprintf(command, "DROP STREAM IF EXISTS %s;", stream->stream_name);
                if (queryDbExec(conn, command)) {
                    close_bench_conn(conn);
                    return -1;
                }
                infoPrint("%s\n",command);
                memset(command, 0, SQL_BUFF_LEN);
            }
        }
    }

    sprintf(command, "DROP DATABASE IF EXISTS %s;", database->dbName);
    if (0 != queryDbExec(conn, command)) {
        close_bench_conn(conn);
        return -1;
    }

    geneDbCreateCmd(database, command);

    int32_t code = queryDbExec(conn, command);
    int32_t trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds then re-create database %s\n",
                          g_arguments->trying_interval, database->dbName);
        toolsMsleep(g_arguments->trying_interval);
        code = queryDbExec(conn, command);
        if (trying != -1) {
            trying --;
        }
    }

    if (code) {
        close_bench_conn(conn);
        errorPrint("\ncreate database %s failed!\n\n",
                   database->dbName);
        return -1;
    }
    infoPrint("create database: <%s>\n", command);

#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (database->superTbls) {
        if (g_arguments->nthreads_auto) {
            int32_t vgroups = getVgroupsOfDb(conn, database);
            if (vgroups <=0) {
                close_bench_conn(conn);
                errorPrint("Database %s's vgroups is %d\n",
                           database->dbName, vgroups);
                return -1;
            }
        }
    }
#endif  // TD_VER_COMPATIBLE_3_0_0_0

#if 0
#ifdef LINUX
    sleep(2);
#elif defined(DARWIN)
    sleep(2);
#else
    Sleep(2);
#endif
#endif
    close_bench_conn(conn);
    return 0;
}

int createDatabase(SDataBase* database) {
    int ret = 0;
    if (REST_IFACE == g_arguments->iface) {
        ret = createDatabaseRest(database);
    } else {
        ret = createDatabaseTaosc(database);
    }
    return ret;
}

static void *createTable(void *sarg) {
    if (g_arguments->supplementInsert) {
        return NULL;
    }

    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
#ifdef LINUX
    prctl(PR_SET_NAME, "createTable");
#endif
    uint64_t lastPrintTime = toolsGetTimestampMs();
    pThreadInfo->buffer = benchCalloc(1, TSDB_MAX_SQL_LEN, false);
    int len = 0;
    int batchNum = 0;
    infoPrint(
              "thread[%d] start creating table from %" PRIu64 " to %" PRIu64
              "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    char ttl[20] = "";
    if (stbInfo->ttl != 0) {
        sprintf(ttl, "TTL %d", stbInfo->ttl);
    }

    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        if (g_arguments->terminate) {
            goto create_table_end;
        }
        if (!stbInfo->use_metric || stbInfo->tags->size == 0) {
            if (stbInfo->childTblCount == 1) {
                snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                         stbInfo->escape_character
                         ? "CREATE TABLE %s.`%s` %s;"
                         : "CREATE TABLE %s.%s %s;",
                         database->dbName, stbInfo->stbName,
                         stbInfo->colsOfCreateChildTable);
            } else {
                snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                         stbInfo->escape_character
                         ? "CREATE TABLE %s.`%s%" PRIu64 "` %s;"
                         : "CREATE TABLE %s.%s%" PRIu64 " %s;",
                         database->dbName, stbInfo->childTblPrefix, i,
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
                stbInfo->escape_character ? "%s.`%s%" PRIu64
                                            "` USING %s.`%s` TAGS (%s) %s "
                                          : "%s.%s%" PRIu64
                                            " USING %s.%s TAGS (%s) %s ",
                database->dbName, stbInfo->childTblPrefix, i, database->dbName,
                stbInfo->stbName, stbInfo->tagDataBuf + i * stbInfo->lenOfTags, ttl);
            batchNum++;
            if ((batchNum < stbInfo->batchCreateTableNum) &&
                ((TSDB_MAX_SQL_LEN - len) >=
                 (stbInfo->lenOfTags + EXTRA_SQL_LEN))) {
                continue;
            }
        }

        len = 0;

        int ret = 0;
        debugPrint("creating table: %s\n", pThreadInfo->buffer);
        if (REST_IFACE == stbInfo->iface) {
            ret = queryDbExecRest(pThreadInfo->buffer,
                                  database->dbName,
                                  database->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd);
        } else {
            ret = queryDbExec(pThreadInfo->conn, pThreadInfo->buffer);
            int32_t trying = g_arguments->keep_trying;
            while (ret && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-create "
                          "table %s\n",
                          g_arguments->trying_interval, pThreadInfo->buffer);
                toolsMsleep(g_arguments->trying_interval);
                ret = queryDbExec(pThreadInfo->conn, pThreadInfo->buffer);
                if (trying != -1) {
                    trying --;
                }
            }
        }
        if (0 != ret) {
            g_fail = true;
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        batchNum = 0;
        uint64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            infoPrint(
                       "thread[%d] already created %" PRId64 " tables\n",
                       pThreadInfo->threadID, pThreadInfo->tables_created);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        int ret = 0;
        if (REST_IFACE == stbInfo->iface) {
            ret = queryDbExecRest(pThreadInfo->buffer,
                                  database->dbName,
                                  database->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd);
        } else {
            ret = queryDbExec(pThreadInfo->conn, pThreadInfo->buffer);
        }
        if (0 != ret) {
            g_fail = true;
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        debugPrint("thread[%d] already created %" PRId64 " tables\n",
                   pThreadInfo->threadID, pThreadInfo->tables_created);
    }
create_table_end:
    tmfree(pThreadInfo->buffer);
    return NULL;
}

static int startMultiThreadCreateChildTable(
        SDataBase* database, SSuperTable* stbInfo) {
    int code = -1;
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
        errorPrint("failed to create child table, childTblCount: %"PRId64"\n",
                ntables);
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
        pThreadInfo->dbInfo = database;
        if (REST_IFACE == stbInfo->iface) {
            int sockfd = createSockFd();
            if (sockfd < 0) {
                tmfree(pids);
                tmfree(infos);
                return -1;
            }
            pThreadInfo->sockfd = sockfd;
        } else {
            pThreadInfo->conn = init_bench_conn();
            if (NULL == pThreadInfo->conn) {
                goto over;
            }
        }
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->tables_created = 0;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
    }

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        g_arguments->actualChildTables += pThreadInfo->tables_created;

        if (REST_IFACE != stbInfo->iface) {
            close_bench_conn(pThreadInfo->conn);
        }
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

static int createChildTables() {
    int32_t    code;
    infoPrint("start creating %" PRId64 " table(s) with %d thread(s)\n",
              g_arguments->totalChildTables, g_arguments->table_threads);
    if (g_arguments->fpOfInsertResult) {
        infoPrintToFile(g_arguments->fpOfInsertResult,
                  "start creating %" PRId64 " table(s) with %d thread(s)\n",
                  g_arguments->totalChildTables, g_arguments->table_threads);
    }
    double start = (double)toolsGetTimestampMs();

    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (int j = 0; j < database->superTbls->size; j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                if (stbInfo->autoCreateTable || stbInfo->iface == SML_IFACE ||
                        stbInfo->iface == SML_REST_IFACE) {
                    g_arguments->autoCreatedChildTables +=
                            stbInfo->childTblCount;
                    continue;
                }
                if (stbInfo->childTblExists) {
                    g_arguments->existedChildTables +=
                            stbInfo->childTblCount;
                    continue;
                }
                debugPrint("colsOfCreateChildTable: %s\n",
                        stbInfo->colsOfCreateChildTable);

                code = startMultiThreadCreateChildTable(database, stbInfo);
                if (code && !g_arguments->terminate) {
                    return code;
                }
            }
        }
    }

    double end = (double)toolsGetTimestampMs();
    succPrint(
            "Spent %.4f seconds to create %" PRId64
            " table(s) with %d thread(s), already exist %" PRId64
            " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
            " table(s) will be auto created\n",
            (end - start) / 1000.0, g_arguments->totalChildTables,
            g_arguments->table_threads, g_arguments->existedChildTables,
            g_arguments->actualChildTables,
            g_arguments->autoCreatedChildTables);
    return 0;
}

void postFreeResource() {
    tmfree(g_arguments->base64_buf);
    tmfclose(g_arguments->fpOfInsertResult);
    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->cfgs) {
            for (int c = 0; c < database->cfgs->size; c++) {
                SDbCfg *cfg = benchArrayGet(database->cfgs, c);
                if ((NULL == root) && (0 == strcmp(cfg->name, "replica"))) {
                    tmfree(cfg->name);
                }
            }
            benchArrayDestroy(database->cfgs);
        }
        if (database->superTbls) {
            for (uint64_t j = 0; j < database->superTbls->size; j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                tmfree(stbInfo->colsOfCreateChildTable);
                tmfree(stbInfo->sampleDataBuf);
                tmfree(stbInfo->tagDataBuf);
                tmfree(stbInfo->partialColNameBuf);
                for (int k = 0; k < stbInfo->tags->size; ++k) {
                    Field * tag = benchArrayGet(stbInfo->tags, k);
                    tmfree(tag->data);
                }
                benchArrayDestroy(stbInfo->tags);

                debugPrint("%s() LN%d, col size: %"PRIu64"\n",
                        __func__, __LINE__, (uint64_t)stbInfo->cols->size);
                for (int k = 0; k < stbInfo->cols->size; ++k) {
                    Field * col = benchArrayGet(stbInfo->cols, k);
                    tmfree(col->data);
                    tmfree(col->is_null);
                }
                benchArrayDestroy(stbInfo->cols);
                if (g_arguments->test_mode == INSERT_TEST &&
                        stbInfo->insertRows != 0) {
                    for (int64_t k = 0; k < stbInfo->childTblCount;
                        ++k) {
                        if (stbInfo->childTblName) {
                            tmfree(stbInfo->childTblName[k]);
                        }
                    }
                }
                tmfree(stbInfo->childTblName);
                benchArrayDestroy(stbInfo->tsmas);
#ifdef TD_VER_COMPATIBLE_3_0_0_0
                if ((0 == stbInfo->interlaceRows)
                        && (g_arguments->nthreads_auto)) {
                    for (int32_t v = 0; v < database->vgroups; v++) {
                        SVGroup *vg = benchArrayGet(database->vgArray, v);
                        for (int64_t t = 0; t < vg->tbCountPerVgId; t ++) {
                            tmfree(vg->childTblName[t]);
                        }
                        tmfree(vg->childTblName);
                    }
                    benchArrayDestroy(database->vgArray);
                }
#endif  // TD_VER_COMPATIBLE_3_0_0_0
            }
            benchArrayDestroy(database->superTbls);
        }
    }
    benchArrayDestroy(g_arguments->databases);
    benchArrayDestroy(g_arguments->streams);
    tools_cJSON_Delete(root);
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_RES *   res = NULL;
    int32_t      code = 0;
    uint16_t     iface = stbInfo->iface;

    debugPrint("iface: %d\n", iface);

    int32_t trying = (stbInfo->keep_trying)?
        stbInfo->keep_trying:g_arguments->keep_trying;
    int32_t trying_interval = stbInfo->trying_interval?
        stbInfo->trying_interval:g_arguments->trying_interval;

    switch (iface) {
        case TAOSC_IFACE:
            debugPrint("buffer: %s\n", pThreadInfo->buffer);
            code = queryDbExec(pThreadInfo->conn, pThreadInfo->buffer);
            while (code && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-insert\n",
                          trying_interval);
                toolsMsleep(trying_interval);
                code = queryDbExec(pThreadInfo->conn, pThreadInfo->buffer);
                if (trying != -1) {
                    trying --;
                }
            }
            break;

        case REST_IFACE:
            debugPrint("buffer: %s\n", pThreadInfo->buffer);
            code = postProceSql(pThreadInfo->buffer,
                                  database->dbName,
                                  database->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd,
                                  pThreadInfo->filePath);
            while (code && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-insert\n",
                          trying_interval);
                toolsMsleep(trying_interval);
                code = postProceSql(pThreadInfo->buffer,
                                  database->dbName,
                                  database->precision,
                                  stbInfo->iface,
                                  stbInfo->lineProtocol,
                                  stbInfo->tcpTransfer,
                                  pThreadInfo->sockfd,
                                  pThreadInfo->filePath);
                if (trying != -1) {
                    trying --;
                }
            }
            break;

        case STMT_IFACE:
            code = taos_stmt_execute(pThreadInfo->conn->stmt);
            if (code) {
                errorPrint(
                           "failed to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->conn->stmt));
                code = -1;
            }
            break;

        case SML_IFACE:
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] =
                    tools_cJSON_Print(pThreadInfo->json_array);
            }
            res = taos_schemaless_insert(
                pThreadInfo->conn->taos, pThreadInfo->lines,
                stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                stbInfo->lineProtocol,
                stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL
                    ? database->sml_precision
                    : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            code = taos_errno(res);
            trying = stbInfo->keep_trying;
            while (code && trying) {
                infoPrint("will sleep %"PRIu32" milliseconds then re-insert\n",
                          trying_interval);
                toolsMsleep(trying_interval);
                taos_free_result(res);
                res = taos_schemaless_insert(
                        pThreadInfo->conn->taos, pThreadInfo->lines,
                        stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                        stbInfo->lineProtocol,
                        stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL
                        ? database->sml_precision
                        : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
                code = taos_errno(res);
                if (trying != -1) {
                    trying --;
                }
            }

            if (code != TSDB_CODE_SUCCESS) {
                errorPrint(
                    "failed to execute schemaless insert. "
                        "content: %s, code: 0x%08x reason: %s\n",
                    pThreadInfo->lines[0], code, taos_errstr(res));
            }
            taos_free_result(res);
            break;

        case SML_REST_IFACE: {
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = tools_cJSON_Print(pThreadInfo->json_array);
                code = postProceSql(pThreadInfo->lines[0], database->dbName,
                                      database->precision, stbInfo->iface,
                                      stbInfo->lineProtocol, stbInfo->tcpTransfer,
                                      pThreadInfo->sockfd, pThreadInfo->filePath);
            } else {
                int len = 0;
                for (int i = 0; i < k; ++i) {
                    if (strlen(pThreadInfo->lines[i]) != 0) {
                        if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL
                            && stbInfo->tcpTransfer) {
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
                code = postProceSql(pThreadInfo->buffer, database->dbName,
                        database->precision,
                        stbInfo->iface, stbInfo->lineProtocol,
                        stbInfo->tcpTransfer,
                        pThreadInfo->sockfd, pThreadInfo->filePath);
            }
            break;
        }
    }
    return code;
}

static void *syncWriteInterlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(
              "thread[%d] start interlace inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    int64_t pos = 0;
    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampUs();
    int64_t   endTs;
    uint64_t   tableSeq = pThreadInfo->start_table_from;
    int disorderRange = stbInfo->disorderRange;
    int64_t startTimestamp = stbInfo->startTimestamp;

    while (insertRows > 0) {
        int64_t tmp_total_insert_rows = 0;
        uint32_t generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }
        for (int i = 0; i < batchPerTblTimes; ++i) {
            if (g_arguments->terminate) {
                goto free_of_interlace;
            }
            int64_t timestamp = pThreadInfo->start_time;
            char *  tableName = stbInfo->childTblName[tableSeq];
            char ttl[20] = "";
            if (stbInfo->ttl != 0) {
                sprintf(ttl, "TTL %d", stbInfo->ttl);
            }
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE: {
                    if (i == 0) {
                        ds_add_str(&pThreadInfo->buffer, STR_INSERT_INTO);
                    }
                    if (stbInfo->partialColNum == stbInfo->cols->size) {
                        if (stbInfo->autoCreateTable) {
                            ds_add_strs(&pThreadInfo->buffer, 8,
                                    tableName,
                                    " USING `",
                                    stbInfo->stbName,
                                    "` TAGS (",
                                    stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq,
                                    ") ", ttl, " VALUES ");
                        } else {
                            ds_add_strs(&pThreadInfo->buffer, 2,
                                    tableName, " VALUES ");
                        }
                    } else {
                        if (stbInfo->autoCreateTable) {
                            ds_add_strs(&pThreadInfo->buffer, 10,
                                        tableName,
                                        " (",
                                        stbInfo->partialColNameBuf,
                                        ") USING `",
                                        stbInfo->stbName,
                                        "` TAGS (",
                                        stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq,
                                        ") ", ttl, " VALUES ");
                        } else {
                            ds_add_strs(&pThreadInfo->buffer, 4,
                                        tableName,
                                        "(",
                                        stbInfo->partialColNameBuf,
                                        ") VALUES ");
                        }
                    }

                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        int64_t disorderTs = 0;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                disorderRange --;
                                if (0 == disorderRange) {
                                    disorderRange = stbInfo->disorderRange;
                                }
                                disorderTs = startTimestamp - disorderRange;
                                debugPrint("rand_num: %d, < disorderRatio: %d, "
                                           "disorderTs: %"PRId64"\n",
                                       rand_num, stbInfo->disorderRatio, disorderTs);
                            }
                        }
                        char time_string[BIGINT_BUFF_LEN];
                        sprintf(time_string, "%"PRId64"", disorderTs?disorderTs:timestamp);
                        ds_add_strs(&pThreadInfo->buffer, 5,
                                    "(",
                                    time_string,
                                    ",",
                                    stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols,
                                    ") ");
                        if (ds_len(pThreadInfo->buffer) > stbInfo->max_sql_len) {
                            errorPrint("sql buffer length (%"PRIu64") "
                                    "is larger than max sql length "
                                    "(%"PRId64")\n",
                                    ds_len(pThreadInfo->buffer),
                                    stbInfo->max_sql_len);
                            goto free_of_interlace;
                        }
                        generated++;
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (taos_stmt_set_tbname(pThreadInfo->conn->stmt, tableName)) {
                        errorPrint(
                            "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                            tableName, taos_stmt_errstr(pThreadInfo->conn->stmt));
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
                        int64_t disorderTs = 0;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                disorderRange --;
                                if (0 == disorderRange) {
                                    disorderRange = stbInfo->disorderRange;
                                }
                                disorderTs = startTimestamp - disorderRange;
                                debugPrint("rand_num: %d, < disorderRatio: %d, "
                                            "disorderTs: %"PRId64"\n",
                                            rand_num, stbInfo->disorderRatio,
                                            disorderTs);
                            }
                        }

                        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                            tools_cJSON *tag = tools_cJSON_Duplicate(
                                tools_cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->sml_precision, disorderTs?disorderTs:timestamp);
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
                                disorderTs?disorderTs:timestamp);
                        } else {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                disorderTs?disorderTs:timestamp,
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                        }
                        generated++;
                        timestamp += stbInfo->timestamp_step;
                    }
                    break;
                }
            }
            tableSeq++;
            tmp_total_insert_rows += interlaceRows;
            if (tableSeq > pThreadInfo->end_table_to) {
                tableSeq = pThreadInfo->start_table_from;
                pThreadInfo->start_time +=
                    interlaceRows * stbInfo->timestamp_step;
                if (!stbInfo->non_stop) {
                    insertRows -= interlaceRows;
                }
                if (stbInfo->insert_interval > 0) {
                    debugPrint("%s() LN%d, insert_interval: %"PRIu64"\n",
                          __func__, __LINE__, stbInfo->insert_interval);
                    perfPrint("sleep %" PRIu64 " ms\n",
                                     stbInfo->insert_interval);
                    toolsMsleep((int32_t)stbInfo->insert_interval);
                }
                break;
            }
        }

        startTs = toolsGetTimestampUs();
        if(execInsert(pThreadInfo, generated)) {
            g_fail = true;
            goto free_of_interlace;
        }
        endTs = toolsGetTimestampUs();

        pThreadInfo->totalInsertRows += tmp_total_insert_rows;
        switch (stbInfo->iface) {
            case TAOSC_IFACE:
            case REST_IFACE:
                debugPrint("pThreadInfo->buffer: %s\n",
                           pThreadInfo->buffer);
                free_ds(&pThreadInfo->buffer);
                pThreadInfo->buffer = new_ds(0);
                break;
            case SML_REST_IFACE:
                memset(pThreadInfo->buffer, 0,
                       g_arguments->reqPerReq * (pThreadInfo->max_sql_len + 1));
            case SML_IFACE:
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    debugPrint("pThreadInfo->lines[0]: %s\n",
                               pThreadInfo->lines[0]);
                    tools_cJSON_Delete(pThreadInfo->json_array);
                    pThreadInfo->json_array = tools_cJSON_CreateArray();
                    tmfree(pThreadInfo->lines[0]);
                } else {
                    for (int j = 0; j < generated; ++j) {
                        debugPrint("pThreadInfo->lines[%d]: %s\n", j,
                                   pThreadInfo->lines[j]);
                        memset(pThreadInfo->lines[j], 0,
                               pThreadInfo->max_sql_len);
                    }
                }
                break;
            case STMT_IFACE:
                break;
        }

        int64_t delay = endTs - startTs;
        if (delay <=0) {
            debugPrint("thread[%d]: startTS: %"PRId64", endTS: %"PRId64"\n",
                       pThreadInfo->threadID, startTs, endTs);
        } else {
            perfPrint("insert execution time is %10.2f ms\n",
                      delay / 1E6);

            int64_t * pdelay = benchCalloc(1, sizeof(int64_t), false);
            *pdelay = delay;
            benchArrayPush(pThreadInfo->delayList, pdelay);
            pThreadInfo->totalDelay += delay;
        }

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            infoPrint(
                    "thread[%d] has currently inserted rows: %" PRIu64
                    "\n",
                    pThreadInfo->threadID, pThreadInfo->totalInsertRows);
            lastPrintTime = currentPrintTime;
        }
    }
free_of_interlace:
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;

    succPrint(
            "thread[%d] completed total inserted rows: %" PRIu64
            ", %.2f records/second\n",
            pThreadInfo->threadID, pThreadInfo->totalInsertRows,
            (double)(pThreadInfo->totalInsertRows /
                ((double)pThreadInfo->totalDelay / 1E6)));
    return NULL;
}

void *syncWriteProgressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = pThreadInfo->dbInfo;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if (g_arguments->nthreads_auto) {
        if (0 == pThreadInfo->vg->tbCountPerVgId) {
            return NULL;
        }
    } else {
        infoPrint(
            "thread[%d] start progressive inserting into table from "
            "%" PRIu64 " to %" PRIu64 "\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to + 1);
    }
#else
    infoPrint(
            "thread[%d] start progressive inserting into table from "
            "%" PRIu64 " to %" PRIu64 "\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to + 1);
#endif
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampUs();
    int64_t   endTs;

    int disorderRange = stbInfo->disorderRange;
    int64_t startTimestamp = stbInfo->startTimestamp;
    char *  pstr = pThreadInfo->buffer;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {

        char *   tableName = NULL;

#ifdef TD_VER_COMPATIBLE_3_0_0_0
        if (g_arguments->nthreads_auto) {
            tableName = pThreadInfo->vg->childTblName[tableSeq];
        } else {
            tableName = stbInfo->childTblName[tableSeq];
        }
#else
        tableName = stbInfo->childTblName[tableSeq];
#endif
        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;
        int32_t pos = 0;
        if (stbInfo->iface == STMT_IFACE && stbInfo->autoCreateTable) {
            taos_stmt_close(pThreadInfo->conn->stmt);
            pThreadInfo->conn->stmt = taos_stmt_init(pThreadInfo->conn->taos);
            if (NULL == pThreadInfo->conn->stmt) {
                errorPrint("taos_stmt_init() failed, reason: %s\n",
                        taos_errstr(NULL));
                g_fail = true;
                goto free_of_progressive;
            }

            if (prepareStmt(stbInfo, pThreadInfo->conn->stmt, tableSeq)) {
                g_fail = true;
                goto free_of_progressive;
            }
        }

        char ttl[20] = "";
        if (stbInfo->ttl != 0) {
            sprintf(ttl, "TTL %d", stbInfo->ttl);
        }
        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            uint32_t generated = 0;
            switch (stbInfo->iface) {
                case TAOSC_IFACE:
                case REST_IFACE: {
                    if (stbInfo->partialColNum == stbInfo->cols->size) {
                        if (stbInfo->autoCreateTable) {
                            len =
                                snprintf(pstr, MAX_SQL_LEN,
                                        "%s %s.%s USING %s.%s TAGS (%s) %s VALUES ",
                                        STR_INSERT_INTO, database->dbName,
                                        tableName, database->dbName,
                                        stbInfo->stbName,
                                        stbInfo->tagDataBuf +
                                        stbInfo->lenOfTags * tableSeq, ttl);
                        } else {
                            len = snprintf(pstr, MAX_SQL_LEN,
                                    "%s %s.%s VALUES ", STR_INSERT_INTO,
                                    database->dbName, tableName);
                        }
                    } else {
                        if (stbInfo->autoCreateTable) {
                            len = snprintf(
                                    pstr, MAX_SQL_LEN,
                                    "%s %s.%s (%s) USING %s.%s TAGS (%s) %s VALUES ",
                                    STR_INSERT_INTO, database->dbName, tableName,
                                    stbInfo->partialColNameBuf,
                                    database->dbName, stbInfo->stbName,
                                    stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq, ttl);
                        } else {
                            len = snprintf(pstr, MAX_SQL_LEN,
                                    "%s %s.%s (%s) VALUES ",
                                    STR_INSERT_INTO, database->dbName,
                                    tableName,
                                    stbInfo->partialColNameBuf);
                        }
                    }

                    for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                        if (stbInfo->useSampleTs &&
                                !stbInfo->random_data_source) {
                            len +=
                                snprintf(pstr + len,
                                        MAX_SQL_LEN - len, "(%s)",
                                        stbInfo->sampleDataBuf +
                                        pos * stbInfo->lenOfCols);
                        } else {
                            int64_t disorderTs = 0;
                            if (stbInfo->disorderRatio > 0) {
                                int rand_num = taosRandom() % 100;
                                if (rand_num < stbInfo->disorderRatio) {
                                    disorderRange --;
                                    if (0 == disorderRange) {
                                        disorderRange = stbInfo->disorderRange;
                                    }
                                    disorderTs = startTimestamp - disorderRange;
                                    debugPrint("rand_num: %d, < disorderRatio: %d, disorderTs: %"PRId64"\n",
                                        rand_num, stbInfo->disorderRatio, disorderTs);
                                }
                            }
                            len += snprintf(pstr + len,
                                MAX_SQL_LEN - len,
                                "(%" PRId64 ",%s)",
                                            disorderTs?disorderTs:timestamp,
                                stbInfo->sampleDataBuf +
                                            pos * stbInfo->lenOfCols);
                        }
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
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
                    if (taos_stmt_set_tbname(pThreadInfo->conn->stmt,
                                tableName)) {
                        errorPrint(
                                "taos_stmt_set_tbname(%s) failed,"
                                "reason: %s\n", tableName,
                                taos_stmt_errstr(pThreadInfo->conn->stmt));
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
                            tools_cJSON *tag = tools_cJSON_Duplicate(
                                tools_cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->sml_precision, timestamp);
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
                                disorderRange --;
                                if (0 == disorderRange) {
                                    disorderRange = stbInfo->disorderRange;
                                }
                                timestamp = startTimestamp - disorderRange;
                                debugPrint("rand_num: %d, < disorderRatio: %d, ts: %"PRId64"\n",
                                       rand_num, stbInfo->disorderRatio, timestamp);
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
            // only measure insert
            startTs = toolsGetTimestampUs();
            if(execInsert(pThreadInfo, generated)) {
                g_fail = true;
                goto free_of_progressive;
            }
            endTs = toolsGetTimestampUs();

            if (stbInfo->insert_interval > 0) {
                debugPrint("%s() LN%d, insert_interval: %"PRIu64"\n",
                          __func__, __LINE__, stbInfo->insert_interval);
                perfPrint("sleep %" PRIu64 " ms\n",
                              stbInfo->insert_interval);
                toolsMsleep((int32_t)stbInfo->insert_interval);
            }

            pThreadInfo->totalInsertRows += generated;

            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE:
                    memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                    break;
                case SML_REST_IFACE:
                    memset(pThreadInfo->buffer, 0,
                           g_arguments->reqPerReq *
                               (pThreadInfo->max_sql_len + 1));
                case SML_IFACE:
                    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                        tools_cJSON_Delete(pThreadInfo->json_array);
                        pThreadInfo->json_array = tools_cJSON_CreateArray();
                        tmfree(pThreadInfo->lines[0]);
                    } else {
                        for (int j = 0; j < generated; ++j) {
                            debugPrint("pThreadInfo->lines[%d]: %s\n",
                                       j, pThreadInfo->lines[j]);
                            memset(pThreadInfo->lines[j], 0,
                                   pThreadInfo->max_sql_len);
                        }
                    }
                    break;
                case STMT_IFACE:
                    break;
            }

            int64_t delay = endTs - startTs;
            if (delay <= 0) {
                debugPrint("thread[%d]: startTs: %"PRId64", endTs: %"PRId64"\n",
                        pThreadInfo->threadID, startTs, endTs);
            } else {
                perfPrint("insert execution time is %.6f s\n",
                              delay / 1E6);

                int64_t * pDelay = benchCalloc(1, sizeof(int64_t), false);
                *pDelay = delay;
                benchArrayPush(pThreadInfo->delayList, pDelay);
                pThreadInfo->totalDelay += delay;
            }

            int64_t currentPrintTime = toolsGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(
                        "thread[%d] has currently inserted rows: "
                        "%" PRId64 "\n",
                        pThreadInfo->threadID, pThreadInfo->totalInsertRows);
                lastPrintTime = currentPrintTime;
            }
            if (i >= stbInfo->insertRows) {
                break;
            }
        }  // insertRows
    }      // tableSeq
free_of_progressive:
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;
    succPrint(
            "thread[%d] completed total inserted rows: %" PRIu64
            ", %.2f records/second\n",
            pThreadInfo->threadID, pThreadInfo->totalInsertRows,
            (double)(pThreadInfo->totalInsertRows /
                ((double)pThreadInfo->totalDelay / 1E6)));
    return NULL;
}

static int parseBufferToStmtBatch(
        SSuperTable* stbInfo)
{
    int32_t columnCount;
    if (stbInfo) {
        columnCount = stbInfo->cols->size;
    } else {
        SDataBase *db = benchArrayGet(g_arguments->databases, 0);
        ASSERT(db);
        stbInfo = benchArrayGet(db->superTbls, 0);
        ASSERT(stbInfo);
        columnCount = stbInfo->cols->size;
    }

    for (int c = 0; c < columnCount; c++) {
        Field *col = benchArrayGet(stbInfo->cols, c);
        char dataType = col->type;

        char *tmpP = NULL;
        char *is_null = NULL;

        is_null = calloc(1, sizeof(char) *g_arguments->prepared_rand);
        ASSERT(is_null);
        col->is_null = is_null;

        switch(dataType) {
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                tmpP = calloc(1, sizeof(int) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                tmpP = calloc(1, sizeof(int8_t) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                tmpP = calloc(1, sizeof(int16_t) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                tmpP = calloc(1, sizeof(int64_t) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_BOOL:
                tmpP = calloc(1, sizeof(int8_t) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                tmpP = calloc(1, sizeof(float) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                tmpP = calloc(1, sizeof(double) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                tmpP = calloc(1, g_arguments->prepared_rand * col->length);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                tmpP = calloc(1, sizeof(int64_t) * g_arguments->prepared_rand);
                assert(tmpP);
                col->data = (void*)tmpP;
                break;

            default:
                errorPrint("Unknown data type: %s\n",
                        convertDatatypeToString(dataType));
                exit(EXIT_FAILURE);
        }
    }

    char *sampleDataBuf = stbInfo->sampleDataBuf;
    int64_t lenOfOneRow = stbInfo->lenOfCols;

    if (stbInfo->useSampleTs) {
        columnCount += 1; // for skiping first column
    }
    for (int i=0; i < g_arguments->prepared_rand; i++) {
        int cursor = 0;

        for (int c = 0; c < columnCount; c++) {
            char *restStr = sampleDataBuf
                + lenOfOneRow * i + cursor;
            int lengthOfRest = strlen(restStr);

            int index = 0;
            for (index = 0; index < lengthOfRest; index ++) {
                if (restStr[index] == ',') {
                    break;
                }
            }

            cursor += index + 1; // skip ',' too
            if ((0 == c) && stbInfo->useSampleTs) {
                continue;
            }

            char *tmpStr = calloc(1, index + 1);
            if (NULL == tmpStr) {
                errorPrint("%s() LN%d, Failed to allocate %d bind buffer\n",
                        __func__, __LINE__, index + 1);
                return -1;
            }
            Field *col = benchArrayGet(stbInfo->cols,
                    (stbInfo->useSampleTs?c-1:c));
            char dataType = col->type;

            strncpy(tmpStr, restStr, index);

            if (0 == strcmp(tmpStr, "NULL")) {
                *(col->is_null + i) = true;
            } else {
                switch(dataType) {
                    case TSDB_DATA_TYPE_INT:
                    case TSDB_DATA_TYPE_UINT:
                        *((int32_t*)col->data + i) = atoi(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_FLOAT:
                        *((float*)col->data +i) = (float)atof(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_DOUBLE:
                        *((double*)col->data + i) = atof(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_TINYINT:
                    case TSDB_DATA_TYPE_UTINYINT:
                        *((int8_t*)col->data + i) = (int8_t)atoi(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_SMALLINT:
                    case TSDB_DATA_TYPE_USMALLINT:
                        *((int16_t*) col->data + i) = (int16_t)atoi(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_BIGINT:
                    case TSDB_DATA_TYPE_UBIGINT:
                        *((int64_t*)col->data + i) = (int64_t)atol(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_BOOL:
                        *((int8_t*)col->data + i) = (int8_t)atoi(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_TIMESTAMP:
                        *((int64_t*)col->data + i) = (int64_t)atol(tmpStr);
                        break;

                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_NCHAR:
                        {
                            size_t tmpLen = strlen(tmpStr);
                            debugPrint("%s() LN%d, index: %d, "
                                    "tmpStr len: %"PRIu64", col->length: %d\n",
                                    __func__, __LINE__,
                                    i, (uint64_t)tmpLen, col->length);
                            if (tmpLen-2 > col->length) {
                                errorPrint("data length %"PRIu64" "
                                        "is larger than column length %d\n",
                                        (uint64_t)tmpLen, col->length);
                            }

                            if (tmpLen > 2) {
                                strncpy((char *)col->data + i * col->length,
                                        tmpStr+1, min(col->length, tmpLen - 2));
                            } else {
                                strcpy((char *)col->data + i * col->length, "");
                            }
                        }
                        break;

                    default:
                        break;
                }
            }

            free(tmpStr);
        }
    }

    return 0;
}

static int startMultiThreadInsertData(SDataBase* database,
        SSuperTable* stbInfo) {
    if ((stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) &&
        !stbInfo->use_metric) {
        errorPrint("%s", "schemaless cannot work without stable\n");
        return -1;
    }

    if (stbInfo->interlaceRows > g_arguments->reqPerReq) {
        infoPrint(
            "interlaceRows(%d) is larger than record per request(%u), which "
            "will be set to %u\n",
            stbInfo->interlaceRows, g_arguments->reqPerReq,
            g_arguments->reqPerReq);
        stbInfo->interlaceRows = g_arguments->reqPerReq;
    }

    if (stbInfo->interlaceRows > stbInfo->insertRows) {
        infoPrint(
                "interlaceRows larger than insertRows %d > %" PRId64 "\n",
                stbInfo->interlaceRows, stbInfo->insertRows);
        infoPrint("%s", "interlaceRows will be set to 0\n");
        stbInfo->interlaceRows = 0;
    }

    if (stbInfo->interlaceRows == 0
            && g_arguments->reqPerReq > stbInfo->insertRows) {
        infoPrint("record per request (%u) is larger than "
                "insert rows (%"PRIu64")"
                " in progressive mode, which will be set to %"PRIu64"\n",
                g_arguments->reqPerReq, stbInfo->insertRows,
                stbInfo->insertRows);
        g_arguments->reqPerReq = stbInfo->insertRows;
    }

    if (stbInfo->interlaceRows > 0 && stbInfo->iface == STMT_IFACE
            && stbInfo->autoCreateTable) {
        infoPrint("%s",
                "not support autocreate table with interlace row in stmt "
                "insertion, will change to progressive mode\n");
        stbInfo->interlaceRows = 0;
    }

    uint64_t tableFrom = 0;
    uint64_t ntables = stbInfo->childTblCount;
    stbInfo->childTblName = benchCalloc(stbInfo->childTblCount,
            sizeof(char *), true);
    for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
        stbInfo->childTblName[i] = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    }

    if ((stbInfo->iface != SML_IFACE && stbInfo->iface != SML_REST_IFACE)
            && stbInfo->childTblExists) {
        SBenchConn* conn = init_bench_conn();
        if (NULL == conn) {
            return -1;
        }
        char cmd[SQL_BUFF_LEN] = "\0";
        if (g_arguments->taosc_version == 3) {
            snprintf(cmd, SQL_BUFF_LEN,
                    "SELECT DISTINCT(TBNAME) FROM %s.`%s` LIMIT %" PRId64
                    " OFFSET %" PRIu64 "",
                    database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                    stbInfo->childTblOffset);
        } else {
            snprintf(cmd, SQL_BUFF_LEN,
                    "SELECT TBNAME FROM %s.`%s` LIMIT %" PRId64
                    " OFFSET %" PRIu64 "",
                    database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                    stbInfo->childTblOffset);
        }
        debugPrint("cmd: %s\n", cmd);
        TAOS_RES *res = taos_query(conn->taos, cmd);
        int32_t   code = taos_errno(res);
        int64_t   count = 0;
        if (code) {
            errorPrint("failed to get child table name: %s. reason: %s",
                    cmd, taos_errstr(res));
            taos_free_result(res);
            close_bench_conn(conn);
            return -1;
        }
        TAOS_ROW row = NULL;
        while ((row = taos_fetch_row(res)) != NULL) {
            int *lengths = taos_fetch_lengths(res);
            stbInfo->childTblName[count][0] = '`';
            strncpy(stbInfo->childTblName[count] + 1, row[0], lengths[0]);
            stbInfo->childTblName[count][lengths[0] + 1] = '`';
            stbInfo->childTblName[count][lengths[0] + 2] = '\0';
            debugPrint("stbInfo->childTblName[%" PRId64 "]: %s\n",
                       count, stbInfo->childTblName[count]);
            count++;
        }
        ntables = count;
        taos_free_result(res);
        close_bench_conn(conn);
    } else if (stbInfo->childTblCount == 1 && stbInfo->tags->size == 0) {
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

    int32_t threads = g_arguments->nthreads;
    int64_t a = 0, b = 0;

#ifdef TD_VER_COMPATIBLE_3_0_0_0
    if ((0 == stbInfo->interlaceRows)
            && (g_arguments->nthreads_auto)) {
        SBenchConn* conn = init_bench_conn();
        if (NULL == conn) {
            return -1;
        }

        for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
            int vgId;
            int ret = taos_get_table_vgId(conn->taos, database->dbName,
                                          stbInfo->childTblName[i], &vgId);
            if (ret < 0) {
                errorPrint("Failed to get %s db's %s table's vgId\n",
                           database->dbName, stbInfo->childTblName[i]);
                return -1;
            }
            debugPrint("Db %s\'s table\'s %s vgId is: %d\n",
                       database->dbName, stbInfo->childTblName[i], vgId);
            for (int32_t v = 0; v < database->vgroups; v ++) {
                SVGroup *vg = benchArrayGet(database->vgArray, v);
                if (vgId == vg->vgId) {
                    vg->tbCountPerVgId ++;
                }
            }
        }

        threads = 0;
        for (int v = 0; v < database->vgroups; v++) {
            SVGroup *vg = benchArrayGet(database->vgArray, v);
            infoPrint("Total %"PRId64" tables on bb %s's vgroup %d (id: %d)\n",
                      vg->tbCountPerVgId, database->dbName, v, vg->vgId);
            if (vg->tbCountPerVgId) {
                threads ++;
            } else {
                continue;
            }
            vg->childTblName = benchCalloc(vg->tbCountPerVgId,
                                           sizeof(char *), true);
            for (int64_t n = 0; n < vg->tbCountPerVgId; n++) {
                vg->childTblName[n] = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
                vg->tbOffset = 0;
            }
        }
        for (int64_t i = 0; i < stbInfo->childTblCount; i++) {
            int vgId;
            int ret = taos_get_table_vgId(conn->taos, database->dbName,
                                          stbInfo->childTblName[i], &vgId);
            if (ret < 0) {
                errorPrint("Failed to get %s db's %s table's vgId\n",
                           database->dbName, stbInfo->childTblName[i]);
                return -1;
            }
            debugPrint("Db %s\'s table\'s %s vgId is: %d\n",
                       database->dbName, stbInfo->childTblName[i], vgId);
            for (int32_t v = 0; v < database->vgroups; v ++) {
                SVGroup *vg = benchArrayGet(database->vgArray, v);
                if (vgId == vg->vgId) {
                    strcpy(vg->childTblName[vg->tbOffset], stbInfo->childTblName[i]);
                    vg->tbOffset ++;
                }
            }
        }
        close_bench_conn(conn);
    } else {
        a = ntables / threads;
        if (a < 1) {
            threads = (int32_t)ntables;
            a = 1;
        }
        b = 0;
        if (threads != 0) {
            b = ntables % threads;
        }
    }

    int32_t vgFrom = 0;
#else
    a = ntables / threads;
    if (a < 1) {
        threads = (int32_t)ntables;
        a = 1;
    }
    b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }
#endif   // TD_VER_COMPATIBLE_3_0_0_0


    pthread_t * pids = benchCalloc(1, threads * sizeof(pthread_t), true);
    threadInfo *infos = benchCalloc(1, threads * sizeof(threadInfo), true);

    for (int32_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->dbInfo = database;
        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->start_time = stbInfo->startTimestamp;
        pThreadInfo->totalInsertRows = 0;
        pThreadInfo->samplePos = 0;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
        if ((0 == stbInfo->interlaceRows)
                && (g_arguments->nthreads_auto)) {
            int32_t j;
            for (j = vgFrom; i < database->vgroups; j++) {
                SVGroup *vg = benchArrayGet(database->vgArray, j);
                if (0 == vg->tbCountPerVgId) {
                    continue;
                }
                pThreadInfo->vg = vg;
                pThreadInfo->start_table_from = 0;
                pThreadInfo->ntables = vg->tbCountPerVgId;
                pThreadInfo->end_table_to = vg->tbCountPerVgId-1;
                break;
            }
            vgFrom = j + 1;
        } else {
            pThreadInfo->start_table_from = tableFrom;
            pThreadInfo->ntables = i < b ? a + 1 : a;
            pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
            tableFrom = pThreadInfo->end_table_to + 1;
        }
#else
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
#endif  // TD_VER_COMPATIBLE_3_0_0_0
        pThreadInfo->delayList = benchArrayInit(1, sizeof(int64_t));
        switch (stbInfo->iface) {
            case REST_IFACE: {
                if (stbInfo->interlaceRows > 0) {
                    pThreadInfo->buffer = new_ds(0);
                } else {
                    pThreadInfo->buffer = benchCalloc(1, MAX_SQL_LEN, true);
                }
                int sockfd = createSockFd();
                if (sockfd < 0) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
                break;
            }
            case STMT_IFACE: {
                pThreadInfo->conn = init_bench_conn();
                if (NULL == pThreadInfo->conn) {
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                pThreadInfo->conn->stmt =
                    taos_stmt_init(pThreadInfo->conn->taos);
                if (NULL == pThreadInfo->conn->stmt) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
                    return -1;
                }
                if (taos_select_db(pThreadInfo->conn->taos, database->dbName)) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("taos select database(%s) failed\n",
                            database->dbName);
                    return -1;
                }
                if (!stbInfo->autoCreateTable) {
                    if (prepareStmt(stbInfo, pThreadInfo->conn->stmt, 0)) {
                        return -1;
                    }
                }

                pThreadInfo->bind_ts = benchCalloc(1, sizeof(int64_t), true);
                pThreadInfo->bind_ts_array =
                        benchCalloc(1, sizeof(int64_t) * g_arguments->reqPerReq, true);
                pThreadInfo->bindParams = benchCalloc(
                    1, sizeof(TAOS_MULTI_BIND) * (stbInfo->cols->size + 1), true);
                pThreadInfo->is_null = benchCalloc(1, g_arguments->reqPerReq, true);
                parseBufferToStmtBatch(stbInfo);

                break;
            }
            case SML_REST_IFACE: {
                int sockfd = createSockFd();
                if (sockfd < 0) {
                    free(pids);
                    free(infos);
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
            }
            case SML_IFACE: {
                pThreadInfo->conn = init_bench_conn();
                if (pThreadInfo->conn == NULL) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("%s() init connection failed\n", __func__);
                    return -1;
                }
                if (taos_select_db(pThreadInfo->conn->taos, database->dbName)) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("taos select database(%s) failed\n", database->dbName);
                    return -1;
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
                        if (generateRandData(
                                    stbInfo, pThreadInfo->sml_tags[t],
                                    stbInfo->lenOfCols + stbInfo->lenOfTags,
                                    stbInfo->tags, 1, true)) {
                            return -1;
                        }
                        debugPrint("pThreadInfo->sml_tags[%d]: %s\n", t,
                                   pThreadInfo->sml_tags[t]);
                    }
                    pThreadInfo->lines =
                            benchCalloc(g_arguments->reqPerReq, sizeof(char *), true);

                    for (int j = 0; j < g_arguments->reqPerReq; j++) {
                        pThreadInfo->lines[j] =
                                benchCalloc(1, pThreadInfo->max_sql_len, true);
                    }
                } else {
                    pThreadInfo->json_array = tools_cJSON_CreateArray();
                    pThreadInfo->sml_json_tags = tools_cJSON_CreateArray();
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        generateSmlJsonTags(
                                pThreadInfo->sml_json_tags, stbInfo,
                                pThreadInfo->start_table_from, t);
                    }
                    pThreadInfo->lines = (char **)benchCalloc(1, sizeof(char *), true);
                }
                break;
            }
            case TAOSC_IFACE: {
                pThreadInfo->conn = init_bench_conn();
                if (pThreadInfo->conn == NULL) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("%s() failed to connect\n", __func__);
                    return -1;
                }
                char command[SQL_BUFF_LEN];
                sprintf(command, "USE %s", database->dbName);
                if (queryDbExec(pThreadInfo->conn, command)) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("taos select database(%s) failed\n", database->dbName);
                    return -1;
                }
                if (stbInfo->interlaceRows > 0) {
                    pThreadInfo->buffer = new_ds(0);
                } else {
                    pThreadInfo->buffer = benchCalloc(1, MAX_SQL_LEN, true);
                }

                break;
            }
            default:
                break;
        }

    }

    infoPrint("Estimate memory usage: %.2fMB\n",
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

    int64_t end = toolsGetTimestampUs()+1;

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
                if (stbInfo->interlaceRows > 0) {
                    free_ds(&pThreadInfo->buffer);
                } else {
                    tmfree(pThreadInfo->buffer);
                }
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
                if (stbInfo->interlaceRows > 0) {
                    free_ds(&pThreadInfo->buffer);
                } else {
                    tmfree(pThreadInfo->buffer);
                }
                close_bench_conn(pThreadInfo->conn);
                break;
            default:
                break;
        }
        totalInsertRows += pThreadInfo->totalInsertRows;
        totalDelay += pThreadInfo->totalDelay;
        benchArrayAddBatch(total_delay_list, pThreadInfo->delayList->pData,
                pThreadInfo->delayList->size);
        tmfree(pThreadInfo->delayList);
    }
    qsort(total_delay_list->pData, total_delay_list->size,
            total_delay_list->elemSize, compare);

    free(pids);
    free(infos);

    succPrint("Spent %.6f seconds to insert rows: %" PRIu64
                          " with %d thread(s) into %s %.2f records/second\n",
                  (end - start)/1E6, totalInsertRows, threads,
                  database->dbName,
                  (double)(totalInsertRows / ((end - start)/1E6)));
    if (!total_delay_list->size) {
        benchArrayDestroy(total_delay_list);
        return -1;
    }

    succPrint("insert delay, "
            "min: %.4fms, "
            "avg: %.4fms, "
            "p90: %.4fms, "
            "p95: %.4fms, "
            "p99: %.4fms, "
            "max: %.4fms\n",
            *(int64_t *)(benchArrayGet(total_delay_list, 0))/1E3,
            (double)totalDelay/total_delay_list->size/1E3,
            *(int64_t *)(benchArrayGet(total_delay_list,
                    (int32_t)(total_delay_list->size * 0.9)))/1E3,
            *(int64_t *)(benchArrayGet(total_delay_list,
                    (int32_t)(total_delay_list->size * 0.95)))/1E3,
            *(int64_t *)(benchArrayGet(total_delay_list,
                    (int32_t)(total_delay_list->size * 0.99)))/1E3,
            *(int64_t *)(benchArrayGet(total_delay_list,
                    (int32_t)(total_delay_list->size - 1)))/1E3);

    benchArrayDestroy(total_delay_list);
    if (g_fail) {
        return -1;
    }
    return 0;
}

static int get_stb_inserted_rows(char* dbName, char* stbName, TAOS* taos) {
    int rows = 0;
    char command[SQL_BUFF_LEN];
    sprintf(command, "select count(*) from %s.%s", dbName, stbName);
    TAOS_RES* res = taos_query(taos, command);
    int code = taos_errno(res);
    if (code != 0) {
        errorPrint("Failed to execute <%s>, reason: %s\n",
                command, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }
    TAOS_ROW row = taos_fetch_row(res);
    if (row == NULL) {
        rows = 0;
    } else {
        rows = (int)*(int64_t*)row[0];
    }
    taos_free_result(res);
    return rows;
}

static void create_tsma(TSMA* tsma, SBenchConn* conn, char* stbName) {
    char command[SQL_BUFF_LEN];
    int len = snprintf(command, SQL_BUFF_LEN,
                       "create sma index %s on %s function(%s) "
                       "interval (%s) sliding (%s)",
                       tsma->name, stbName, tsma->func,
                       tsma->interval, tsma->sliding);
    if (tsma->custom) {
        snprintf(command + len, SQL_BUFF_LEN - len, " %s", tsma->custom);
    }
    int code = queryDbExec(conn, command);
    if (code == 0) {
        infoPrint("successfully create tsma with command <%s>\n", command);
    }
}

static void* create_tsmas(void* args) {
    tsmaThreadInfo* pThreadInfo = (tsmaThreadInfo*) args;
    int inserted_rows = 0;
    SBenchConn* conn = init_bench_conn();
    if (NULL == conn) {
        return NULL;
    }
    int finished = 0;
    if (taos_select_db(conn->taos, pThreadInfo->dbName)) {
        errorPrint("failed to use database (%s)\n", pThreadInfo->dbName);
        close_bench_conn(conn);
        return NULL;
    }
    while(finished < pThreadInfo->tsmas->size && inserted_rows >= 0) {
        inserted_rows = (int)get_stb_inserted_rows(
                pThreadInfo->dbName, pThreadInfo->stbName, conn->taos);
        for (int i = 0; i < pThreadInfo->tsmas->size; i++) {
            TSMA* tsma = benchArrayGet(pThreadInfo->tsmas, i);
            if (!tsma->done &&  inserted_rows >= tsma->start_when_inserted) {
                create_tsma(tsma, conn, pThreadInfo->stbName);
                tsma->done = true;
                finished++;
                break;
            }
        }
        toolsMsleep(10);
    }
    benchArrayDestroy(pThreadInfo->tsmas);
    close_bench_conn(conn);
    return NULL;
}

static int32_t createStream(SSTREAM* stream) {
    int32_t code = -1;
    char * command = benchCalloc(1, BUFFER_SIZE, false);
    snprintf(command, BUFFER_SIZE, "DROP STREAM IF EXISTS %s",
             stream->stream_name);
    infoPrint("%s\n", command);
    SBenchConn* conn = init_bench_conn();
    if (NULL == conn) {
        goto END;
    }

    code = queryDbExec(conn, command);
    int32_t trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds then re-drop stream %s\n",
                          g_arguments->trying_interval, stream->stream_name);
        toolsMsleep(g_arguments->trying_interval);
        code = queryDbExec(conn, command);
        if (trying != -1) {
            trying --;
        }
    }

    if (code) {
        close_bench_conn(conn);
        goto END;
    }

    memset(command, 0, BUFFER_SIZE);
    int pos = snprintf(command, BUFFER_SIZE,
            "CREATE STREAM IF NOT EXISTS %s ", stream->stream_name);
    if (stream->trigger_mode[0] != '\0') {
        pos += snprintf(command + pos, BUFFER_SIZE - pos,
                "TRIGGER %s ", stream->trigger_mode);
    }
    if (stream->watermark[0] != '\0') {
        pos += snprintf(command + pos, BUFFER_SIZE - pos,
                "WATERMARK %s ", stream->watermark);
    }
    snprintf(command + pos, BUFFER_SIZE - pos,
            "INTO %s as %s", stream->stream_stb, stream->source_sql);
    infoPrint("%s\n", command);

    code = queryDbExec(conn, command);
    trying = g_arguments->keep_trying;
    while (code && trying) {
        infoPrint("will sleep %"PRIu32" milliseconds "
                  "then re-create stream %s\n",
                  g_arguments->trying_interval, stream->stream_name);
        toolsMsleep(g_arguments->trying_interval);
        code = queryDbExec(conn, command);
        if (trying != -1) {
            trying --;
        }
    }

    if (code) {
        close_bench_conn(conn);
        goto END;
    }
    close_bench_conn(conn);
END:
    tmfree(command);
    return code;
}

int insertTestProcess() {

    prompt(0);

    encodeAuthBase64();
    for (int i = 0; i < g_arguments->databases->size; ++i) {
        if (REST_IFACE == g_arguments->iface) {
            if (0 != convertServAddr(g_arguments->iface,
                                     false,
                                     1)) {
                return -1;
            }
        }
        SDataBase * database = benchArrayGet(g_arguments->databases, i);

        if (database->drop && !(g_arguments->supplementInsert)) {
            if (database->superTbls) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
                if (stbInfo && (REST_IFACE == stbInfo->iface)) {
                    if (0 != convertServAddr(stbInfo->iface,
                                             stbInfo->tcpTransfer,
                                             stbInfo->lineProtocol)) {
                        return -1;
                    }
                }
            }
            if (createDatabase(database)) {
                return -1;
            }
        }
    }
    for (int i = 0; i < g_arguments->databases->size; ++i) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (int j = 0; j < database->superTbls->size; ++j) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                if (stbInfo->iface != SML_IFACE && stbInfo->iface != SML_REST_IFACE) {
                    if (getSuperTableFromServer(database, stbInfo)) {
                        if (createSuperTable(database, stbInfo)) return -1;
                    }
                }
                if (0 != prepareSampleData(database, stbInfo)) {
                    return -1;
                }
            }
        }

    }

    if (g_arguments->taosc_version == 3) {
        for (int i = 0; i < g_arguments->databases->size; i++) {
            SDataBase* database = benchArrayGet(g_arguments->databases, i);
            if (database->superTbls) {
                for (int j = 0; j < database->superTbls->size; ++j) {
                    SSuperTable* stbInfo = benchArrayGet(database->superTbls, j);
                    if (stbInfo->tsmas == NULL) {
                        continue;
                    }
                    if (stbInfo->tsmas->size > 0) {
                        tsmaThreadInfo* pThreadInfo =
                            benchCalloc(1, sizeof(tsmaThreadInfo), true);
                        pthread_t tsmas_pid = {0};
                        pThreadInfo->dbName = database->dbName;
                        pThreadInfo->stbName = stbInfo->stbName;
                        pThreadInfo->tsmas = stbInfo->tsmas;
                        pthread_create(&tsmas_pid, NULL, create_tsmas, pThreadInfo);
                    }
                }
            }
        }
    }

    if (createChildTables()) return -1;

    if (g_arguments->taosc_version == 3) {
        for (int j = 0; j < g_arguments->streams->size; ++j) {
            SSTREAM * stream = benchArrayGet(g_arguments->streams, j);
            if (stream->drop) {
                if (createStream(stream)) {
                    return -1;
                }
            }
        }
    }

    // create sub threads for inserting data
    for (int i = 0; i < g_arguments->databases->size; i++) {
        SDataBase * database = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (uint64_t j = 0; j < database->superTbls->size; j++) {
                SSuperTable * stbInfo = benchArrayGet(database->superTbls, j);
                if (stbInfo->insertRows == 0) {
                    continue;
                }
                prompt(stbInfo->non_stop);
                if (startMultiThreadInsertData(database, stbInfo)) {
                    return -1;
                }
            }
        }
    }
    return 0;
}
