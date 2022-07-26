
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

static int stmt_prepare(SSuperTable *stbInfo, TAOS_STMT *stmt, uint64_t tableSeq) {
    int   len = 0;
    char *prepare = benchCalloc(1, BUFFER_SIZE, true);
    if (stbInfo->autoCreateTable) {
        len += sprintf(prepare + len,
                       "INSERT INTO ? USING `%s` TAGS (%s) VALUES(?",
                       stbInfo->stbName,
                       stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq);
    } else {
        len += sprintf(prepare + len, "INSERT INTO ? VALUES(?");
    }

    for (int col = 0; col < stbInfo->cols->size; col++) {
        len += sprintf(prepare + len, ",?");
    }
    sprintf(prepare + len, ")");
    if (g_arguments->prepared_rand < g_arguments->reqPerReq) {
        infoPrint(stdout,
                  "in stmt mode, batch size(%u) can not larger than prepared "
                  "sample data size(%" PRId64
                  "), restart with larger prepared_rand or batch size will be "
                  "auto set to %" PRId64 "\n",
                  g_arguments->reqPerReq, g_arguments->prepared_rand,
                  g_arguments->prepared_rand);
        g_arguments->reqPerReq = g_arguments->prepared_rand;
    }
    if (taos_stmt_prepare(stmt, prepare, strlen(prepare))) {
        errorPrint(stderr, "taos_stmt_prepare(%s) failed\n", prepare);
        tmfree(prepare);
        return -1;
    }
    tmfree(prepare);
    return 0;
}

int prepare_stmt(threadInfo* pThreadInfo, char* dbName, SSuperTable* stbInfo) {
    pThreadInfo->conn = init_bench_conn();
    if (taos_select_db(pThreadInfo->conn->taos, dbName)) {
        errorPrint(stderr, "select database (%s) failed\n", dbName);
        return -1;
    }
    pThreadInfo->conn->stmt = taos_stmt_init(pThreadInfo->conn->taos);
    if (NULL == pThreadInfo->conn->stmt) {
        errorPrint(stderr, "taos_stmt_init() failed, reason: %s\n", taos_errstr(NULL));
        return -1;
    }
    if (!stbInfo->autoCreateTable) {
        if (stmt_prepare(stbInfo, pThreadInfo->conn->stmt, 0)) {
            return -1;
        }
    }

    pThreadInfo->bind_ts = benchCalloc(1, sizeof(int64_t), true);
    pThreadInfo->bind_ts_array = benchCalloc(1, sizeof(int64_t) * g_arguments->reqPerReq, true);
    pThreadInfo->bindParams = benchCalloc(1, sizeof(TAOS_MULTI_BIND) * (stbInfo->cols->size + 1), true);
    pThreadInfo->is_null = benchCalloc(1, g_arguments->reqPerReq, true);
    return 0;
}

static int bindParamBatch(threadInfo *pThreadInfo, uint32_t batch, int64_t startTime) {
    TAOS_STMT *  stmt = pThreadInfo->conn->stmt;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint32_t     columnCount = stbInfo->cols->size;
    memset(pThreadInfo->bindParams, 0,
           (sizeof(TAOS_MULTI_BIND) * (columnCount + 1)));
    memset(pThreadInfo->is_null, 0, batch);

    for (int c = 0; c < columnCount + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);

        char data_type;

        if (c == 0) {
            data_type = TSDB_DATA_TYPE_TIMESTAMP;
            param->buffer_length = sizeof(int64_t);
            param->buffer = pThreadInfo->bind_ts_array;

        } else {
            Field * col = benchArrayGet(stbInfo->cols, c - 1);
            data_type = col->type;
            param->buffer = col->data;
            param->buffer_length = col->length;
            debugPrint(stdout, "col[%d]: type: %s, len: %d\n", c,
                       taos_convert_datatype_to_string(data_type),
                       col->length);
        }
        param->buffer_type = data_type;
        param->length = benchCalloc(batch, sizeof(int32_t), true);

        for (int b = 0; b < batch; b++) {
            param->length[b] = (int32_t)param->buffer_length;
        }
        param->is_null = pThreadInfo->is_null;
        param->num = batch;
    }

    for (uint32_t k = 0; k < batch; k++) {
        /* columnCount + 1 (ts) */
        if (stbInfo->disorderRatio) {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + getTSRandTail(stbInfo->timestamp_step, k,
                                          stbInfo->disorderRatio,
                                          stbInfo->disorderRange);
        } else {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + stbInfo->timestamp_step * k;
        }
    }

    if (taos_stmt_bind_param_batch(
            stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams)) {
        errorPrint(stderr, "taos_stmt_bind_param_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }

    for (int c = 0; c < stbInfo->cols->size + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);
        tmfree(param->length);
    }

    // if msg > 3MB, break
    if (taos_stmt_add_batch(stmt)) {
        errorPrint(stderr, "taos_stmt_add_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }
    return batch;
}


void *sync_write_stmt_progressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout,
              "thread[%d] start stmt progressive inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampMs();
    int64_t   endTs;

    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *   tableName = stbInfo->childTblName[tableSeq];
        int64_t  timestamp = pThreadInfo->start_time;
        if (stbInfo->autoCreateTable) {
            taos_stmt_close(pThreadInfo->conn->stmt);
            pThreadInfo->conn->stmt = taos_stmt_init(pThreadInfo->conn->taos);
            if (stmt_prepare(stbInfo, pThreadInfo->conn->stmt, tableSeq)) {
                g_fail = true;
                goto free_of_progressive;
            }
        }

        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            int32_t generated = 0;
            if (taos_stmt_set_tbname(pThreadInfo->conn->stmt, tableName)) {
                errorPrint(stderr, "taos_stmt_set_tbname(%s) failed," "reason: %s\n", tableName,
                                taos_stmt_errstr(pThreadInfo->conn->stmt));
                g_fail = true;
                goto free_of_progressive;
            }
            generated = bindParamBatch(pThreadInfo, (g_arguments->reqPerReq > (stbInfo->insertRows - i))
                                    ? (stbInfo->insertRows - i)
                                    : g_arguments->reqPerReq, timestamp);
            timestamp += generated * stbInfo->timestamp_step;
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
            pThreadInfo->totalInsertRows += generated;

            int64_t delay = endTs - startTs;
            performancePrint(stdout, "insert execution time is %.6f s\n",
                             delay / 1E6);

            int64_t * pDelay = benchCalloc(1, sizeof(int64_t), false);
            *pDelay = delay;
            benchArrayPush(pThreadInfo->delayList, pDelay);
            pThreadInfo->totalDelay += delay;

            int64_t currentPrintTime = toolsGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(stdout,
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
        infoPrint(stdout,
                  "thread[%d] completed inserted %" PRIu64" row(s), %.2f records/second\n",
                  pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                  (double)(pThreadInfo->totalInsertRows /
                           ((double)pThreadInfo->totalDelay / 1000000.0)));
    return NULL;
}

void *sync_write_stmt_interlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout,
              "thread[%d] start interlace inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampMs();
    int64_t   endTs = toolsGetTimestampMs();
    int32_t    generated = 0;
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
            if (taos_stmt_set_tbname(pThreadInfo->conn->stmt, tableName)) {
                errorPrint(stderr, "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                            tableName, taos_stmt_errstr(pThreadInfo->conn->stmt));
                g_fail = true;
                goto free_of_interlace;
            }

            generated = bindParamBatch(pThreadInfo, interlaceRows, timestamp);

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
        int64_t delay = endTs - startTs;
        performancePrint(stdout, "insert execution time is %10.2f ms\n",
                         delay / 1000.0);

        int64_t * pdelay = benchCalloc(1, sizeof(int64_t), false);
        *pdelay = delay;
        benchArrayPush(pThreadInfo->delayList, pdelay);
        pThreadInfo->totalDelay += delay;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(stdout,
                          "thread[%d] has currently inserted rows: %" PRIu64
                                  "\n",
                          pThreadInfo->threadID, pThreadInfo->totalInsertRows);
            lastPrintTime = currentPrintTime;
        }
    }
free_of_interlace:
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;

    infoPrint(stdout,
                  "thread[%d] completed total inserted rows: %" PRIu64
                          ", %.2f records/second\n",
                  pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                  (double)(pThreadInfo->totalInsertRows /
                           ((double)pThreadInfo->totalDelay / 1000000.0)));
    return NULL;
}
