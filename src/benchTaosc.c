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

int prepare_taosc(threadInfo* pThreadInfo, char* dbName) {
    pThreadInfo->conn = init_bench_conn();
    if (pThreadInfo->conn == NULL) {
        return -1;
    }
    if (taos_select_db(pThreadInfo->conn->taos, dbName)) {
        errorPrint(stderr, "select database (%s) failed\n", dbName);
        return -1;
    }
    return 0;
}

int prepare_rest(threadInfo* pThreadInfo) {
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
        errorPrint(stderr, "Could not create socket : %d",WSAGetLastError());
#endif
        errorPrint(stderr, "%s\n", "failed to create socket");
        return -1;
    }

    int retConn = connect(sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
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
    return 0;
}

void *sync_write_taosc_progressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout,
              "thread[%d] start taosc progressive inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampMs();
    int64_t   endTs;
    char    buffer[MAX_SQL_LEN];
    int32_t pos = 0;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *   tableName = stbInfo->childTblName[tableSeq];
        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;

        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            memset(buffer, 0, MAX_SQL_LEN);
            int32_t generated = 0;
            if (stbInfo->partialColumnNum == stbInfo->cols->size) {
                if (stbInfo->autoCreateTable) {
                    len = snprintf(buffer, MAX_SQL_LEN,
                            "%s %s using %s tags (%s) values ",
                            STR_INSERT_INTO, tableName, stbInfo->stbName,
                            stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq);
                 } else {
                    len = snprintf(buffer, MAX_SQL_LEN,
                            "%s %s values ", STR_INSERT_INTO, tableName);
                }
            } else {
                if (stbInfo->autoCreateTable) {
                    len = snprintf(buffer, MAX_SQL_LEN,
                                "%s %s (%s) using %s tags (%s) values ",
                                STR_INSERT_INTO, tableName,
                                stbInfo->partialColumnNameBuf, stbInfo->stbName,
                                stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq);
                } else {
                    len = snprintf(buffer, MAX_SQL_LEN, "%s %s (%s) values ",
                                    STR_INSERT_INTO, tableName, stbInfo->partialColumnNameBuf);
                }
            }

            for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                if (stbInfo->useSampleTs && !stbInfo->random_data_source) {
                    len += snprintf(buffer + len, MAX_SQL_LEN - len, "(%s)",
                                    stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols);
                } else {
                    len += snprintf(buffer + len, MAX_SQL_LEN - len,
                                    "(%" PRId64 ",%s)", timestamp,
                                    stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols);
                }
                pos++;
                if (pos >= g_arguments->prepared_rand) {
                    pos = 0;
                }
                timestamp += stbInfo->timestamp_step;
                if (stbInfo->disorderRatio > 0) {
                    int rand_num = taosRandom() % 100;
                    if (rand_num < stbInfo->disorderRatio) {
                        timestamp -= (taosRandom() % stbInfo->disorderRange);
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
            if (!stbInfo->non_stop) {
                i += generated;
            }
            // only measure insert
            startTs = toolsGetTimestampUs();
            if (execInsert(pThreadInfo, generated)) {
                g_fail = true;
                goto free_of_progressive;
                
            }
            endTs = toolsGetTimestampUs();
            pThreadInfo->totalInsertRows += generated;

            int64_t delay = endTs - startTs;
            performancePrint(stdout, "insert execution time is %.6f s\n", delay/1E6);
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
                  "thread[%d] completed total inserted rows: %" PRIu64
                          ", %.2f records/second\n",
                  pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                  (double)(pThreadInfo->totalInsertRows /
                           ((double)pThreadInfo->totalDelay / 1000000.0)));
    return NULL;
}

void *sync_write_taosc_interlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout,
              "thread[%d] start taosc interlace inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    int64_t pos = 0;
    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampMs();
    int64_t   endTs = toolsGetTimestampMs();
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
            if (i == 0) {
                len = snprintf(pThreadInfo->buffer,
                    strlen(STR_INSERT_INTO) + 1, "%s",
                    STR_INSERT_INTO);
            }
            if (stbInfo->partialColumnNum == stbInfo->cols->size) {
                if (stbInfo->autoCreateTable) {
                    len += snprintf(pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s using `%s` tags (%s) values ",
                                tableName, stbInfo->stbName,
                                stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq);
                } else {
                    len += snprintf(pThreadInfo->buffer + len,
                                    pThreadInfo->max_sql_len - len,
                                    "%s values", tableName);
                }
            } else {
                if (stbInfo->autoCreateTable) {
                    len += snprintf(pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s (%s) using `%s` tags (%s) values ",
                                tableName, stbInfo->partialColumnNameBuf, stbInfo->stbName,
                                stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq);
                } else {
                    len += snprintf(pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s (%s) values", tableName,
                                stbInfo->partialColumnNameBuf);
                }
            }

            for (int64_t j = 0; j < interlaceRows; ++j) {
                len += snprintf(pThreadInfo->buffer + len,
                            pThreadInfo->max_sql_len - len, "(%" PRId64 ",%s)",
                            timestamp, stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols);
                generated++;
                pos++;
                if (pos >= g_arguments->prepared_rand) {
                    pos = 0;
                }
                timestamp += stbInfo->timestamp_step;
                if (stbInfo->disorderRatio > 0) {
                    int rand_num = taosRandom() % 100;
                    if (rand_num < stbInfo->disorderRatio) {
                        timestamp -= (taosRandom() % stbInfo->disorderRange);
                    }
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
        memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
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
