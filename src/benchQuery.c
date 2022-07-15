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

int selectAndGetResult(threadInfo *pThreadInfo, char *command) {
    if (g_queryInfo.iface == REST_IFACE) {
        int retCode = postProceSql(command, g_queryInfo.dbName, 0, REST_IFACE, 0, false, pThreadInfo->sockfd, pThreadInfo->filePath);
        if (0 != retCode) {
            errorPrint(stderr, "====restful return fail, threadID[%d]\n",
                       pThreadInfo->threadID);
            return -1;
        }
    } else {
        TAOS_RES *res = taos_query(pThreadInfo->taos, command);
        if (res == NULL || taos_errno(res) != 0) {
            errorPrint(stderr, "failed to execute sql:%s, reason:%s\n", command,
                       taos_errstr(res));
            taos_free_result(res);
            return -1;
        }
        if (strlen(pThreadInfo->filePath) > 0) {
            fetchResult(res, pThreadInfo);
        }
        taos_free_result(res);
    }
    return 0;
}

static void *mixedQuery(void *sarg) {
    queryThreadInfo *pThreadInfo = (queryThreadInfo*)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "mixedQuery");
#endif
    int64_t lastPrintTs = toolsGetTimestampMs();
    int64_t st;
    int64_t et;
    uint64_t  queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    for (int i = pThreadInfo->start_sql; i <= pThreadInfo->end_sql; ++i) {
        SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
        for (int j = 0; j < queryTimes; ++j) {
            if (g_arguments->terminate) {
                return NULL;
            }
            if (g_queryInfo.reset_query_cache) {
                queryDbExec(pThreadInfo->taos, "reset query cache");
            }
            st = toolsGetTimestampUs();
            if (g_queryInfo.iface == REST_IFACE) {
                int retCode = postProceSql(sql->command, g_queryInfo.dbName, 0, g_queryInfo.iface, 0, false, pThreadInfo->sockfd, "");
                if (retCode) {
                    errorPrint(stderr, "thread[%d]: restful query <%s> failed\n", pThreadInfo->threadId, sql->command);
                    continue;
                }
            } else {
                TAOS_RES *res = taos_query(pThreadInfo->taos, sql->command);
                if (res == NULL) {
                    errorPrint(stderr, "thread[%d]: failed to execute sql :%s, reason: %s\n",pThreadInfo->threadId, sql->command, taos_errstr(res));
                    continue;
                }
            }
            et = toolsGetTimestampUs();
            int64_t* delay = benchCalloc(1, sizeof(int64_t), false);
            *delay = et - st;
            pThreadInfo->total_delay += (et - st);
            benchArrayPush(pThreadInfo->query_delay_list, delay);
            int64_t currentPrintTs = toolsGetTimestampMs();
            if (currentPrintTs - lastPrintTs > 10 * 1000) {
                infoPrint(stdout, "thread[%d] has currently complete query %d times\n", pThreadInfo->threadId, (int)pThreadInfo->query_delay_list->size);
                lastPrintTs = currentPrintTs;
            }
        }
    }
    return NULL;
}

static void *specifiedTableQuery(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "specTableQuery");
#endif
    uint64_t st = 0;
    uint64_t et = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t maxDelay = 0;
    uint64_t totalDelay = 0;
    int32_t  index = 0;

    uint64_t  queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    pThreadInfo->query_delay_list = benchCalloc(queryTimes, sizeof(uint64_t), false);
    uint64_t  lastPrintTime = toolsGetTimestampMs();
    uint64_t  startTs = toolsGetTimestampMs();

    SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, pThreadInfo->querySeq);

    if (sql->result[0] != '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d", sql->result, pThreadInfo->threadID);
    }

    while (index < queryTimes) {
        if (g_queryInfo.specifiedQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.specifiedQueryInfo.queryInterval) {
            toolsMsleep((int32_t)(g_queryInfo.specifiedQueryInfo.queryInterval -
                                 (et - st)));  // ms
        }
        if (g_queryInfo.reset_query_cache) {
            queryDbExec(pThreadInfo->taos, "reset query cache");
        }

        st = toolsGetTimestampUs();
        if (selectAndGetResult(pThreadInfo, sql->command)) {
            g_fail = true;
        }

        et = toolsGetTimestampUs();
        uint64_t delay = et - st;
        pThreadInfo->query_delay_list[index] = delay;
        index++;
        totalDelay += delay;
        if (delay > maxDelay) {
            maxDelay = delay;
        }
        if (delay < minDelay) {
            minDelay = delay;
        }

        pThreadInfo->totalQueried++;
        uint64_t currentPrintTime = toolsGetTimestampMs();
        uint64_t endTs = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            debugPrint(stdout,
                      "thread[%d] has currently completed queries: %" PRIu64
                      ", QPS: %10.6f\n",
                      pThreadInfo->threadID, pThreadInfo->totalQueried,
                      (double)(pThreadInfo->totalQueried /
                               ((endTs - startTs) / 1000.0)));
            lastPrintTime = currentPrintTime;
        }
    }
    qsort(pThreadInfo->query_delay_list, queryTimes, sizeof(uint64_t), compare);
    pThreadInfo->avg_delay = (double)totalDelay / queryTimes;
    return NULL;
}

static void *superTableQuery(void *sarg) {
    char *sqlstr = benchCalloc(1, BUFFER_SIZE, false);
    threadInfo *pThreadInfo = (threadInfo *)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "superTableQuery");
#endif

    uint64_t st = 0;
    uint64_t et = (int64_t)g_queryInfo.superQueryInfo.queryInterval;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    uint64_t startTs = toolsGetTimestampMs();

    uint64_t lastPrintTime = toolsGetTimestampMs();
    while (queryTimes--) {
        if (g_queryInfo.superQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.superQueryInfo.queryInterval) {
            toolsMsleep((int32_t)(g_queryInfo.superQueryInfo.queryInterval -
                                 (et - st)));
        }

        st = toolsGetTimestampMs();
        for (int i = (int)pThreadInfo->start_table_from;
             i <= pThreadInfo->end_table_to; i++) {
            for (int j = 0; j < g_queryInfo.superQueryInfo.sqlCount; j++) {
                memset(sqlstr, 0, BUFFER_SIZE);
                replaceChildTblName(g_queryInfo.superQueryInfo.sql[j], sqlstr,
                                    i);
                if (g_queryInfo.superQueryInfo.result[j][0] != '\0') {
                    sprintf(pThreadInfo->filePath, "%s-%d",
                            g_queryInfo.superQueryInfo.result[j],
                            pThreadInfo->threadID);
                }
                if (selectAndGetResult(pThreadInfo, sqlstr)){
                    g_fail = true;
                }

                pThreadInfo->totalQueried++;

                int64_t currentPrintTime = toolsGetTimestampMs();
                int64_t endTs = toolsGetTimestampMs();
                if (currentPrintTime - lastPrintTime > 30 * 1000) {
                    infoPrint(
                        stdout,
                        "thread[%d] has currently completed queries: %" PRIu64
                        ", QPS: %10.3f\n",
                        pThreadInfo->threadID, pThreadInfo->totalQueried,
                        (double)(pThreadInfo->totalQueried /
                                 ((endTs - startTs) / 1000.0)));
                    lastPrintTime = currentPrintTime;
                }
            }
        }
        et = toolsGetTimestampMs();
        infoPrint(
            stdout,
            "thread[%d] complete all sqls to allocate all sub-tables[%" PRIu64
            " - %" PRIu64 "] once queries duration:%.4fs\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to, (double)(et - st) / 1000.0);
    }
    tmfree(sqlstr);
    return NULL;
}

static int multi_thread_super_table_query(uint16_t iface, char* dbName) {
    int ret = -1;
    pthread_t * pidsOfSub = NULL;
    threadInfo *infosOfSub = NULL;
    //==== create sub threads for query from all sub table of the super table
    if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
        (g_queryInfo.superQueryInfo.threadCnt > 0)) {

        pidsOfSub = benchCalloc(1, g_queryInfo.superQueryInfo.threadCnt * sizeof(pthread_t), false);
        infosOfSub = benchCalloc(1, g_queryInfo.superQueryInfo.threadCnt * sizeof(threadInfo), false);

        int64_t ntables = g_queryInfo.superQueryInfo.childTblCount;
        int     threads = g_queryInfo.superQueryInfo.threadCnt;

        int64_t a = ntables / threads;
        if (a < 1) {
            threads = (int)ntables;
            a = 1;
        }

        int64_t b = 0;
        if (threads != 0) {
            b = ntables % threads;
        }

        uint64_t tableFrom = 0;
        for (int i = 0; i < threads; i++) {
            threadInfo *pThreadInfo = infosOfSub + i;
            pThreadInfo->threadID = i;
            pThreadInfo->db_index = 0;
            pThreadInfo->stb_index = 0;
            pThreadInfo->start_table_from = tableFrom;
            pThreadInfo->ntables = i < b ? a + 1 : a;
            pThreadInfo->end_table_to =
                    i < b ? tableFrom + a : tableFrom + a - 1;
            tableFrom = pThreadInfo->end_table_to + 1;
            if (iface == REST_IFACE) {
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
                    debugPrint(stdout, "sockfd=%d\n", sockfd);
                    goto OVER;
                }

                int retConn = connect(
                        sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                        sizeof(struct sockaddr));
                if (retConn < 0) {
                    errorPrint(stderr, "connect return %d\n", retConn);
                    goto OVER;
                }
                pThreadInfo->sockfd = sockfd;
            } else {
                pThreadInfo->taos =
                        select_one_from_pool(dbName);
                if (pThreadInfo->taos == NULL) {
                    goto OVER;
                }
            }
            pthread_create(pidsOfSub + i, NULL, superTableQuery, pThreadInfo);
        }
        g_queryInfo.superQueryInfo.threadCnt = threads;

        for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; i++) {
            pthread_join(pidsOfSub[i], NULL);
            if (iface == REST_IFACE) {
                threadInfo *pThreadInfo = infosOfSub + i;
#ifdef WINDOWS
                closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
                close(pThreadInfo->sockfd);
#endif
            }
            if (g_fail) {
                goto OVER;
            }
        }
        for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; ++i) {
            g_queryInfo.superQueryInfo.totalQueried += infosOfSub[i].totalQueried;
        }
    } else {
        return 0;
    }

    ret = 0;
OVER:
    tmfree((char *)pidsOfSub);
    tmfree((char *)infosOfSub);

    for (int64_t i = 0; i < g_queryInfo.superQueryInfo.childTblCount; ++i) {
        tmfree(g_queryInfo.superQueryInfo.childTblName[i]);
    }
    tmfree(g_queryInfo.superQueryInfo.childTblName);
    return ret;
}

static int multi_thread_specified_table_query(uint16_t iface, char* dbName) {

    pthread_t * pids = NULL;
    threadInfo *infos = NULL;
    //==== create sub threads for query from specify table
    int      nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqls->size;
    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        pids = benchCalloc(1, nConcurrent * nSqlCount * sizeof(pthread_t), false);
        infos = benchCalloc(1, nConcurrent * nSqlCount * sizeof(threadInfo), false);
        for (uint64_t i = 0; i < nSqlCount; i++) {
            SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t    seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = (int)seq;
                pThreadInfo->querySeq = i;
                pThreadInfo->db_index = 0;
                pThreadInfo->stb_index = 0;

                if (iface == REST_IFACE) {
#ifdef WINDOWS
                    WSADATA wsaData;
                    WSAStartup(MAKEWORD(2, 2), &wsaData);
                    SOCKET sockfd;
#else
                    int sockfd;
#endif

                    sockfd = socket(AF_INET, SOCK_STREAM, 0);
                    // int iMode = 1;
                    // ioctl(sockfd, FIONBIO, &iMode);
                    debugPrint(stdout, "sockfd=%d\n", sockfd);
                    if (sockfd < 0) {
#ifdef WINDOWS
                        errorPrint(stderr, "Could not create socket : %d",
                                   WSAGetLastError());
#endif
                        errorPrint(stderr,
                                   "failed to create socket, reason: %s\n",
                                   strerror(errno));
                        tmfree((char *)pids);
                        tmfree((char *)infos);
                        return -1;
                    }

                    int retConn = connect(
                            sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                            sizeof(struct sockaddr));
                    if (retConn < 0) {
                        errorPrint(
                                stderr,
                                "failed to connect with socket, reason: %s\n",
                                strerror(errno));
                        tmfree((char *)pids);
                        tmfree((char *)infos);
                        return -1;
                    }
                    pThreadInfo->sockfd = sockfd;
                } else {
                    pThreadInfo->taos =
                            select_one_from_pool(dbName);
                    if (pThreadInfo->taos == NULL) {
                        return -1;
                    }
                }

                pthread_create(pids + seq, NULL, specifiedTableQuery,
                               pThreadInfo);
            }
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t seq = i * nConcurrent + j;
                pthread_join(pids[seq], NULL);
                if (iface == REST_IFACE) {
                    threadInfo *pThreadInfo = infos + j * nSqlCount + i;
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                }
                if (g_fail) {
                    return -1;
                }
            }
            uint64_t query_times = g_queryInfo.specifiedQueryInfo.queryTimes;
            uint64_t total_query_times = query_times * nConcurrent;
            double avg_delay = 0.0;
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t    seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                avg_delay += pThreadInfo->avg_delay;
                for (uint64_t k = 0; k < g_queryInfo.specifiedQueryInfo.queryTimes; k++) {
                    sql->delay_list[j*query_times + k] = pThreadInfo->query_delay_list[k];
                }
                tmfree(pThreadInfo->query_delay_list);
            }
            avg_delay /= nConcurrent;
            qsort(sql->delay_list, total_query_times, sizeof(uint64_t), compare);
            infoPrint(stdout, "complete query <%s> with %d threads and %"PRIu64
                    " times for each, query delay min: %.6fs,"
                    "avg: %.6fs, p90: %.6fs, p95: %.6fs, p99: %.6fs, max: %.6fs\n",
                      sql->command, nConcurrent, query_times,
                      sql->delay_list[0]/1E6,
                      avg_delay/1E6,
                      sql->delay_list[(int32_t)(total_query_times * 0.90)]/1E6,
                      sql->delay_list[(int32_t)(total_query_times * 0.95)]/1E6,
                      sql->delay_list[(int32_t)(total_query_times * 0.99)]/1E6,
                      sql->delay_list[(int32_t)total_query_times - 1]/1E6);
        }
    } else {
        return 0;
    }

    tmfree((char *)pids);
    tmfree((char *)infos);
    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqls->size; ++i) {
        SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
        tmfree(sql->command);
        tmfree(sql->delay_list);
    }
    benchArrayDestroy(g_queryInfo.specifiedQueryInfo.sqls);
    return 0;
}

static int multi_thread_specified_mixed_query(uint16_t iface, char* dbName) {
    int code = -1;
    int thread = g_queryInfo.specifiedQueryInfo.concurrent;
    pthread_t * pids = benchCalloc(thread, sizeof(pthread_t), true);
    queryThreadInfo *infos = benchCalloc(thread, sizeof(queryThreadInfo), true);
    int total_sql_num = g_queryInfo.specifiedQueryInfo.sqls->size;
    int start_sql = 0;
    int a = total_sql_num / thread;
    if (a < 1) {
        thread = total_sql_num;
        a = 1;
    }
    int b = 0;
    if (thread != 0) {
        b = total_sql_num % thread;
    }
    for (int i = 0; i < thread; ++i) {
        queryThreadInfo *pQueryThreadInfo = infos + i;
        pQueryThreadInfo->threadId = i;
        pQueryThreadInfo->start_sql = start_sql;
        pQueryThreadInfo->end_sql = i < b ? start_sql + a : start_sql + a - 1;
        start_sql = pQueryThreadInfo->end_sql + 1;
        pQueryThreadInfo->total_delay = 0;
        pQueryThreadInfo->query_delay_list = benchArrayInit(1, sizeof(int64_t));
        if (iface == REST_IFACE) {
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
                errorPrint(stderr, "failed to create socket, reason: %s\n", strerror(errno));
                goto OVER;
            }

            int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
            if (retConn < 0) {
                errorPrint(stderr, "failed to connect with socket, reason: %s\n", strerror(errno));
                goto OVER;
            }
            pQueryThreadInfo->sockfd = sockfd;
        } else {
            pQueryThreadInfo->taos = select_one_from_pool(dbName);
            if (pQueryThreadInfo->taos == NULL) {
                goto OVER;
            }
        }
        pthread_create(pids + i, NULL, mixedQuery, pQueryThreadInfo);
    }

    int64_t start = toolsGetTimestampUs();
    for (int i = 0; i < thread; ++i) {
        pthread_join(pids[i], NULL);
    }
    int64_t end = toolsGetTimestampUs();

    //statistic
    BArray * delay_list = benchArrayInit(1, sizeof(int64_t));
    int64_t total_delay = 0;
    for (int i = 0; i < thread; ++i) {
        queryThreadInfo * pThreadInfo = infos + i;
        benchArrayAddBatch(delay_list, pThreadInfo->query_delay_list->pData, pThreadInfo->query_delay_list->size);
        total_delay += pThreadInfo->total_delay;
        tmfree(pThreadInfo->query_delay_list);
        if (iface == REST_IFACE) {
#ifdef  WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        } else {
            taos_close(pThreadInfo->taos);
        }
    }
    qsort(delay_list->pData, delay_list->size, delay_list->elemSize, compare);
    infoPrint(stdout,
              "spend %.6fs using "
              "%d threads complete query %d times,cd  "
              "min delay: %.6fs, "
              "avg delay: %.6fs, "
              "p90: %.6fs, "
              "p95: %.6fs, "
              "p99: %.6fs, "
              "max: %.6fs\n",
              (end - start)/1E6,
              thread, (int)delay_list->size,
              *(int64_t *)(benchArrayGet(delay_list, 0))/1E6,
              (double)total_delay/delay_list->size/1E6,
              *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size * 0.9)))/1E6,
              *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size * 0.95)))/1E6,
              *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size * 0.99)))/1E6,
              *(int64_t *)(benchArrayGet(delay_list, (int32_t)(delay_list->size - 1)))/1E6);
    benchArrayDestroy(delay_list);
    code = 0;
OVER:
    tmfree(pids);
    tmfree(infos);
    return code;
}

int queryTestProcess() {
    if (init_taos_list()) return -1;
    encode_base_64();
    prompt(0);
    if (g_queryInfo.iface == REST_IFACE) {
        if (convertHostToServAddr(g_arguments->host,
                                  g_arguments->port + TSDB_PORT_HTTP,
                                  &(g_arguments->serv_addr)) != 0) {
            errorPrint(stderr, "%s", "convert host to server address\n");
            return -1;
        }
    }

    if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
        (g_queryInfo.superQueryInfo.threadCnt > 0)) {
        TAOS *taos = select_one_from_pool(g_queryInfo.dbName);
        char  cmd[SQL_BUFF_LEN] = "\0";
        snprintf(cmd, SQL_BUFF_LEN, "select count(tbname) from %s.%s",
                 g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
        TAOS_RES *res = taos_query(taos, cmd);
        int32_t   code = taos_errno(res);
        if (code) {
            errorPrint(stderr,
                       "failed to count child table name: %s. reason: %s\n",
                       cmd, taos_errstr(res));
            taos_free_result(res);

            return -1;
        }
        TAOS_ROW    row = NULL;
        int         num_fields = taos_num_fields(res);
        TAOS_FIELD *fields = taos_fetch_fields(res);
        while ((row = taos_fetch_row(res)) != NULL) {
            if (0 == strlen((char *)(row[0]))) {
                errorPrint(stderr, "stable %s have no child table\n",
                           g_queryInfo.superQueryInfo.stbName);
                return -1;
            }
            char temp[256] = {0};
            taos_print_row(temp, row, fields, num_fields);
            g_queryInfo.superQueryInfo.childTblCount = (int64_t)atol(temp);
        }
        infoPrint(stdout, "%s's childTblCount: %" PRId64 "\n",
                  g_queryInfo.superQueryInfo.stbName,
                  g_queryInfo.superQueryInfo.childTblCount);
        taos_free_result(res);
        g_queryInfo.superQueryInfo.childTblName =
                benchCalloc(g_queryInfo.superQueryInfo.childTblCount, sizeof(char *), false);
        if (getAllChildNameOfSuperTable(
                taos, g_queryInfo.dbName,
                g_queryInfo.superQueryInfo.stbName,
                g_queryInfo.superQueryInfo.childTblName,
                g_queryInfo.superQueryInfo.childTblCount)) {
            tmfree(g_queryInfo.superQueryInfo.childTblName);
            return -1;
        }
    }

    uint64_t startTs = toolsGetTimestampMs();

    if (g_queryInfo.specifiedQueryInfo.mixed_query) {
        if (multi_thread_specified_mixed_query(g_queryInfo.iface, g_queryInfo.dbName)) {
            return -1;
        }
    } else {
        if (multi_thread_specified_table_query(g_queryInfo.iface, g_queryInfo.dbName)) {
            return -1;
        }
    }

    if (multi_thread_super_table_query(g_queryInfo.iface, g_queryInfo.dbName)) {
        return -1;
    }

    //  // workaround to use separate taos connection;
    uint64_t endTs = toolsGetTimestampMs();

    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried +
                            g_queryInfo.superQueryInfo.totalQueried;

    int64_t t = endTs - startTs;
    double  tInS = (double)t / 1000.0;

    debugPrint(stdout,
              "Spend %.4f second completed total queries: %" PRIu64
              ", the QPS of all threads: %10.3f\n\n",
              tInS, totalQueried, (double)totalQueried / tInS);
    return 0;
}
