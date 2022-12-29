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
extern int g_majorVersionOfClient;

int selectAndGetResult(threadInfo *pThreadInfo, char *command) {
    int ret = 0;
    uint32_t threadID = pThreadInfo->threadID;
    char dbName[TSDB_DB_NAME_LEN] = {0};
    tstrncpy(dbName, g_queryInfo.dbName, TSDB_DB_NAME_LEN);

    if (g_queryInfo.iface == REST_IFACE) {
        int retCode = postProceSql(command, g_queryInfo.dbName, 0, REST_IFACE,
                                   0, g_arguments->port, false,
                                   pThreadInfo->sockfd, pThreadInfo->filePath);
        if (0 != retCode) {
            errorPrint("====restful return fail, threadID[%u]\n",
                       threadID);
            ret = -1;
        }
    } else {
        TAOS *taos = pThreadInfo->conn->taos;
        if (taos_select_db(taos, g_queryInfo.dbName)) {
            errorPrint("thread[%u]: failed to select database(%s)\n",
                threadID, dbName);
            ret = -2;
        } else {
            TAOS_RES *res = taos_query(taos, command);
            if (res == NULL || taos_errno(res) != 0) {
                if (g_queryInfo.continue_if_fail) {
                    warnPrint("failed to execute sql:%s, reason:%s\n", command,
                            taos_errstr(res));
                } else {
                    errorPrint("failed to execute sql:%s, reason:%s\n", command,
                            taos_errstr(res));
                    ret = -1;
                }
            } else {
                if (strlen(pThreadInfo->filePath) > 0) {
                    fetchResult(res, pThreadInfo);
                }
            }
            taos_free_result(res);
        }
    }
    return ret;
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
                queryDbExec(pThreadInfo->conn, "reset query cache");
            }
            st = toolsGetTimestampUs();
            if (g_queryInfo.iface == REST_IFACE) {
                int retCode = postProceSql(sql->command, g_queryInfo.dbName,
                                           0, g_queryInfo.iface, 0, g_arguments->port,
                                           false, pThreadInfo->sockfd, "");
                if (retCode) {
                    errorPrint("thread[%d]: restful query <%s> failed\n",
                            pThreadInfo->threadId, sql->command);
                    continue;
                }
            } else {
                if (g_queryInfo.dbName != NULL) {
                    if (taos_select_db(pThreadInfo->conn->taos, g_queryInfo.dbName)) {
                        errorPrint("thread[%d]: failed to select database(%s)\n",
                                pThreadInfo->threadId, g_queryInfo.dbName);
                        return NULL;
                    }
                }
                TAOS_RES *res = taos_query(pThreadInfo->conn->taos, sql->command);
                if (res == NULL || taos_errno(res) != 0) {
                    if (g_queryInfo.continue_if_fail) {
                        warnPrint(
                                "thread[%d]: failed to execute sql :%s, "
                                "code: 0x%x, reason: %s\n",
                                pThreadInfo->threadId,
                                sql->command,
                                taos_errno(res), taos_errstr(res));
                    } else {
                        errorPrint(
                                "thread[%d]: failed to execute sql :%s, "
                                "code: 0x%x, reason: %s\n",
                                pThreadInfo->threadId,
                                sql->command,
                                taos_errno(res), taos_errstr(res));
                        if (TSDB_CODE_RPC_NETWORK_UNAVAIL ==
                                taos_errno(res)) {
                            return NULL;
                        }
                    }
                    continue;
                }
                taos_free_result(res);
            }
            et = toolsGetTimestampUs();
            int64_t* delay = benchCalloc(1, sizeof(int64_t), false);
            *delay = et - st;
            debugPrint("%s() LN%d, delay: %"PRId64"\n", __func__, __LINE__, *delay);

            pThreadInfo->total_delay += (et - st);
            benchArrayPush(pThreadInfo->query_delay_list, delay);
            int64_t currentPrintTs = toolsGetTimestampMs();
            if (currentPrintTs - lastPrintTs > 10 * 1000) {
                infoPrint("thread[%d] has currently complete query %d times\n",
                        pThreadInfo->threadId,
                        (int)pThreadInfo->query_delay_list->size);
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
    pThreadInfo->query_delay_list = benchCalloc(queryTimes,
            sizeof(uint64_t), false);
    uint64_t  lastPrintTime = toolsGetTimestampMs();
    uint64_t  startTs = toolsGetTimestampMs();

    SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls,
            pThreadInfo->querySeq);

    if (sql->result[0] != '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d",
                sql->result, pThreadInfo->threadID);
    }

    while (index < queryTimes) {
        if (g_queryInfo.specifiedQueryInfo.queryInterval
            && (et - st) <
                (int64_t)g_queryInfo.specifiedQueryInfo.queryInterval*1000) {
            toolsMsleep((int32_t)(
                        g_queryInfo.specifiedQueryInfo.queryInterval*1000
                        - (et - st)));  // ms
        }
        if (g_queryInfo.reset_query_cache) {
            queryDbExec(pThreadInfo->conn, "reset query cache");
        }

        st = toolsGetTimestampUs();
        int ret = selectAndGetResult(pThreadInfo, sql->command);
        if (ret) {
            g_fail = true;
        }

        et = toolsGetTimestampUs();
        int64_t delay = et - st;
        debugPrint("%s() LN%d, delay: %"PRId64"\n", __func__, __LINE__, delay);

        if (ret == 0) {
            pThreadInfo->query_delay_list[index] = delay;
            pThreadInfo->totalQueried++;
        }
        index++;
        totalDelay += delay;
        if (delay > maxDelay) {
            maxDelay = delay;
        }
        if (delay < minDelay) {
            minDelay = delay;
        }

        uint64_t currentPrintTime = toolsGetTimestampMs();
        uint64_t endTs = toolsGetTimestampMs();

        if ((ret == 0) && (currentPrintTime - lastPrintTime > 30 * 1000)) {
            debugPrint(
                    "thread[%d] has currently completed queries: %" PRIu64
                    ", QPS: %10.6f\n",
                    pThreadInfo->threadID, pThreadInfo->totalQueried,
                    (double)(pThreadInfo->totalQueried /
                        ((endTs - startTs) / 1000.0)));
            lastPrintTime = currentPrintTime;
        }

        if (-2 == ret) {
            toolsMsleep(1000);
            return NULL;
        }
    }
    qsort(pThreadInfo->query_delay_list, queryTimes,
            sizeof(uint64_t), compare);
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
    uint64_t et = (int64_t)g_queryInfo.superQueryInfo.queryInterval*1000;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    uint64_t startTs = toolsGetTimestampMs();

    uint64_t lastPrintTime = toolsGetTimestampMs();
    while (queryTimes--) {
        if (g_queryInfo.superQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.superQueryInfo.queryInterval*1000) {
            toolsMsleep((int32_t)(g_queryInfo.superQueryInfo.queryInterval*1000
                        - (et - st)));
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
            pThreadInfo->start_table_from = tableFrom;
            pThreadInfo->ntables = i < b ? a + 1 : a;
            pThreadInfo->end_table_to =
                    i < b ? tableFrom + a : tableFrom + a - 1;
            tableFrom = pThreadInfo->end_table_to + 1;
            if (iface == REST_IFACE) {
                int sockfd = createSockFd();
                if (sockfd < 0) {
                    goto OVER;
                }
                pThreadInfo->sockfd = sockfd;
            } else {
                pThreadInfo->conn = init_bench_conn();
                if (pThreadInfo->conn == NULL) {
                    goto OVER;
                }
            }
            pthread_create(pidsOfSub + i, NULL, superTableQuery, pThreadInfo);
        }
        g_queryInfo.superQueryInfo.threadCnt = threads;

        for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; i++) {
            pthread_join(pidsOfSub[i], NULL);
            threadInfo *pThreadInfo = infosOfSub + i;
            if (iface == REST_IFACE) {
                destroySockFd(pThreadInfo->sockfd);
            } else {
                close_bench_conn(pThreadInfo->conn);
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
                if (iface == REST_IFACE) {
                    int sockfd = createSockFd();
                    // int iMode = 1;
                    // ioctl(sockfd, FIONBIO, &iMode);
                    if (sockfd < 0) {
                        tmfree((char *)pids);
                        tmfree((char *)infos);
                        return -1;
                    }
                    pThreadInfo->sockfd = sockfd;
                } else {
                    pThreadInfo->conn = init_bench_conn();
                    if (pThreadInfo->conn == NULL) {
                        destroySockFd(pThreadInfo->sockfd);
                        tmfree((char *)pids);
                        tmfree((char *)infos);
                        return -1;
                    }
                }

                pthread_create(pids + seq, NULL, specifiedTableQuery,
                               pThreadInfo);
            }
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t seq = i * nConcurrent + j;
                pthread_join(pids[seq], NULL);
                threadInfo *pThreadInfo = infos + seq;
                if (iface == REST_IFACE) {
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                } else {
                    close_bench_conn(pThreadInfo->conn);
                }
                if (g_fail) {
                    tmfree(pThreadInfo->query_delay_list);
                }
            }

            if (g_fail) {
                tmfree((char *)pids);
                tmfree((char *)infos);
                return -1;
            }
            uint64_t query_times = g_queryInfo.specifiedQueryInfo.queryTimes;
            uint64_t total_query_times = query_times * nConcurrent;
            double avg_delay = 0.0;
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t    seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                avg_delay += pThreadInfo->avg_delay;
                for (uint64_t k = 0;
                        k < g_queryInfo.specifiedQueryInfo.queryTimes; k++) {
                    sql->delay_list[j*query_times + k] =
                        pThreadInfo->query_delay_list[k];
                }
                tmfree(pThreadInfo->query_delay_list);
            }
            avg_delay /= nConcurrent;
            qsort(sql->delay_list, total_query_times, sizeof(uint64_t), compare);
            infoPrintNoTimestamp("complete query with %d threads and %"PRIu64
                    " query delay "
                    "avg: \t%.6fs "
                    "min: \t%.6fs "
                    "max: \t%.6fs "
                    "p90: \t%.6fs "
                    "p95: \t%.6fs "
                    "p99: \t%.6fs "
                    "SQL command: %s"
                    "\n",
                      nConcurrent, query_times,
                      avg_delay/1E6,  /* avg */
                      sql->delay_list[0]/1E6, /* min */
                      sql->delay_list[(int32_t)total_query_times - 1]/1E6,  /*  max */
                      sql->delay_list[(int32_t)(total_query_times * 0.90)]/1E6, /*  p90 */
                      sql->delay_list[(int32_t)(total_query_times * 0.95)]/1E6, /*  p95 */
                      sql->delay_list[(int32_t)(total_query_times * 0.99)]/1E6,  /* p88 */
                      sql->command
                      );
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
            int sockfd = createSockFd();
            if (sockfd < 0) {
                goto OVER;
            }
            pQueryThreadInfo->sockfd = sockfd;
        } else {
            pQueryThreadInfo->conn = init_bench_conn();
            if (pQueryThreadInfo->conn == NULL) {
                goto OVER;
            }
        }
        pthread_create(pids + i, NULL, mixedQuery, pQueryThreadInfo);
    }

    int64_t start = toolsGetTimestampUs();
    for (int i = 0; i < thread; ++i) {
// temporary disabled       pthread_cancel(pids[i]);
        pthread_join(pids[i], NULL);
    }
    int64_t end = toolsGetTimestampUs();

    //statistic
    BArray * delay_list = benchArrayInit(1, sizeof(int64_t));
    int64_t total_delay = 0;
    for (int i = 0; i < thread; ++i) {
        queryThreadInfo * pThreadInfo = infos + i;
        benchArrayAddBatch(delay_list, pThreadInfo->query_delay_list->pData,
                pThreadInfo->query_delay_list->size);
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
            close_bench_conn(pThreadInfo->conn);
        }
    }
    qsort(delay_list->pData, delay_list->size, delay_list->elemSize, compare);
    if (delay_list->size) {
        infoPrint(
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
                *(int64_t *)(benchArrayGet(delay_list,
                                           (int32_t)(delay_list->size * 0.9)))/1E6,
                *(int64_t *)(benchArrayGet(delay_list,
                                           (int32_t)(delay_list->size * 0.95)))/1E6,
                *(int64_t *)(benchArrayGet(delay_list,
                                           (int32_t)(delay_list->size * 0.99)))/1E6,
                *(int64_t *)(benchArrayGet(delay_list,
                                           (int32_t)(delay_list->size - 1)))/1E6);
    } else {
        errorPrint("%s() LN%d, delay_list size: %"PRId64"\n",
                   __func__, __LINE__, (int64_t)delay_list->size);
    }
    benchArrayDestroy(delay_list);
    code = 0;
OVER:
    tmfree(pids);
    tmfree(infos);
    return code;
}

#define KILLID_LEN  20

void *queryKiller(void *arg) {
    char host[MAX_HOSTNAME_LEN] = {0};
    tstrncpy(host, g_arguments->host, MAX_HOSTNAME_LEN);

    while (true) {
        TAOS *taos = taos_connect(g_arguments->host, g_arguments->user,
                g_arguments->password, NULL, g_arguments->port);
        if (NULL == taos) {
            errorPrint("Slow query killer thread "
                    "failed to connect to TDengine server %s\n",
                    g_arguments->host);
            return NULL;
        }

        char command[TSDB_MAX_ALLOWED_SQL_LEN] =
            "SELECT kill_id,exec_usec,sql FROM performance_schema.perf_queries";
        TAOS_RES *res = taos_query(taos, command);
        int32_t code = taos_errno(res);
        if (code != 0) {
            errorPrint("%s execution failed. Reason: %s\n",
                    command, taos_errstr(res));
            taos_free_result(res);
        }

        TAOS_ROW row = NULL;
        while ((row = taos_fetch_row(res)) != NULL) {
            int32_t *lengths = taos_fetch_lengths(res);
            if (lengths[0] <= 0) {
                infoPrint("No valid query found by %s\n", command);
            } else {
                int64_t execUSec = *(int64_t*)row[1];

                if (execUSec > g_queryInfo.killQueryThreshold * 1000000) {
                    char sql[SQL_BUFF_LEN] = {0};
                    tstrncpy(sql, (char*)row[2],
                             min(strlen((char*)row[2]), SQL_BUFF_LEN));

                    char killId[KILLID_LEN] = {0};
                    tstrncpy(killId, (char*)row[0], KILLID_LEN);
                    char killCommand[KILLID_LEN + 15] = {0};
                    snprintf(killCommand, KILLID_LEN + 15, "KILL QUERY '%s'", killId);
                    TAOS_RES *resKill = taos_query(taos, killCommand);
                    int32_t codeKill = taos_errno(resKill);
                    if (codeKill != 0) {
                        errorPrint("%s execution failed. Reason: %s\n",
                                killCommand, taos_errstr(resKill));
                    } else {
                        infoPrint("%s succeed, sql: %s\n", killCommand, sql);
                    }

                    taos_free_result(resKill);
                }
            }
        }

        taos_free_result(res);
        taos_close(taos);
        toolsMsleep(g_queryInfo.killQueryInterval*1000);
    }

    return NULL;
}

int queryTestProcess() {
    prompt(0);

    if (REST_IFACE == g_queryInfo.iface) {
        encodeAuthBase64();
    }

    pthread_t pidKiller = {0};
    if (g_queryInfo.iface == TAOSC_IFACE && g_queryInfo.killQueryThreshold) {
        pthread_create(&pidKiller, NULL, queryKiller, NULL);
        pthread_join(pidKiller, NULL);
        toolsMsleep(1000);
    }

    if (g_queryInfo.iface == REST_IFACE) {
        if (convertHostToServAddr(g_arguments->host,
                    g_arguments->port + TSDB_PORT_HTTP,
                    &(g_arguments->serv_addr)) != 0) {
            errorPrint("%s", "convert host to server address\n");
            return -1;
        }
    }

    if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
            (g_queryInfo.superQueryInfo.threadCnt > 0)) {
        SBenchConn* conn = init_bench_conn();
        if (conn == NULL) {
            return -1;
        }
        char  cmd[SQL_BUFF_LEN] = "\0";
        if (3 == g_majorVersionOfClient) {
            snprintf(cmd, SQL_BUFF_LEN,
                    "SELECT COUNT(*) FROM( SELECT DISTINCT(TBNAME) FROM %s.%s)",
                    g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
        } else {
            snprintf(cmd, SQL_BUFF_LEN, "SELECT COUNT(TBNAME) FROM %s.%s",
                    g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
        }
        TAOS_RES *res = taos_query(conn->taos, cmd);
        int32_t   code = taos_errno(res);
        if (code) {
            errorPrint(
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
                errorPrint("stable %s have no child table\n",
                        g_queryInfo.superQueryInfo.stbName);
                return -1;
            }
            char temp[256] = {0};
            taos_print_row(temp, row, fields, num_fields);
            g_queryInfo.superQueryInfo.childTblCount = (int64_t)atol(temp);
        }
        infoPrint("%s's childTblCount: %" PRId64 "\n",
                g_queryInfo.superQueryInfo.stbName,
                g_queryInfo.superQueryInfo.childTblCount);
        taos_free_result(res);
        g_queryInfo.superQueryInfo.childTblName =
            benchCalloc(g_queryInfo.superQueryInfo.childTblCount,
                    sizeof(char *), false);
        if (getAllChildNameOfSuperTable(
                    conn->taos, g_queryInfo.dbName,
                    g_queryInfo.superQueryInfo.stbName,
                    g_queryInfo.superQueryInfo.childTblName,
                    g_queryInfo.superQueryInfo.childTblCount)) {
            tmfree(g_queryInfo.superQueryInfo.childTblName);
            close_bench_conn(conn);
            return -1;
        }
        close_bench_conn(conn);
    }

    uint64_t startTs = toolsGetTimestampMs();

    if (g_queryInfo.specifiedQueryInfo.mixed_query) {
        if (multi_thread_specified_mixed_query(g_queryInfo.iface,
                    g_queryInfo.dbName)) {
            return -1;
        }
    } else {
        if (multi_thread_specified_table_query(g_queryInfo.iface,
                    g_queryInfo.dbName)) {
            return -1;
        }
    }

    if (multi_thread_super_table_query(g_queryInfo.iface,
                g_queryInfo.dbName)) {
        return -1;
    }

    //  // workaround to use separate taos connection;
    uint64_t endTs = toolsGetTimestampMs();

    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried
        + g_queryInfo.superQueryInfo.totalQueried;

    int64_t t = endTs - startTs;
    double  tInS = (double)t / 1000.0;

    debugPrint(
            "Spend %.4f second completed total queries: %" PRIu64
            ", the QPS of all threads: %10.3f\n\n",
            tInS, totalQueried, (double)totalQueried / tInS);

    return 0;
}
