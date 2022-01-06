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

void selectAndGetResult(threadInfo *pThreadInfo, char *command) {
    if (0 == strncasecmp(g_queryInfo.queryMode, "taosc", strlen("taosc"))) {
        TAOS_RES *res = taos_query(pThreadInfo->taos, command);
        if (res == NULL || taos_errno(res) != 0) {
            errorPrint("failed to execute sql:%s, reason:%s\n", command,
                       taos_errstr(res));
            taos_free_result(res);
            return;
        }

        fetchResult(res, pThreadInfo);
        taos_free_result(res);

    } else if (0 ==
               strncasecmp(g_queryInfo.queryMode, "rest", strlen("rest"))) {
        int retCode = postProceSql(g_queryInfo.host, g_queryInfo.port, command,
                                   pThreadInfo);
        if (0 != retCode) {
            printf("====restful return fail, threadID[%d]\n",
                   pThreadInfo->threadID);
        }

    } else {
        errorPrint("unknown query mode: %s\n", g_queryInfo.queryMode);
    }
}

void *specifiedTableQuery(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    int32_t *   code = calloc(1, sizeof(int32_t));
    *code = -1;
    prctl(PR_SET_NAME, "specTableQuery");

    uint64_t st = 0;
    uint64_t et = 0;

    uint64_t queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;

    uint64_t totalQueried = 0;
    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();

    if (g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq][0] !=
        '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d",
                g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq],
                pThreadInfo->threadID);
    }

    while (queryTimes--) {
        if (g_queryInfo.specifiedQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.specifiedQueryInfo.queryInterval) {
            taosMsleep((int32_t)(g_queryInfo.specifiedQueryInfo.queryInterval -
                                 (et - st)));  // ms
        }

        st = taosGetTimestampMs();

        selectAndGetResult(
            pThreadInfo,
            g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq]);

        et = taosGetTimestampMs();
        infoPrint("thread[%d] use %s complete <%s>, Spent %10.3f s\n",
                  pThreadInfo->threadID, g_queryInfo.queryMode,
                  g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq],
                  (et - st) / 1000.0);

        totalQueried++;
        g_queryInfo.specifiedQueryInfo.totalQueried++;

        uint64_t currentPrintTime = taosGetTimestampMs();
        uint64_t endTs = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            debugPrint("%s() LN%d, endTs=%" PRIu64 " ms, startTs=%" PRIu64
                       " ms\n",
                       __func__, __LINE__, endTs, startTs);
            infoPrint("thread[%d] has currently completed queries: %" PRIu64
                      ", QPS: %10.6f\n",
                      pThreadInfo->threadID, totalQueried,
                      (double)(totalQueried / ((endTs - startTs) / 1000.0)));
            lastPrintTime = currentPrintTime;
        }
    }
    *code = 0;
    return code;
}

void *superTableQuery(void *sarg) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    char *sqlstr = calloc(1, BUFFER_SIZE);
    if (NULL == sqlstr) {
        errorPrint("%s", "failed to allocate memory\n");
        goto free_of_super_query;
    }

    threadInfo *pThreadInfo = (threadInfo *)sarg;
    prctl(PR_SET_NAME, "superTableQuery");

    uint64_t st = 0;
    uint64_t et = (int64_t)g_queryInfo.superQueryInfo.queryInterval;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    uint64_t totalQueried = 0;
    uint64_t startTs = taosGetTimestampMs();

    uint64_t lastPrintTime = taosGetTimestampMs();
    while (queryTimes--) {
        if (g_queryInfo.superQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.superQueryInfo.queryInterval) {
            taosMsleep((int32_t)(g_queryInfo.superQueryInfo.queryInterval -
                                 (et - st)));
        }

        st = taosGetTimestampMs();
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
                selectAndGetResult(pThreadInfo, sqlstr);

                totalQueried++;
                g_queryInfo.superQueryInfo.totalQueried++;

                int64_t currentPrintTime = taosGetTimestampMs();
                int64_t endTs = taosGetTimestampMs();
                if (currentPrintTime - lastPrintTime > 30 * 1000) {
                    infoPrint(
                        "thread[%d] has currently completed queries: %" PRIu64
                        ", QPS: %10.3f\n",
                        pThreadInfo->threadID, totalQueried,
                        (double)(totalQueried / ((endTs - startTs) / 1000.0)));
                    lastPrintTime = currentPrintTime;
                }
            }
        }
        et = taosGetTimestampMs();
        infoPrint(
            "thread[%d] complete all sqls to allocate all sub-tables[%" PRIu64
            " - %" PRIu64 "] once queries duration:%.4fs\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to, (double)(et - st) / 1000.0);
    }
    *code = 0;
free_of_super_query:
    tmfree(sqlstr);
    return code;
}

int queryTestProcess(SArguments *argument) {
    printfQueryMeta(argument);
    if (init_taos_list(argument->pool, g_args.nthreads_pool)) {
        return -1;
    }
    TAOS *taos = select_one_from_pool(argument->pool, g_queryInfo.dbName);
    if (taos == NULL) {
        return -1;
    }

    if (0 != g_queryInfo.superQueryInfo.sqlCount) {
        char cmd[SQL_BUFF_LEN] = "\0";
        snprintf(cmd, SQL_BUFF_LEN, "select count(tbname) from %s.%s",
                 g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
        TAOS_RES *res = taos_query(taos, cmd);
        int32_t   code = taos_errno(res);
        if (code) {
            errorPrint("failed to count child table name: %s. reason: %s\n",
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
            calloc(g_queryInfo.superQueryInfo.childTblCount, sizeof(char *));
        if (getAllChildNameOfSuperTable(
                taos, g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName,
                g_queryInfo.superQueryInfo.childTblName,
                g_queryInfo.superQueryInfo.childTblCount)) {
            return -1;
        }
    }

    prompt(argument);

    if (argument->debug_print) {
        printfQuerySystemInfo(taos);
    }

    if (0 == strncasecmp(g_queryInfo.queryMode, "rest", strlen("rest"))) {
        if (convertHostToServAddr(g_queryInfo.host, g_queryInfo.port,
                                  &(g_queryInfo.serv_addr)) != 0) {
            errorPrint("%s", "convert host to server address\n");
            return -1;
        }
    }

    pthread_t * pids = NULL;
    threadInfo *infos = NULL;
    //==== create sub threads for query from specify table
    int      nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqlCount;

    uint64_t startTs = taosGetTimestampMs();

    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        pids = calloc(1, nConcurrent * nSqlCount * sizeof(pthread_t));
        infos = calloc(1, nConcurrent * nSqlCount * sizeof(threadInfo));
        for (uint64_t i = 0; i < nSqlCount; i++) {
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t    seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = (int)seq;
                pThreadInfo->querySeq = i;

                if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
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
                        errorPrint("Could not create socket : %d",
                                   WSAGetLastError());
#endif
                        debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__,
                                   sockfd);
                        ERROR_EXIT("opening socket");
                    }

                    int retConn = connect(
                        sockfd, (struct sockaddr *)&(g_queryInfo.serv_addr),
                        sizeof(struct sockaddr));
                    if (retConn < 0) {
                        ERROR_EXIT("connecting");
                    }
                    pThreadInfo->sockfd = sockfd;
                } else {
                    pThreadInfo->taos = select_one_from_pool(
                        argument->pool, g_queryInfo.dbName);
                }

                pthread_create(pids + seq, NULL, specifiedTableQuery,
                               pThreadInfo);
            }
        }
    } else {
        g_queryInfo.specifiedQueryInfo.concurrent = 0;
    }
    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        for (int i = 0; i < nConcurrent; i++) {
            for (int j = 0; j < nSqlCount; j++) {
                void *result;
                pthread_join(pids[i * nSqlCount + j], &result);
                if (*(int32_t *)result) {
                    g_fail = true;
                }
                tmfree(result);
                if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
                    threadInfo *pThreadInfo = infos + i * nSqlCount + j;
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                }
            }
        }
    }

    tmfree((char *)pids);
    tmfree((char *)infos);

    pthread_t * pidsOfSub = NULL;
    threadInfo *infosOfSub = NULL;
    //==== create sub threads for query from all sub table of the super table
    if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
        (g_queryInfo.superQueryInfo.threadCnt > 0)) {
        pidsOfSub =
            calloc(1, g_queryInfo.superQueryInfo.threadCnt * sizeof(pthread_t));
        infosOfSub = calloc(
            1, g_queryInfo.superQueryInfo.threadCnt * sizeof(threadInfo));

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
            if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
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
                    errorPrint("Could not create socket : %d",
                               WSAGetLastError());
#endif
                    debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__,
                               sockfd);
                    ERROR_EXIT("opening socket");
                }

                int retConn =
                    connect(sockfd, (struct sockaddr *)&(g_queryInfo.serv_addr),
                            sizeof(struct sockaddr));
                debugPrint("%s() LN%d connect() return %d\n", __func__,
                           __LINE__, retConn);
                if (retConn < 0) {
                    ERROR_EXIT("connecting");
                }
                pThreadInfo->sockfd = sockfd;
            } else {
                pThreadInfo->taos =
                    select_one_from_pool(argument->pool, g_queryInfo.dbName);
            }
            pthread_create(pidsOfSub + i, NULL, superTableQuery, pThreadInfo);
        }

        g_queryInfo.superQueryInfo.threadCnt = threads;
    } else {
        g_queryInfo.superQueryInfo.threadCnt = 0;
    }

    for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; i++) {
        void *result;
        pthread_join(pidsOfSub[i], &result);
        if (*(int32_t *)result) {
            g_fail = true;
        }
        tmfree(result);
        if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
            threadInfo *pThreadInfo = infosOfSub + i;
#ifdef WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        }
    }

    tmfree((char *)pidsOfSub);
    tmfree((char *)infosOfSub);

    if (g_fail) {
        return -1;
    }

    //  // workaround to use separate taos connection;
    uint64_t endTs = taosGetTimestampMs();

    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried +
                            g_queryInfo.superQueryInfo.totalQueried;

    infoPrint("completed total queries: %" PRIu64
              ", the QPS of all threads: %10.3f\n\n",
              totalQueried,
              (double)(totalQueried / ((endTs - startTs) / 1000.0)));
    return 0;
}