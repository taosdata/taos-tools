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

#include <sys/ioctl.h>
#include "bench.h"

void selectAndGetResult(threadInfo *pThreadInfo, char *command) {
    if (g_arguments->db->superTbls->iface == REST_IFACE) {
        int retCode = postProceSql(command, pThreadInfo);
        if (0 != retCode) {
            errorPrint("====restful return fail, threadID[%d]\n",
                       pThreadInfo->threadID);
        }
    } else {
        TAOS_RES *res = taos_query(pThreadInfo->taos, command);
        if (res == NULL || taos_errno(res) != 0) {
            errorPrint("failed to execute sql:%s, reason:%s\n", command,
                       taos_errstr(res));
            taos_free_result(res);
            return;
        }

        fetchResult(res, pThreadInfo);
        taos_free_result(res);
    }
}

static void *specifiedTableQuery(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    int32_t *   code = calloc(1, sizeof(int32_t));
    *code = -1;
    prctl(PR_SET_NAME, "specTableQuery");

    uint64_t st = 0;
    uint64_t et = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t maxDelay = 0;
    uint64_t totalDelay = 0;
    int32_t  index = 0;

    uint64_t  queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    uint64_t *total_delay_list = calloc(queryTimes, sizeof(uint64_t));
    uint64_t  lastPrintTime = toolsGetTimestampMs();
    uint64_t  startTs = toolsGetTimestampMs();

    if (g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq][0] !=
        '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d",
                g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq],
                pThreadInfo->threadID);
    }

    while (index < queryTimes) {
        if (g_queryInfo.specifiedQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.specifiedQueryInfo.queryInterval) {
            taosMsleep((int32_t)(g_queryInfo.specifiedQueryInfo.queryInterval -
                                 (et - st)));  // ms
        }
        if (g_queryInfo.reset_query_cache) {
            queryDbExec(pThreadInfo->taos, "reset query cache", NO_INSERT_TYPE,
                        false);
        }

        st = toolsGetTimestampUs();
        debugPrint("st: %" PRId64 "\n", st);
        selectAndGetResult(
            pThreadInfo,
            g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq]);

        et = toolsGetTimestampUs();
        debugPrint("et: %" PRId64 "\n", et);
        uint64_t delay = et - st;
        total_delay_list[index] = delay;
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
            infoPrint("thread[%d] has currently completed queries: %" PRIu64
                      ", QPS: %10.6f\n",
                      pThreadInfo->threadID, pThreadInfo->totalQueried,
                      (double)(pThreadInfo->totalQueried /
                               ((endTs - startTs) / 1000.0)));
            lastPrintTime = currentPrintTime;
        }
    }
    if (g_arguments->debug_print) {
        for (int i = 0; i < queryTimes; ++i) {
            debugPrint("total_delay_list[%d]: %" PRIu64 "\n", i,
                       total_delay_list[i]);
        }
    }
    qsort(total_delay_list, queryTimes, sizeof(uint64_t), compare);
    if (g_arguments->debug_print) {
        for (int i = 0; i < queryTimes; ++i) {
            debugPrint("total_delay_list[%d]: %" PRIu64 "\n", i,
                       total_delay_list[i]);
        }
    }
    infoPrint("thread[%d] complete query <%s> %" PRIu64
              " times,"
              "insert delay, min: %5" PRIu64
              "us, avg: %5.2fus,"
              " p90: %5" PRIu64 "us, p95: %5" PRIu64 "us, p99: %5" PRIu64
              "us,"
              " max: %5" PRIu64 "us\n\n ",
              pThreadInfo->threadID,
              g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq],
              queryTimes, minDelay, (double)totalDelay / queryTimes,
              total_delay_list[(int32_t)(queryTimes * 0.9)],
              total_delay_list[(int32_t)(queryTimes * 0.95)],
              total_delay_list[(int32_t)(queryTimes * 0.99)], maxDelay);
    tmfree(total_delay_list);
    *code = 0;
    return code;
}

static void *superTableQuery(void *sarg) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    char *sqlstr = calloc(1, BUFFER_SIZE);

    threadInfo *pThreadInfo = (threadInfo *)sarg;
    prctl(PR_SET_NAME, "superTableQuery");

    uint64_t st = 0;
    uint64_t et = (int64_t)g_queryInfo.superQueryInfo.queryInterval;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    uint64_t startTs = toolsGetTimestampMs();

    uint64_t lastPrintTime = toolsGetTimestampMs();
    while (queryTimes--) {
        if (g_queryInfo.superQueryInfo.queryInterval &&
            (et - st) < (int64_t)g_queryInfo.superQueryInfo.queryInterval) {
            taosMsleep((int32_t)(g_queryInfo.superQueryInfo.queryInterval -
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
                selectAndGetResult(pThreadInfo, sqlstr);

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
        infoPrint(
            "thread[%d] complete all sqls to allocate all sub-tables[%" PRIu64
            " - %" PRIu64 "] once queries duration:%.4fs\n",
            pThreadInfo->threadID, pThreadInfo->start_table_from,
            pThreadInfo->end_table_to, (double)(et - st) / 1000.0);
    }
    *code = 0;
    tmfree(sqlstr);
    return code;
}

int queryTestProcess() {
    printfQueryMeta(g_arguments);
    if (init_taos_list()) return -1;
    encode_base_64();
    if (0 != g_queryInfo.superQueryInfo.sqlCount) {
        TAOS *taos = select_one_from_pool(g_arguments->db->dbName);
        char  cmd[SQL_BUFF_LEN] = "\0";
        snprintf(cmd, SQL_BUFF_LEN, "select count(tbname) from %s.%s",
                 g_arguments->db->dbName, g_queryInfo.superQueryInfo.stbName);
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
                taos, g_arguments->db->dbName,
                g_queryInfo.superQueryInfo.stbName,
                g_queryInfo.superQueryInfo.childTblName,
                g_queryInfo.superQueryInfo.childTblCount)) {
            return -1;
        }
    }

    prompt(0);

    if (g_arguments->db->superTbls->iface == REST_IFACE) {
        if (convertHostToServAddr(g_arguments->host,
                                  g_arguments->port + TSDB_PORT_HTTP,
                                  &(g_arguments->serv_addr)) != 0) {
            errorPrint("%s", "convert host to server address\n");
            return -1;
        }
    }

    pthread_t * pids = NULL;
    threadInfo *infos = NULL;
    //==== create sub threads for query from specify table
    int      nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqlCount;

    uint64_t startTs = toolsGetTimestampMs();

    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        pids = calloc(1, nConcurrent * nSqlCount * sizeof(pthread_t));
        infos = calloc(1, nConcurrent * nSqlCount * sizeof(threadInfo));
        for (uint64_t i = 0; i < nSqlCount; i++) {
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t    seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = (int)seq;
                pThreadInfo->querySeq = i;
                pThreadInfo->db_index = 0;
                pThreadInfo->stb_index = 0;

                if (g_arguments->db->superTbls->iface == REST_IFACE) {
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
                    debugPrint("sockfd=%d\n", sockfd);
                    if (sockfd < 0) {
#ifdef WINDOWS
                        errorPrint("Could not create socket : %d",
                                   WSAGetLastError());
#endif
                        errorPrint("failed to create socket, reason: %s\n",
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
                            "failed to connect with socket, reason: %s\n",
                            strerror(errno));
                        tmfree((char *)pids);
                        tmfree((char *)infos);
                        return -1;
                    }
                    pThreadInfo->sockfd = sockfd;
                } else {
                    pThreadInfo->taos =
                        select_one_from_pool(g_arguments->db->dbName);
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
                if (g_arguments->db->superTbls->iface == REST_IFACE) {
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
        for (int i = 0; i < nConcurrent; i++) {
            for (int j = 0; j < nSqlCount; j++) {
                g_queryInfo.specifiedQueryInfo.totalQueried +=
                    infos[i * nSqlCount + j].totalQueried;
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
            pThreadInfo->db_index = 0;
            pThreadInfo->stb_index = 0;
            pThreadInfo->start_table_from = tableFrom;
            pThreadInfo->ntables = i < b ? a + 1 : a;
            pThreadInfo->end_table_to =
                i < b ? tableFrom + a : tableFrom + a - 1;
            tableFrom = pThreadInfo->end_table_to + 1;
            if (g_arguments->db->superTbls->iface == REST_IFACE) {
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
                    debugPrint("sockfd=%d\n", sockfd);
                    ERROR_EXIT("opening socket");
                }

                int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
                debugPrint("connect() return %d\n", retConn);
                if (retConn < 0) {
                    ERROR_EXIT("connecting");
                }
                pThreadInfo->sockfd = sockfd;
            } else {
                pThreadInfo->taos =
                    select_one_from_pool(g_arguments->db->dbName);
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
        if (g_arguments->db->superTbls->iface == REST_IFACE) {
            threadInfo *pThreadInfo = infosOfSub + i;
#ifdef WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        }
    }
    for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; ++i) {
        g_queryInfo.superQueryInfo.totalQueried += infosOfSub[i].totalQueried;
    }

    tmfree((char *)pidsOfSub);
    tmfree((char *)infosOfSub);

    if (g_fail) {
        return -1;
    }

    //  // workaround to use separate taos connection;
    uint64_t endTs = toolsGetTimestampMs();

    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried +
                            g_queryInfo.superQueryInfo.totalQueried;

    int64_t t = endTs - startTs;
    double  tInS = (double)t / 1000.0;

    infoPrint("Spend %.4f second completed total queries: %" PRIu64
              ", the QPS of all threads: %10.3f\n\n",
              tInS, totalQueried, (double)totalQueried / tInS);
    return 0;
}
