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
    if (g_arguments->db->superTbls->iface == REST_IFACE) {
        int retCode = postProceSql(command, pThreadInfo);
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

        fetchResult(res, pThreadInfo);
        taos_free_result(res);
    }
    return 0;
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
            taosMsleep((int32_t)(g_queryInfo.specifiedQueryInfo.queryInterval -
                                 (et - st)));  // ms
        }
        if (g_queryInfo.reset_query_cache) {
            queryDbExec(pThreadInfo->taos, "reset query cache", NO_INSERT_TYPE,
                        false);
        }

        st = toolsGetTimestampUs();
        debugPrint(stdout, "st: %" PRId64 "\n", st);
        if (selectAndGetResult(
                pThreadInfo,
                sql->command)) {
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
    debugPrint(stdout,
              "thread[%d] complete query <%s> %" PRIu64
              " times,"
              "insert delay, min: %5" PRIu64
              "us, avg: %5.2fus,"
              " p90: %5" PRIu64 "us, p95: %5" PRIu64 "us, p99: %5" PRIu64
              "us,"
              " max: %5" PRIu64 "us\n\n ",
              pThreadInfo->threadID,
              sql->command,
              queryTimes, minDelay, pThreadInfo->avg_delay,
              pThreadInfo->query_delay_list[(int32_t)(queryTimes * 0.9)],
              pThreadInfo->query_delay_list[(int32_t)(queryTimes * 0.95)],
              pThreadInfo->query_delay_list[(int32_t)(queryTimes * 0.99)], maxDelay);
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

int queryTestProcess() {
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
            errorPrint(stderr, "%s", "convert host to server address\n");
            return -1;
        }
    }

    pthread_t * pids = NULL;
    threadInfo *infos = NULL;
    //==== create sub threads for query from specify table
    int      nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqls->size;

    uint64_t startTs = toolsGetTimestampMs();

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
                        select_one_from_pool(g_arguments->db->dbName);
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
                if (g_arguments->db->superTbls->iface == REST_IFACE) {
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
        g_queryInfo.specifiedQueryInfo.concurrent = 0;
    }

    tmfree((char *)pids);
    tmfree((char *)infos);
    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqls->size; ++i) {
        SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
        tmfree(sql->command);
        tmfree(sql->delay_list);
    }
    benchArrayDestroy(g_queryInfo.specifiedQueryInfo.sqls);

    // start super table query
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
                    errorPrint(stderr, "Could not create socket : %d",
                               WSAGetLastError());
#endif
                    debugPrint(stdout, "sockfd=%d\n", sockfd);
                    ERROR_EXIT("opening socket");
                }

                int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
                debugPrint(stdout, "connect() return %d\n", retConn);
                if (retConn < 0) {
                    ERROR_EXIT("connecting");
                }
                pThreadInfo->sockfd = sockfd;
            } else {
                pThreadInfo->taos =
                    select_one_from_pool(g_arguments->db->dbName);
                if (pThreadInfo->taos == NULL) {
                    return -1;
                }
            }
            pthread_create(pidsOfSub + i, NULL, superTableQuery, pThreadInfo);
        }

        g_queryInfo.superQueryInfo.threadCnt = threads;
    } else {
        g_queryInfo.superQueryInfo.threadCnt = 0;
    }

    for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; i++) {
        pthread_join(pidsOfSub[i], NULL);
        if (g_arguments->db->superTbls->iface == REST_IFACE) {
            threadInfo *pThreadInfo = infosOfSub + i;
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
    for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; ++i) {
        g_queryInfo.superQueryInfo.totalQueried += infosOfSub[i].totalQueried;
    }

    tmfree((char *)pidsOfSub);
    tmfree((char *)infosOfSub);

    //  // workaround to use separate taos connection;
    uint64_t endTs = toolsGetTimestampMs();

    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried +
                            g_queryInfo.superQueryInfo.totalQueried;

    int64_t t = endTs - startTs;
    double  tInS = (double)t / 1000.0;

    for (int64_t i = 0; i < g_queryInfo.superQueryInfo.childTblCount; ++i) {
        tmfree(g_queryInfo.superQueryInfo.childTblName[i]);
    }
    tmfree(g_queryInfo.superQueryInfo.childTblName);

    debugPrint(stdout,
              "Spend %.4f second completed total queries: %" PRIu64
              ", the QPS of all threads: %10.3f\n\n",
              tInS, totalQueried, (double)totalQueried / tInS);
    return 0;
}
