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

void stable_sub_callback(TAOS_SUB *tsub, TAOS_RES *res, void *param, int code) {
    if (res == NULL || taos_errno(res) != 0) {
        errorPrint(
            "failed to subscribe result, code:%d, reason:%s\n",
             code, taos_errstr(res));
        return;
    }

    if (param) fetchResult(res, (threadInfo *)param);
    // tao_unsubscribe() will free result.
}

void specified_sub_callback(TAOS_SUB *tsub, TAOS_RES *res, void *param,
                            int code) {
    if (res == NULL || taos_errno(res) != 0) {
        errorPrint(
            "failed to subscribe result, code:%d, reason:%s\n",
             code, taos_errstr(res));
        return;
    }

    if (param) fetchResult(res, (threadInfo *)param);
    // tao_unsubscribe() will free result.
}

TAOS_SUB *subscribeImpl(QUERY_CLASS class, threadInfo *pThreadInfo, char *sql,
                        char *topic, bool restart, uint64_t interval) {
    TAOS_SUB *tsub = NULL;

    if ((SPECIFIED_CLASS == class) &&
        (ASYNC_MODE == g_queryInfo.specifiedQueryInfo.asyncMode)) {
        tsub = taos_subscribe(
            pThreadInfo->taos, restart, topic, sql, specified_sub_callback,
            (void *)pThreadInfo,
            (int)g_queryInfo.specifiedQueryInfo.subscribeInterval);
    } else if ((STABLE_CLASS == class) &&
               (ASYNC_MODE == g_queryInfo.superQueryInfo.asyncMode)) {
        tsub =
            taos_subscribe(pThreadInfo->taos, restart, topic, sql,
                           stable_sub_callback, (void *)pThreadInfo,
                           (int)g_queryInfo.superQueryInfo.subscribeInterval);
    } else {
        tsub = taos_subscribe(pThreadInfo->taos, restart, topic, sql, NULL,
                              NULL, (int)interval);
    }

    if (tsub == NULL) {
        errorPrint("failed to create subscription. topic:%s, sql:%s\n", topic,
                   sql);
        return NULL;
    }

    return tsub;
}

void *specifiedSubscribe(void *sarg) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    //  TAOS_SUB*  tsub = NULL;

    prctl(PR_SET_NAME, "specSub");

    pThreadInfo->taos = select_one_from_pool(&g_taos_pool, g_queryInfo.dbName);
    if (pThreadInfo->taos == NULL) {
        goto free_of_specified_subscribe;
    }

    char sqlStr[TSDB_DB_NAME_LEN + 5];
    sprintf(sqlStr, "USE %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        goto free_of_specified_subscribe;
    }

    sprintf(g_queryInfo.specifiedQueryInfo.topic[pThreadInfo->threadID],
            "taosbenchmark-subscribe-%" PRIu64 "-%d", pThreadInfo->querySeq,
            pThreadInfo->threadID);
    if (g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq][0] !=
        '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d",
                g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq],
                pThreadInfo->threadID);
    }
    g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID] = subscribeImpl(
        SPECIFIED_CLASS, pThreadInfo,
        g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq],
        g_queryInfo.specifiedQueryInfo.topic[pThreadInfo->threadID],
        g_queryInfo.specifiedQueryInfo.subscribeRestart,
        g_queryInfo.specifiedQueryInfo.subscribeInterval);
    if (NULL == g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID]) {
        goto free_of_specified_subscribe;
    }

    // start loop to consume result

    g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] = 0;
    while ((g_queryInfo.specifiedQueryInfo
                .endAfterConsume[pThreadInfo->querySeq] == -1) ||
           (g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] <
            g_queryInfo.specifiedQueryInfo
                .endAfterConsume[pThreadInfo->querySeq])) {
        printf("consumed[%d]: %d, endAfterConsum[%" PRId64 "]: %d\n",
               pThreadInfo->threadID,
               g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID],
               pThreadInfo->querySeq,
               g_queryInfo.specifiedQueryInfo
                   .endAfterConsume[pThreadInfo->querySeq]);
        if (ASYNC_MODE == g_queryInfo.specifiedQueryInfo.asyncMode) {
            continue;
        }

        g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID] =
            taos_consume(
                g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID]);
        if (g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID]) {
            if (g_queryInfo.specifiedQueryInfo
                    .result[pThreadInfo->querySeq][0] != 0) {
                sprintf(pThreadInfo->filePath, "%s-%d",
                        g_queryInfo.specifiedQueryInfo
                            .result[pThreadInfo->querySeq],
                        pThreadInfo->threadID);
            }
            fetchResult(
                g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID],
                pThreadInfo);

            g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID]++;
            if ((g_queryInfo.specifiedQueryInfo
                     .resubAfterConsume[pThreadInfo->querySeq] != -1) &&
                (g_queryInfo.specifiedQueryInfo
                     .consumed[pThreadInfo->threadID] >=
                 g_queryInfo.specifiedQueryInfo
                     .resubAfterConsume[pThreadInfo->querySeq])) {
                printf("keepProgress:%d, resub specified query: %" PRIu64 "\n",
                       g_queryInfo.specifiedQueryInfo.subscribeKeepProgress,
                       pThreadInfo->querySeq);
                g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] =
                    0;
                taos_unsubscribe(
                    g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID],
                    g_queryInfo.specifiedQueryInfo.subscribeKeepProgress);
                g_queryInfo.specifiedQueryInfo
                    .tsub[pThreadInfo->threadID] = subscribeImpl(
                    SPECIFIED_CLASS, pThreadInfo,
                    g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq],
                    g_queryInfo.specifiedQueryInfo.topic[pThreadInfo->threadID],
                    g_queryInfo.specifiedQueryInfo.subscribeRestart,
                    g_queryInfo.specifiedQueryInfo.subscribeInterval);
                if (NULL == g_queryInfo.specifiedQueryInfo
                                .tsub[pThreadInfo->threadID]) {
                    goto free_of_specified_subscribe;
                }
            }
        }
    }
    *code = 0;
    taos_free_result(g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID]);
free_of_specified_subscribe:

    return code;
}

static void *superSubscribe(void *sarg) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS_SUB *  tsub[MAX_QUERY_SQL_COUNT] = {0};
    uint64_t    tsubSeq;
    char *      subSqlStr = calloc(1, BUFFER_SIZE);

    prctl(PR_SET_NAME, "superSub");

    if (pThreadInfo->ntables > MAX_QUERY_SQL_COUNT) {
        errorPrint("The table number(%" PRId64
                   ") of the thread is more than max query sql count: %d\n",
                   pThreadInfo->ntables, MAX_QUERY_SQL_COUNT);
        goto free_of_super_subscribe;
    }

    if (pThreadInfo->taos == NULL) {
        pThreadInfo->taos =
            select_one_from_pool(&g_taos_pool, g_queryInfo.dbName);
        if (pThreadInfo->taos == NULL) {
            goto free_of_super_subscribe;
        }
    }

    char sqlStr[TSDB_DB_NAME_LEN + 5];
    sprintf(sqlStr, "USE %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        errorPrint("use database %s failed!\n\n", g_queryInfo.dbName);
        goto free_of_super_subscribe;
    }

    char topic[32] = {0};
    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        tsubSeq = i - pThreadInfo->start_table_from;
        sprintf(topic, "taosbenchmark-subscribe-%" PRIu64 "-%" PRIu64 "", i,
                pThreadInfo->querySeq);
        memset(subSqlStr, 0, BUFFER_SIZE);
        replaceChildTblName(
            g_queryInfo.superQueryInfo.sql[pThreadInfo->querySeq], subSqlStr,
            (int)i);
        if (g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq][0] != 0) {
            sprintf(pThreadInfo->filePath, "%s-%d",
                    g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq],
                    pThreadInfo->threadID);
        }
        tsub[tsubSeq] =
            subscribeImpl(STABLE_CLASS, pThreadInfo, subSqlStr, topic,
                          g_queryInfo.superQueryInfo.subscribeRestart,
                          g_queryInfo.superQueryInfo.subscribeInterval);
        if (NULL == tsub[tsubSeq]) {
            goto free_of_super_subscribe;
        }
    }

    // start loop to consume result
    int consumed[MAX_QUERY_SQL_COUNT];
    for (int i = 0; i < MAX_QUERY_SQL_COUNT; i++) {
        consumed[i] = 0;
    }
    TAOS_RES *res = NULL;

    uint64_t st = 0, et = 0;

    while (
        (g_queryInfo.superQueryInfo.endAfterConsume == -1) ||
        (g_queryInfo.superQueryInfo.endAfterConsume >
         consumed[pThreadInfo->end_table_to - pThreadInfo->start_table_from])) {
        for (uint64_t i = pThreadInfo->start_table_from;
             i <= pThreadInfo->end_table_to; i++) {
            tsubSeq = i - pThreadInfo->start_table_from;
            if (ASYNC_MODE == g_queryInfo.superQueryInfo.asyncMode) {
                continue;
            }

            st = taosGetTimestampMs();
            performancePrint("st: %" PRIu64 " et: %" PRIu64 " st-et: %" PRIu64
                             "\n",
                             st, et, (st - et));
            res = taos_consume(tsub[tsubSeq]);
            et = taosGetTimestampMs();
            performancePrint("st: %" PRIu64 " et: %" PRIu64 " delta: %" PRIu64
                             "\n",
                             st, et, (et - st));

            if (res) {
                if (g_queryInfo.superQueryInfo
                        .result[pThreadInfo->querySeq][0] != 0) {
                    sprintf(pThreadInfo->filePath, "%s-%d",
                            g_queryInfo.superQueryInfo
                                .result[pThreadInfo->querySeq],
                            pThreadInfo->threadID);
                    fetchResult(res, pThreadInfo);
                }
                consumed[tsubSeq]++;

                if ((g_queryInfo.superQueryInfo.resubAfterConsume != -1) &&
                    (consumed[tsubSeq] >=
                     g_queryInfo.superQueryInfo.resubAfterConsume)) {
                    taos_unsubscribe(
                        tsub[tsubSeq],
                        g_queryInfo.superQueryInfo.subscribeKeepProgress);
                    consumed[tsubSeq] = 0;
                    tsub[tsubSeq] = subscribeImpl(
                        STABLE_CLASS, pThreadInfo, subSqlStr, topic,
                        g_queryInfo.superQueryInfo.subscribeRestart,
                        g_queryInfo.superQueryInfo.subscribeInterval);
                    if (NULL == tsub[tsubSeq]) {
                        goto free_of_super_subscribe;
                    }
                }
            }
        }
    }
    taos_free_result(res);

    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        tsubSeq = i - pThreadInfo->start_table_from;
        taos_unsubscribe(tsub[tsubSeq], 0);
    }
    *code = 0;
free_of_super_subscribe:

    tmfree(subSqlStr);
    return code;
}

int subscribeTestProcess() {
    setupForAnsiEscape();
    printfQueryMeta();
    resetAfterAnsiEscape();

    prompt();

    if (init_taos_list(&g_taos_pool, g_args.nthreads_pool)) {
        return -1;
    }

    TAOS *taos = select_one_from_pool(&g_taos_pool, NULL);
    if (taos == NULL) {
        return -1;
    }

    if (0 != g_queryInfo.superQueryInfo.sqlCount) {
        getAllChildNameOfSuperTable(taos, g_queryInfo.dbName,
                                    g_queryInfo.superQueryInfo.stbName,
                                    g_queryInfo.superQueryInfo.childTblName,
                                    g_queryInfo.superQueryInfo.childTblCount);
    }

    pthread_t * pids = NULL;
    threadInfo *infos = NULL;

    pthread_t * pidsOfStable = NULL;
    threadInfo *infosOfStable = NULL;

    //==== create threads for query for specified table
    if (g_queryInfo.specifiedQueryInfo.sqlCount <= 0) {
        debugPrint("%s() LN%d, specified query sqlCount %d.\n", __func__,
                   __LINE__, g_queryInfo.specifiedQueryInfo.sqlCount);
    } else {
        if (g_queryInfo.specifiedQueryInfo.concurrent <= 0) {
            errorPrint("specified query sqlCount %d.\n",
                       g_queryInfo.specifiedQueryInfo.sqlCount);
            exit(EXIT_FAILURE);
        }

        pids = calloc(1, g_queryInfo.specifiedQueryInfo.sqlCount *
                             g_queryInfo.specifiedQueryInfo.concurrent *
                             sizeof(pthread_t));

        infos = calloc(1, g_queryInfo.specifiedQueryInfo.sqlCount *
                              g_queryInfo.specifiedQueryInfo.concurrent *
                              sizeof(threadInfo));

        for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
            for (int j = 0; j < g_queryInfo.specifiedQueryInfo.concurrent;
                 j++) {
                uint64_t seq =
                    i * g_queryInfo.specifiedQueryInfo.concurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = (int)seq;
                pThreadInfo->querySeq = i;
                pThreadInfo->taos =
                    NULL;  // workaround to use separate taos connection;
                pthread_create(pids + seq, NULL, specifiedSubscribe,
                               pThreadInfo);
            }
        }
    }

    //==== create threads for super table query
    if (g_queryInfo.superQueryInfo.sqlCount <= 0) {
        debugPrint("%s() LN%d, super table query sqlCount %d.\n", __func__,
                   __LINE__, g_queryInfo.superQueryInfo.sqlCount);
    } else {
        if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
            (g_queryInfo.superQueryInfo.threadCnt > 0)) {
            pidsOfStable = calloc(1, g_queryInfo.superQueryInfo.sqlCount *
                                         g_queryInfo.superQueryInfo.threadCnt *
                                         sizeof(pthread_t));

            infosOfStable = calloc(1, g_queryInfo.superQueryInfo.sqlCount *
                                          g_queryInfo.superQueryInfo.threadCnt *
                                          sizeof(threadInfo));

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

            for (uint64_t i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                uint64_t tableFrom = 0;
                for (int j = 0; j < threads; j++) {
                    uint64_t    seq = i * threads + j;
                    threadInfo *pThreadInfo = infosOfStable + seq;
                    pThreadInfo->threadID = (int)seq;
                    pThreadInfo->querySeq = i;

                    pThreadInfo->start_table_from = tableFrom;
                    pThreadInfo->ntables = j < b ? a + 1 : a;
                    pThreadInfo->end_table_to =
                        j < b ? tableFrom + a : tableFrom + a - 1;
                    tableFrom = pThreadInfo->end_table_to + 1;
                    pthread_create(pidsOfStable + seq, NULL, superSubscribe,
                                   pThreadInfo);
                }
            }

            g_queryInfo.superQueryInfo.threadCnt = threads;

            for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                for (int j = 0; j < threads; j++) {
                    uint64_t seq = (uint64_t)i * threads + j;
                    void *   result;
                    pthread_join(pidsOfStable[seq], &result);
                    if (*(int32_t *)result) {
                        g_fail = true;
                    }
                    tmfree(result);
                }
            }
        }
    }

    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
        for (int j = 0; j < g_queryInfo.specifiedQueryInfo.concurrent; j++) {
            uint64_t seq = i * g_queryInfo.specifiedQueryInfo.concurrent + j;
            void *   result;
            pthread_join(pids[seq], &result);
            if (*(int32_t *)result) {
                g_fail = true;
            }
            tmfree(result);
        }
    }

    tmfree((char *)pids);
    tmfree((char *)infos);

    tmfree((char *)pidsOfStable);
    tmfree((char *)infosOfStable);
    //
    if (g_fail) {
        return -1;
    }
    return 0;
}