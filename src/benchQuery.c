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

#include <bench.h>
#include "benchLog.h"

// query and get result  other true is no test sql
int selectAndGetResult(qThreadInfo *pThreadInfo, char *command, bool other) {
    int ret = 0;

    // user cancel
    if (g_arguments->terminate) {
        return -1;
    }

    // execute sql
    uint32_t threadID = pThreadInfo->threadID;
    char dbName[TSDB_DB_NAME_LEN] = {0};
    tstrncpy(dbName, g_queryInfo.dbName, TSDB_DB_NAME_LEN);

    if (g_queryInfo.iface == REST_IFACE) {
        int retCode = postProceSql(command, g_queryInfo.dbName, 0, REST_IFACE,
                                   0, g_arguments->port, false,
                                   pThreadInfo->sockfd, pThreadInfo->filePath);
        if (0 != retCode) {
            errorPrint("====restful return fail, threadID[%u]\n", threadID);
            ret = -1;
        }
    } else {
        // query
        TAOS *taos = pThreadInfo->conn->taos;
        int64_t rows  = 0;
        TAOS_RES *res = taos_query(taos, command);
        int code = taos_errno(res);
        if (res == NULL || code) {
            // failed query
            errorPrint("failed to execute sql:%s, "
                        "code: 0x%08x, reason:%s\n",
                        command, code, taos_errstr(res));
            ret = -1;
        } else {
            // succ query
            if (!other)
                rows = fetchResult(res, pThreadInfo->filePath);
        }

        // free result
        if (res) {
            taos_free_result(res);
        }
        debugPrint("query sql:%s rows:%"PRId64"\n", command, rows);
    }

    // record count
    if (ret ==0) {
        // succ
        if (!other) 
            pThreadInfo->nSucc ++;
    } else {
        // fail
        if (!other)
            pThreadInfo->nFail ++;

        // continue option
        if (YES_IF_FAILED == g_arguments->continueIfFail) {
            ret = 0; // force continue
        }
    }

    return ret;
}

// interlligent sleep
void autoSleep(uint64_t st, uint64_t et) {
    if (g_queryInfo.specifiedQueryInfo.queryInterval &&
        (et - st) < (int64_t)g_queryInfo.specifiedQueryInfo.queryInterval * 1000) {
        toolsMsleep((int32_t)(
                    g_queryInfo.specifiedQueryInfo.queryInterval*1000
                    - (et - st)));  // ms
    }
}

// reset 
int32_t resetQueryCache(qThreadInfo* pThreadInfo) {    
    // execute sql 
    if (selectAndGetResult(pThreadInfo, "RESET QUERY CACHE", true)) {
        errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
        return -1;
    }
    // succ
    return 0;
}



//
//  ---------------------------------  second levle funtion for Thread -----------------------------------
//

// show rela qps
int64_t showRealQPS(qThreadInfo* pThreadInfo, int64_t lastPrintTime, int64_t startTs) {
    int64_t now = toolsGetTimestampMs();
    if (now - lastPrintTime > 10 * 1000) {
        // real total
        uint64_t totalQueried = pThreadInfo->nSucc;
        if(g_arguments->continueIfFail == YES_IF_FAILED) {
            totalQueried += pThreadInfo->nFail;
        }
        infoPrint(
            "thread[%d] has currently completed queries: %" PRIu64 ", QPS: %10.3f\n",
            pThreadInfo->threadID, totalQueried,
            (double)(totalQueried / ((now - startTs) / 1000.0)));
        return now;
    } else {
        return lastPrintTime;
    }    
}

// spec query mixed thread
static void *specQueryMixThread(void *sarg) {
    qThreadInfo *pThreadInfo = (qThreadInfo*)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "specQueryMixThread");
#endif
    // use db
    if (g_queryInfo.dbName) {
        if (pThreadInfo->conn &&
            pThreadInfo->conn->taos &&
            taos_select_db(pThreadInfo->conn->taos, g_queryInfo.dbName)) {
                errorPrint("thread[%d]: failed to select database(%s)\n", pThreadInfo->threadID, g_queryInfo.dbName);
                return NULL;
        }
    }

    int64_t st = 0;
    int64_t et = 0;
    int64_t startTs = toolsGetTimestampMs();
    int64_t lastPrintTime = startTs;
    uint64_t  queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    pThreadInfo->query_delay_list = benchArrayInit(queryTimes, sizeof(int64_t));
    for (int i = pThreadInfo->start_sql; i <= pThreadInfo->end_sql; ++i) {
        SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);
        for (int j = 0; j < queryTimes; ++j) {
            // use cancel
            if(g_arguments->terminate) {
                infoPrint("%s\n", "user cancel , so exit testing.");
                break;
            }

            // reset cache
            if (g_queryInfo.reset_query_cache) {
                if (resetQueryCache(pThreadInfo)) {
                    errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
                    return NULL;
                }
            }

            // execute sql
            st = toolsGetTimestampUs();
            int ret = selectAndGetResult(pThreadInfo, sql->command, false);
            if (ret) {
                g_fail = true;
                errorPrint("failed call mix selectAndGetResult, i=%d j=%d", i, j);
                return NULL;
            }
            et = toolsGetTimestampUs();

            // sleep
            autoSleep(st, et);

            // delay
            if (ret == 0) {
                int64_t* delay = benchCalloc(1, sizeof(int64_t), false);
                *delay = et - st;
                debugPrint("%s() LN%d, delay: %"PRId64"\n", __func__, __LINE__, *delay);

                pThreadInfo->total_delay += *delay;
                if(benchArrayPush(pThreadInfo->query_delay_list, delay) == NULL){
                    tmfree(delay);
                }
            }

            // real show
            lastPrintTime = showRealQPS(pThreadInfo, lastPrintTime, startTs);
        }
    }

    return NULL;
}

// spec query thread
static void *specQueryThread(void *sarg) {
    qThreadInfo *pThreadInfo = (qThreadInfo *)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "specQueryThread");
#endif
    uint64_t st = 0;
    uint64_t et = 0;
    int32_t  index = 0;

    // use db
    if (g_queryInfo.dbName) {
        if (pThreadInfo->conn &&
            pThreadInfo->conn->taos &&
            taos_select_db(pThreadInfo->conn->taos, g_queryInfo.dbName)) {
                errorPrint("thread[%d]: failed to select database(%s)\n", pThreadInfo->threadID, g_queryInfo.dbName);
                return NULL;
        }
    }

    uint64_t  queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    pThreadInfo->query_delay_list = benchArrayInit(queryTimes, sizeof(int64_t));

    uint64_t  startTs       = toolsGetTimestampMs();
    uint64_t  lastPrintTime = startTs;

    SSQL * sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, pThreadInfo->querySeq);

    if (sql->result[0] != '\0') {
        snprintf(pThreadInfo->filePath, MAX_PATH_LEN, "%s-%d",
                sql->result, pThreadInfo->threadID);
    }

    while (index < queryTimes) {
        // use cancel
        if(g_arguments->terminate) {
            infoPrint("thread[%d] user cancel , so exit testing.\n", pThreadInfo->threadID);
            break;
        }

        // reset cache
        if (g_queryInfo.reset_query_cache) {
            if (resetQueryCache(pThreadInfo)) {
                errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
                return NULL;
            }
        }

        // execute sql
        st = toolsGetTimestampUs();
        int ret = selectAndGetResult(pThreadInfo, sql->command, false);
        if (ret) {
            g_fail = true;
            errorPrint("failed call spec selectAndGetResult, index=%d\n", index);
            break;
        }
        et = toolsGetTimestampUs();

        // sleep
        autoSleep(st, et);

        uint64_t delay = et - st;
        debugPrint("%s() LN%d, delay: %"PRIu64"\n", __func__, __LINE__, delay);

        if (ret == 0) {
            // only succ add delay list
            benchArrayPushNoFree(pThreadInfo->query_delay_list, &delay);
            pThreadInfo->total_delay += delay;
        }
        index++;

        // real show
        lastPrintTime = showRealQPS(pThreadInfo, lastPrintTime, startTs);
    }

    return NULL;
}

// super table query thread
static void *stbQueryThread(void *sarg) {
    char *sqlstr = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    qThreadInfo *pThreadInfo = (qThreadInfo *)sarg;
#ifdef LINUX
    prctl(PR_SET_NAME, "stbQueryThread");
#endif

    uint64_t st = 0;
    uint64_t et = 0;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    pThreadInfo->query_delay_list = benchArrayInit(queryTimes, sizeof(uint64_t));
    
    uint64_t startTs = toolsGetTimestampMs();
    uint64_t lastPrintTime = startTs;
    while (queryTimes--) {
        // use cancel
        if(g_arguments->terminate) {
            infoPrint("%s\n", "user cancel , so exit testing.");
            break;
        }

        // reset cache
        if (g_queryInfo.reset_query_cache) {
            if (resetQueryCache(pThreadInfo)) {
                errorPrint("%s() LN%d, reset query cache failed\n", __func__, __LINE__);
                return NULL;
            }
        }

        // execute
        st = toolsGetTimestampMs();
        // for each table
        for (int i = (int)pThreadInfo->start_table_from; i <= pThreadInfo->end_table_to; i++) {
            // use cancel
            if(g_arguments->terminate) {
                infoPrint("%s\n", "user cancel , so exit testing.");
                break;
            }

            // for each sql
            for (int j = 0; j < g_queryInfo.superQueryInfo.sqlCount; j++) {
                memset(sqlstr, 0, TSDB_MAX_ALLOWED_SQL_LEN);
                // use cancel
                if(g_arguments->terminate) {
                    infoPrint("%s\n", "user cancel , so exit testing.");
                    break;
                }

                
                // get real child name sql
                if (replaceChildTblName(g_queryInfo.superQueryInfo.sql[j], sqlstr, i)) {
                    // fault
                    tmfree(sqlstr);
                    return NULL;
                }

                if (g_queryInfo.superQueryInfo.result[j][0] != '\0') {
                    snprintf(pThreadInfo->filePath, MAX_PATH_LEN, "%s-%d",
                            g_queryInfo.superQueryInfo.result[j],
                            pThreadInfo->threadID);
                }

                // execute sql
                uint64_t s = toolsGetTimestampUs();
                int ret = selectAndGetResult(pThreadInfo, sqlstr, false);
                if (ret) {
                    // found error
                    errorPrint("failed call stb selectAndGetResult, i=%d j=%d\n", i, j);
                    g_fail = true;
                    tmfree(sqlstr);
                    return NULL;
                }
                uint64_t delay = toolsGetTimestampUs() - s;
                debugPrint("%s() LN%d, delay: %"PRIu64"\n", __func__, __LINE__, delay);
                if (ret == 0) {
                    // only succ add delay list
                    benchArrayPushNoFree(pThreadInfo->query_delay_list, &delay);
                    pThreadInfo->total_delay += delay;
                }

                // show real QPS
                lastPrintTime = showRealQPS(pThreadInfo, lastPrintTime, startTs);
            }
        }
        et = toolsGetTimestampMs();

        // sleep
        autoSleep(st, et);
    }
    tmfree(sqlstr);

    return NULL;
}

//
// ---------------------------------  firse level function ------------------------------
//

void totalChildQuery(qThreadInfo* infos, int threadCnt, int64_t spend) {
    // valid check
    if (infos == NULL || threadCnt == 0) {
        return ;
    }
    
    // statistic
    BArray * delay_list = benchArrayInit(1, sizeof(int64_t));
    double total_delays = 0;

    // clear
    for (int i = 0; i < threadCnt; ++i) {
        qThreadInfo * pThreadInfo = infos + i;
        
        // append delay
        benchArrayAddBatch(delay_list, pThreadInfo->query_delay_list->pData,
                pThreadInfo->query_delay_list->size, false);
        total_delays += pThreadInfo->total_delay;

        // free delay
        benchArrayDestroy(pThreadInfo->query_delay_list);
        pThreadInfo->query_delay_list = NULL;

    }

    // succ is zero
    if (delay_list->size == 0) {
        errorPrint("%s", "succ queries count is zero.\n");
        benchArrayDestroy(delay_list);
        return ;
    }


    // sort
    qsort(delay_list->pData, delay_list->size, delay_list->elemSize, compare);

    // show delay min max
    if (delay_list->size) {
        infoPrint(
                "spend %.6fs using "
                "%d threads complete query %d times,  "
                "min delay: %.6fs, "
                "avg delay: %.6fs, "
                "p90: %.6fs, "
                "p95: %.6fs, "
                "p99: %.6fs, "
                "max: %.6fs\n",
                spend/1E6,
                threadCnt, (int)delay_list->size,
                *(int64_t *)(benchArrayGet(delay_list, 0))/1E6,
                (double)total_delays/delay_list->size/1E6,
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
}

//
// super table query
//
static int stbQuery(uint16_t iface, char* dbName) {
    int ret = -1;
    pthread_t * pidsOfSub   = NULL;
    qThreadInfo *threadInfos = NULL;
    g_queryInfo.superQueryInfo.totalQueried = 0;
    g_queryInfo.superQueryInfo.totalFail    = 0;

    // check
    if ((g_queryInfo.superQueryInfo.sqlCount == 0)
        || (g_queryInfo.superQueryInfo.threadCnt == 0)) {
        return 0;
    }

    // malloc 
    pidsOfSub = benchCalloc(1, g_queryInfo.superQueryInfo.threadCnt
                            *sizeof(pthread_t),
                            false);
    threadInfos = benchCalloc(1, g_queryInfo.superQueryInfo.threadCnt
                                *sizeof(qThreadInfo), false);

    int64_t ntables = g_queryInfo.superQueryInfo.childTblCount;
    int nConcurrent = g_queryInfo.superQueryInfo.threadCnt;

    int64_t a = ntables / nConcurrent;
    if (a < 1) {
        nConcurrent = (int)ntables;
        a = 1;
    }

    int64_t b = 0;
    if (nConcurrent != 0) {
        b = ntables % nConcurrent;
    }

    uint64_t tableFrom = 0;
    int threadCnt = 0;
    for (int i = 0; i < nConcurrent; i++) {
        qThreadInfo *pThreadInfo = threadInfos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to =
                i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        // create conn
        if (initQueryConn(pThreadInfo, iface)){
            break;
        }
        int code = pthread_create(pidsOfSub + i, NULL, stbQueryThread, pThreadInfo);
        if (code != 0) {
            errorPrint("failed stbQueryThread create. error code =%d \n", code);
            break;
        }
        threadCnt ++;
    }

    bool needExit = false;
    // if failed, set termainte flag true like ctrl+c exit
    if (threadCnt != nConcurrent  ) {
        needExit = true;
        g_arguments->terminate = true;
        goto OVER;
    }

    // reset total
    g_queryInfo.superQueryInfo.totalQueried = 0;
    g_queryInfo.superQueryInfo.totalFail    = 0;

    // real thread count
    g_queryInfo.superQueryInfo.threadCnt = threadCnt;
    int64_t start = toolsGetTimestampUs();

    for (int i = 0; i < threadCnt; i++) {
        pthread_join(pidsOfSub[i], NULL);
        qThreadInfo *pThreadInfo = threadInfos + i;
        // add succ
        g_queryInfo.superQueryInfo.totalQueried += pThreadInfo->nSucc;
        if (g_arguments->continueIfFail == YES_IF_FAILED) {
            // "yes" need add fail cnt
            g_queryInfo.superQueryInfo.totalQueried += pThreadInfo->nFail;
            g_queryInfo.superQueryInfo.totalFail    += pThreadInfo->nFail;
        }

        // close conn
        closeQueryConn(pThreadInfo, iface);
    }
    int64_t end = toolsGetTimestampUs();

    if (needExit) {
        goto OVER;
    }

    // total show
    totalChildQuery(threadInfos, threadCnt, end - start);

    ret = 0;

OVER:
    tmfree((char *)pidsOfSub);
    tmfree((char *)threadInfos);

    for (int64_t i = 0; i < g_queryInfo.superQueryInfo.childTblCount; ++i) {
        tmfree(g_queryInfo.superQueryInfo.childTblName[i]);
    }
    tmfree(g_queryInfo.superQueryInfo.childTblName);
    return ret;
}

//
// specQuery
//
static int specQuery(uint16_t iface, char* dbName) {
    int ret = -1;
    pthread_t    *pids = NULL;
    qThreadInfo *infos = NULL;
    int    nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqls->size;
    g_queryInfo.specifiedQueryInfo.totalQueried = 0;
    g_queryInfo.specifiedQueryInfo.totalFail    = 0;

    // check invaid
    if(nSqlCount == 0 || nConcurrent == 0 ) {
        if(nSqlCount == 0)
           warnPrint("specified table query sql count is %" PRIu64 ".\n", nSqlCount);
        if(nConcurrent == 0)
           warnPrint("nConcurrent is %d , specified_table_query->nConcurrent is zero. \n", nConcurrent);
        return 0;
    }

    // malloc threads memory
    pids  = benchCalloc(1, nConcurrent * sizeof(pthread_t),  false);
    infos = benchCalloc(1, nConcurrent * sizeof(qThreadInfo), false);

    for (uint64_t i = 0; i < nSqlCount; i++) {
        if( g_arguments->terminate ) {
            break;
        }

        // reset
        memset(pids,  0, nConcurrent * sizeof(pthread_t));
        memset(infos, 0, nConcurrent * sizeof(qThreadInfo));

        // get execute sql
        SSQL *sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, i);

        // create threads
        int threadCnt = 0;
        for (int j = 0; j < nConcurrent; j++) {
           qThreadInfo *pThreadInfo = infos + j;
           pThreadInfo->threadID = i * nConcurrent + j;
           pThreadInfo->querySeq = i;

           // create conn
           if (initQueryConn(pThreadInfo, iface)) {
               break;
           }

           int code = pthread_create(pids + j, NULL, specQueryThread, pThreadInfo);
           if (code != 0) {
               errorPrint("failed specQueryThread create. error code =%d \n", code);
               break;
           }
           threadCnt++;
        }

        bool needExit = false;
        // if failed, set termainte flag true like ctrl+c exit
        if (threadCnt != nConcurrent  ) {
            needExit = true;
            g_arguments->terminate = true;
        }

        int64_t start = toolsGetTimestampUs();
        // wait threads execute finished one by one
        for (int j = 0; j < threadCnt ; j++) {
           pthread_join(pids[j], NULL);
           qThreadInfo *pThreadInfo = infos + j;
           closeQueryConn(pThreadInfo, iface);

           // need exit in loop
           if (needExit) {
                // free BArray
                benchArrayDestroy(pThreadInfo->query_delay_list);
                pThreadInfo->query_delay_list = NULL;
           }
        }
        int64_t spend = toolsGetTimestampUs() - start;
        if(spend == 0) {
            // avoid xx/spend expr throw error
            spend = 1;
        }

        // create 
        if (needExit) {
            errorPrint("failed to create thread. expect nConcurrent=%d real threadCnt=%d,  exit testing.\n", nConcurrent, threadCnt);
            goto OVER;
        }

        //
        // show QPS and P90 ...
        //
        uint64_t n = 0;
        double  total_delays = 0.0;
        uint64_t totalQueried = 0;
        uint64_t totalFail    = 0;
        for (int j = 0; j < threadCnt; j++) {
           qThreadInfo *pThreadInfo = infos + j;
           
           // total one sql
           for (uint64_t k = 0; k < pThreadInfo->query_delay_list->size; k++) {
                int64_t * delay = benchArrayGet(pThreadInfo->query_delay_list, k);
                sql->delay_list[n++] = *delay;
                total_delays += *delay;
           }

           // total queries
           totalQueried += pThreadInfo->nSucc;
           if (g_arguments->continueIfFail == YES_IF_FAILED) {
                totalQueried += pThreadInfo->nFail;
                totalFail    += pThreadInfo->nFail;
           }

           // free BArray query_delay_list
           benchArrayDestroy(pThreadInfo->query_delay_list);
           pThreadInfo->query_delay_list = NULL;
        }

        // appand current sql
        g_queryInfo.specifiedQueryInfo.totalQueried += totalQueried;
        g_queryInfo.specifiedQueryInfo.totalFail    += totalFail;

        // succ is zero
        if(totalQueried == 0 || n == 0) {
            errorPrint("%s", "succ queries count is zero.\n");
            goto OVER;
        }

        qsort(sql->delay_list, n, sizeof(uint64_t), compare);
        int32_t bufLen = strlen(sql->command) + 512;
        char * buf = benchCalloc(bufLen, sizeof(char), false);
        snprintf(buf , bufLen, "complete query with %d threads and %" PRIu64 " "
                             "sql %"PRIu64" spend %.6fs QPS: %.3f "
                             "query delay "
                             "avg: %.6fs "
                             "min: %.6fs "
                             "max: %.6fs "
                             "p90: %.6fs "
                             "p95: %.6fs "
                             "p99: %.6fs "
                             "SQL command: %s \n",
                             threadCnt, totalQueried,
                             i + 1, spend/1E6, totalQueried / (spend/1E6),
                             total_delays/n/1E6,           /* avg */
                             sql->delay_list[0] / 1E6,     /* min */
                             sql->delay_list[n - 1] / 1E6, /* max */
                             /*  p90 */
                             sql->delay_list[(uint64_t)(n * 0.90)] / 1E6,
                             /*  p95 */
                             sql->delay_list[(uint64_t)(n * 0.95)] / 1E6,
                             /*  p99 */
                             sql->delay_list[(uint64_t)(n * 0.99)] / 1E6, 
                             sql->command);

        infoPrintNoTimestamp("%s", buf);
        infoPrintNoTimestampToFile("%s", buf);
        tmfree(buf);
    }
    ret = 0;

OVER:
    tmfree((char *)pids);
    tmfree((char *)infos);

    // free specialQueryInfo
    freeSpecialQueryInfo();
    return ret;
}

//
// specQueryMix
//
static int specQueryMix(uint16_t iface, char* dbName) {
    // init
    int ret            = -1;
    int nConcurrent    = g_queryInfo.specifiedQueryInfo.concurrent;
    pthread_t * pids   = benchCalloc(nConcurrent, sizeof(pthread_t), true);
    qThreadInfo *infos = benchCalloc(nConcurrent, sizeof(qThreadInfo), true);

    // concurent calc
    int total_sql_num = g_queryInfo.specifiedQueryInfo.sqls->size;
    int start_sql     = 0;
    int a             = total_sql_num / nConcurrent;
    if (a < 1) {
        nConcurrent = total_sql_num;
        a = 1;
    }
    int b = 0;
    if (nConcurrent != 0) {
        b = total_sql_num % nConcurrent;
    }

    //
    // running
    //
    int threadCnt = 0;
    for (int i = 0; i < nConcurrent; ++i) {
        qThreadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID    = i;
        pThreadInfo->start_sql   = start_sql;
        pThreadInfo->end_sql     = i < b ? start_sql + a : start_sql + a - 1;
        start_sql = pThreadInfo->end_sql + 1;
        pThreadInfo->total_delay = 0;

        // create conn
        if (initQueryConn(pThreadInfo, iface)){
            break;
        }
        // main run
        int code = pthread_create(pids + i, NULL, specQueryMixThread, pThreadInfo);
        if (code != 0) {
            errorPrint("failed specQueryMixThread create. error code =%d \n", code);
            break;
        }
        
        threadCnt ++;
    }

    bool needExit = false;
    // if failed, set termainte flag true like ctrl+c exit
    if (threadCnt != nConcurrent) {
        needExit = true;
        g_arguments->terminate = true;
    }

    // reset total
    g_queryInfo.specifiedQueryInfo.totalQueried = 0;
    g_queryInfo.specifiedQueryInfo.totalFail    = 0;

    int64_t start = toolsGetTimestampUs();
    for (int i = 0; i < threadCnt; ++i) {
        pthread_join(pids[i], NULL);
        qThreadInfo *pThreadInfo = infos + i;
        closeQueryConn(pThreadInfo, iface);

        // total queries
        g_queryInfo.specifiedQueryInfo.totalQueried += pThreadInfo->nSucc;
        if (g_arguments->continueIfFail == YES_IF_FAILED) {
            // yes need add failed count
            g_queryInfo.specifiedQueryInfo.totalQueried += pThreadInfo->nFail;
            g_queryInfo.specifiedQueryInfo.totalFail    += pThreadInfo->nFail;
        }

        // destory
        if (needExit) {
            benchArrayDestroy(pThreadInfo->query_delay_list);
            pThreadInfo->query_delay_list = NULL;
        }
    }
    int64_t end = toolsGetTimestampUs();

    // create 
    if (needExit) {
        errorPrint("failed to create thread. expect nConcurrent=%d real threadCnt=%d,  exit testing.\n", nConcurrent, threadCnt);
        goto OVER;
    }

    // statistic
    totalChildQuery(infos, threadCnt, end - start);
    ret = 0;

OVER:
    tmfree(pids);
    tmfree(infos);

    // free sqls
    freeSpecialQueryInfo();

    return ret;
}

// total query for end 
void totalQuery(int64_t spends) {
    // total QPS
    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried
        + g_queryInfo.superQueryInfo.totalQueried;

    // error rate
    char errRate[128] = "";
    if(g_arguments->continueIfFail == YES_IF_FAILED) {
        uint64_t totalFail = g_queryInfo.specifiedQueryInfo.totalFail + g_queryInfo.superQueryInfo.totalFail;
        if (totalQueried > 0) {
            snprintf(errRate, sizeof(errRate), " Error %" PRIu64 " Rate:%.3f%%", totalFail, ((float)totalFail * 100)/totalQueried);
        }
    }

    // show
    double  tInS = (double)spends / 1000;
    char buf[512] = "";
    snprintf(buf, sizeof(buf),
                "Spend %.4f second completed total queries: %" PRIu64
                ", the QPS of all threads: %10.3f%s\n\n",
                tInS, totalQueried, (double)totalQueried / tInS, errRate);
    infoPrint("%s", buf);
    infoPrintToFile("%s", buf);
}

int queryTestProcess() {
    prompt(0);

    if (REST_IFACE == g_queryInfo.iface) {
        encodeAuthBase64();
    }

    // kill sql for executing seconds over "kill_slow_query_threshold"
    if (g_queryInfo.iface == TAOSC_IFACE && g_queryInfo.killQueryThreshold) {
        int32_t ret = killSlowQuery();
        if (ret != 0) {
            return ret;
        }
    }

    // covert addr
    if (g_queryInfo.iface == REST_IFACE) {
        if (convertHostToServAddr(g_arguments->host,
                    g_arguments->port + TSDB_PORT_HTTP,
                    &(g_arguments->serv_addr)) != 0) {
            errorPrint("%s", "convert host to server address\n");
            return -1;
        }
    }

    // fetch child name if super table
    if ((g_queryInfo.superQueryInfo.sqlCount > 0) &&
            (g_queryInfo.superQueryInfo.threadCnt > 0)) {
        int32_t ret = fetchChildTableName(g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
        if (ret != 0) {
            errorPrint("fetchChildTableName dbName=%s stb=%s failed.", g_queryInfo.dbName, g_queryInfo.superQueryInfo.stbName);
            return -1;
        }
    }

    // 
    // start running
    //

    // specified table
    uint64_t startTs = toolsGetTimestampMs();
    if (g_queryInfo.specifiedQueryInfo.mixed_query) {
        // mixed
        if (specQueryMix(g_queryInfo.iface, g_queryInfo.dbName)) {
            return -1;
        }
    } else {
        // no mixied
        if (specQuery(g_queryInfo.iface, g_queryInfo.dbName)) {
            return -1;
        }
    }

    // super table
    if (stbQuery(g_queryInfo.iface, g_queryInfo.dbName)) {
        return -1;
    }

    // total 
    totalQuery(toolsGetTimestampMs() - startTs); 
    return 0;
}
