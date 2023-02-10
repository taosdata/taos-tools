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

typedef struct {
    tmq_t* tmq;
    int    rows;
    int    id;
} tmqThreadInfo;

static int create_topic(BArray* sqls) {
    SBenchConn* conn = initBenchConn();
    if (conn == NULL) {
        return -1;
    }
    TAOS* taos = conn->taos;
    char command[SQL_BUFF_LEN];
    memset(command, 0, SQL_BUFF_LEN);
    sprintf(command, "use %s", g_queryInfo.dbName);
    if (queryDbExecCall(conn, command)) {
        closeBenchConn(conn);
        return -1;
    }
    for (int i = 0; i < sqls->size; ++i) {
        SSQL * sql = benchArrayGet(sqls, i);
        char buffer[SQL_BUFF_LEN];
        memset(buffer, 0, SQL_BUFF_LEN);
        snprintf(buffer, SQL_BUFF_LEN, "create topic if not exists "
                "topic_%d as %s",
                i, sql->command);
        TAOS_RES *res = taos_query(taos, buffer);
        if (taos_errno(res) != 0) {
            errorPrint("failed to create topic_%d, reason: %s\n",
                    i, taos_errstr(res));
            closeBenchConn(conn);
            return -1;
        }
        infoPrint("successfully create topic_%d\n", i);
    }
    closeBenchConn(conn);
    return 0;
}

static tmq_list_t * buildTopicList(int size) {
    tmq_list_t * topic_list = tmq_list_new();
    for (int i = 0; i < size; ++i) {
        char buf[INT_BUFF_LEN + 7];
        snprintf(buf, INT_BUFF_LEN + 6, "topic_%d", i);
        tmq_list_append(topic_list, buf);
    }
    infoPrint("%s", "successfully build topic list\n");
    return topic_list;
}

static void* tmqConsume(void* arg) {
    tmqThreadInfo *pThreadInfo = (tmqThreadInfo*)arg;
    bool first_time = true;
    int64_t st = toolsGetTimestampUs();
    int64_t et = toolsGetTimestampUs();
    uint64_t subscribeTimes = g_queryInfo.specifiedQueryInfo.subscribeTimes;
    while(!g_arguments->terminate
        && subscribeTimes > 0) {
        debugPrint("%s", "tmq_consumer_poll()");
        TAOS_RES * tmqMessage = tmq_consumer_poll(
                pThreadInfo->tmq, g_queryInfo.specifiedQueryInfo.queryInterval);
        if (tmqMessage != NULL) {
            if (first_time) {
                st = toolsGetTimestampUs();
                first_time = false;
            } else {
                et = toolsGetTimestampUs();
            }
            int numOfRows;
            void * data;
            int code = taos_fetch_raw_block(tmqMessage, &numOfRows, &data);
            if (code) {
                errorPrint("thread[%d]: failed to execute "
                        "taos_fetch_raw_block(), code: %d\n",
                        pThreadInfo->id, code);
                return NULL;
            }
            pThreadInfo->rows += numOfRows;
        }
        subscribeTimes --;
    }
    int code = tmq_consumer_close(pThreadInfo->tmq);
    if (code) {
        errorPrint("failed to close consumer: %s\n", tmq_err2str(code));
    }
    infoPrint("thread[%d] spend %.6f seconds consume %d rows\n",
            pThreadInfo->id, (et - st)/1E6, pThreadInfo->rows);
    return NULL;
}

int subscribeTestProcess() {
    int ret = 0;
    if (g_queryInfo.specifiedQueryInfo.sqls->size > 0) {
        if (create_topic(g_queryInfo.specifiedQueryInfo.sqls)) {
            return -1;
        }
    }

    tmq_list_t * topic_list =
        buildTopicList(g_queryInfo.specifiedQueryInfo.sqls->size);

    pthread_t * pids = benchCalloc(
            g_queryInfo.specifiedQueryInfo.concurrent,
            sizeof(pthread_t), true);
    tmqThreadInfo *infos = benchCalloc(
            g_queryInfo.specifiedQueryInfo.concurrent,
            sizeof(tmqThreadInfo), true);

    if (!g_arguments->answer_yes) {
        printf("\n\n         Press enter key to continue\n\n");
        (void)getchar();
    }

    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.concurrent; ++i) {
        tmqThreadInfo * pThreadInfo = infos + i;
        pThreadInfo->rows = 0;
        pThreadInfo->id = i;
        tmq_conf_t * conf = tmq_conf_new();
        char groupid[BIGINT_BUFF_LEN];
        memset(groupid, 0, BIGINT_BUFF_LEN);
        sprintf(groupid, "tg%d", i);
        tmq_conf_set(conf, "group.id", groupid);
        tmq_conf_set(conf, "td.connect.user", g_arguments->user);
        tmq_conf_set(conf, "td.connect.pass", g_arguments->password);
        pThreadInfo->tmq = tmq_consumer_new(conf, NULL, 0);
        tmq_conf_destroy(conf);
        if (pThreadInfo->tmq == NULL) {
            errorPrint("%s" ,"failed to execute tmq_consumer_new\n");
            ret = -1;
            goto tmq_over;
        }
        infoPrint("thread[%d]: successfully create consumer\n", i);
        int32_t code = tmq_subscribe(pThreadInfo->tmq, topic_list);
        if (code) {
            errorPrint("failed to execute tmq_subscribe, reason: %s\n",
                    tmq_err2str(code));
            ret = -1;
            goto tmq_over;
        }
        infoPrint("thread[%d]: successfully subscribe topics\n", i);
        pthread_create(pids + i, NULL, tmqConsume, pThreadInfo);
    }

    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.concurrent; i++) {
        pthread_join(pids[i], NULL);
    }

tmq_over:
    free(pids);
    free(infos);
    tmq_list_destroy(topic_list);
    return ret;
}
