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
#include "toolsdef.h"

SArguments*    g_arguments;
SQueryMetaInfo g_queryInfo;
bool           g_fail = false;
uint64_t       g_memoryUsage = 0;
tools_cJSON*   root;

static char     g_client_info[32] = {0};
int             g_majorVersionOfClient = 0;

#ifdef LINUX
void benchQueryInterruptHandler(int32_t signum, void* sigingo, void* context) {
    infoPrint("%s", "Receive SIGINT or other signal, quit taosBenchmark\n");
    sem_post(&g_arguments->cancelSem);
}

void* benchCancelHandler(void* arg) {
    if (bsem_wait(&g_arguments->cancelSem) != 0) {
        toolsMsleep(10);
    }

    g_arguments->terminate = true;
    toolsMsleep(10);

    if (g_arguments->in_prompt || INSERT_TEST != g_arguments->test_mode) {
        toolsMsleep(100);
        postFreeResource();
        exit(EXIT_SUCCESS);
    }
    return NULL;
}
#endif

int main(int argc, char* argv[]) {
    int ret = 0;

    init_argument();
    srand(time(NULL)%1000000);

    sprintf(g_client_info, "%s", taos_get_client_info());
    g_majorVersionOfClient = atoi(g_client_info);
    debugPrint("Client info: %s, major version: %d\n",
            g_client_info,
            g_majorVersionOfClient);

#ifdef LINUX
    if (sem_init(&g_arguments->cancelSem, 0, 0) != 0) {
        errorPrint("%s", "failed to create cancel semaphore\n");
        exit(EXIT_FAILURE);
    }
    pthread_t spid = {0};
    pthread_create(&spid, NULL, benchCancelHandler, NULL);

    benchSetSignal(SIGINT, benchQueryInterruptHandler);

#endif
    if (benchParseArgs(argc, argv)) {
        return -1;
    }
#ifdef WEBSOCKET
    if (g_arguments->debug_print) {
        ws_enable_log();
    }

    if (g_arguments->dsn != NULL) {
        g_arguments->websocket = true;
    } else {
        char * dsn = getenv("TDENGINE_CLOUD_DSN");
        if (dsn != NULL) {
            g_arguments->dsn = dsn;
            g_arguments->websocket = true;
            g_arguments->nthreads_auto = false;
        } else {
            g_arguments->dsn = false;
        }
    }
#endif
    if (g_arguments->metaFile) {
        g_arguments->totalChildTables = 0;
        if (getInfoFromJsonFile()) exit(EXIT_FAILURE);
    } else {
        modify_argument();
    }

    g_arguments->fpOfInsertResult = fopen(g_arguments->output_file, "a");
    if (NULL == g_arguments->fpOfInsertResult) {
        errorPrint("failed to open %s for save result\n",
                   g_arguments->output_file);
    }
    infoPrint("taos client version: %s\n", taos_get_client_info());

    if (g_arguments->test_mode == INSERT_TEST) {
        if (insertTestProcess()) {
            errorPrint("%s", "insert test process failed\n");
            ret = -1;
        }
    } else if (g_arguments->test_mode == QUERY_TEST) {
        if (queryTestProcess(g_arguments)) {
            errorPrint("%s", "query test process failed\n");
            ret = -1;
        }
    } else if (g_arguments->test_mode == SUBSCRIBE_TEST) {
        if (subscribeTestProcess(g_arguments)) {
            errorPrint("%s", "sub test process failed\n");
            ret = -1;
        }
    }

    if ((ret == 0) && g_arguments->aggr_func) {
        queryAggrFunc();
    }
    postFreeResource();

#ifdef LINUX
    pthread_cancel(spid);
    pthread_join(spid, NULL);
#endif

    return ret;
}
