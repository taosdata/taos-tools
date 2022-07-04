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
SArguments*    g_arguments;
SQueryMetaInfo g_queryInfo;
bool           g_fail = false;
uint64_t       g_memoryUsage = 0;
cJSON*         root;

void benchQueryInterruptHandler(int32_t signum, void* sigingo, void* context) {
    sem_post(g_arguments->cancelSem);
}

void* benchCancelHandler(void* arg) {
    if (bsem_wait(g_arguments->cancelSem) != 0) {
        taosMsleep(10);
    }
    infoPrint(stdout, "%s", "Receive SIGINT or other signal, quit taosBenchmark\n");
    g_arguments->terminate = true;
    return NULL;
}

int main(int argc, char* argv[]) {
    init_argument();
#ifdef LINUX
    if (sem_init(g_arguments->cancelSem, 0, 0) != 0) {
        errorPrint(stderr, "%s", "failed to create cancel semaphore\n");
        exit(EXIT_FAILURE);
    }
#elif DARWIN
    g_arguments->cancelSem = sem_open("cancelSem", O_CREAT|O_EXCL, S_IRWXU, 0);
    if (g_arguments->cancelSem == SEM_FAILED) {
        errorPrint(stderr, "failed to create cancel semaphore, reason: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
#endif
    pthread_t spid = {0};
    pthread_create(&spid, NULL, benchCancelHandler, NULL);
    benchSetSignal(SIGINT, benchQueryInterruptHandler);
    commandLineParseArgument(argc, argv);
    if (g_arguments->metaFile) {
        g_arguments->g_totalChildTables = 0;
        if (getInfoFromJsonFile()) exit(EXIT_FAILURE);
    } else {
        modify_argument();
    }

    g_arguments->fpOfInsertResult = fopen(g_arguments->output_file, "a");
    if (NULL == g_arguments->fpOfInsertResult) {
        errorPrint(stderr, "failed to open %s for save result\n",
                   g_arguments->output_file);
    }
    infoPrint(stdout, "taos client version: %s\n", taos_get_client_info());
    if (g_arguments->test_mode == INSERT_TEST) {
        if (insertTestProcess()) exit(EXIT_FAILURE);
    } else if (g_arguments->test_mode == QUERY_TEST) {
        if (queryTestProcess(g_arguments)) {
            exit(EXIT_FAILURE);
        }
    } else if (g_arguments->test_mode == SUBSCRIBE_TEST) {
        if (subscribeTestProcess(g_arguments)) exit(EXIT_FAILURE);
    }
    if (g_arguments->aggr_func) {
        queryAggrFunc(g_arguments, g_arguments->pool);
    }
    postFreeResource();
    return 0;
}