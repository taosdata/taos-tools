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

int main(int argc, char* argv[]) {
    init_argument();
    commandLineParseArgument(argc, argv);
    if (g_arguments->metaFile) {
        g_arguments->g_totalChildTables = 0;
        if (getInfoFromJsonFile()) exit(EXIT_FAILURE);
    } else {
        modify_argument();
    }

    if (strlen(configDir)) {
        wordexp_t full_path;
        if (wordexp(configDir, &full_path, 0) != 0) {
            errorPrint("Invalid path %s\n", configDir);
            return -1;
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
    }

    if (g_arguments->test_mode == INSERT_TEST) {
        if (insertTestProcess()) exit(EXIT_FAILURE);
    } else if (g_arguments->test_mode == QUERY_TEST) {
        if (queryTestProcess(g_arguments)) exit(EXIT_FAILURE);
        for (int64_t i = 0; i < g_queryInfo.superQueryInfo.childTblCount; ++i) {
            tmfree(g_queryInfo.superQueryInfo.childTblName[i]);
        }
        tmfree(g_queryInfo.superQueryInfo.childTblName);
    } else if (g_arguments->test_mode == SUBSCRIBE_TEST) {
        if (subscribeTestProcess(g_arguments)) exit(EXIT_FAILURE);
    }
    if (g_arguments->aggr_func) {
        queryAggrFunc(g_arguments, g_arguments->pool);
    }
    postFreeResource();
    return 0;
}