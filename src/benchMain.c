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
int64_t        g_totalChildTables = DEFAULT_CHILDTABLES;
int64_t        g_actualChildTables = 0;
int64_t        g_autoCreatedChildTables = 0;
int64_t        g_existedChildTables = 0;
FILE *         g_fpOfInsertResult = NULL;
SDataBase *    db;
SArguments     g_args;
SQueryMetaInfo g_queryInfo;
bool           g_fail = false;
bool           custom_col_num = false;
cJSON *        root;
TAOS_POOL      g_taos_pool;

int main(int argc, char *argv[]) {
    init_g_args(&g_args);
    if (parse_args(argc, argv, &g_args)) {
        exit(EXIT_FAILURE);
    }
    if (g_args.metaFile) {
        g_totalChildTables = 0;
        if (getInfoFromJsonFile(g_args.metaFile)) {
            exit(EXIT_FAILURE);
        }
    } else {
        db = calloc(1, sizeof(SDataBase));
        db[0].superTbls = calloc(1, sizeof(SSuperTable));
        setParaFromArg(&g_args);
    }
    if (test(&g_args)) {
        exit(EXIT_FAILURE);
    }
    postFreeResource();
    return 0;
}