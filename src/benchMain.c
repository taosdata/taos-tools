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
FILE *         g_fpOfInsertResult = NULL;
SDataBase *    db;
SArguments     g_args;
SQueryMetaInfo g_queryInfo;
bool           g_fail = false;
cJSON *        root;

int main(int argc, char *argv[]) {
    init_argument(&g_args);
    commandLineParseArgument(argc, argv, &g_args);
    if (g_args.metaFile) {
        g_args.g_totalChildTables = 0;
        if (getInfoFromJsonFile(g_args.metaFile, &g_args)) {
            exit(EXIT_FAILURE);
        }
    } else {
        db = calloc(1, sizeof(SDataBase));
        db[0].superTbls = calloc(1, sizeof(SSuperTable));
        setParaFromArg(&g_args, db);
    }
    if (test(&g_args, db)) {
        exit(EXIT_FAILURE);
    }
    postFreeResource(&g_args, db);
    return 0;
}