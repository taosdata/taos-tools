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
cJSON*         root;

int main(int argc, char* argv[]) {
    g_arguments = init_argument(g_arguments);
    commandLineParseArgument(argc, argv, g_arguments);
    if (g_arguments->metaFile) {
        g_arguments->g_totalChildTables = 0;
        if (getInfoFromJsonFile(g_arguments->metaFile, g_arguments)) {
            exit(EXIT_FAILURE);
        }
    } else {
        resize_schema(g_arguments, g_arguments->db->superTbls);
    }
    if (start(g_arguments)) {
        exit(EXIT_FAILURE);
    }
    postFreeResource(g_arguments);
    return 0;
}