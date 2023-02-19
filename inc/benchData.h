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

#ifndef INC_BENCHDATA_H_
#define INC_BENCHDATA_H_

#include <bench.h>
/***** Global variables ******/
/***** Declare functions *****/
int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
        int disorderRange);
int generateRandData(SSuperTable *stbInfo, char *sampleDataBuf,
        int bufLen,
        int lenOfOneRow, BArray * fields, int64_t loop,
        bool tag);
int prepareStmt(SSuperTable *stbInfo, TAOS_STMT *stmt, uint64_t tableSeq);
uint32_t bindParamBatch(threadInfo *pThreadInfo,
        uint32_t batch, int64_t startTime);
int prepareSampleData(SDataBase* database, SSuperTable* stbInfo);
void generateSmlJsonTags(tools_cJSON *tagsList,
        char **sml_tags_json_array,
        SSuperTable *stbInfo,
        uint64_t start_table_from, int tbSeq);
void generateSmlJsonCols(tools_cJSON *array,
        tools_cJSON *tag, SSuperTable *stbInfo,
        uint32_t time_precision, int64_t timestamp);
void generateSmlTaosJsonTags(tools_cJSON *tagsList,
        SSuperTable *stbInfo,
        uint64_t start_table_from, int tbSeq);
void generateSmlTaosJsonCols(tools_cJSON *array,
        tools_cJSON *tag, SSuperTable *stbInfo,
        uint32_t time_precision, int64_t timestamp);
#endif  // INC_BENCHDATA_H_
