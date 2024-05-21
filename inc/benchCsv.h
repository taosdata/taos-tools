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

#ifndef INC_BENCHCSV_H_
#define INC_BENCHCSV_H_

#include <bench.h>

int csvTestProcess();

int genWithSTable(SDataBase* db, SSuperTable* stb, char* outDir);

void csvWriteThread(void* param);

char * genTagData(char* buf, SSuperTable* stb, int64_t i, int64_t *k);

char * genColumnData(char* buf, SSuperTable* stb, int64_t i, int precision, int64_t *k)

int32_t genRowByField(char* buf, int32_t pos1, BArray* fields, int16_t fieldCnt, char* binanryPrefix, char* ncharPrefix, int64_t *k);

void obtainCsvFile(char * outFile, SDataBase* db, SSuperTable* stb, char* outDir);

#endif  // INC_BENCHCSV_H_
