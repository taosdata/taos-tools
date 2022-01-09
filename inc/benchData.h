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

#ifndef __DEMODATA__
#define __DEMODATA__

#include "bench.h"
/***** Global variables ******/

extern int8_t *  g_randbool;
extern int8_t *  g_randtinyint;
extern uint8_t * g_randutinyint;
extern int16_t * g_randsmallint;
extern uint16_t *g_randusmallint;
extern int32_t * g_randint;
extern uint32_t *g_randuint;
extern int64_t * g_randbigint;
extern uint64_t *g_randubigint;
extern float *   g_randfloat;
extern double *  g_randdouble;
extern char *    g_randbool_buff;
extern char *    g_randint_buff;
extern char *    g_randuint_buff;
extern char *    g_rand_voltage_buff;
extern char *    g_randbigint_buff;
extern char *    g_randubigint_buff;
extern char *    g_randsmallint_buff;
extern char *    g_randusmallint_buff;
extern char *    g_randtinyint_buff;
extern char *    g_randutinyint_buff;
extern char *    g_randfloat_buff;
extern char *    g_rand_current_buff;
extern char *    g_rand_phase_buff;
extern char *    g_randdouble_buff;
extern char **   g_string_grid;
/***** Declare functions *****/
int     init_rand_data(SArguments *arguments);
int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
                      int disorderRange);
void generateStmtBuffer(char *stmtBuffer, SSuperTable *stbInfo,
                        SArguments *arguments);
int  bindParamBatch(threadInfo *pThreadInfo, uint32_t batch, int64_t startTime);
int  generateSampleFromRand(char *sampleDataBuf, int32_t lenOfOneRow, int count,
                            char *data_type, int32_t *data_length, int64_t size,
                            uint16_t iface, bool demo_mode, bool chinese,
                            uint64_t prepared_rand);
int     prepare_sample_data(SArguments *argument, SSuperTable *stbInfo);
int32_t generateSmlTags(char *sml, SSuperTable *stbInfo, uint64_t prepared_rand,
                        bool chinese);
int32_t generateSmlJsonTags(cJSON *tagsList, SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq,
                            uint64_t prepared_rand, bool chinese);
int32_t generateSmlJsonCols(cJSON *array, cJSON *tag, SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp, uint64_t prepared_rand, bool chinese);
#endif