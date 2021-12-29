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

extern char *    g_sampleDataBuf;
extern char *    g_sampleBindBatchArray;
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
int     init_rand_data();
char *  rand_bool_str();
int32_t rand_bool();
char *  rand_tinyint_str();
int32_t rand_tinyint();
char *  rand_utinyint_str();
int32_t rand_utinyint();
char *  rand_smallint_str();
int32_t rand_smallint();
char *  rand_usmallint_str();
int32_t rand_usmallint();
char *  rand_int_str();
int32_t rand_int();
char *  rand_uint_str();
int32_t rand_uint();
char *  rand_bigint_str();
int64_t rand_bigint();
char *  rand_ubigint_str();
int64_t rand_ubigint();
char *  rand_float_str();
float   rand_float();
char *  demo_current_float_str();
char *  demo_voltage_int_str();
char *  demo_phase_float_str();
void    rand_string(char *str, int size);
char *  rand_double_str();
double  rand_double();

int     generateTagValuesForStb(SSuperTable *stbInfo, int64_t tableSeq,
                                char *tagsValBuf);
int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
                      int disorderRange);
int bindParamBatch(threadInfo *pThreadInfo, uint32_t batch, int64_t startTime);
int32_t prepareStmtWithoutStb(threadInfo *pThreadInfo, char *tableName,
                              uint32_t batch, int64_t startTime);

int     generateSampleFromRand(char *sampleDataBuf, int32_t lenOfOneRow,
                               int columnCount, char *data_type,
                               int32_t *data_length);
int     prepareSampleData();
int32_t generateSmlConstPart(char *sml, SSuperTable *stbInfo,
                             threadInfo *pThreadInfo, int tbSeq);
int32_t generateSmlMutablePart(char *line, char *sml, SSuperTable *stbInfo,
                               threadInfo *pThreadInfo, int64_t timestamp);
int32_t generateSmlJsonTags(cJSON *tagsList, SSuperTable *stbInfo,
                            threadInfo *pThreadInfo, int tbSeq);
int32_t generateSmlJsonCols(cJSON *array, cJSON *tag, SSuperTable *stbInfo,
                            threadInfo *pThreadInfo, int64_t timestamp);
#endif