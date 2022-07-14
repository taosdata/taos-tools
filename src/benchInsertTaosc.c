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

void init_taosc_thread(threadInfo* pThreadInfo) {
    pThreadInfo->taos = benchConnectTaos();
    pThreadInfo->buffer = benchCalloc(1, MAX_SQL_LEN, true);
}

static void update_timestamp(SSuperTable* stbInfo, int64_t* timestamp) {
    *timestamp += stbInfo->timestamp_step;
    if (stbInfo->disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if (rand_num < stbInfo->disorderRatio) {
            *timestamp -= (taosRandom() % stbInfo->disorderRange);
        }
    }
}

static bool add_sql_header(threadInfo* pThreadInfo, int64_t tableIndex, int64_t* timestamp, int* pos, int* sample_pos) {
    SDataBase * database = pThreadInfo->db;
    SSuperTable * stbInfo = pThreadInfo->stbInfo;
    char* tableName = stbInfo->childTblName[tableIndex];
    if (stbInfo->partialColumnNum < stbInfo->cols->size) {
        if (stbInfo->autoCreateTable) {
            int length = strlen(pThreadInfo->buffer) + TSDB_DB_NAME_LEN + 2*TSDB_TABLE_NAME_LEN +
                    stbInfo->lenOfCols + stbInfo->lenOfTags + strlen(stbInfo->partialColumnNameBuf) + 128;
            if (length < MAX_SQL_LEN) {
                if (stbInfo->useSampleTs && !stbInfo->random_data_source) {
                    *pos += sprintf(pThreadInfo->buffer + *pos,
                                    " %s.%s (%s) using %s tags (%s) values (%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->partialColumnNameBuf,
                                    stbInfo->stbName,
                                    stbInfo->tagDataBuf + stbInfo->lenOfTags * tableIndex,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                } else {
                    *pos += sprintf(pThreadInfo->buffer + *pos, " %s.%s (%s) using %s tags (%s) values (%" PRId64 ",%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->partialColumnNameBuf,
                                    stbInfo->stbName,
                                    stbInfo->tagDataBuf + stbInfo->lenOfTags * tableIndex,
                                    *timestamp,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                }
                (*sample_pos)++;
                if (*sample_pos >= g_arguments->prepared_rand) {
                    *sample_pos = 0;
                }
                update_timestamp(stbInfo, timestamp);
                return true;
            }
        } else {
            int length = strlen(pThreadInfo->buffer) + TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN
                    + strlen(stbInfo->partialColumnNameBuf) + stbInfo->lenOfCols + 128;
            if (length < MAX_SQL_LEN) {
                if (stbInfo->useSampleTs && !stbInfo->random_data_source) {
                    *pos += sprintf(pThreadInfo->buffer + *pos, "%s.%s (%s) values (%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->partialColumnNameBuf,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                } else {
                    *pos += sprintf(pThreadInfo->buffer + *pos, " %s.%s (%s) values (%" PRId64 ",%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->partialColumnNameBuf,
                                    *timestamp,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                }
                (*sample_pos)++;
                if (*sample_pos >= g_arguments->prepared_rand) {
                    *sample_pos = 0;
                }
                update_timestamp(stbInfo, timestamp);
                return true;
            }
        }
    } else {
        if (stbInfo->autoCreateTable) {
            int length = strlen(pThreadInfo->buffer) + TSDB_DB_NAME_LEN + 2*TSDB_TABLE_NAME_LEN
                    + stbInfo->lenOfTags + stbInfo->lenOfCols + 128;
            if (length < MAX_SQL_LEN) {
                if (stbInfo->useSampleTs && !stbInfo->random_data_source) {
                    *pos += sprintf(pThreadInfo->buffer + *pos,
                                    " %s.%s using %s tags (%s) values (%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->stbName,
                                    stbInfo->tagDataBuf + stbInfo->lenOfTags * tableIndex,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                } else {
                    *pos += sprintf(pThreadInfo->buffer + *pos, " %s.%s using %s tags (%s) values (%" PRId64 ",%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->stbName,
                                    stbInfo->tagDataBuf + stbInfo->lenOfTags * tableIndex,
                                    *timestamp,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                }
                (*sample_pos)++;
                if (*sample_pos >= g_arguments->prepared_rand) {
                    *sample_pos = 0;
                }
                update_timestamp(stbInfo, timestamp);
                return true;
            }
        } else {
            int length = strlen(pThreadInfo->buffer) + TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN +
                    stbInfo->lenOfCols + 128;
            if (length < MAX_SQL_LEN) {
                if (stbInfo->useSampleTs && !stbInfo->random_data_source) {
                    *pos += sprintf(pThreadInfo->buffer + *pos, "%s.%s values (%s)",
                                    database->dbName,
                                    tableName,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                } else {
                    *pos += sprintf(pThreadInfo->buffer + *pos, " %s.%s values (%" PRId64 ",%s)",
                                    database->dbName,
                                    tableName,
                                    *timestamp,
                                    stbInfo->sampleDataBuf + *sample_pos * stbInfo->lenOfCols);
                }
                (*sample_pos)++;
                if (*sample_pos >= g_arguments->prepared_rand) {
                    *sample_pos = 0;
                }
                update_timestamp(stbInfo, timestamp);
                return true;
            }
        }
    }
    return false;
}

static inline bool add_sql_values(threadInfo* pThreadInfo, int* pos, int* sample_pos, int64_t* timestamp) {
    SSuperTable * stbInfo = pThreadInfo->stbInfo;
    if (stbInfo->lenOfCols + strlen(pThreadInfo->buffer) < MAX_SQL_LEN) {
        if (stbInfo->useSampleTs && !stbInfo->random_data_source) {
             sprintf(pThreadInfo->buffer + *pos,"(%s)",
                             stbInfo->sampleDataBuf +
                             *sample_pos * stbInfo->lenOfCols);
        } else {
            sprintf(pThreadInfo->buffer + *pos, "(%" PRId64 ",%s)", *timestamp,
                           stbInfo->sampleDataBuf +
                           *sample_pos * stbInfo->lenOfCols);
        }
        *pos = strlen(pThreadInfo->buffer);
        (*sample_pos)++;
        if (*sample_pos >= g_arguments->prepared_rand) {
            *sample_pos = 0;
        }
        update_timestamp(pThreadInfo->stbInfo, timestamp);
        return true;
    }
    return false;
}

void* taosc_progressive_insert(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable * stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout, "thread[%d] start taosc progressive inserting\n", pThreadInfo->threadID);
    int64_t lastPrintTime = toolsGetTimestampMs();
    int64_t startTs, endTs;
    bool start_sql = true;
    int pos = 0;
    int sample_pos = 0;
    int inserted = 0;
    int batch = 0;
    int64_t tableIndex = pThreadInfo->start_table_from;
    int64_t timestamp = stbInfo->startTimestamp;
    while (tableIndex <= pThreadInfo->end_table_to) {
        if (start_sql) {
            memset(pThreadInfo->buffer, 0, MAX_SQL_LEN);
            strcpy(pThreadInfo->buffer, STR_INSERT_INTO);
            pos = strlen(pThreadInfo->buffer);
            batch = 0;
            start_sql = false;
        }
        if (add_sql_header(pThreadInfo, tableIndex, &timestamp, &pos, &sample_pos)) {
            inserted ++;
            batch ++;
            if (batch == g_arguments->reqPerReq) {
                start_sql = true;
            }
            while (inserted < stbInfo->insertRows && batch < g_arguments->reqPerReq) {
                if (add_sql_values(pThreadInfo, &pos, &sample_pos, &timestamp)) {
                    inserted ++;
                    batch ++;
                    if (batch == g_arguments->reqPerReq) {
                        start_sql = true;
                        break;
                    }
                } else {
                    start_sql = true;
                    break;
                }
            }
            if (inserted == stbInfo->insertRows) {
                tableIndex++;
                timestamp = stbInfo->startTimestamp;
                inserted = 0;
                if (tableIndex <= pThreadInfo->end_table_to && !start_sql) {
                    continue;
                }
            }
        } else {
            start_sql = true;
        }
        if (g_arguments->terminate) {
            return NULL;
        }
        startTs = toolsGetTimestampUs();
        if (queryDbExec(pThreadInfo->taos, pThreadInfo->buffer, true)) {
            g_fail = true;
            return NULL;
        }
        endTs = toolsGetTimestampUs();
        pThreadInfo->totalInsertRows += batch;
        record_delay(pThreadInfo, startTs, endTs);
        debugPrint(stdout, "actual batch: %d\n", batch);
        thread_print_insert_progress(pThreadInfo, &lastPrintTime);
    }
    return NULL;
}

void clean_taosc_thread(threadInfo* pThreadInfo) {
    taos_close(pThreadInfo->taos);
    tmfree(pThreadInfo->buffer);
}