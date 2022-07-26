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

static void generateSmlJsonTags(tools_cJSON *tagsList, SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    tools_cJSON * tags = tools_cJSON_CreateObject();
    char *  tbName = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64 "",
             stbInfo->childTblPrefix, tbSeq + start_table_from);
    tools_cJSON_AddStringToObject(tags, "id", tbName);
    char *tagName = benchCalloc(1, TSDB_MAX_TAGS, true);
    for (int i = 0; i < stbInfo->tags->size; i++) {
        Field * tag = benchArrayGet(stbInfo->tags, i);
        tools_cJSON *tagObj = tools_cJSON_CreateObject();
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (tag->type) {
            case TSDB_DATA_TYPE_BOOL: {
                tools_cJSON_AddBoolToObject(tagObj, "value", (taosRandom() % 2) & 1);
                tools_cJSON_AddStringToObject(tagObj, "type", "bool");
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                tools_cJSON_AddNumberToObject(
                        tagObj, "value",
                        (float)(tag->min +
                            (taosRandom() % (tag->max - tag->min)) +
                            taosRandom() % 1000 / 1000.0));
                tools_cJSON_AddStringToObject(tagObj, "type", "float");
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                tools_cJSON_AddNumberToObject(
                        tagObj, "value",
                        (double)(tag->min + (taosRandom() % (tag->max - tag->min)) +
                             taosRandom() % 1000000 / 1000000.0));
                tools_cJSON_AddStringToObject(tagObj, "type", "double");
                break;
            }

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)benchCalloc(tag->length + 1, 1, false);
                rand_string(buf, tag->length, g_arguments->chinese);
                if (tag->type == TSDB_DATA_TYPE_BINARY) {
                    tools_cJSON_AddStringToObject(tagObj, "value", buf);
                    tools_cJSON_AddStringToObject(tagObj, "type", "binary");
                } else {
                    tools_cJSON_AddStringToObject(tagObj, "value", buf);
                    tools_cJSON_AddStringToObject(tagObj, "type", "nchar");
                }
                tmfree(buf);
                break;
            }
            default:
                tools_cJSON_AddNumberToObject(
                        tagObj, "value",
                        tag->min + (taosRandom() % (tag->max - tag->min)));
                tools_cJSON_AddStringToObject(tagObj, "type", taos_convert_datatype_to_string(tag->type));
                break;
        }
        tools_cJSON_AddItemToObject(tags, tagName, tagObj);
    }
    tools_cJSON_AddItemToArray(tagsList, tags);
    tmfree(tagName);
    tmfree(tbName);
}

int prepare_sml(threadInfo* pThreadInfo, char* dbName, SSuperTable* stbInfo) {
    pThreadInfo->conn = init_bench_conn();
    if (pThreadInfo->conn == NULL) {
        return -1;
    }
    if (taos_select_db(pThreadInfo->conn->taos, dbName)) {
        errorPrint(stderr, "taos select database(%s) failed\n", dbName);
        return -1;
    }
    pThreadInfo->max_sql_len = stbInfo->lenOfCols + stbInfo->lenOfTags;
    if (stbInfo->iface == SML_REST_IFACE) {
        pThreadInfo->buffer = benchCalloc(1, g_arguments->reqPerReq * (1 + pThreadInfo->max_sql_len), true);
    }
    if (stbInfo->lineProtocol != TSDB_SML_JSON_PROTOCOL) {
        pThreadInfo->sml_tags = (char **)benchCalloc(pThreadInfo->ntables, sizeof(char *), true);
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            pThreadInfo->sml_tags[t] = benchCalloc(1, stbInfo->lenOfTags, true);
        }

        for (int t = 0; t < pThreadInfo->ntables; t++) {
            generateRandData(stbInfo, pThreadInfo->sml_tags[t],
                            stbInfo->lenOfCols + stbInfo->lenOfTags,
                            stbInfo->tags, 1, true);
        }
        pThreadInfo->lines = benchCalloc(g_arguments->reqPerReq, sizeof(char *), true);

        for (int j = 0; j < g_arguments->reqPerReq; j++) {
            pThreadInfo->lines[j] = benchCalloc(1, pThreadInfo->max_sql_len, true);
        }
    } else {
        pThreadInfo->json_array = tools_cJSON_CreateArray();
        pThreadInfo->sml_json_tags = tools_cJSON_CreateArray();
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            generateSmlJsonTags(pThreadInfo->sml_json_tags, stbInfo,
                                pThreadInfo->start_table_from, t);
        }
        pThreadInfo->lines = (char **)benchCalloc(1, sizeof(char *), true);
    }
    return 0;
}

static void generateSmlJsonCols(tools_cJSON *array, tools_cJSON *tag, SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp) {
    tools_cJSON * record = tools_cJSON_CreateObject();
    tools_cJSON * ts = tools_cJSON_CreateObject();
    tools_cJSON_AddNumberToObject(ts, "value", (double)timestamp);
    if (time_precision == TSDB_SML_TIMESTAMP_MILLI_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "ms");
    } else if (time_precision == TSDB_SML_TIMESTAMP_MICRO_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "us");
    } else if (time_precision == TSDB_SML_TIMESTAMP_NANO_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "ns");
    }
    tools_cJSON *value = tools_cJSON_CreateObject();
    Field* col = benchArrayGet(stbInfo->cols, 0);
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL:
            tools_cJSON_AddBoolToObject(value, "value", (taosRandom() % 2) & 1);
            tools_cJSON_AddStringToObject(value, "type", "bool");
            break;
        case TSDB_DATA_TYPE_FLOAT:
            tools_cJSON_AddNumberToObject(
                value, "value",
                (float)(col->min +
                        (taosRandom() % (col->max - col->min)) +
                        taosRandom() % 1000 / 1000.0));
            tools_cJSON_AddStringToObject(value, "type", "float");
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            tools_cJSON_AddNumberToObject(
                value, "value",
                (double)(col->min +
                         (taosRandom() % (col->max - col->min)) +
                         taosRandom() % 1000000 / 1000000.0));
            tools_cJSON_AddStringToObject(value, "type", "double");
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            if (col->type == TSDB_DATA_TYPE_BINARY) {
                tools_cJSON_AddStringToObject(value, "value", buf);
                tools_cJSON_AddStringToObject(value, "type", "binary");
            } else {
                tools_cJSON_AddStringToObject(value, "value", buf);
                tools_cJSON_AddStringToObject(value, "type", "nchar");
            }
            tmfree(buf);
            break;
        }
        default:
            tools_cJSON_AddNumberToObject(
                    value, "value",
                    (double)col->min +
                    (taosRandom() % (col->max - col->min)));
            tools_cJSON_AddStringToObject(value, "type", taos_convert_datatype_to_string(col->type));
            break;
    }
    tools_cJSON_AddItemToObject(record, "timestamp", ts);
    tools_cJSON_AddItemToObject(record, "value", value);
    tools_cJSON_AddItemToObject(record, "tags", tag);
    tools_cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    tools_cJSON_AddItemToArray(array, record);
}

void *sync_write_sml_progressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout,
              "thread[%d] start progressive schemaless inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampMs();
    int64_t   endTs;

    int32_t pos = 0;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        int64_t  timestamp = pThreadInfo->start_time;

        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            if (g_arguments->terminate) {
                goto free_of_progressive;
            }
            int32_t generated = 0;
            for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    tools_cJSON *tag = tools_cJSON_Duplicate(
                        tools_cJSON_GetArrayItem(
                            pThreadInfo->sml_json_tags,
                            (int)tableSeq - pThreadInfo->start_table_from),true);
                    generateSmlJsonCols( pThreadInfo->json_array, tag, stbInfo,
                                pThreadInfo->sml_precision, timestamp);
                } else if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
                    snprintf( pThreadInfo->lines[j], 
                              stbInfo->lenOfCols + stbInfo->lenOfTags,
                              "%s %s %" PRId64 "",
                              pThreadInfo->sml_tags[(int)tableSeq - pThreadInfo->start_table_from],
                              stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols, timestamp);
                } else {
                    snprintf( pThreadInfo->lines[j],
                              stbInfo->lenOfCols + stbInfo->lenOfTags,
                              "%s %" PRId64 " %s %s", stbInfo->stbName,
                              timestamp,
                              stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols,
                              pThreadInfo->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                }
                pos++;
                if (pos >= g_arguments->prepared_rand) {
                    pos = 0;
                }
                timestamp += stbInfo->timestamp_step;
                if (stbInfo->disorderRatio > 0) {
                    int rand_num = taosRandom() % 100;
                    if (rand_num < stbInfo->disorderRatio) {
                        timestamp -= (taosRandom() % stbInfo->disorderRange);
                    }
                }
                generated++;
                if (i + generated >= stbInfo->insertRows) {
                    break;
                }
            }
            if (!stbInfo->non_stop) {
                i += generated;
            }
            // only measure insert
            startTs = toolsGetTimestampUs();
            if (execInsert(pThreadInfo, generated)) {
                g_fail = true;
                goto free_of_progressive;
                
            }
            endTs = toolsGetTimestampUs();
            pThreadInfo->totalInsertRows += generated;
            if (stbInfo->iface == SML_REST_IFACE) {
                memset(pThreadInfo->buffer, 0, g_arguments->reqPerReq *
                        (pThreadInfo->max_sql_len + 1));
            } else {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    tools_cJSON_Delete(pThreadInfo->json_array);
                    pThreadInfo->json_array = tools_cJSON_CreateArray();
                    tmfree(pThreadInfo->lines[0]);
                } else {
                    for (int j = 0; j < generated; ++j) {
                        memset(pThreadInfo->lines[j], 0, pThreadInfo->max_sql_len);
                    }
                }
            }

            int64_t delay = endTs - startTs;
            performancePrint(stdout, "insert execution time is %.6f s\n",
                             delay / 1E6);

            int64_t * pDelay = benchCalloc(1, sizeof(int64_t), false);
            *pDelay = delay;
            benchArrayPush(pThreadInfo->delayList, pDelay);
            pThreadInfo->totalDelay += delay;

            int64_t currentPrintTime = toolsGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(stdout,
                          "thread[%d] has currently inserted rows: "
                          "%" PRId64 "\n",
                          pThreadInfo->threadID, pThreadInfo->totalInsertRows);
                lastPrintTime = currentPrintTime;
            }
            if (i >= stbInfo->insertRows) {
                break;
            }
        }  // insertRows
    }      // tableSeq
free_of_progressive:
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;
        infoPrint(stdout,
                  "thread[%d] completed total inserted rows: %" PRIu64
                          ", %.2f records/second\n",
                  pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                  (double)(pThreadInfo->totalInsertRows /
                           ((double)pThreadInfo->totalDelay / 1000000.0)));
    return NULL;
}

void *sync_write_sml_interlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    infoPrint(stdout,
              "thread[%d] start interlace inserting into table from "
              "%" PRIu64 " to %" PRIu64 "\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from,
              pThreadInfo->end_table_to);

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    int64_t pos = 0;
    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    int64_t   startTs = toolsGetTimestampMs();
    int64_t   endTs = toolsGetTimestampMs();
    int32_t    generated = 0;
    uint64_t   tableSeq = pThreadInfo->start_table_from;

    while (insertRows > 0) {
        generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }
        for (int i = 0; i < batchPerTblTimes; ++i) {
            if (g_arguments->terminate) {
                goto free_of_interlace;
            }
            int64_t timestamp = pThreadInfo->start_time;
            for (int64_t j = 0; j < interlaceRows; ++j) {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    tools_cJSON *tag = tools_cJSON_Duplicate(
                        tools_cJSON_GetArrayItem(
                            pThreadInfo->sml_json_tags,
                            (int)tableSeq - pThreadInfo->start_table_from), true);
                        generateSmlJsonCols(pThreadInfo->json_array, tag, stbInfo,
                                pThreadInfo->sml_precision, timestamp);
                } else if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
                    snprintf(pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %s %" PRId64 "", 
                                pThreadInfo->sml_tags[(int)tableSeq -
                                pThreadInfo->start_table_from],
                                stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols,
                                timestamp);
                } else {
                    snprintf(pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                timestamp,
                                stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols,
                                pThreadInfo->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                }
                generated++;
                timestamp += stbInfo->timestamp_step;
                if (stbInfo->disorderRatio > 0) {
                    int rand_num = taosRandom() % 100;
                    if (rand_num < stbInfo->disorderRatio) {
                        timestamp -= (taosRandom() % stbInfo->disorderRange);
                    }
                }
            }

            tableSeq++;
            pThreadInfo->totalInsertRows += interlaceRows;
            if (tableSeq > pThreadInfo->end_table_to) {
                tableSeq = pThreadInfo->start_table_from;
                pThreadInfo->start_time +=
                    interlaceRows * stbInfo->timestamp_step;
                if (!stbInfo->non_stop) {
                    insertRows -= interlaceRows;
                }
                if (stbInfo->insert_interval > 0) {
                    performancePrint(stdout, "sleep %" PRIu64 " ms\n",
                                     stbInfo->insert_interval);
                    toolsMsleep((int32_t)stbInfo->insert_interval);
                }
                break;
            }
        }

        startTs = toolsGetTimestampUs();

        if(execInsert(pThreadInfo, generated)) {
            g_fail = true;
            goto free_of_interlace;
        }

        endTs = toolsGetTimestampUs();
        if (stbInfo->iface == SML_REST_IFACE) {
            memset(pThreadInfo->buffer, 0,
                    g_arguments->reqPerReq * (pThreadInfo->max_sql_len + 1));
        } else {
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                tools_cJSON_Delete(pThreadInfo->json_array);
                pThreadInfo->json_array = tools_cJSON_CreateArray();
                tmfree(pThreadInfo->lines[0]);
            } else {
                for (int j = 0; j < generated; ++j) {
                    memset(pThreadInfo->lines[j], 0, pThreadInfo->max_sql_len);
                }
            }
        }

        int64_t delay = endTs - startTs;
        performancePrint(stdout, "insert execution time is %10.2f ms\n",
                         delay / 1000.0);

        int64_t * pdelay = benchCalloc(1, sizeof(int64_t), false);
        *pdelay = delay;
        benchArrayPush(pThreadInfo->delayList, pdelay);
        pThreadInfo->totalDelay += delay;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(stdout,
                          "thread[%d] has currently inserted rows: %" PRIu64
                                  "\n",
                          pThreadInfo->threadID, pThreadInfo->totalInsertRows);
            lastPrintTime = currentPrintTime;
        }
    }
free_of_interlace:
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;

    infoPrint(stdout,
                  "thread[%d] completed total inserted rows: %" PRIu64
                          ", %.2f records/second\n",
                  pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                  (double)(pThreadInfo->totalInsertRows /
                           ((double)pThreadInfo->totalDelay / 1000000.0)));
    return NULL;
}
