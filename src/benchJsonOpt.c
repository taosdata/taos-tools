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

static int parseStringParameter(char *name, cJSON *src, char *target) {
    cJSON *item = cJSON_GetObjectItem(src, name);
    if (item) {
        if (cJSON_IsString(item) && item->valuestring != NULL) {
            target = item->valuestring;
        } else {
            errorPrint("Invalid %s value\n", name);
            return -1;
        }
    }
    return 0;
}

static int parseBoolParameter(char *name, cJSON *src, bool target) {
    cJSON *item = cJSON_GetObjectItem(src, name);
    if (item) {
        if (cJSON_IsString(item) && item->valuestring != NULL) {
            if (0 == strcasecmp(item->valuestring, "yes")) {
                target = true;
                return 0;
            } else if (0 == strcasecmp(item->valuestring, "no")) {
                target = false;
                return 0;
            } else {
                errorPrint("Invalid %s value\n", name);
                return -1;
            }
        } else {
            errorPrint("Invalid %s value\n", name);
            return -1;
        }
    }
    return 0;
}

static int parseNumberParameter(char *name, cJSON *src, int64_t target,
                                bool can_be_zero) {
    cJSON *item = cJSON_GetObjectItem(src, name);
    if (item) {
        if (cJSON_IsNumber(item)) {
            if (!can_be_zero && item->valueint <= 0) {
                errorPrint("Invalid %s value: %" PRId64 "\n", name,
                           item->valueint);
                return -1;
            } else if (can_be_zero && item->valueint < 0) {
                errorPrint("Invalid %s value: %" PRId64 "\n", name,
                           item->valueint);
                return -1;
            } else {
                target = item->valueint;
            }
        } else {
            errorPrint("Invalid %s value\n", name);
            return -1;
        }
    }
    return 0;
}

static int getColumnAndTagTypeFromInsertJsonFile(cJSON *      stbInfo,
                                                 SSuperTable *superTbls) {
    int32_t code = -1;

    // columns
    cJSON *columns = cJSON_GetObjectItem(stbInfo, "columns");
    if (!columns || columns->type != cJSON_Array) goto PARSE_OVER;

    int columnSize = cJSON_GetArraySize(columns);

    int count = 1;
    int index = 0;
    int col_count = 0;
    for (int k = 0; k < columnSize; ++k) {
        cJSON *column = cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;
        cJSON *countObj = cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == cJSON_Number &&
            (int)countObj->valueint != 0) {
            col_count += (int)countObj->valueint;
        } else {
            col_count++;
        }
    }
    superTbls->col_type = (char *)calloc(col_count, sizeof(char));
    superTbls->col_length = (int32_t *)calloc(col_count, sizeof(int32_t));
    superTbls->col_null = (bool *)calloc(col_count, sizeof(bool));
    for (int k = 0; k < columnSize; ++k) {
        cJSON *column = cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;

        count = 1;
        cJSON *countObj = cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        // column info
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!dataType || dataType->type != cJSON_String) goto PARSE_OVER;

        cJSON * dataLen = cJSON_GetObjectItem(column, "len");
        int32_t length;
        if (dataLen && dataLen->type == cJSON_Number) {
            length = (int32_t)dataLen->valueint;
            if (length == 0) {
                superTbls->col_null[index] = true;
            }
        } else {
            switch (taos_convert_string_to_datatype(dataType->valuestring)) {
                case TSDB_DATA_TYPE_BOOL:
                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    length = sizeof(int8_t);
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    length = sizeof(int16_t);
                    break;
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    length = sizeof(int32_t);
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP:
                    length = sizeof(int64_t);
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    length = sizeof(float);
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    length = sizeof(double);
                    break;
                default:
                    length = g_arguments->binwidth;
                    break;
            }
        }

        for (int n = 0; n < count; ++n) {
            superTbls->col_type[index] =
                taos_convert_string_to_datatype(dataType->valuestring);
            superTbls->col_length[index] = length;
            index++;
        }
    }

    superTbls->columnCount = index;

    count = 1;
    index = 0;
    // tags
    cJSON *tags = cJSON_GetObjectItem(stbInfo, "tags");
    if (!tags || tags->type != cJSON_Array) goto PARSE_OVER;
    int tag_count = 0;
    int tagSize = cJSON_GetArraySize(tags);

    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        cJSON *countObj = cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == cJSON_Number &&
            (int)countObj->valueint != 0) {
            tag_count += (int)countObj->valueint;
        } else {
            tag_count += 1;
        }
    }

    superTbls->use_metric = true;
    superTbls->tag_type = calloc(tag_count, sizeof(char));
    superTbls->tag_null = calloc(tag_count, sizeof(bool));
    superTbls->tag_length = calloc(tag_count, sizeof(int32_t));

    // superTbls->tagCount = tagSize;
    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;

        cJSON *dataType = cJSON_GetObjectItem(tag, "type");
        if (!dataType || dataType->type != cJSON_String) goto PARSE_OVER;
        int    data_length = 0;
        cJSON *dataLen = cJSON_GetObjectItem(tag, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
            data_length = (int32_t)dataLen->valueint;
        } else {
            switch (taos_convert_string_to_datatype(dataType->valuestring)) {
                case TSDB_DATA_TYPE_BOOL:
                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    data_length = sizeof(int8_t);
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    data_length = sizeof(int16_t);
                    break;
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    data_length = sizeof(int32_t);
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP:
                    data_length = sizeof(int64_t);
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    data_length = sizeof(float);
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    data_length = sizeof(double);
                    break;
                default:
                    data_length = g_arguments->binwidth;
                    break;
            }
        }

        cJSON *countObj = cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        if ((tagSize == 1) &&
            (0 == strcasecmp(dataType->valuestring, "JSON"))) {
            superTbls->tag_type[0] = TSDB_DATA_TYPE_JSON;
            superTbls->tag_length[0] = data_length;
            superTbls->tagCount = count;
            code = 0;
            goto PARSE_OVER;
        }

        for (int n = 0; n < count; ++n) {
            superTbls->tag_type[index] =
                taos_convert_string_to_datatype(dataType->valuestring);
            superTbls->tag_length[index] = data_length;
            index++;
        }
    }

    superTbls->tagCount = index;
    code = 0;

PARSE_OVER:
    return code;
}

static int getMetaFromInsertJsonFile(cJSON *json) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }
    if (parseStringParameter("host", json, g_arguments->host) ||
        parseStringParameter("user", json, g_arguments->user) ||
        parseStringParameter("password", json, g_arguments->password) ||
        parseStringParameter("result_file", json, g_arguments->output_file) ||
        parseNumberParameter("port", json, g_arguments->port, false) ||
        parseNumberParameter("thread_count", json, g_arguments->nthreads,
                             false) ||
        parseNumberParameter("thread_pool_size", json,
                             g_arguments->nthreads_pool, false) ||
        init_taos_list() ||
        parseNumberParameter("num_of_records_per_req", json,
                             g_arguments->reqPerReq, false) ||
        parseNumberParameter("prepared_rand", json, g_arguments->prepared_rand,
                             false) ||
        parseNumberParameter("insert_interval", json,
                             g_arguments->insert_interval, true) ||
        parseBoolParameter("chinese", json, g_arguments->chinese)) {
        goto PARSE_OVER;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (!dbs || dbs->type != cJSON_Array) {
        errorPrint("%s", "Invalid databases object\n");
        goto PARSE_OVER;
    }

    int dbSize = cJSON_GetArraySize(dbs);
    tmfree(g_arguments->db->superTbls->col_length);
    tmfree(g_arguments->db->superTbls->col_type);
    tmfree(g_arguments->db->superTbls->col_null);
    tmfree(g_arguments->db->superTbls->tag_type);
    tmfree(g_arguments->db->superTbls->tag_length);
    tmfree(g_arguments->db->superTbls->tag_null);
    tmfree(g_arguments->db->superTbls);
    tmfree(g_arguments->db);
    g_arguments->db = calloc(dbSize, sizeof(SDataBase));
    g_memoryUsage += dbSize * sizeof(SDataBase);
    g_arguments->dbCount = dbSize;
    for (int i = 0; i < dbSize; ++i) {
        SDataBase *database = &(g_arguments->db[i]);
        database->dbCfg.minRows = -1;
        database->dbCfg.maxRows = -1;
        database->dbCfg.comp = -1;
        database->dbCfg.walLevel = -1;
        database->dbCfg.cacheLast = -1;
        database->dbCfg.fsync = -1;
        database->dbCfg.replica = -1;
        database->dbCfg.update = -1;
        database->dbCfg.keep = -1;
        database->dbCfg.days = -1;
        database->dbCfg.cache = -1;
        database->dbCfg.blocks = -1;
        database->dbCfg.quorum = -1;
        database->drop = true;
        cJSON *dbinfos = cJSON_GetArrayItem(dbs, i);
        if (dbinfos == NULL) continue;

        // dbinfo
        cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
        if (!dbinfo || dbinfo->type != cJSON_Object) {
            errorPrint("%s", "Cannot find dbinfo\n");
            goto PARSE_OVER;
        }

        if (parseStringParameter("name", dbinfo, database->dbName) ||
            parseNumberParameter("update", dbinfo, database->dbCfg.update,
                                 true) ||
            parseNumberParameter("replica", dbinfo, database->dbCfg.replica,
                                 true) ||
            parseNumberParameter("keep", dbinfo, database->dbCfg.keep, true) ||
            parseNumberParameter("days", dbinfo, database->dbCfg.days, true) ||
            parseNumberParameter("cache", dbinfo, database->dbCfg.cache,
                                 true) ||
            parseNumberParameter("blocks", dbinfo, database->dbCfg.blocks,
                                 true) ||
            parseNumberParameter("minRows", dbinfo, database->dbCfg.minRows,
                                 true) ||
            parseNumberParameter("maxRows", dbinfo, database->dbCfg.maxRows,
                                 true) ||
            parseNumberParameter("comp", dbinfo, database->dbCfg.comp, true) ||
            parseNumberParameter("walLevel", dbinfo, database->dbCfg.walLevel,
                                 true) ||
            parseNumberParameter("cachelast", dbinfo, database->dbCfg.cacheLast,
                                 true) ||
            parseNumberParameter("quorum", dbinfo, database->dbCfg.quorum,
                                 true) ||
            parseNumberParameter("fsync", dbinfo, database->dbCfg.fsync,
                                 true) ||
            parseBoolParameter("drop", dbinfo, database->drop)) {
            goto PARSE_OVER;
        }

        cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
        if (precision && precision->type == cJSON_String &&
            precision->valuestring != NULL) {
            if (0 == strcasecmp(precision->valuestring, "us")) {
                database->dbCfg.precision = TSDB_TIME_PRECISION_MICRO;
                database->dbCfg.sml_precision =
                    TSDB_SML_TIMESTAMP_MICRO_SECONDS;
            } else if (0 == strcasecmp(precision->valuestring, "ns")) {
                database->dbCfg.precision = TSDB_TIME_PRECISION_NANO;
                database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
            } else {
                database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
                database->dbCfg.sml_precision =
                    TSDB_SML_TIMESTAMP_MILLI_SECONDS;
            }
        } else {
            database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        }

        // super_tables
        cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
        if (!stables || stables->type != cJSON_Array) {
            errorPrint("%s", "Cannot find super_tables\n");
            goto PARSE_OVER;
        }

        int stbSize = cJSON_GetArraySize(stables);
        database->superTbls = calloc(stbSize, sizeof(SSuperTable));
        g_memoryUsage += stbSize * sizeof(SSuperTable);
        database->superTblCount = stbSize;
        for (int j = 0; j < stbSize; ++j) {
            SSuperTable *superTable = &(database->superTbls[j]);
            superTable->batchCreateTableNum = DEFAULT_CREATE_BATCH;
            superTable->childTblCount = DEFAULT_CHILDTABLES;
            superTable->insertRows = DEFAULT_INSERT_ROWS;
            superTable->disorderRange = DEFAULT_DISORDER_RANGE;
            superTable->insert_interval = g_arguments->insert_interval;
            cJSON *stbInfo = cJSON_GetArrayItem(stables, j);
            if (stbInfo == NULL) continue;

            if (parseStringParameter("name", stbInfo, superTable->stbName) ||
                parseStringParameter("childtable_prefix", stbInfo,
                                     superTable->childTblPrefix) ||
                parseNumberParameter("batch_create_tbl_num", stbInfo,
                                     superTable->batchCreateTableNum, false) ||
                parseNumberParameter("childtable_count", stbInfo,
                                     superTable->childTblCount, true) ||
                parseNumberParameter("childtable_limit", stbInfo,
                                     superTable->childTblLimit, true) ||
                parseNumberParameter("childtable_offset", stbInfo,
                                     superTable->childTblOffset, true) ||
                parseNumberParameter("timestamp_step", stbInfo,
                                     superTable->timestamp_step, false) ||
                parseNumberParameter("insert_rows", stbInfo,
                                     superTable->insertRows, true) ||
                parseNumberParameter("interlace_rows", stbInfo,
                                     superTable->interlaceRows, true) ||
                parseNumberParameter("disorder_range", stbInfo,
                                     superTable->disorderRange, true) ||
                parseNumberParameter("insert_interval", stbInfo,
                                     superTable->insert_interval, true) ||
                parseNumberParameter("partial_col_num", stbInfo,
                                     superTable->partialColumnNum, true) ||
                parseBoolParameter("escape_character", stbInfo,
                                   superTable->escape_character) ||
                parseBoolParameter("auto_create_table", stbInfo,
                                   superTable->autoCreateTable) ||
                parseBoolParameter("child_table_exists", stbInfo,
                                   superTable->childTblExists) ||
                parseBoolParameter("tcp_transfer", stbInfo,
                                   superTable->tcpTransfer) ||
                parseBoolParameter("use_sample_ts", stbInfo,
                                   superTable->useSampleTs)) {
                goto PARSE_OVER;
            }
            g_arguments->g_totalChildTables += superTable->childTblCount;

            cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
            if (dataSource && dataSource->type == cJSON_String &&
                dataSource->valuestring != NULL) {
                if (0 == strcasecmp(dataSource->valuestring, "sample")) {
                    superTable->random_data_source = false;
                } else {
                    superTable->random_data_source = true;
                }
            } else {
                superTable->random_data_source = true;
            }

            cJSON *stbIface = cJSON_GetObjectItem(
                stbInfo, "insert_mode");  // taosc , rest, stmt
            if (stbIface && stbIface->type == cJSON_String &&
                stbIface->valuestring != NULL) {
                if (0 == strcasecmp(stbIface->valuestring, "rest")) {
                    superTable->iface = REST_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
                    superTable->iface = STMT_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "sml")) {
                    superTable->iface = SML_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "sml-rest")) {
                    superTable->iface = SML_REST_IFACE;
                } else {
                    superTable->iface = TAOSC_IFACE;
                }
            } else {
                superTable->iface = TAOSC_IFACE;
            }

            cJSON *stbLineProtocol =
                cJSON_GetObjectItem(stbInfo, "line_protocol");
            if (stbLineProtocol && stbLineProtocol->type == cJSON_String &&
                stbLineProtocol->valuestring != NULL) {
                if (0 == strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                    superTable->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
                } else if (0 ==
                           strcasecmp(stbLineProtocol->valuestring, "json")) {
                    superTable->lineProtocol = TSDB_SML_JSON_PROTOCOL;
                } else {
                    superTable->lineProtocol = TSDB_SML_LINE_PROTOCOL;
                }
            } else {
                superTable->lineProtocol = TSDB_SML_LINE_PROTOCOL;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                if (0 == strcasecmp(ts->valuestring, "now")) {
                    superTable->startTimestamp =
                        taosGetTimestamp(database->dbCfg.precision);
                } else {
                    if (taos_parse_time(ts->valuestring,
                                        &(superTable->startTimestamp),
                                        (int32_t)strlen(ts->valuestring),
                                        database->dbCfg.precision, 0)) {
                        errorPrint("failed to parse time %s\n",
                                   ts->valuestring);
                        return -1;
                    }
                }
            } else {
                superTable->startTimestamp =
                    taosGetTimestamp(database->dbCfg.precision);
            }

            cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
            if (sampleFile && sampleFile->type == cJSON_String &&
                sampleFile->valuestring != NULL) {
                tstrncpy(superTable->sampleFile, sampleFile->valuestring,
                         min(MAX_FILE_NAME_LEN,
                             strlen(sampleFile->valuestring) + 1));
            } else {
                memset(superTable->sampleFile, 0, MAX_FILE_NAME_LEN);
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String) &&
                (tagsFile->valuestring != NULL)) {
                tstrncpy(superTable->tagsFile, tagsFile->valuestring,
                         MAX_FILE_NAME_LEN);
            } else {
                memset(superTable->tagsFile, 0, MAX_FILE_NAME_LEN);
            }

            cJSON *disorderRatio =
                cJSON_GetObjectItem(stbInfo, "disorder_ratio");
            if (disorderRatio && disorderRatio->type == cJSON_Number) {
                if (disorderRatio->valueint > 50) disorderRatio->valueint = 50;
                if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

                superTable->disorderRatio = (int)disorderRatio->valueint;
            } else {
                superTable->disorderRatio = 0;
            }

            if (getColumnAndTagTypeFromInsertJsonFile(
                    stbInfo, &database->superTbls[j])) {
                goto PARSE_OVER;
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}
static int getMetaFromQueryJsonFile(cJSON *json) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(json, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        g_arguments->host = host->valuestring;
    }

    cJSON *port = cJSON_GetObjectItem(json, "port");
    if (port && port->type == cJSON_Number) {
        g_arguments->port = (uint16_t)port->valueint;
    }

    cJSON *telnet_tcp_port = cJSON_GetObjectItem(json, "telnet_tcp_port");
    if (telnet_tcp_port && telnet_tcp_port->type == cJSON_Number) {
        g_arguments->telnet_tcp_port = (uint16_t)telnet_tcp_port->valueint;
    }

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        g_arguments->user = user->valuestring;
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        g_arguments->password = password->valuestring;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    cJSON *gQueryTimes = cJSON_GetObjectItem(json, "query_times");
    if (gQueryTimes && gQueryTimes->type == cJSON_Number) {
        g_queryInfo.query_times = gQueryTimes->valueint;
    } else {
        g_queryInfo.query_times = 1;
    }

    cJSON *resetCache = cJSON_GetObjectItem(json, "reset_query_cache");
    if (resetCache && resetCache->type == cJSON_String &&
        resetCache->valuestring != NULL) {
        if (0 == strcasecmp(resetCache->valuestring, "yes")) {
            g_queryInfo.reset_query_cache = true;
        }
    } else {
        g_queryInfo.reset_query_cache = false;
    }

    cJSON *threadspool = cJSON_GetObjectItem(json, "thread_pool_size");
    if (threadspool && threadspool->type == cJSON_Number) {
        g_arguments->nthreads_pool = (uint32_t)threadspool->valueint;
    } else {
        g_arguments->nthreads_pool = g_arguments->nthreads + 5;
    }

    cJSON *respBuffer = cJSON_GetObjectItem(json, "response_buffer");
    if (respBuffer && respBuffer->type == cJSON_Number) {
        g_queryInfo.response_buffer = respBuffer->valueint;
    } else {
        g_queryInfo.response_buffer = RESP_BUF_LEN;
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
        g_arguments->db->dbName = dbs->valuestring;
    }

    cJSON *queryMode = cJSON_GetObjectItem(json, "query_mode");
    if (queryMode && queryMode->type == cJSON_String &&
        queryMode->valuestring != NULL) {
        if (0 == strcasecmp(queryMode->valuestring, "rest")) {
            g_arguments->db->superTbls->iface = REST_IFACE;
        }
    }

    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(json, "specified_table_query");
    if (!specifiedQuery || specifiedQuery->type != cJSON_Object) {
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.sqlCount = 0;
    } else {
        cJSON *queryInterval =
            cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (queryInterval && queryInterval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.queryInterval =
                queryInterval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        cJSON *specifiedQueryTimes =
            cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (specifiedQueryTimes && specifiedQueryTimes->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.queryTimes =
                specifiedQueryTimes->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        cJSON *concurrent = cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (concurrent && concurrent->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)concurrent->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.concurrent = 1;
        }

        cJSON *specifiedAsyncMode = cJSON_GetObjectItem(specifiedQuery, "mode");
        if (specifiedAsyncMode && specifiedAsyncMode->type == cJSON_String &&
            specifiedAsyncMode->valuestring != NULL) {
            if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *interval = cJSON_GetObjectItem(specifiedQuery, "interval");
        if (interval && interval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                interval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                DEFAULT_SUB_INTERVAL;
        }

        cJSON *restart = cJSON_GetObjectItem(specifiedQuery, "restart");
        if (restart && restart->type == cJSON_String &&
            restart->valuestring != NULL) {
            if (0 == strcmp("no", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = false;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
        }

        cJSON *keepProgress =
            cJSON_GetObjectItem(specifiedQuery, "keepProgress");
        if (keepProgress && keepProgress->type == cJSON_String &&
            keepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 1;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // sqls
        cJSON *specifiedSqls = cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (!specifiedSqls || specifiedSqls->type != cJSON_Array) {
            g_queryInfo.specifiedQueryInfo.sqlCount = 0;
        } else {
            int superSqlSize = cJSON_GetArraySize(specifiedSqls);
            if (superSqlSize * g_queryInfo.specifiedQueryInfo.concurrent >
                MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    "failed to read json, query sql(%d) * concurrent(%d) "
                    "overflow, max is %d\n",
                    superSqlSize, g_queryInfo.specifiedQueryInfo.concurrent,
                    MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.specifiedQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                cJSON *sql = cJSON_GetArrayItem(specifiedSqls, j);
                if (sql == NULL) continue;

                cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
                if (sqlStr && sqlStr->type == cJSON_String) {
                    tstrncpy(g_queryInfo.specifiedQueryInfo.sql[j],
                             sqlStr->valuestring, BUFFER_SIZE);
                }
                // default value is -1, which mean infinite loop
                g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                cJSON *endAfterConsume =
                    cJSON_GetObjectItem(specifiedQuery, "endAfterConsume");
                if (endAfterConsume && endAfterConsume->type == cJSON_Number) {
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] =
                        (int)endAfterConsume->valueint;
                }
                if (g_queryInfo.specifiedQueryInfo.endAfterConsume[j] < -1)
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;

                g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;
                cJSON *resubAfterConsume =
                    cJSON_GetObjectItem(specifiedQuery, "resubAfterConsume");
                if ((resubAfterConsume) &&
                    (resubAfterConsume->type == cJSON_Number) &&
                    (resubAfterConsume->valueint >= 0)) {
                    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] =
                        (int)resubAfterConsume->valueint;
                }

                if (g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] < -1)
                    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if ((NULL != result) && (result->type == cJSON_String) &&
                    (result->valuestring != NULL)) {
                    tstrncpy(g_queryInfo.specifiedQueryInfo.result[j],
                             result->valuestring, MAX_FILE_NAME_LEN);
                } else {
                    memset(g_queryInfo.specifiedQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                }
            }
        }
    }

    // super_table_query
    cJSON *superQuery = cJSON_GetObjectItem(json, "super_table_query");
    if (!superQuery || superQuery->type != cJSON_Object) {
        g_queryInfo.superQueryInfo.threadCnt = 1;
        g_queryInfo.superQueryInfo.sqlCount = 0;
    } else {
        cJSON *subrate = cJSON_GetObjectItem(superQuery, "query_interval");
        if (subrate && subrate->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.queryInterval = subrate->valueint;
        } else {
            g_queryInfo.superQueryInfo.queryInterval = 0;
        }

        cJSON *superQueryTimes = cJSON_GetObjectItem(superQuery, "query_times");
        if (superQueryTimes && superQueryTimes->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
        } else {
            g_queryInfo.superQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        cJSON *threads = cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt = (uint32_t)threads->valueint;
        } else {
            g_queryInfo.superQueryInfo.threadCnt = 1;
        }

        cJSON *stblname = cJSON_GetObjectItem(superQuery, "stblname");
        if (stblname && stblname->type == cJSON_String &&
            stblname->valuestring != NULL) {
            tstrncpy(g_queryInfo.superQueryInfo.stbName, stblname->valuestring,
                     TSDB_TABLE_NAME_LEN);
        }

        cJSON *superAsyncMode = cJSON_GetObjectItem(superQuery, "mode");
        if (superAsyncMode && superAsyncMode->type == cJSON_String &&
            superAsyncMode->valuestring != NULL) {
            if (0 == strcmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *superInterval = cJSON_GetObjectItem(superQuery, "interval");
        if (superInterval && superInterval->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.subscribeInterval =
                superInterval->valueint;
        } else {
            g_queryInfo.superQueryInfo.subscribeInterval =
                DEFAULT_QUERY_INTERVAL;
        }

        cJSON *subrestart = cJSON_GetObjectItem(superQuery, "restart");
        if (subrestart && subrestart->type == cJSON_String &&
            subrestart->valuestring != NULL) {
            if (0 == strcmp("no", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = false;
            } else {
                g_queryInfo.superQueryInfo.subscribeRestart = true;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeRestart = true;
        }

        cJSON *superkeepProgress =
            cJSON_GetObjectItem(superQuery, "keepProgress");
        if (superkeepProgress && superkeepProgress->type == cJSON_String &&
            superkeepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 1;
            } else {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
        }

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.endAfterConsume = -1;
        cJSON *superEndAfterConsume =
            cJSON_GetObjectItem(superQuery, "endAfterConsume");
        if (superEndAfterConsume &&
            superEndAfterConsume->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.endAfterConsume =
                (int)superEndAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.endAfterConsume < -1)
            g_queryInfo.superQueryInfo.endAfterConsume = -1;

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.resubAfterConsume = -1;
        cJSON *superResubAfterConsume =
            cJSON_GetObjectItem(superQuery, "resubAfterConsume");
        if ((superResubAfterConsume) &&
            (superResubAfterConsume->type == cJSON_Number) &&
            (superResubAfterConsume->valueint >= 0)) {
            g_queryInfo.superQueryInfo.resubAfterConsume =
                (int)superResubAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.resubAfterConsume < -1)
            g_queryInfo.superQueryInfo.resubAfterConsume = -1;

        // supert table sqls
        cJSON *superSqls = cJSON_GetObjectItem(superQuery, "sqls");
        if (!superSqls || superSqls->type != cJSON_Array) {
            g_queryInfo.superQueryInfo.sqlCount = 0;
        } else {
            int superSqlSize = cJSON_GetArraySize(superSqls);
            if (superSqlSize > MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    "failed to read json, query sql size overflow, max is %d\n",
                    MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                cJSON *sql = cJSON_GetArrayItem(superSqls, j);
                if (sql == NULL) continue;

                cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
                if (sqlStr && sqlStr->type == cJSON_String) {
                    tstrncpy(g_queryInfo.superQueryInfo.sql[j],
                             sqlStr->valuestring, BUFFER_SIZE);
                }

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if (result != NULL && result->type == cJSON_String &&
                    result->valuestring != NULL) {
                    tstrncpy(g_queryInfo.superQueryInfo.result[j],
                             result->valuestring, MAX_FILE_NAME_LEN);
                } else {
                    memset(g_queryInfo.superQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                }
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}

int getInfoFromJsonFile() {
    char *  file = g_arguments->metaFile;
    int32_t code = -1;
    FILE *  fp = fopen(file, "r");
    if (!fp) {
        errorPrint("failed to read %s, reason:%s\n", file, strerror(errno));
        return code;
    }

    int   maxLen = MAX_JSON_BUFF;
    char *content = calloc(1, maxLen + 1);
    int   len = (int)fread(content, 1, maxLen, fp);
    if (len <= 0) {
        errorPrint("failed to read %s, content is null\n", file);
        goto PARSE_OVER;
    }

    content[len] = 0;
    root = cJSON_Parse(content);
    if (root == NULL) {
        errorPrint("failed to cjson parse %s, invalid json format\n", file);
        goto PARSE_OVER;
    }

    cJSON *filetype = cJSON_GetObjectItem(root, "filetype");
    if (filetype && filetype->type == cJSON_String &&
        filetype->valuestring != NULL) {
        if (0 == strcasecmp("insert", filetype->valuestring)) {
            g_arguments->test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            g_arguments->test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            g_arguments->test_mode = SUBSCRIBE_TEST;
        } else {
            errorPrint("%s", "failed to read json, filetype not support\n");
            goto PARSE_OVER;
        }
    } else {
        g_arguments->test_mode = INSERT_TEST;
    }

    if (INSERT_TEST == g_arguments->test_mode) {
        code = getMetaFromInsertJsonFile(root);
    } else {
        memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
        code = getMetaFromQueryJsonFile(root);
    }
PARSE_OVER:
    free(content);
    fclose(fp);
    return code;
}
