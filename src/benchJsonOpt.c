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

static int getFieldFromInsertJsonFile(cJSON* srcs, BArray* fields, bool isTag) {
    int code = 1;
    int fieldSize = 0;
    for (int i = 0; i < cJSON_GetArraySize(srcs); ++i) {
        cJSON *src = cJSON_GetArrayItem(srcs, i);
        if (cJSON_IsObject(src)) {
            int64_t count = 1;
            cJSON *countObj = cJSON_GetObjectItem(src, "count");
            if (cJSON_IsNumber(countObj)) {
                if (countObj->valueint > 0) {
                    count = countObj->valueint;
                } else {
                    errorPrint(stderr, "Invalid count value: %"PRId64"\n", countObj->valueint);
                    goto PARSE_OVER;
                }
            }
            uint16_t type;
            cJSON * dataType = cJSON_GetObjectItem(src, "type");
            if (cJSON_IsString(dataType)) {
                type = taos_convert_string_to_datatype(dataType->valuestring, 0);
            } else {
                errorPrint(stderr, "%s", "Invalid or miss type value\n");
                goto PARSE_OVER;
            }
            for (int j = 0; j < count; ++j) {
                char fieldName[TSDB_COL_NAME_LEN] = {0};
                Field * field = benchCalloc(1, sizeof(Field), true);
                cJSON* dataName = cJSON_GetObjectItem(src, "name");
                if (cJSON_IsString(dataName)) {
                    if (j == 0) {
                        tstrncpy(fieldName, dataName->valuestring, TSDB_COL_NAME_LEN);
                    } else {
                        snprintf(fieldName, TSDB_COL_NAME_LEN, "%s_%d", dataName->valuestring, j);
                    }
                } else {
                    if (isTag) {
                        snprintf(fieldName, TSDB_COL_NAME_LEN, "t%d", fieldSize);
                    } else {
                        snprintf(fieldName, TSDB_COL_NAME_LEN, "c%d", fieldSize);
                    }
                }
                setField(field, type, taos_convert_type_to_length(type), fieldName,
                         get_min_from_data_type(type), get_max_from_data_type(type));

                cJSON* dataLen = cJSON_GetObjectItem(src, "len");
                if (cJSON_IsNumber(dataLen) &&
                    (type == TSDB_DATA_TYPE_BINARY ||
                     type == TSDB_DATA_TYPE_NCHAR ||
                     type == TSDB_DATA_TYPE_JSON)) {
                    field->length = (uint32_t)dataLen->valueint;
                }
                cJSON* dataValues = cJSON_GetObjectItem(src, "values");
                if (cJSON_IsArray(dataValues)) {
                    if (type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BINARY) {
                        field->values = dataValues;
                    } else {
                        errorPrint(stderr, "Invalid values value for %s field in json\n", taos_convert_datatype_to_string(type));
                        goto PARSE_OVER;
                    }
                }

                cJSON* dataMin = cJSON_GetObjectItem(src, "max");
                if (cJSON_IsNumber(dataMin)) {
                    field->min = dataMin->valueint;
                }
                cJSON* dataMax = cJSON_GetObjectItem(src, "min");
                if (cJSON_IsNumber(dataMax)) {
                    field->max = dataMax->valueint;
                }
                benchArrayPush(fields, field);
                fieldSize++;
            }
        } else {
            errorPrint(stderr, "%s", "Invalid format in json\n");
            goto PARSE_OVER;
        }
    }
    code = 0;
PARSE_OVER:
    return code;
}

static int getStringValueFromJson(cJSON* source, char* target, int length, char* key, bool required) {
    cJSON* src = cJSON_GetObjectItem(source, key);
    if (cJSON_IsString(src)) {
        tstrncpy(target, src->valuestring, length);
    } else if (required) {
        errorPrint(stderr, "Miss or invalid %s key value in json\n", key);
        return 1;
    }
    return 0;
}

static int getBoolValueFromJson(cJSON* source, bool* target, char* key, bool required) {
    cJSON* src = cJSON_GetObjectItem(source, key);
    if (cJSON_IsString(src)) {
        if (0 == strcasecmp(src->valuestring, "yes")) {
            *target = true;
        } else if (0 == strcasecmp(src->valuestring, "no")) {
            *target = false;
        } else {
            errorPrint(stderr, "Invalid %s's value: %s, should be \"yes\" or \"no\"\n", key, src->valuestring);
            return 1;
        }
    } else if (required) {
        errorPrint(stderr, "Miss or Invalid %s key value in json\n", key);
        return 1;
    }
    return 0;
}

static int getNumberValueFromJson(cJSON* source, int64_t* target, char* key, bool required) {
    cJSON* src = cJSON_GetObjectItem(source, key);
    if (cJSON_IsNumber(src)) {
        *target = src->valueint;
    } else if (required) {
        errorPrint(stderr, "Miss or invalid %s key value in json\n", key);
        return 1;
    }
    return 0;
}

static int getDatabaseInfo(cJSON *dbinfos, int index) {
    int code = 1;
    SDataBase *database = benchArrayGet(g_arguments->db, index);
    database->drop = true;
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
    database->dbCfg.strict = -1;
    database->dbCfg.page_size = -1;
    database->dbCfg.pages = -1;
    database->dbCfg.vgroups = -1;
    database->dbCfg.single_stable = -1;
    database->dbCfg.buffer = -1;
    database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
    database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    cJSON *dbinfo = cJSON_GetArrayItem(dbinfos, index);
    cJSON *db = cJSON_GetObjectItem(dbinfo, "dbinfo");
    if (!cJSON_IsObject(db)) {
        errorPrint(stderr, "%s", "Invalid dbinfo format in json\n");
        goto PARSE_OVER;
    }

    if (getStringValueFromJson(db, database->dbName, TSDB_DB_NAME_LEN, "name", true) ||
            getBoolValueFromJson(db, &(database->drop), "no", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.keep), "keep", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.days), "days", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.maxRows), "maxRows", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.minRows), "minRows", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.walLevel), "walLevel", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.fsync), "fsync", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.cacheLast), "cachelast", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.replica), "replica", false)
        ) {
        goto PARSE_OVER;
    }

    cJSON *precision = cJSON_GetObjectItem(db, "precision");
    if (cJSON_IsString(precision)) {
        if (0 == strcasecmp(precision->valuestring, "us")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_MICRO;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
        } else if (0 == strcasecmp(precision->valuestring, "ns")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_NANO;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
        } else if (0 == strcasecmp(precision->valuestring, "ms")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        } else {
            errorPrint(stderr, "Invalid precision value: %s\n", precision->valuestring);
            goto PARSE_OVER;
        }
    }

    if (g_arguments->taosc_version == 2) {
        if (getNumberValueFromJson(db, &(database->dbCfg.update), "update", false) ||
                getNumberValueFromJson(db, &(database->dbCfg.cache), "cache", false) ||
                getNumberValueFromJson(db, &(database->dbCfg.blocks), "blocks", false) ||
                getNumberValueFromJson(db, &(database->dbCfg.quorum), "quorum", false)
                ) {
            goto PARSE_OVER;
        }
    } else if (g_arguments->taosc_version == 3) {
        if (getNumberValueFromJson(db, &(database->dbCfg.buffer), "buffer", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.strict), "strict", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.page_size), "page_size", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.pages), "pages", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.vgroups), "vgroups", false) ||
            getNumberValueFromJson(db, &(database->dbCfg.single_stable), "single_stable", false) ||
            getStringValueFromJson(db, database->dbCfg.retentions, 128, "retentions", false)
                ) {
            goto PARSE_OVER;
        }
    }
    code = 0;
    PARSE_OVER:
    return code;
}

static int getStableInfo(cJSON *dbinfos, int index) {
    int code = 1;
    SDataBase *database = benchArrayGet(g_arguments->db, index);
    cJSON *    dbinfo = cJSON_GetArrayItem(dbinfos, index);
    cJSON *    stables = cJSON_GetObjectItem(dbinfo, "super_tables");
    if (!cJSON_IsArray(stables)) {
        errorPrint(stderr, "%s", "invalid super_tables format in json\n");
        return -1;
    }
    int stbSize = cJSON_GetArraySize(stables);
    database->superTbls = benchArrayInit(stbSize, sizeof(SSuperTable));
    for (int i = 0; i < stbSize; ++i) {
        SSuperTable *superTable = benchArrayGet(database->superTbls, i);
        superTable->escape_character = false;
        superTable->autoCreateTable = false;
        superTable->batchCreateTableNum = DEFAULT_CREATE_BATCH;
        superTable->childTblExists = false;
        superTable->random_data_source = true;
        superTable->iface = TAOSC_IFACE;
        superTable->lineProtocol = TSDB_SML_LINE_PROTOCOL;
        superTable->tcpTransfer = false;
        superTable->childTblOffset = 0;
        superTable->timestamp_step = 1;
        superTable->useSampleTs = false;
        superTable->non_stop = false;
        superTable->insertRows = 10;
        superTable->interlaceRows = 0;
        superTable->disorderRatio = 0;
        superTable->disorderRange = DEFAULT_DISORDER_RANGE;
        superTable->insert_interval = g_arguments->insert_interval;
        superTable->partialColumnNum = 0;
        superTable->delay = -1;
        superTable->file_factor = -1;
        cJSON *stbInfo = cJSON_GetArrayItem(stables, i);
        if (getStringValueFromJson(stbInfo, superTable->stbName, TSDB_TABLE_NAME_LEN, "name", true) ||
                getStringValueFromJson(stbInfo, superTable->childTblPrefix, TSDB_TABLE_NAME_LEN, "childtable_prefix", false) ||
                getBoolValueFromJson(stbInfo, &(superTable->escape_character), "escape_character", false) ||
                getBoolValueFromJson(stbInfo, &(superTable->autoCreateTable), "auto_create_table", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->batchCreateTableNum), "batch_create_tbl_num", false) ||
                getBoolValueFromJson(stbInfo, &(superTable->childTblExists), "child_table_exists", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->childTblCount), "childtable_count", false) ||
                getBoolValueFromJson(stbInfo, &(superTable->tcpTransfer), "tcp_transfer", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->childTblLimit), "childtable_limit", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->childTblOffset), "childtable_offset", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->timestamp_step), "timestamp_step", false) ||
                getStringValueFromJson(stbInfo, superTable->sampleFile, MAX_FILE_NAME_LEN, "sample_file", false) ||
                getBoolValueFromJson(stbInfo, &(superTable->useSampleTs), "use_sample_ts", false) ||
                getBoolValueFromJson(stbInfo, &(superTable->non_stop), "non_stop_mode", false) ||
                getStringValueFromJson(stbInfo, superTable->tagsFile, MAX_FILE_NAME_LEN, "tags_file", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->insertRows), "insert_rows", true) ||
                getNumberValueFromJson(stbInfo, &(superTable->interlaceRows), "interlace_rows", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->disorderRatio), "disorder_ratio", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->disorderRange), "disorder_range", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->insert_interval), "insert_interval", false) ||
                getNumberValueFromJson(stbInfo, &(superTable->partialColumnNum), "partial_col_num", false)
            ) {
            goto PARSE_OVER;
        }

        cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
        if (cJSON_IsString(dataSource)) {
            if (0 == strcasecmp(dataSource->valuestring, "sample")) {
                superTable->random_data_source = false;
            } else if (0 == strcasecmp(dataSource->valuestring, "rand")) {
                superTable->random_data_source = true;
            } else {
                errorPrint(stderr, "Invalid data_source value:%s\n", dataSource->valuestring);
                goto PARSE_OVER;
            }
        }
        cJSON *stbIface = cJSON_GetObjectItem(stbInfo, "insert_mode");
        if (cJSON_IsString(stbIface)) {
            if (0 == strcasecmp(stbIface->valuestring, "rest")) {
                superTable->iface = REST_IFACE;
            } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
                superTable->iface = STMT_IFACE;
                if (g_arguments->reqPerReq > INT16_MAX) {
                    g_arguments->reqPerReq = INT16_MAX;
                }
                if (g_arguments->reqPerReq > g_arguments->prepared_rand) {
                    g_arguments->prepared_rand = g_arguments->reqPerReq;
                }
            } else if (0 == strcasecmp(stbIface->valuestring, "sml")) {
                superTable->iface = SML_IFACE;
            } else if (0 == strcasecmp(stbIface->valuestring, "sml-rest")) {
                superTable->iface = SML_REST_IFACE;
            } else if (0 == strcasecmp(stbIface->valuestring, "taosc")) {
                superTable->iface = TAOSC_IFACE;
            } else {
                errorPrint(stderr, "Invalid insert_mode value: %s\n", stbIface->valuestring);
                goto PARSE_OVER;
            }
        }
        cJSON *stbLineProtocol = cJSON_GetObjectItem(stbInfo, "line_protocol");
        if (cJSON_IsString(stbLineProtocol)) {
            if (0 == strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                superTable->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
            } else if (0 == strcasecmp(stbLineProtocol->valuestring, "json")) {
                superTable->lineProtocol = TSDB_SML_JSON_PROTOCOL;
            } else if (0 == strcasecmp(stbLineProtocol->valuestring, "line")) {
                superTable->lineProtocol = TSDB_SML_LINE_PROTOCOL;
            } else {
                errorPrint(stderr, "Invalid line_protocl value: %s\n", stbLineProtocol->valuestring);
                goto PARSE_OVER;
            }
        }

        cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
        if (cJSON_IsString(ts)) {
            if (0 == strcasecmp(ts->valuestring, "now")) {
                superTable->startTimestamp =
                        toolsGetTimestamp(database->dbCfg.precision);
            } else {
                if (toolsParseTime(ts->valuestring,
                                   &(superTable->startTimestamp),
                                   (int32_t)strlen(ts->valuestring),
                                   database->dbCfg.precision, 0)) {
                    errorPrint(stderr, "failed to parse time %s\n",
                               ts->valuestring);
                    goto PARSE_OVER;
                }
            }
        } else {
            superTable->startTimestamp =
                    toolsGetTimestamp(database->dbCfg.precision);
        }

        if (g_arguments->taosc_version == 3) {
            if (getNumberValueFromJson(stbInfo, &(superTable->delay), "delay", false) ||
                    getNumberValueFromJson(stbInfo, &(superTable->file_factor), "file_factor", false) ||
                    getStringValueFromJson(stbInfo, superTable->rollup, 128, "rollup", false)
            ) {
              goto PARSE_OVER;
            }
        }
        if (superTable->childTblExists) {
            code = 0;
            goto PARSE_OVER;
        }
        cJSON* columns = cJSON_GetObjectItem(stbInfo, "columns");
        if (cJSON_IsArray(columns)) {
            superTable->columns =  benchArrayInit(1, sizeof(Field));
            if (getFieldFromInsertJsonFile(columns, superTable->columns, false)) {
                goto PARSE_OVER;
            }
        } else {
            errorPrint(stderr, "%s", "Invalid or miss columns key value in json\n");
            goto PARSE_OVER;
        }
        cJSON* tags = cJSON_GetObjectItem(stbInfo, "tags");
        if (cJSON_IsArray(tags)) {
            superTable->tags = benchArrayInit(1, sizeof(Field));
            if (getFieldFromInsertJsonFile(tags, superTable->tags, true)) {
                goto PARSE_OVER;
            }
        }
    }
    code = 0;
PARSE_OVER:
    return code;
}


static int getMetaFromInsertJsonFile(cJSON *json) {
    int32_t code = 1;

    if (getStringValueFromJson(json, configDir, MAX_FILE_NAME_LEN, "cfgdir", false) ||
            getStringValueFromJson(json, g_arguments->host, HOST_NAME_MAX, "host", false) ||
            getNumberValueFromJson(json, &(g_arguments->port), "port", false) ||
            getNumberValueFromJson(json, &(g_arguments->telnet_tcp_port), "telnet_tcp_port", false) ||
            getStringValueFromJson(json, g_arguments->user, NAME_MAX, "user",  false) ||
            getStringValueFromJson(json, g_arguments->password, TSDB_PASS_LEN, "password", false) ||
            getStringValueFromJson(json, g_arguments->result_file, MAX_FILE_NAME_LEN, "result_file", false) ||
            getNumberValueFromJson(json, &(g_arguments->nthreads), "thread_count", false) ||
            getNumberValueFromJson(json, &(g_arguments->table_threads), "create_table_thread_count", false) ||
            getNumberValueFromJson(json, &(g_arguments->connection_pool), "connection_pool_size", false) ||
            getNumberValueFromJson(json, &(g_arguments->reqPerReq), "num_of_records_per_req", false) ||
            getNumberValueFromJson(json, &(g_arguments->prepared_rand), "prepared_rand", false) ||
            getBoolValueFromJson(json, &(g_arguments->chinese), "chinese", false) ||
            getNumberValueFromJson(json, &(g_arguments->insert_interval), "insert_interval", false) ||
            getBoolValueFromJson(json, &(g_arguments->answer_yes), "confirm_parameter_prompt", false)) {
        goto PARSE_OVER;
    }

    if (init_taos_list()) goto PARSE_OVER;

    cJSON *dbinfos = cJSON_GetObjectItem(json, "databases");
    if (!cJSON_IsArray(dbinfos)) {
        errorPrint(stderr, "%s", "Invalid databases format in json\n");
        goto PARSE_OVER;
    }
    int dbSize = cJSON_GetArraySize(dbinfos);
    benchArrayClear(g_arguments->db);

    for (int i = 0; i < dbSize; ++i) {
        if (getDatabaseInfo(dbinfos, i)) {
            goto PARSE_OVER;
        }
        if (getStableInfo(dbinfos, i)) {
            goto PARSE_OVER;
        }
    }
    code = 0;
PARSE_OVER:
    return code;
}

static int getMetaFromQueryJsonFile(cJSON *json) {
    int32_t code = 1;
    g_queryInfo.response_buffer = RESP_BUF_LEN;
    if (getStringValueFromJson(json, configDir, MAX_FILE_NAME_LEN, "cfgdir", false) ||
            getStringValueFromJson(json, g_arguments->host, HOST_NAME_MAX, "host", false) ||
            getNumberValueFromJson(json, &(g_arguments->port), "port", false) ||
            getNumberValueFromJson(json, &(g_arguments->telnet_tcp_port), "telnet_tcp_port", false) ||
            getStringValueFromJson(json, g_arguments->user, NAME_MAX, "user", false) ||
            getStringValueFromJson(json, g_arguments->password, TSDB_PASS_LEN, "password", false) ||
            getBoolValueFromJson(json, &(g_arguments->answer_yes), "confirm_parameter_prompt", false) ||
            getNumberValueFromJson(json, &(g_queryInfo.query_times), "query_times", true) ||
            getBoolValueFromJson(json, &(g_queryInfo.reset_query_cache), "reset_query_cache", false) ||
            getNumberValueFromJson(json, &(g_arguments->connection_pool), "connection_pool_size", false) ||
            getNumberValueFromJson(json, &(g_queryInfo.response_buffer), "response_buffer", false) ||
            getStringValueFromJson(json, g_queryInfo.database, TSDB_DB_NAME_LEN, "databases", true)
        ) {
        goto PARSE_OVER;
    }

    cJSON *queryMode = cJSON_GetObjectItem(json, "query_mode");
    if (cJSON_IsString(queryMode)) {
        if (0 == strcasecmp(queryMode->valuestring, "rest")) {
            g_queryInfo.iface = REST_IFACE;
        } else if (0 == strcasecmp(queryMode->valuestring, "taosc")) {
            g_queryInfo.iface = TAOSC_IFACE;
        } else {
            errorPrint(stderr, "Invalid query_mode value: %s\n", queryMode->valuestring);
            goto PARSE_OVER;
        }
    }

    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(json, "specified_table_query");
    if (cJSON_IsObject(specifiedQuery)) {
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        g_queryInfo.specifiedQueryInfo.queryTimes = g_queryInfo.query_times;
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        g_queryInfo.specifiedQueryInfo.subscribeInterval = DEFAULT_SUB_INTERVAL;
        g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
        g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        g_queryInfo.specifiedQueryInfo.sqlCount = 0;
        if (getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.queryInterval), "query_interval", false) ||
                getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.queryTimes), "query_times", false) ||
                getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.concurrent), "concurrent", false) ||
                getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.concurrent), "threads", false) ||
                getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.subscribeInterval), "interval", false) ||
                getBoolValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.subscribeRestart), "restart", false) ||
                getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.subscribeKeepProgress), "keepProgress", false)
                ) {
            goto PARSE_OVER;
        }

        cJSON *specifiedAsyncMode = cJSON_GetObjectItem(specifiedQuery, "mode");
        if (cJSON_IsString(specifiedAsyncMode)) {
            if (0 == strcasecmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else if (0 == strcasecmp("sync", specifiedAsyncMode->valuestring)){
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            } else {
                errorPrint(stderr, "Invalid specified query mode value: %s\n", specifiedAsyncMode->valuestring);
                goto PARSE_OVER;
            }
        }


        // sqls
        cJSON *specifiedSqls = cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (cJSON_IsArray(specifiedSqls)) {
            int specifiedSize = cJSON_GetArraySize(specifiedSqls);
            if (specifiedSize * g_queryInfo.specifiedQueryInfo.concurrent >
                MAX_QUERY_SQL_COUNT) {
                errorPrint(
                        stderr,
                        "failed to read json, query sql(%d) * concurrent(%"PRId64") "
                    "overflow, max is %d\n",
                        specifiedSize, g_queryInfo.specifiedQueryInfo.concurrent,
                        MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.specifiedQueryInfo.sqlCount = specifiedSize;
            for (int j = 0; j < specifiedSize; ++j) {
                cJSON *sql = cJSON_GetArrayItem(specifiedSqls, j);
                if (cJSON_IsObject(sql)) {
                    g_queryInfo.specifiedQueryInfo.sql[j].delay_list = 
                        benchCalloc(g_queryInfo.specifiedQueryInfo.queryTimes *
                        g_queryInfo.specifiedQueryInfo.concurrent, sizeof(int64_t), true);
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                    if (getStringValueFromJson(sql, g_queryInfo.specifiedQueryInfo.sql[j].command, BUFFER_SIZE, "sql", true) ||
                            getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.endAfterConsume[j]), "endAfterConsume", false) ||
                            getNumberValueFromJson(specifiedQuery, &(g_queryInfo.specifiedQueryInfo.resubAfterConsume[j]), "resubAfterConsume", false) ||
                            getStringValueFromJson(sql, g_queryInfo.specifiedQueryInfo.sql[j].result, MAX_FILE_NAME_LEN, "result", false)
                            ) {
                        goto PARSE_OVER;
                    }
                }
            }
        }
    }

    // super_table_query
    g_queryInfo.superQueryInfo.sqlCount = 0;
    cJSON *superQuery = cJSON_GetObjectItem(json, "super_table_query");
    
    if (cJSON_IsObject(superQuery)) {
        g_queryInfo.superQueryInfo.threadCnt = 1;
        g_queryInfo.superQueryInfo.queryInterval = 0;
        g_queryInfo.superQueryInfo.queryTimes = g_queryInfo.query_times;
        g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        g_queryInfo.superQueryInfo.subscribeInterval = DEFAULT_QUERY_INTERVAL;
        g_queryInfo.superQueryInfo.subscribeRestart = true;
        g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
        g_queryInfo.superQueryInfo.endAfterConsume = -1;
        g_queryInfo.superQueryInfo.resubAfterConsume = -1;
        g_queryInfo.superQueryInfo.sqlCount = 0;
        if (getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.queryInterval), "query_interval", false) ||
                getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.queryTimes), "query_times", false) ||
                getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.threadCnt), "concurrent", false) ||
                getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.threadCnt), "threads", false) ||
                getStringValueFromJson(superQuery, g_queryInfo.superQueryInfo.stbName, TSDB_TABLE_NAME_LEN, "stblname", true) ||
                getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.subscribeInterval), "interval", false) ||
                getBoolValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.subscribeRestart), "restart", false) ||
                getBoolValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.subscribeKeepProgress), "keepProgress", false) ||
                getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.endAfterConsume), "endAfterConsume", false) ||
                getNumberValueFromJson(superQuery, &(g_queryInfo.superQueryInfo.resubAfterConsume), "resubAfterConsume", false)
                ) {
            goto PARSE_OVER;
        }
        cJSON *superAsyncMode = cJSON_GetObjectItem(superQuery, "mode");
        if (cJSON_IsString(superAsyncMode)) {
            if (0 == strcasecmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else if (0 == strcasecmp("sync", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            } else {
                errorPrint(stderr, "Invalid super query mode value: %s\n", superAsyncMode->valuestring);
                goto PARSE_OVER;
            }
        }
    }

    // supert table sqls
    cJSON *superSqls = cJSON_GetObjectItem(superQuery, "sqls");
    if (cJSON_IsArray(superSqls)) {
        int superSqlSize = cJSON_GetArraySize(superSqls);
        if (superSqlSize > MAX_QUERY_SQL_COUNT) {
            errorPrint(
                    stderr,
                    "failed to read json, query sql size overflow, max is %d\n",
                    MAX_QUERY_SQL_COUNT);
            goto PARSE_OVER;
        }
        g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
        for (int j = 0; j < superSqlSize; ++j) {
            cJSON *sql = cJSON_GetArrayItem(superSqls, j);
            if (getStringValueFromJson(sql, g_queryInfo.superQueryInfo.sql[j], BUFFER_SIZE, "sql", true) ||
                    getStringValueFromJson(sql, g_queryInfo.superQueryInfo.result[j], MAX_FILE_NAME_LEN, "result", false)
                ) {
                goto PARSE_OVER;
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
        errorPrint(stderr, "failed to read %s, reason:%s\n", file,
                   strerror(errno));
        return code;
    }

    int   maxLen = MAX_JSON_BUFF;
    char *content = benchCalloc(1, maxLen + 1, false);
    int   len = (int)fread(content, 1, maxLen, fp);
    if (len <= 0) {
        errorPrint(stderr, "failed to read %s, content is null", file);
        goto PARSE_OVER;
    }

    content[len] = 0;
    root = cJSON_Parse(content);
    if (root == NULL) {
        errorPrint(stderr, "Failed to parse %s, invalid json format\n",
                   file);
        goto PARSE_OVER;
    }

    char *pstr = cJSON_Print(root);
    infoPrint(stdout, "%s\n%s\n", file, pstr);
    tmfree(pstr);

    cJSON *filetype = cJSON_GetObjectItem(root, "filetype");
    if (cJSON_IsString(filetype)) {
        if (0 == strcasecmp("insert", filetype->valuestring)) {
            g_arguments->test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            g_arguments->test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            g_arguments->test_mode = SUBSCRIBE_TEST;
        } else {
            errorPrint(stderr, "%s",
                       "failed to read json, filetype not support\n");
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
