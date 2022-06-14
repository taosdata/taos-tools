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
    superTbls->columns = benchCalloc(col_count, sizeof(Column), true);
    for (int k = 0; k < columnSize; ++k) {
        bool   customName = false;
        bool   customMax = false;
        bool   customMin = false;
        cJSON *column = cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;

        count = 1;
        cJSON *countObj = cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        cJSON *dataName = cJSON_GetObjectItem(column, "name");
        if (cJSON_IsString(dataName)) {
            customName = true;
        }

        // column info
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!cJSON_IsString(dataType)) goto PARSE_OVER;

        cJSON *dataMax = cJSON_GetObjectItem(column, "max");
        if (dataMax && cJSON_IsNumber(dataMax) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMax = true;
        }

        cJSON *dataMin = cJSON_GetObjectItem(column, "min");
        if (dataMin && cJSON_IsNumber(dataMin) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMin = true;
        }

        cJSON *dataValues = cJSON_GetObjectItem(column, "values");
        bool   sma = false;
        if (g_arguments->taosc_version == 3) {
            cJSON *sma_value = cJSON_GetObjectItem(column, "sma");
            if (cJSON_IsString(sma_value) &&
                (0 == strcasecmp(sma_value->valuestring, "yes"))) {
                sma = true;
            }
        }

        cJSON * dataLen = cJSON_GetObjectItem(column, "len");
        int32_t length;
        if (dataLen && dataLen->type == cJSON_Number) {
            length = (int32_t)dataLen->valueint;
        } else {
            switch (taos_convert_string_to_datatype(dataType->valuestring, 0)) {
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
            superTbls->columns[index].type =
                taos_convert_string_to_datatype(dataType->valuestring, 0);
            superTbls->columns[index].length = length;
            superTbls->columns[index].sma = sma;
            if (customMax) {
                superTbls->columns[index].max = dataMax->valueint;
            } else {
                superTbls->columns[index].max = RAND_MAX >> 1;
            }
            if (customMin) {
                superTbls->columns[index].min = dataMin->valueint;
            } else {
                superTbls->columns[index].min = (RAND_MAX >> 1) * -1;
            }
            if (customMin && customMax && dataMin->valueint > dataMax->valueint) {
                errorPrint(stderr, "%s", "Invalid max and min column value in json\n");
                goto PARSE_OVER;
            }
            
            superTbls->columns[index].values = dataValues;

            if (customName) {
                if (n >= 1) {
                    sprintf(superTbls->columns[index].name, "%s_%d",
                            dataName->valuestring, n);
                } else {
                    sprintf(superTbls->columns[index].name, "%s",
                            dataName->valuestring);
                }
            } else {
                sprintf(superTbls->columns[index].name, "c%d", index);
            }
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
    superTbls->tags = benchCalloc(tag_count, sizeof(Column), true);
    superTbls->tagCount = tag_count;
    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;
        bool   customMax = false;
        bool   customMin = false;
        bool   customName = false;
        cJSON *dataType = cJSON_GetObjectItem(tag, "type");
        if (!cJSON_IsString(dataType)) goto PARSE_OVER;
        int    data_length = 0;
        cJSON *dataLen = cJSON_GetObjectItem(tag, "len");
        if (cJSON_IsNumber(dataLen)) {
            data_length = (int32_t)dataLen->valueint;
        } else {
            switch (taos_convert_string_to_datatype(dataType->valuestring, 0)) {
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

        cJSON *dataMax = cJSON_GetObjectItem(tag, "max");
        if (cJSON_IsNumber(dataMax) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMax = true;
        }

        cJSON *dataMin = cJSON_GetObjectItem(tag, "min");
        if (cJSON_IsNumber(dataMin) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMin = true;
        }

        cJSON *dataValues = cJSON_GetObjectItem(tag, "values");

        cJSON *dataName = cJSON_GetObjectItem(tag, "name");
        if (dataName && dataName->type == cJSON_String &&
            dataName->valuestring != NULL) {
            customName = true;
        }
        cJSON *countObj = cJSON_GetObjectItem(tag, "count");
        if (cJSON_IsNumber(countObj)) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        if ((tagSize == 1) &&
            (0 == strcasecmp(dataType->valuestring, "JSON"))) {
            if (customName) {
                sprintf(superTbls->tags[0].name, "%s", dataName->valuestring);
            } else {
                sprintf(superTbls->tags[0].name, "jtag");
            }
            superTbls->tags[0].type = TSDB_DATA_TYPE_JSON;
            superTbls->tags[0].length = data_length;
            superTbls->tagCount = count;
            code = 0;
            goto PARSE_OVER;
        }

        for (int n = 0; n < count; ++n) {
            if (customMax) {
                superTbls->tags[index].max = dataMax->valueint;
            } else {
                superTbls->tags[index].max = RAND_MAX >> 1;
            }
            if (customMin) {
                superTbls->tags[index].min = dataMin->valueint;
            } else {
                superTbls->tags[index].min = (RAND_MAX >> 1) * -1;
            }
            if (customMax && customMin && dataMin->valueint > dataMax->valueint) {
                errorPrint(stderr, "%s","Invalid max and min tag value in json\n");
                goto PARSE_OVER;
            }
            
            superTbls->tags[index].values = dataValues;
            if (customName) {
                if (n >= 1) {
                    sprintf(superTbls->tags[index].name, "%s_%d",
                            dataName->valuestring, n);
                } else {
                    sprintf(superTbls->tags[index].name, "%s",
                            dataName->valuestring);
                }
            } else {
                sprintf(superTbls->tags[index].name, "t%d", index);
            }
            superTbls->tags[index].type =
                taos_convert_string_to_datatype(dataType->valuestring, 0);
            superTbls->tags[index].length = data_length;
            index++;
        }
    }

    superTbls->tagCount = index;
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
    SDataBase *database = &(g_arguments->db[index]);
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
    SDataBase *database = &(g_arguments->db[index]);
    cJSON *    dbinfo = cJSON_GetArrayItem(dbinfos, index);
    cJSON *    stables = cJSON_GetObjectItem(dbinfo, "super_tables");
    if (!cJSON_IsArray(stables)) {
        errorPrint(stderr, "%s", "invalid super_tables format in json\n");
        return -1;
    }
    int stbSize = cJSON_GetArraySize(stables);
    database->superTbls = benchCalloc(stbSize, sizeof(SSuperTable), true);
    database->superTblCount = stbSize;
    for (int i = 0; i < database->superTblCount; ++i) {
        SSuperTable *superTable = &(database->superTbls[i]);
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
        if (getColumnAndTagTypeFromInsertJsonFile(stbInfo,
                                                  &database->superTbls[i])) {
            goto PARSE_OVER;
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
    tmfree(g_arguments->db->superTbls->columns);
    tmfree(g_arguments->db->superTbls->tags);
    tmfree(g_arguments->db->superTbls);
    tmfree(g_arguments->db);
    g_arguments->db = benchCalloc(dbSize, sizeof(SDataBase), true);
    g_arguments->dbCount = dbSize;

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
            getStringValueFromJson(json, g_arguments->db->dbName, TSDB_DB_NAME_LEN, "databases", true)
        ) {
        goto PARSE_OVER;
    }

    cJSON *queryMode = cJSON_GetObjectItem(json, "query_mode");
    if (cJSON_IsString(queryMode)) {
        if (0 == strcasecmp(queryMode->valuestring, "rest")) {
            g_arguments->db->superTbls->iface = REST_IFACE;
        } else if (0 == strcasecmp(queryMode->valuestring, "taosc")) {
            g_arguments->db->superTbls->iface = TAOSC_IFACE;
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
