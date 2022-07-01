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

static int getDatabaseInfo(cJSON *dbinfos, int index) {
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
    database->dbCfg.retentions = NULL;
    database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
    database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    cJSON *dbinfo = cJSON_GetArrayItem(dbinfos, index);
    cJSON *db = cJSON_GetObjectItem(dbinfo, "dbinfo");
    if (!cJSON_IsObject(db)) {
        errorPrint(stderr, "%s", "Invalid dbinfo format in json\n");
        return -1;
    }
    cJSON *dbName = cJSON_GetObjectItem(db, "name");
    if (cJSON_IsString(dbName)) {
        database->dbName = dbName->valuestring;
    } else {
        errorPrint(stderr, "%s", "miss name in dbinfo\n");
        return -1;
    }
    cJSON *drop = cJSON_GetObjectItem(db, "drop");
    if (cJSON_IsString(drop) && (0 == strcasecmp(drop->valuestring, "no"))) {
        database->drop = false;
    }
    cJSON *keep = cJSON_GetObjectItem(db, "keep");
    if (cJSON_IsNumber(keep)) {
        database->dbCfg.keep = (int)keep->valueint;
    }
    cJSON *days = cJSON_GetObjectItem(db, "days");
    if (cJSON_IsNumber(days)) {
        database->dbCfg.days = (int)days->valueint;
    }

    cJSON *maxRows = cJSON_GetObjectItem(db, "maxRows");
    if (cJSON_IsNumber(maxRows)) {
        database->dbCfg.maxRows = (int)maxRows->valueint;
    }

    cJSON *minRows = cJSON_GetObjectItem(db, "minRows");
    if (cJSON_IsNumber(minRows)) {
        database->dbCfg.minRows = (int)minRows->valueint;
    }
    cJSON *walLevel = cJSON_GetObjectItem(db, "walLevel");
    if (cJSON_IsNumber(walLevel)) {
        database->dbCfg.walLevel = (int)walLevel->valueint;
    }
    cJSON *fsync = cJSON_GetObjectItem(db, "fsync");
    if (cJSON_IsNumber(fsync)) {
        database->dbCfg.fsync = (int)fsync->valueint;
    }
    cJSON *cacheLast = cJSON_GetObjectItem(db, "cachelast");
    if (cJSON_IsNumber(cacheLast)) {
        database->dbCfg.cacheLast = (int)cacheLast->valueint;
    }
    cJSON *replica = cJSON_GetObjectItem(db, "replica");
    if (cJSON_IsNumber(replica)) {
        database->dbCfg.replica = (int)replica->valueint;
    }
    cJSON *precision = cJSON_GetObjectItem(db, "precision");
    if (cJSON_IsString(precision)) {
        if (0 == strcasecmp(precision->valuestring, "us")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_MICRO;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
        } else if (0 == strcasecmp(precision->valuestring, "ns")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_NANO;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
        }
    }

    if (g_arguments->taosc_version == 2) {
        cJSON *update = cJSON_GetObjectItem(db, "update");
        if (cJSON_IsNumber(update)) {
            database->dbCfg.update = (int)update->valueint;
        }
        cJSON *cache = cJSON_GetObjectItem(db, "cache");
        if (cJSON_IsNumber(cache)) {
            database->dbCfg.cache = (int)cache->valueint;
        }
        cJSON *blocks = cJSON_GetObjectItem(db, "blocks");
        if (cJSON_IsNumber(blocks)) {
            database->dbCfg.blocks = (int)blocks->valueint;
        }
        cJSON *quorum = cJSON_GetObjectItem(db, "quorum");
        if (cJSON_IsNumber(quorum)) {
            database->dbCfg.quorum = (int)quorum->valueint;
        }
    } else if (g_arguments->taosc_version == 3) {
        cJSON *buffer = cJSON_GetObjectItem(db, "buffer");
        if (cJSON_IsNumber(buffer)) {
            database->dbCfg.buffer = (int)buffer->valueint;
        }
        cJSON *strict = cJSON_GetObjectItem(db, "strict");
        if (cJSON_IsNumber(strict)) {
            database->dbCfg.strict = (int)strict->valueint;
        }
        cJSON *page_size = cJSON_GetObjectItem(db, "page_size");
        if (cJSON_IsNumber(page_size)) {
            database->dbCfg.page_size = (int)page_size->valueint;
        }
        cJSON *pages = cJSON_GetObjectItem(db, "pages");
        if (cJSON_IsNumber(pages)) {
            database->dbCfg.pages = (int)pages->valueint;
        }
        cJSON *vgroups = cJSON_GetObjectItem(db, "vgroups");
        if (cJSON_IsNumber(vgroups)) {
            database->dbCfg.vgroups = (int)vgroups->valueint;
        }
        cJSON *single_stable = cJSON_GetObjectItem(db, "single_stable");
        if (cJSON_IsNumber(single_stable)) {
            database->dbCfg.single_stable = (int)single_stable->valueint;
        }
        cJSON *retentions = cJSON_GetObjectItem(db, "retentions");
        if (cJSON_IsString(retentions)) {
            database->dbCfg.retentions = retentions->valuestring;
        }
    }
    return 0;
}

static int getStableInfo(cJSON *dbinfos, int index) {
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
        superTable->comment = NULL;
        superTable->delay = -1;
        superTable->file_factor = -1;
        superTable->rollup = NULL;
        cJSON *stbInfo = cJSON_GetArrayItem(stables, i);
        cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
        if (cJSON_IsString(stbName)) {
            superTable->stbName = stbName->valuestring;
        }
        cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
        if (cJSON_IsString(prefix)) {
            superTable->childTblPrefix = prefix->valuestring;
        }
        cJSON *escapeChar = cJSON_GetObjectItem(stbInfo, "escape_character");
        if (cJSON_IsString(escapeChar) &&
            (0 == strcasecmp(escapeChar->valuestring, "yes"))) {
            superTable->escape_character = true;
        }
        cJSON *autoCreateTbl =
            cJSON_GetObjectItem(stbInfo, "auto_create_table");
        if (cJSON_IsString(autoCreateTbl) &&
            (0 == strcasecmp(autoCreateTbl->valuestring, "yes"))) {
            superTable->autoCreateTable = true;
        }
        cJSON *batchCreateTbl =
            cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
        if (cJSON_IsNumber(batchCreateTbl)) {
            superTable->batchCreateTableNum = batchCreateTbl->valueint;
        }
        cJSON *childTblExists =
            cJSON_GetObjectItem(stbInfo, "child_table_exists");
        if (cJSON_IsString(childTblExists) &&
            (0 == strcasecmp(childTblExists->valuestring, "yes")) &&
            !database->drop) {
            superTable->childTblExists = true;
            superTable->autoCreateTable = false;
        }
        cJSON *count = cJSON_GetObjectItem(stbInfo, "childtable_count");
        if (cJSON_IsNumber(count)) {
            superTable->childTblCount = count->valueint;
            g_arguments->g_totalChildTables += superTable->childTblCount;
        } else {
            superTable->childTblCount = 10;
            g_arguments->g_totalChildTables += superTable->childTblCount;
        }
        cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
        if (cJSON_IsString(dataSource) &&
            (0 == strcasecmp(dataSource->valuestring, "sample"))) {
            superTable->random_data_source = false;
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
                if (g_arguments->reqPerReq > SML_MAX_BATCH) {
                    errorPrint(stderr, "reqPerReq (%u) larget than maximum (%d)\n", g_arguments->reqPerReq, SML_MAX_BATCH);
                    return -1;
                }
                superTable->iface = SML_IFACE;
            } else if (0 == strcasecmp(stbIface->valuestring, "sml-rest")) {
                if (g_arguments->reqPerReq > SML_MAX_BATCH) {
                    errorPrint(stderr, "reqPerReq (%u) larget than maximum (%d)\n", g_arguments->reqPerReq, SML_MAX_BATCH);
                    return -1;
                }
                superTable->iface = SML_REST_IFACE;
            }
        }
        cJSON *stbLineProtocol = cJSON_GetObjectItem(stbInfo, "line_protocol");
        if (cJSON_IsString(stbLineProtocol)) {
            if (0 == strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                superTable->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
            } else if (0 == strcasecmp(stbLineProtocol->valuestring, "json")) {
                superTable->lineProtocol = TSDB_SML_JSON_PROTOCOL;
            }
        }
        cJSON *transferProtocol = cJSON_GetObjectItem(stbInfo, "tcp_transfer");
        if (cJSON_IsString(transferProtocol) &&
            (0 == strcasecmp(transferProtocol->valuestring, "yes"))) {
            superTable->tcpTransfer = true;
        }
        cJSON *childTbl_limit =
            cJSON_GetObjectItem(stbInfo, "childtable_limit");
        if (cJSON_IsNumber(childTbl_limit) && (childTbl_limit->valueint >= 0)) {
            superTable->childTblLimit = childTbl_limit->valueint;
        } else {
            superTable->childTblLimit = superTable->childTblCount;
        };
        cJSON *childTbl_offset =
            cJSON_GetObjectItem(stbInfo, "childtable_offset");
        if (cJSON_IsNumber(childTbl_offset)) {
            superTable->childTblOffset = childTbl_offset->valueint;
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
                    return -1;
                }
            }
        } else {
            superTable->startTimestamp =
                toolsGetTimestamp(database->dbCfg.precision);
        }
        cJSON *timestampStep = cJSON_GetObjectItem(stbInfo, "timestamp_step");
        if (cJSON_IsNumber(timestampStep)) {
            superTable->timestamp_step = timestampStep->valueint;
        }
        cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
        if (cJSON_IsString(sampleFile)) {
            tstrncpy(
                superTable->sampleFile, sampleFile->valuestring,
                min(MAX_FILE_NAME_LEN, strlen(sampleFile->valuestring) + 1));
        } else {
            memset(superTable->sampleFile, 0, MAX_FILE_NAME_LEN);
        }
        cJSON *useSampleTs = cJSON_GetObjectItem(stbInfo, "use_sample_ts");
        if (cJSON_IsString(useSampleTs) &&
            (0 == strcasecmp(useSampleTs->valuestring, "yes"))) {
            superTable->useSampleTs = true;
        }
        cJSON *nonStop = cJSON_GetObjectItem(stbInfo, "non_stop_mode");
        if (cJSON_IsString(nonStop) &&
            (0 == strcasecmp(nonStop->valuestring, "yes"))) {
            superTable->non_stop = true;
        }
        cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
        if (cJSON_IsString(tagsFile)) {
            tstrncpy(superTable->tagsFile, tagsFile->valuestring,
                     MAX_FILE_NAME_LEN);
        } else {
            memset(superTable->tagsFile, 0, MAX_FILE_NAME_LEN);
        }
        cJSON *insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
        if (cJSON_IsNumber(insertRows)) {
            superTable->insertRows = insertRows->valueint;
        }
        cJSON *stbInterlaceRows =
            cJSON_GetObjectItem(stbInfo, "interlace_rows");
        if (cJSON_IsNumber(stbInterlaceRows)) {
            superTable->interlaceRows = (uint32_t)stbInterlaceRows->valueint;
        }
        cJSON *disorderRatio = cJSON_GetObjectItem(stbInfo, "disorder_ratio");
        if (cJSON_IsNumber(disorderRatio)) {
            if (disorderRatio->valueint > 50) disorderRatio->valueint = 50;
            if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

            superTable->disorderRatio = (int)disorderRatio->valueint;
        }
        cJSON *disorderRange = cJSON_GetObjectItem(stbInfo, "disorder_range");
        if (cJSON_IsNumber(disorderRange)) {
            superTable->disorderRange = (int)disorderRange->valueint;
        }
        cJSON *insertInterval = cJSON_GetObjectItem(stbInfo, "insert_interval");
        if (cJSON_IsNumber(insertInterval)) {
            superTable->insert_interval = insertInterval->valueint;
        }
        cJSON *pCoumnNum = cJSON_GetObjectItem(stbInfo, "partial_col_num");
        if (cJSON_IsNumber(pCoumnNum)) {
            superTable->partialColumnNum = pCoumnNum->valueint;
        }
        if (g_arguments->taosc_version == 3) {
            cJSON *delay = cJSON_GetObjectItem(stbInfo, "delay");
            if (cJSON_IsNumber(delay)) {
                superTable->delay = (int)delay->valueint;
            }
            cJSON *file_factor = cJSON_GetObjectItem(stbInfo, "file_factor");
            if (cJSON_IsNumber(file_factor)) {
                superTable->file_factor = (int)file_factor->valueint;
            }
            cJSON *rollup = cJSON_GetObjectItem(stbInfo, "rollup");
            if (cJSON_IsString(rollup)) {
                superTable->rollup = rollup->valuestring;
            }
        }
        if (getColumnAndTagTypeFromInsertJsonFile(stbInfo,
                                                  &database->superTbls[i])) {
            return -1;
        }
    }
    return 0;
}

static int getStreamInfo(cJSON* dbinfos, int index) {
    SDataBase *database = &(g_arguments->db[index]);
    database->streams = benchArrayInit(1, sizeof(SSTREAM));
    cJSON* dbinfo = cJSON_GetArrayItem(dbinfos, index);
    cJSON* streamsObj = cJSON_GetObjectItem(dbinfo, "stream");
    if (!cJSON_IsArray(streamsObj)) {
        errorPrint(stderr, "%s", "invalid stream format in json\n");
        return -1;
    }
    int streamCnt = cJSON_GetArraySize(streamsObj);
    for (int i = 0; i < streamCnt; ++i) {
        cJSON* streamObj = cJSON_GetArrayItem(streamsObj, i);
        if (!cJSON_IsObject(streamObj)) {
            errorPrint(stderr, "%s", "invalid stream format in json\n");
            return -1;
        }
        cJSON* stream_name = cJSON_GetObjectItem(streamObj, "stream_name");
        cJSON* stream_stb = cJSON_GetObjectItem(streamObj, "stream_stb");
        cJSON* source_sql = cJSON_GetObjectItem(streamObj, "source_sql");
        if (!cJSON_IsString(stream_name) || !cJSON_IsString(stream_stb) || !cJSON_IsString(source_sql)) {
            errorPrint(stderr, "%s", "Invalid or miss 'stream_name'/'stream_stb'/'source_sql' key in json\n");
            return -1;
        }
        SSTREAM * stream = benchCalloc(1, sizeof(SSTREAM), true);
        tstrncpy(stream->stream_name, stream_name->valuestring, TSDB_TABLE_NAME_LEN);
        tstrncpy(stream->stream_stb, stream_stb->valuestring, TSDB_TABLE_NAME_LEN);
        tstrncpy(stream->source_sql, source_sql->valuestring, TSDB_MAX_SQL_LEN);
        cJSON* trigger_mode = cJSON_GetObjectItem(streamObj, "trigger_mode");
        if (cJSON_IsString(trigger_mode)) {
            tstrncpy(stream->trigger_mode, trigger_mode->valuestring, BIGINT_BUFF_LEN);
        }
        cJSON* watermark = cJSON_GetObjectItem(streamObj, "watermark");
        if (cJSON_IsString(watermark)) {
            tstrncpy(stream->watermark, watermark->valuestring, BIGINT_BUFF_LEN);
        }
        cJSON* drop = cJSON_GetObjectItem(streamObj, "drop");
        if (cJSON_IsString(drop)) {
            if (0 == strcasecmp(drop->valuestring, "yes")) {
                stream->drop = true;
            } else if (0 == strcasecmp(drop->valuestring, "no")) {
                stream->drop = false;
            } else {
                errorPrint(stderr, "invalid value for drop field: %s\n", drop->valuestring);
                return -1;
            }
        }
        benchArrayPush(database->streams, stream);
    }
    return 0;
}

static int getMetaFromInsertJsonFile(cJSON *json) {
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

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        g_arguments->user = user->valuestring;
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        g_arguments->password = password->valuestring;
    }

    cJSON *resultfile = cJSON_GetObjectItem(json, "result_file");
    if (resultfile && resultfile->type == cJSON_String &&
        resultfile->valuestring != NULL) {
        g_arguments->output_file = resultfile->valuestring;
    }

    cJSON *threads = cJSON_GetObjectItem(json, "thread_count");
    if (threads && threads->type == cJSON_Number) {
        g_arguments->nthreads = (uint32_t)threads->valueint;
    }

    cJSON *table_theads =
        cJSON_GetObjectItem(json, "create_table_thread_count");
    if (cJSON_IsNumber(table_theads)) {
        g_arguments->table_threads = (uint32_t)table_theads->valueint;
    }

    cJSON *threadspool = cJSON_GetObjectItem(json, "connection_pool_size");
    if (threadspool && threadspool->type == cJSON_Number) {
        g_arguments->connection_pool = (uint32_t)threadspool->valueint;
    }

    if (init_taos_list()) goto PARSE_OVER;

    cJSON *numRecPerReq = cJSON_GetObjectItem(json, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == cJSON_Number) {
        g_arguments->reqPerReq = (uint32_t)numRecPerReq->valueint;
        if (g_arguments->reqPerReq <= 0) goto PARSE_OVER;
    }

    cJSON *prepareRand = cJSON_GetObjectItem(json, "prepared_rand");
    if (prepareRand && prepareRand->type == cJSON_Number) {
        g_arguments->prepared_rand = prepareRand->valueint;
    }

    cJSON *chineseOpt = cJSON_GetObjectItem(json, "chinese");  // yes, no,
    if (chineseOpt && chineseOpt->type == cJSON_String &&
        chineseOpt->valuestring != NULL) {
        if (0 == strncasecmp(chineseOpt->valuestring, "yes", 3)) {
            g_arguments->chinese = true;
        }
    }

    cJSON *top_insertInterval = cJSON_GetObjectItem(json, "insert_interval");
    if (top_insertInterval && top_insertInterval->type == cJSON_Number) {
        g_arguments->insert_interval = top_insertInterval->valueint;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    cJSON *dbinfos = cJSON_GetObjectItem(json, "databases");
    if (!cJSON_IsArray(dbinfos)) {
        errorPrint(stderr, "%s", "Invalid databases format in json\n");
        return -1;
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
        if (g_arguments->taosc_version == 3) {
            if (getStreamInfo(dbinfos, i)) {
                goto PARSE_OVER;
            }
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
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cJSON_IsString(cfgdir)) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(json, "host");
    if (cJSON_IsString(host)) {
        g_arguments->host = host->valuestring;
    }

    cJSON *port = cJSON_GetObjectItem(json, "port");
    if (cJSON_IsNumber(port)) {
        g_arguments->port = (uint16_t)port->valueint;
    }

    cJSON *telnet_tcp_port = cJSON_GetObjectItem(json, "telnet_tcp_port");
    if (cJSON_IsNumber(telnet_tcp_port)) {
        g_arguments->telnet_tcp_port = (uint16_t)telnet_tcp_port->valueint;
    }

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (cJSON_IsString(user)) {
        g_arguments->user = user->valuestring;
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (cJSON_IsString(password)) {
        g_arguments->password = password->valuestring;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (cJSON_IsString(answerPrompt)) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    cJSON *gQueryTimes = cJSON_GetObjectItem(json, "query_times");
    if (cJSON_IsNumber(gQueryTimes)) {
        g_queryInfo.query_times = gQueryTimes->valueint;
    } else {
        g_queryInfo.query_times = 1;
    }

    cJSON *resetCache = cJSON_GetObjectItem(json, "reset_query_cache");
    if (cJSON_IsString(resetCache)) {
        if (0 == strcasecmp(resetCache->valuestring, "yes")) {
            g_queryInfo.reset_query_cache = true;
        }
    } else {
        g_queryInfo.reset_query_cache = false;
    }

    cJSON *threadspool = cJSON_GetObjectItem(json, "connection_pool_size");
    if (cJSON_IsNumber(threadspool)) {
        g_arguments->connection_pool = (uint32_t)threadspool->valueint;
    }

    cJSON *respBuffer = cJSON_GetObjectItem(json, "response_buffer");
    if (cJSON_IsNumber(respBuffer)) {
        g_queryInfo.response_buffer = respBuffer->valueint;
    } else {
        g_queryInfo.response_buffer = RESP_BUF_LEN;
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (cJSON_IsString(dbs)) {
        g_arguments->db->dbName = dbs->valuestring;
    }

    cJSON *queryMode = cJSON_GetObjectItem(json, "query_mode");
    if (cJSON_IsString(queryMode)) {
        if (0 == strcasecmp(queryMode->valuestring, "rest")) {
            g_arguments->db->superTbls->iface = REST_IFACE;
        }
    }
    // init sqls
    g_queryInfo.specifiedQueryInfo.sqls = benchArrayInit(1, sizeof(SSQL));

    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(json, "specified_table_query");
    g_queryInfo.specifiedQueryInfo.concurrent = 1;
    if (cJSON_IsObject(specifiedQuery)) {
        cJSON *queryInterval =
            cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (cJSON_IsNumber(queryInterval)) {
            g_queryInfo.specifiedQueryInfo.queryInterval =
                queryInterval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        cJSON *specifiedQueryTimes =
            cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (cJSON_IsNumber(specifiedQueryTimes)) {
            g_queryInfo.specifiedQueryInfo.queryTimes =
                specifiedQueryTimes->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        cJSON *concurrent = cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (cJSON_IsNumber(concurrent)) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)concurrent->valueint;
        }

        cJSON *threads = cJSON_GetObjectItem(specifiedQuery, "threads");
        if (cJSON_IsNumber(threads)) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)threads->valueint;
        }

        cJSON *specifiedAsyncMode = cJSON_GetObjectItem(specifiedQuery, "mode");
        if (cJSON_IsString(specifiedAsyncMode)) {
            if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *interval = cJSON_GetObjectItem(specifiedQuery, "interval");
        if (cJSON_IsNumber(interval)) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                interval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                DEFAULT_SUB_INTERVAL;
        }

        cJSON *restart = cJSON_GetObjectItem(specifiedQuery, "restart");
        if (cJSON_IsString(restart)) {
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
        if (cJSON_IsString(keepProgress)) {
            if (0 == strcmp("yes", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 1;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // read sqls from file
        cJSON *sqlFileObj = cJSON_GetObjectItem(specifiedQuery, "sql_file");
        if (cJSON_IsString(sqlFileObj)) {
            FILE * fp = fopen(sqlFileObj->valuestring, "r");
            if (fp == NULL) {
                errorPrint(stderr, "failed to open file: %s\n", sqlFileObj->valuestring);
                goto PARSE_OVER;
            }
            char buf[BUFFER_SIZE];
            memset(buf, 0, BUFFER_SIZE);
            while (fgets(buf, BUFFER_SIZE, fp)) {
                SSQL * sql = benchCalloc(1, sizeof(SSQL), true);
                benchArrayPush(g_queryInfo.specifiedQueryInfo.sqls, sql);
                sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, g_queryInfo.specifiedQueryInfo.sqls->size - 1);
                sql->command = benchCalloc(1, strlen(buf), true);
                sql->delay_list = benchCalloc(g_queryInfo.specifiedQueryInfo.queryTimes *
                        g_queryInfo.specifiedQueryInfo.concurrent, sizeof(int64_t), true);
                tstrncpy(sql->command, buf, strlen(buf));
                debugPrint(stdout, "read file buffer: %s\n", sql->command);
                memset(buf, 0, BUFFER_SIZE);
            }
        }
        // sqls
        cJSON *specifiedSqls = cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (cJSON_IsArray(specifiedSqls)) {
            int specifiedSqlSize = cJSON_GetArraySize(specifiedSqls);
            if (specifiedSqlSize * g_queryInfo.specifiedQueryInfo.concurrent >
                MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    stderr,
                    "failed to read json, query sql(%d) * concurrent(%d) "
                    "overflow, max is %d\n",
                    specifiedSqlSize, g_queryInfo.specifiedQueryInfo.concurrent,
                    MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            for (int j = 0; j < specifiedSqlSize; ++j) {
                cJSON *sqlObj = cJSON_GetArrayItem(specifiedSqls, j);
                if (cJSON_IsObject(sqlObj)) {
                    SSQL * sql = benchCalloc(1, sizeof(SSQL), true);
                    benchArrayPush(g_queryInfo.specifiedQueryInfo.sqls, sql);
                    sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, g_queryInfo.specifiedQueryInfo.sqls->size -1);
                    sql->delay_list = benchCalloc(g_queryInfo.specifiedQueryInfo.queryTimes *
                        g_queryInfo.specifiedQueryInfo.concurrent, sizeof(int64_t), true);
                    
                    cJSON *sqlStr = cJSON_GetObjectItem(sqlObj, "sql");
                    if (cJSON_IsString(sqlStr)) {
                        sql->command = benchCalloc(1, strlen(sqlStr->valuestring) + 1, true);
                        tstrncpy(sql->command, sqlStr->valuestring, strlen(sqlStr->valuestring) + 1);
                        // default value is -1, which mean infinite loop
                        g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                        cJSON *endAfterConsume =
                            cJSON_GetObjectItem(specifiedQuery, "endAfterConsume");
                        if (cJSON_IsNumber(endAfterConsume)) {
                            g_queryInfo.specifiedQueryInfo.endAfterConsume[j] =
                                (int)endAfterConsume->valueint;
                        }
                        if (g_queryInfo.specifiedQueryInfo.endAfterConsume[j] < -1)
                            g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;

                        g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;
                        cJSON *resubAfterConsume =
                            cJSON_GetObjectItem(specifiedQuery, "resubAfterConsume");
                        if (cJSON_IsNumber(resubAfterConsume)) {
                            g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] =
                                (int)resubAfterConsume->valueint;
                        }

                        if (g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] < -1)
                            g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;

                        cJSON *result = cJSON_GetObjectItem(sqlObj, "result");
                        if (cJSON_IsString(result)) {
                            tstrncpy(sql->result, result->valuestring, MAX_FILE_NAME_LEN);
                        } else {
                            memset(sql->result, 0, MAX_FILE_NAME_LEN);
                        }
                    } else {
                        errorPrint(stderr, "%s","Invalid sql in json\n");
                        goto PARSE_OVER;
                    }
                }
            }
        }
    }

    // super_table_query
    cJSON *superQuery = cJSON_GetObjectItem(json, "super_table_query");
    g_queryInfo.superQueryInfo.threadCnt = 1;
    if (!superQuery || superQuery->type != cJSON_Object) {
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

        cJSON *concurrent = cJSON_GetObjectItem(superQuery, "concurrent");
        if (concurrent && concurrent->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt =
                (uint32_t)concurrent->valueint;
        }

        cJSON *threads = cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt = (uint32_t)threads->valueint;
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
                    stderr,
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
        errorPrint(stderr, "failed to cjson parse %s, invalid json format\n",
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
