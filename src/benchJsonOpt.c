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

static int getColumnAndTagTypeFromInsertJsonFile(tools_cJSON *      stbInfo,
                                                 SSuperTable *superTbls) {
    int32_t code = -1;

    // columns
    tools_cJSON *columns = tools_cJSON_GetObjectItem(stbInfo, "columns");
    if (!columns || columns->type != tools_cJSON_Array) goto PARSE_OVER;

    int columnSize = tools_cJSON_GetArraySize(columns);

    int count = 1;
    int index = 0;
    int col_count = 0;
    for (int k = 0; k < columnSize; ++k) {
        tools_cJSON *column = tools_cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;
        tools_cJSON *countObj = tools_cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == tools_cJSON_Number &&
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
        tools_cJSON *column = tools_cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;

        count = 1;
        tools_cJSON *countObj = tools_cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == tools_cJSON_Number) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        tools_cJSON *dataName = tools_cJSON_GetObjectItem(column, "name");
        if (tools_cJSON_IsString(dataName)) {
            customName = true;
        }

        // column info
        tools_cJSON *dataType = tools_cJSON_GetObjectItem(column, "type");
        if (!tools_cJSON_IsString(dataType)) goto PARSE_OVER;

        tools_cJSON *dataMax = tools_cJSON_GetObjectItem(column, "max");
        if (dataMax && tools_cJSON_IsNumber(dataMax) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMax = true;
        }

        tools_cJSON *dataMin = tools_cJSON_GetObjectItem(column, "min");
        if (dataMin && tools_cJSON_IsNumber(dataMin) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMin = true;
        }

        tools_cJSON *dataValues = tools_cJSON_GetObjectItem(column, "values");
        bool   sma = false;
        if (g_arguments->taosc_version == 3) {
            tools_cJSON *sma_value = tools_cJSON_GetObjectItem(column, "sma");
            if (tools_cJSON_IsString(sma_value) &&
                (0 == strcasecmp(sma_value->valuestring, "yes"))) {
                sma = true;
            }
        }

        tools_cJSON * dataLen = tools_cJSON_GetObjectItem(column, "len");
        int32_t length;
        if (dataLen && dataLen->type == tools_cJSON_Number) {
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
    tools_cJSON *tags = tools_cJSON_GetObjectItem(stbInfo, "tags");
    if (!tags || tags->type != tools_cJSON_Array) goto PARSE_OVER;
    int tag_count = 0;
    int tagSize = tools_cJSON_GetArraySize(tags);

    for (int k = 0; k < tagSize; ++k) {
        tools_cJSON *tag = tools_cJSON_GetArrayItem(tags, k);
        tools_cJSON *countObj = tools_cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == tools_cJSON_Number &&
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
        tools_cJSON *tag = tools_cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;
        bool   customMax = false;
        bool   customMin = false;
        bool   customName = false;
        tools_cJSON *dataType = tools_cJSON_GetObjectItem(tag, "type");
        if (!tools_cJSON_IsString(dataType)) goto PARSE_OVER;
        int    data_length = 0;
        tools_cJSON *dataLen = tools_cJSON_GetObjectItem(tag, "len");
        if (tools_cJSON_IsNumber(dataLen)) {
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

        tools_cJSON *dataMax = tools_cJSON_GetObjectItem(tag, "max");
        if (tools_cJSON_IsNumber(dataMax) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMax = true;
        }

        tools_cJSON *dataMin = tools_cJSON_GetObjectItem(tag, "min");
        if (tools_cJSON_IsNumber(dataMin) &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring, 0) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMin = true;
        }

        tools_cJSON *dataValues = tools_cJSON_GetObjectItem(tag, "values");

        tools_cJSON *dataName = tools_cJSON_GetObjectItem(tag, "name");
        if (dataName && dataName->type == tools_cJSON_String &&
            dataName->valuestring != NULL) {
            customName = true;
        }
        tools_cJSON *countObj = tools_cJSON_GetObjectItem(tag, "count");
        if (tools_cJSON_IsNumber(countObj)) {
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

static int getDatabaseInfo(tools_cJSON *dbinfos, int index) {
    SDataBase *database;
    if (index > 0) {
        database = benchCalloc(1, sizeof(SDataBase), true);
        benchArrayPush(g_arguments->databases, database);

    }
    database = benchArrayGet(g_arguments->databases, index);
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
    tools_cJSON *dbinfo = tools_cJSON_GetArrayItem(dbinfos, index);
    tools_cJSON *db = tools_cJSON_GetObjectItem(dbinfo, "dbinfo");
    if (!tools_cJSON_IsObject(db)) {
        errorPrint(stderr, "%s", "Invalid dbinfo format in json\n");
        return -1;
    }
    tools_cJSON *dbName = tools_cJSON_GetObjectItem(db, "name");
    if (tools_cJSON_IsString(dbName)) {
        database->dbName = dbName->valuestring;
    } else {
        errorPrint(stderr, "%s", "miss name in dbinfo\n");
        return -1;
    }
    tools_cJSON *drop = tools_cJSON_GetObjectItem(db, "drop");
    if (tools_cJSON_IsString(drop) && (0 == strcasecmp(drop->valuestring, "no"))) {
        database->drop = false;
    }
    tools_cJSON *keep = tools_cJSON_GetObjectItem(db, "keep");
    if (tools_cJSON_IsNumber(keep)) {
        database->dbCfg.keep = (int)keep->valueint;
    }
    tools_cJSON *days = tools_cJSON_GetObjectItem(db, "days");
    if (tools_cJSON_IsNumber(days)) {
        database->dbCfg.days = (int)days->valueint;
    }

    tools_cJSON *maxRows = tools_cJSON_GetObjectItem(db, "maxRows");
    if (tools_cJSON_IsNumber(maxRows)) {
        database->dbCfg.maxRows = (int)maxRows->valueint;
    }

    tools_cJSON *minRows = tools_cJSON_GetObjectItem(db, "minRows");
    if (tools_cJSON_IsNumber(minRows)) {
        database->dbCfg.minRows = (int)minRows->valueint;
    }
    tools_cJSON *walLevel = tools_cJSON_GetObjectItem(db, "walLevel");
    if (tools_cJSON_IsNumber(walLevel)) {
        database->dbCfg.walLevel = (int)walLevel->valueint;
    }
    tools_cJSON *fsync = tools_cJSON_GetObjectItem(db, "fsync");
    if (tools_cJSON_IsNumber(fsync)) {
        database->dbCfg.fsync = (int)fsync->valueint;
    }
    tools_cJSON *cacheLast = tools_cJSON_GetObjectItem(db, "cachelast");
    if (tools_cJSON_IsNumber(cacheLast)) {
        database->dbCfg.cacheLast = (int)cacheLast->valueint;
    }
    tools_cJSON *replica = tools_cJSON_GetObjectItem(db, "replica");
    if (tools_cJSON_IsNumber(replica)) {
        database->dbCfg.replica = (int)replica->valueint;
    }
    tools_cJSON *precision = tools_cJSON_GetObjectItem(db, "precision");
    if (tools_cJSON_IsString(precision)) {
        if (0 == strcasecmp(precision->valuestring, "us")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_MICRO;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
        } else if (0 == strcasecmp(precision->valuestring, "ns")) {
            database->dbCfg.precision = TSDB_TIME_PRECISION_NANO;
            database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
        }
    }

    if (g_arguments->taosc_version == 2) {
        tools_cJSON *update = tools_cJSON_GetObjectItem(db, "update");
        if (tools_cJSON_IsNumber(update)) {
            database->dbCfg.update = (int)update->valueint;
        }
        tools_cJSON *cache = tools_cJSON_GetObjectItem(db, "cache");
        if (tools_cJSON_IsNumber(cache)) {
            database->dbCfg.cache = (int)cache->valueint;
        }
        tools_cJSON *blocks = tools_cJSON_GetObjectItem(db, "blocks");
        if (tools_cJSON_IsNumber(blocks)) {
            database->dbCfg.blocks = (int)blocks->valueint;
        }
        tools_cJSON *quorum = tools_cJSON_GetObjectItem(db, "quorum");
        if (tools_cJSON_IsNumber(quorum)) {
            database->dbCfg.quorum = (int)quorum->valueint;
        }
    } else if (g_arguments->taosc_version == 3) {
        tools_cJSON *buffer = tools_cJSON_GetObjectItem(db, "buffer");
        if (tools_cJSON_IsNumber(buffer)) {
            database->dbCfg.buffer = (int)buffer->valueint;
        }
        tools_cJSON *strict = tools_cJSON_GetObjectItem(db, "strict");
        if (tools_cJSON_IsNumber(strict)) {
            database->dbCfg.strict = (int)strict->valueint;
        }
        tools_cJSON *page_size = tools_cJSON_GetObjectItem(db, "page_size");
        if (tools_cJSON_IsNumber(page_size)) {
            database->dbCfg.page_size = (int)page_size->valueint;
        }
        tools_cJSON *pages = tools_cJSON_GetObjectItem(db, "pages");
        if (tools_cJSON_IsNumber(pages)) {
            database->dbCfg.pages = (int)pages->valueint;
        }
        tools_cJSON *vgroups = tools_cJSON_GetObjectItem(db, "vgroups");
        if (tools_cJSON_IsNumber(vgroups)) {
            database->dbCfg.vgroups = (int)vgroups->valueint;
        }
        tools_cJSON *single_stable = tools_cJSON_GetObjectItem(db, "single_stable");
        if (tools_cJSON_IsNumber(single_stable)) {
            database->dbCfg.single_stable = (int)single_stable->valueint;
        }
        tools_cJSON *retentions = tools_cJSON_GetObjectItem(db, "retentions");
        if (tools_cJSON_IsString(retentions)) {
            database->dbCfg.retentions = retentions->valuestring;
        }
    }
    return 0;
}

static int getStableInfo(tools_cJSON *dbinfos, int index) {
    SDataBase *database = benchArrayGet(g_arguments->databases, index);
    tools_cJSON *    dbinfo = tools_cJSON_GetArrayItem(dbinfos, index);
    tools_cJSON *    stables = tools_cJSON_GetObjectItem(dbinfo, "super_tables");
    if (!tools_cJSON_IsArray(stables)) {
        errorPrint(stderr, "%s", "invalid super_tables format in json\n");
        return -1;
    }
    for (int i = 0; i < tools_cJSON_GetArraySize(stables); ++i) {
        SSuperTable *superTable;
        if (index > 0 || i > 0) {
            superTable = benchCalloc(1, sizeof(SSuperTable), true);
            benchArrayPush(database->superTbls, superTable);
        }
        superTable = benchArrayGet(database->superTbls, i);
        tmfree(superTable->columns);
        tmfree(superTable->tags);
        superTable->escape_character = false;
        superTable->autoCreateTable = false;
        superTable->no_check_for_affected_rows = false;
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
        tools_cJSON *stbInfo = tools_cJSON_GetArrayItem(stables, i);
        tools_cJSON *stbName = tools_cJSON_GetObjectItem(stbInfo, "name");
        if (tools_cJSON_IsString(stbName)) {
            superTable->stbName = stbName->valuestring;
        }
        tools_cJSON *prefix = tools_cJSON_GetObjectItem(stbInfo, "childtable_prefix");
        if (tools_cJSON_IsString(prefix)) {
            superTable->childTblPrefix = prefix->valuestring;
        }
        tools_cJSON *escapeChar = tools_cJSON_GetObjectItem(stbInfo, "escape_character");
        if (tools_cJSON_IsString(escapeChar) &&
            (0 == strcasecmp(escapeChar->valuestring, "yes"))) {
            superTable->escape_character = true;
        }
        tools_cJSON *autoCreateTbl =
            tools_cJSON_GetObjectItem(stbInfo, "auto_create_table");
        if (tools_cJSON_IsString(autoCreateTbl) &&
            (0 == strcasecmp(autoCreateTbl->valuestring, "yes"))) {
            superTable->autoCreateTable = true;
        }
        tools_cJSON *batchCreateTbl =
            tools_cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
        if (tools_cJSON_IsNumber(batchCreateTbl)) {
            superTable->batchCreateTableNum = batchCreateTbl->valueint;
        }
        tools_cJSON *childTblExists =
            tools_cJSON_GetObjectItem(stbInfo, "child_table_exists");
        if (tools_cJSON_IsString(childTblExists) &&
            (0 == strcasecmp(childTblExists->valuestring, "yes")) &&
            !database->drop) {
            superTable->childTblExists = true;
            superTable->autoCreateTable = false;
        }

        tools_cJSON *affectRowObj = tools_cJSON_GetObjectItem(stbInfo, "no_check_for_affected_rows");
        if (tools_cJSON_IsString(affectRowObj)) {
            if (0 == strcasecmp(affectRowObj->valuestring, "yes")) {
                superTable->no_check_for_affected_rows = true;
            } else if (0 == strcasecmp(affectRowObj->valuestring, "no")) {
                superTable->no_check_for_affected_rows = false;
            } else {
                errorPrint(stderr, "Invalid value for 'no_check_for_affected_rows': %s\n", affectRowObj->valuestring);
                return -1;
            }
        }

        tools_cJSON *count = tools_cJSON_GetObjectItem(stbInfo, "childtable_count");
        if (tools_cJSON_IsNumber(count)) {
            superTable->childTblCount = count->valueint;
            g_arguments->g_totalChildTables += superTable->childTblCount;
        } else {
            superTable->childTblCount = 10;
            g_arguments->g_totalChildTables += superTable->childTblCount;
        }
        tools_cJSON *dataSource = tools_cJSON_GetObjectItem(stbInfo, "data_source");
        if (tools_cJSON_IsString(dataSource) &&
            (0 == strcasecmp(dataSource->valuestring, "sample"))) {
            superTable->random_data_source = false;
        }
        tools_cJSON *stbIface = tools_cJSON_GetObjectItem(stbInfo, "insert_mode");
        if (tools_cJSON_IsString(stbIface)) {
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
        tools_cJSON *stbLineProtocol = tools_cJSON_GetObjectItem(stbInfo, "line_protocol");
        if (tools_cJSON_IsString(stbLineProtocol)) {
            if (0 == strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                superTable->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
            } else if (0 == strcasecmp(stbLineProtocol->valuestring, "json")) {
                superTable->lineProtocol = TSDB_SML_JSON_PROTOCOL;
            }
        }
        tools_cJSON *transferProtocol = tools_cJSON_GetObjectItem(stbInfo, "tcp_transfer");
        if (tools_cJSON_IsString(transferProtocol) &&
            (0 == strcasecmp(transferProtocol->valuestring, "yes"))) {
            superTable->tcpTransfer = true;
        }
        tools_cJSON *childTbl_limit =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_limit");
        if (tools_cJSON_IsNumber(childTbl_limit) && (childTbl_limit->valueint >= 0)) {
            superTable->childTblLimit = childTbl_limit->valueint;
        } else {
            superTable->childTblLimit = superTable->childTblCount;
        };
        tools_cJSON *childTbl_offset =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_offset");
        if (tools_cJSON_IsNumber(childTbl_offset)) {
            superTable->childTblOffset = childTbl_offset->valueint;
        }
        tools_cJSON *ts = tools_cJSON_GetObjectItem(stbInfo, "start_timestamp");
        if (tools_cJSON_IsString(ts)) {
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
        tools_cJSON *timestampStep = tools_cJSON_GetObjectItem(stbInfo, "timestamp_step");
        if (tools_cJSON_IsNumber(timestampStep)) {
            superTable->timestamp_step = timestampStep->valueint;
        }
        tools_cJSON *sampleFile = tools_cJSON_GetObjectItem(stbInfo, "sample_file");
        if (tools_cJSON_IsString(sampleFile)) {
            tstrncpy(
                superTable->sampleFile, sampleFile->valuestring,
                min(MAX_FILE_NAME_LEN, strlen(sampleFile->valuestring) + 1));
        } else {
            memset(superTable->sampleFile, 0, MAX_FILE_NAME_LEN);
        }
        tools_cJSON *useSampleTs = tools_cJSON_GetObjectItem(stbInfo, "use_sample_ts");
        if (tools_cJSON_IsString(useSampleTs) &&
            (0 == strcasecmp(useSampleTs->valuestring, "yes"))) {
            superTable->useSampleTs = true;
        }
        tools_cJSON *nonStop = tools_cJSON_GetObjectItem(stbInfo, "non_stop_mode");
        if (tools_cJSON_IsString(nonStop) &&
            (0 == strcasecmp(nonStop->valuestring, "yes"))) {
            superTable->non_stop = true;
        }
        tools_cJSON *tagsFile = tools_cJSON_GetObjectItem(stbInfo, "tags_file");
        if (tools_cJSON_IsString(tagsFile)) {
            tstrncpy(superTable->tagsFile, tagsFile->valuestring,
                     MAX_FILE_NAME_LEN);
        } else {
            memset(superTable->tagsFile, 0, MAX_FILE_NAME_LEN);
        }
        tools_cJSON *insertRows = tools_cJSON_GetObjectItem(stbInfo, "insert_rows");
        if (tools_cJSON_IsNumber(insertRows)) {
            superTable->insertRows = insertRows->valueint;
        }
        tools_cJSON *stbInterlaceRows =
            tools_cJSON_GetObjectItem(stbInfo, "interlace_rows");
        if (tools_cJSON_IsNumber(stbInterlaceRows)) {
            superTable->interlaceRows = (uint32_t)stbInterlaceRows->valueint;
        }
        tools_cJSON *disorderRatio = tools_cJSON_GetObjectItem(stbInfo, "disorder_ratio");
        if (tools_cJSON_IsNumber(disorderRatio)) {
            if (disorderRatio->valueint > 50) disorderRatio->valueint = 50;
            if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

            superTable->disorderRatio = (int)disorderRatio->valueint;
        }
        tools_cJSON *disorderRange = tools_cJSON_GetObjectItem(stbInfo, "disorder_range");
        if (tools_cJSON_IsNumber(disorderRange)) {
            superTable->disorderRange = (int)disorderRange->valueint;
        }
        tools_cJSON *insertInterval = tools_cJSON_GetObjectItem(stbInfo, "insert_interval");
        if (tools_cJSON_IsNumber(insertInterval)) {
            superTable->insert_interval = insertInterval->valueint;
        }
        tools_cJSON *pCoumnNum = tools_cJSON_GetObjectItem(stbInfo, "partial_col_num");
        if (tools_cJSON_IsNumber(pCoumnNum)) {
            superTable->partialColumnNum = pCoumnNum->valueint;
        }
        if (g_arguments->taosc_version == 3) {
            tools_cJSON *delay = tools_cJSON_GetObjectItem(stbInfo, "delay");
            if (tools_cJSON_IsNumber(delay)) {
                superTable->delay = (int)delay->valueint;
            }
            tools_cJSON *file_factor = tools_cJSON_GetObjectItem(stbInfo, "file_factor");
            if (tools_cJSON_IsNumber(file_factor)) {
                superTable->file_factor = (int)file_factor->valueint;
            }
            tools_cJSON *rollup = tools_cJSON_GetObjectItem(stbInfo, "rollup");
            if (tools_cJSON_IsString(rollup)) {
                superTable->rollup = rollup->valuestring;
            }
        }
        if (getColumnAndTagTypeFromInsertJsonFile(stbInfo, superTable)) {
            return -1;
        }
    }
    return 0;
}

static int getStreamInfo(tools_cJSON* dbinfos, int index) {
    SDataBase *database = benchArrayGet(g_arguments->databases, index);
    tools_cJSON* dbinfo = tools_cJSON_GetArrayItem(dbinfos, index);
    tools_cJSON* streamsObj = tools_cJSON_GetObjectItem(dbinfo, "stream");
    if (tools_cJSON_IsArray(streamsObj)) {
        int streamCnt = tools_cJSON_GetArraySize(streamsObj);
        for (int i = 0; i < streamCnt; ++i) {
            tools_cJSON* streamObj = tools_cJSON_GetArrayItem(streamsObj, i);
            if (!tools_cJSON_IsObject(streamObj)) {
                errorPrint(stderr, "%s", "invalid stream format in json\n");
                return -1;
            }
            tools_cJSON* stream_name = tools_cJSON_GetObjectItem(streamObj, "stream_name");
            tools_cJSON* stream_stb = tools_cJSON_GetObjectItem(streamObj, "stream_stb");
            tools_cJSON* source_sql = tools_cJSON_GetObjectItem(streamObj, "source_sql");
            if (!tools_cJSON_IsString(stream_name) || !tools_cJSON_IsString(stream_stb) || !tools_cJSON_IsString(source_sql)) {
                errorPrint(stderr, "%s", "Invalid or miss 'stream_name'/'stream_stb'/'source_sql' key in json\n");
                return -1;
            }
            SSTREAM * stream = benchCalloc(1, sizeof(SSTREAM), true);
            tstrncpy(stream->stream_name, stream_name->valuestring, TSDB_TABLE_NAME_LEN);
            tstrncpy(stream->stream_stb, stream_stb->valuestring, TSDB_TABLE_NAME_LEN);
            tstrncpy(stream->source_sql, source_sql->valuestring, TSDB_MAX_SQL_LEN);
            tools_cJSON* trigger_mode = tools_cJSON_GetObjectItem(streamObj, "trigger_mode");
            if (tools_cJSON_IsString(trigger_mode)) {
                tstrncpy(stream->trigger_mode, trigger_mode->valuestring, BIGINT_BUFF_LEN);
            }
            tools_cJSON* watermark = tools_cJSON_GetObjectItem(streamObj, "watermark");
            if (tools_cJSON_IsString(watermark)) {
                tstrncpy(stream->watermark, watermark->valuestring, BIGINT_BUFF_LEN);
            }
            tools_cJSON* drop = tools_cJSON_GetObjectItem(streamObj, "drop");
            if (tools_cJSON_IsString(drop)) {
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
    }
    return 0;
}

static int getMetaFromInsertJsonFile(tools_cJSON *json) {
    int32_t code = -1;

    tools_cJSON *cfgdir = tools_cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == tools_cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    tools_cJSON *host = tools_cJSON_GetObjectItem(json, "host");
    if (host && host->type == tools_cJSON_String && host->valuestring != NULL) {
        g_arguments->host = host->valuestring;
    }

    tools_cJSON *port = tools_cJSON_GetObjectItem(json, "port");
    if (port && port->type == tools_cJSON_Number) {
        g_arguments->port = (uint16_t)port->valueint;
    }

    tools_cJSON *user = tools_cJSON_GetObjectItem(json, "user");
    if (user && user->type == tools_cJSON_String && user->valuestring != NULL) {
        g_arguments->user = user->valuestring;
    }

    tools_cJSON *password = tools_cJSON_GetObjectItem(json, "password");
    if (password && password->type == tools_cJSON_String &&
        password->valuestring != NULL) {
        g_arguments->password = password->valuestring;
    }

    tools_cJSON *resultfile = tools_cJSON_GetObjectItem(json, "result_file");
    if (resultfile && resultfile->type == tools_cJSON_String &&
        resultfile->valuestring != NULL) {
        g_arguments->output_file = resultfile->valuestring;
    }

    tools_cJSON *threads = tools_cJSON_GetObjectItem(json, "thread_count");
    if (threads && threads->type == tools_cJSON_Number) {
        g_arguments->nthreads = (uint32_t)threads->valueint;
    }

    tools_cJSON *table_theads =
        tools_cJSON_GetObjectItem(json, "create_table_thread_count");
    if (tools_cJSON_IsNumber(table_theads)) {
        g_arguments->table_threads = (uint32_t)table_theads->valueint;
    }

    tools_cJSON *threadspool = tools_cJSON_GetObjectItem(json, "connection_pool_size");
    if (threadspool && threadspool->type == tools_cJSON_Number) {
        g_arguments->connection_pool = (uint32_t)threadspool->valueint;
    }

    if (init_taos_list()) goto PARSE_OVER;

    tools_cJSON *numRecPerReq = tools_cJSON_GetObjectItem(json, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == tools_cJSON_Number) {
        g_arguments->reqPerReq = (uint32_t)numRecPerReq->valueint;
        if (g_arguments->reqPerReq <= 0) goto PARSE_OVER;
    }

    tools_cJSON *prepareRand = tools_cJSON_GetObjectItem(json, "prepared_rand");
    if (prepareRand && prepareRand->type == tools_cJSON_Number) {
        g_arguments->prepared_rand = prepareRand->valueint;
    }

    tools_cJSON *chineseOpt = tools_cJSON_GetObjectItem(json, "chinese");  // yes, no,
    if (chineseOpt && chineseOpt->type == tools_cJSON_String &&
        chineseOpt->valuestring != NULL) {
        if (0 == strncasecmp(chineseOpt->valuestring, "yes", 3)) {
            g_arguments->chinese = true;
        }
    }

    tools_cJSON *top_insertInterval = tools_cJSON_GetObjectItem(json, "insert_interval");
    if (top_insertInterval && top_insertInterval->type == tools_cJSON_Number) {
        g_arguments->insert_interval = top_insertInterval->valueint;
    }

    tools_cJSON *answerPrompt =
        tools_cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == tools_cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    tools_cJSON *dbinfos = tools_cJSON_GetObjectItem(json, "databases");
    if (!tools_cJSON_IsArray(dbinfos)) {
        errorPrint(stderr, "%s", "Invalid databases format in json\n");
        return -1;
    }
    int dbSize = tools_cJSON_GetArraySize(dbinfos);

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

static int getMetaFromQueryJsonFile(tools_cJSON *json) {
    int32_t code = -1;
    SDataBase * dataBase = benchArrayGet(g_arguments->databases, 0);

    tools_cJSON *cfgdir = tools_cJSON_GetObjectItem(json, "cfgdir");
    if (tools_cJSON_IsString(cfgdir)) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    tools_cJSON *host = tools_cJSON_GetObjectItem(json, "host");
    if (tools_cJSON_IsString(host)) {
        g_arguments->host = host->valuestring;
    }

    tools_cJSON *port = tools_cJSON_GetObjectItem(json, "port");
    if (tools_cJSON_IsNumber(port)) {
        g_arguments->port = (uint16_t)port->valueint;
    }

    tools_cJSON *telnet_tcp_port = tools_cJSON_GetObjectItem(json, "telnet_tcp_port");
    if (tools_cJSON_IsNumber(telnet_tcp_port)) {
        g_arguments->telnet_tcp_port = (uint16_t)telnet_tcp_port->valueint;
    }

    tools_cJSON *user = tools_cJSON_GetObjectItem(json, "user");
    if (tools_cJSON_IsString(user)) {
        g_arguments->user = user->valuestring;
    }

    tools_cJSON *password = tools_cJSON_GetObjectItem(json, "password");
    if (tools_cJSON_IsString(password)) {
        g_arguments->password = password->valuestring;
    }

    tools_cJSON *answerPrompt =
        tools_cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (tools_cJSON_IsString(answerPrompt)) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    tools_cJSON *gQueryTimes = tools_cJSON_GetObjectItem(json, "query_times");
    if (tools_cJSON_IsNumber(gQueryTimes)) {
        g_queryInfo.query_times = gQueryTimes->valueint;
    } else {
        g_queryInfo.query_times = 1;
    }

    tools_cJSON *resetCache = tools_cJSON_GetObjectItem(json, "reset_query_cache");
    if (tools_cJSON_IsString(resetCache)) {
        if (0 == strcasecmp(resetCache->valuestring, "yes")) {
            g_queryInfo.reset_query_cache = true;
        }
    } else {
        g_queryInfo.reset_query_cache = false;
    }

    tools_cJSON *threadspool = tools_cJSON_GetObjectItem(json, "connection_pool_size");
    if (tools_cJSON_IsNumber(threadspool)) {
        g_arguments->connection_pool = (uint32_t)threadspool->valueint;
    }

    tools_cJSON *respBuffer = tools_cJSON_GetObjectItem(json, "response_buffer");
    if (tools_cJSON_IsNumber(respBuffer)) {
        g_queryInfo.response_buffer = respBuffer->valueint;
    } else {
        g_queryInfo.response_buffer = RESP_BUF_LEN;
    }

    tools_cJSON *dbs = tools_cJSON_GetObjectItem(json, "databases");
    if (tools_cJSON_IsString(dbs)) {
        dataBase->dbName = dbs->valuestring;
    }

    tools_cJSON *queryMode = tools_cJSON_GetObjectItem(json, "query_mode");
    if (tools_cJSON_IsString(queryMode)) {
        if (0 == strcasecmp(queryMode->valuestring, "rest")) {
            SSuperTable * stbInfo = benchArrayGet(dataBase->superTbls, 0);
            stbInfo->iface = REST_IFACE;
        }
    }
    // init sqls
    g_queryInfo.specifiedQueryInfo.sqls = benchArrayInit(1, sizeof(SSQL));

    // specified_table_query
    tools_cJSON *specifiedQuery = tools_cJSON_GetObjectItem(json, "specified_table_query");
    g_queryInfo.specifiedQueryInfo.concurrent = 1;
    if (tools_cJSON_IsObject(specifiedQuery)) {
        tools_cJSON *queryInterval =
            tools_cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (tools_cJSON_IsNumber(queryInterval)) {
            g_queryInfo.specifiedQueryInfo.queryInterval =
                queryInterval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        tools_cJSON *specifiedQueryTimes =
            tools_cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (tools_cJSON_IsNumber(specifiedQueryTimes)) {
            g_queryInfo.specifiedQueryInfo.queryTimes =
                specifiedQueryTimes->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        tools_cJSON *concurrent = tools_cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (tools_cJSON_IsNumber(concurrent)) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)concurrent->valueint;
        }

        tools_cJSON *threads = tools_cJSON_GetObjectItem(specifiedQuery, "threads");
        if (tools_cJSON_IsNumber(threads)) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)threads->valueint;
        }

        tools_cJSON *specifiedAsyncMode = tools_cJSON_GetObjectItem(specifiedQuery, "mode");
        if (tools_cJSON_IsString(specifiedAsyncMode)) {
            if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        tools_cJSON *interval = tools_cJSON_GetObjectItem(specifiedQuery, "interval");
        if (tools_cJSON_IsNumber(interval)) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                interval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                DEFAULT_SUB_INTERVAL;
        }

        tools_cJSON *restart = tools_cJSON_GetObjectItem(specifiedQuery, "restart");
        if (tools_cJSON_IsString(restart)) {
            if (0 == strcmp("no", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = false;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
        }

        tools_cJSON *keepProgress =
            tools_cJSON_GetObjectItem(specifiedQuery, "keepProgress");
        if (tools_cJSON_IsString(keepProgress)) {
            if (0 == strcmp("yes", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 1;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // read sqls from file
        tools_cJSON *sqlFileObj = tools_cJSON_GetObjectItem(specifiedQuery, "sql_file");
        if (tools_cJSON_IsString(sqlFileObj)) {
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
        tools_cJSON *specifiedSqls = tools_cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (tools_cJSON_IsArray(specifiedSqls)) {
            int specifiedSqlSize = tools_cJSON_GetArraySize(specifiedSqls);
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
                tools_cJSON *sqlObj = tools_cJSON_GetArrayItem(specifiedSqls, j);
                if (tools_cJSON_IsObject(sqlObj)) {
                    SSQL * sql = benchCalloc(1, sizeof(SSQL), true);
                    benchArrayPush(g_queryInfo.specifiedQueryInfo.sqls, sql);
                    sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls, g_queryInfo.specifiedQueryInfo.sqls->size -1);
                    sql->delay_list = benchCalloc(g_queryInfo.specifiedQueryInfo.queryTimes *
                        g_queryInfo.specifiedQueryInfo.concurrent, sizeof(int64_t), true);
                    
                    tools_cJSON *sqlStr = tools_cJSON_GetObjectItem(sqlObj, "sql");
                    if (tools_cJSON_IsString(sqlStr)) {
                        sql->command = benchCalloc(1, strlen(sqlStr->valuestring) + 1, true);
                        tstrncpy(sql->command, sqlStr->valuestring, strlen(sqlStr->valuestring) + 1);
                        // default value is -1, which mean infinite loop
                        g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                        tools_cJSON *endAfterConsume =
                            tools_cJSON_GetObjectItem(specifiedQuery, "endAfterConsume");
                        if (tools_cJSON_IsNumber(endAfterConsume)) {
                            g_queryInfo.specifiedQueryInfo.endAfterConsume[j] =
                                (int)endAfterConsume->valueint;
                        }
                        if (g_queryInfo.specifiedQueryInfo.endAfterConsume[j] < -1)
                            g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;

                        g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;
                        tools_cJSON *resubAfterConsume =
                            tools_cJSON_GetObjectItem(specifiedQuery, "resubAfterConsume");
                        if (tools_cJSON_IsNumber(resubAfterConsume)) {
                            g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] =
                                (int)resubAfterConsume->valueint;
                        }

                        if (g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] < -1)
                            g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;

                        tools_cJSON *result = tools_cJSON_GetObjectItem(sqlObj, "result");
                        if (tools_cJSON_IsString(result)) {
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
    tools_cJSON *superQuery = tools_cJSON_GetObjectItem(json, "super_table_query");
    g_queryInfo.superQueryInfo.threadCnt = 1;
    if (!superQuery || superQuery->type != tools_cJSON_Object) {
        g_queryInfo.superQueryInfo.sqlCount = 0;
    } else {
        tools_cJSON *subrate = tools_cJSON_GetObjectItem(superQuery, "query_interval");
        if (subrate && subrate->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.queryInterval = subrate->valueint;
        } else {
            g_queryInfo.superQueryInfo.queryInterval = 0;
        }

        tools_cJSON *superQueryTimes = tools_cJSON_GetObjectItem(superQuery, "query_times");
        if (superQueryTimes && superQueryTimes->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
        } else {
            g_queryInfo.superQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        tools_cJSON *concurrent = tools_cJSON_GetObjectItem(superQuery, "concurrent");
        if (concurrent && concurrent->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt =
                (uint32_t)concurrent->valueint;
        }

        tools_cJSON *threads = tools_cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt = (uint32_t)threads->valueint;
        }

        tools_cJSON *stblname = tools_cJSON_GetObjectItem(superQuery, "stblname");
        if (stblname && stblname->type == tools_cJSON_String &&
            stblname->valuestring != NULL) {
            tstrncpy(g_queryInfo.superQueryInfo.stbName, stblname->valuestring,
                     TSDB_TABLE_NAME_LEN);
        }

        tools_cJSON *superAsyncMode = tools_cJSON_GetObjectItem(superQuery, "mode");
        if (superAsyncMode && superAsyncMode->type == tools_cJSON_String &&
            superAsyncMode->valuestring != NULL) {
            if (0 == strcmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        }

        tools_cJSON *superInterval = tools_cJSON_GetObjectItem(superQuery, "interval");
        if (superInterval && superInterval->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.subscribeInterval =
                superInterval->valueint;
        } else {
            g_queryInfo.superQueryInfo.subscribeInterval =
                DEFAULT_QUERY_INTERVAL;
        }

        tools_cJSON *subrestart = tools_cJSON_GetObjectItem(superQuery, "restart");
        if (subrestart && subrestart->type == tools_cJSON_String &&
            subrestart->valuestring != NULL) {
            if (0 == strcmp("no", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = false;
            } else {
                g_queryInfo.superQueryInfo.subscribeRestart = true;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeRestart = true;
        }

        tools_cJSON *superkeepProgress =
            tools_cJSON_GetObjectItem(superQuery, "keepProgress");
        if (superkeepProgress && superkeepProgress->type == tools_cJSON_String &&
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
        tools_cJSON *superEndAfterConsume =
            tools_cJSON_GetObjectItem(superQuery, "endAfterConsume");
        if (superEndAfterConsume &&
            superEndAfterConsume->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.endAfterConsume =
                (int)superEndAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.endAfterConsume < -1)
            g_queryInfo.superQueryInfo.endAfterConsume = -1;

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.resubAfterConsume = -1;
        tools_cJSON *superResubAfterConsume =
            tools_cJSON_GetObjectItem(superQuery, "resubAfterConsume");
        if ((superResubAfterConsume) &&
            (superResubAfterConsume->type == tools_cJSON_Number) &&
            (superResubAfterConsume->valueint >= 0)) {
            g_queryInfo.superQueryInfo.resubAfterConsume =
                (int)superResubAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.resubAfterConsume < -1)
            g_queryInfo.superQueryInfo.resubAfterConsume = -1;

        // supert table sqls
        tools_cJSON *superSqls = tools_cJSON_GetObjectItem(superQuery, "sqls");
        if (!superSqls || superSqls->type != tools_cJSON_Array) {
            g_queryInfo.superQueryInfo.sqlCount = 0;
        } else {
            int superSqlSize = tools_cJSON_GetArraySize(superSqls);
            if (superSqlSize > MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    stderr,
                    "failed to read json, query sql size overflow, max is %d\n",
                    MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                tools_cJSON *sql = tools_cJSON_GetArrayItem(superSqls, j);
                if (sql == NULL) continue;

                tools_cJSON *sqlStr = tools_cJSON_GetObjectItem(sql, "sql");
                if (sqlStr && sqlStr->type == tools_cJSON_String) {
                    tstrncpy(g_queryInfo.superQueryInfo.sql[j],
                             sqlStr->valuestring, BUFFER_SIZE);
                }

                tools_cJSON *result = tools_cJSON_GetObjectItem(sql, "result");
                if (result != NULL && result->type == tools_cJSON_String &&
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
    root = tools_cJSON_Parse(content);
    if (root == NULL) {
        errorPrint(stderr, "failed to cjson parse %s, invalid json format\n",
                   file);
        goto PARSE_OVER;
    }

    char *pstr = tools_cJSON_Print(root);
    infoPrint(stdout, "%s\n%s\n", file, pstr);
    tmfree(pstr);

    tools_cJSON *filetype = tools_cJSON_GetObjectItem(root, "filetype");
    if (tools_cJSON_IsString(filetype)) {
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
