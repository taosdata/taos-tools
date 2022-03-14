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
    superTbls->columns = calloc(col_count, sizeof(Column));
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
        if (dataName && dataName->type == cJSON_String &&
            dataName->valuestring != NULL) {
            customName = true;
        }

        // column info
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!dataType || dataType->type != cJSON_String) goto PARSE_OVER;

        cJSON *dataMax = cJSON_GetObjectItem(column, "max");
        if (dataMax && cJSON_IsNumber(dataMax) &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMax = true;
        }

        cJSON *dataMin = cJSON_GetObjectItem(column, "min");
        if (dataMin && cJSON_IsNumber(dataMin) &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMin = true;
        }

        cJSON *dataValues = cJSON_GetObjectItem(column, "values");

        cJSON * dataLen = cJSON_GetObjectItem(column, "len");
        int32_t length;
        if (dataLen && dataLen->type == cJSON_Number) {
            length = (int32_t)dataLen->valueint;
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
            superTbls->columns[index].name = calloc(1, TSDB_COL_NAME_LEN);
            superTbls->columns[index].type =
                taos_convert_string_to_datatype(dataType->valuestring);
            superTbls->columns[index].length = length;
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
    superTbls->tags = calloc(tag_count, sizeof(Column));
    // superTbls->tagCount = tagSize;
    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;
        bool   customMax = false;
        bool   customMin = false;
        bool   customName = false;
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

        cJSON *dataMax = cJSON_GetObjectItem(tag, "max");
        if (dataMax && cJSON_IsNumber(dataMax) &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_NCHAR) {
            customMax = true;
        }

        cJSON *dataMin = cJSON_GetObjectItem(tag, "min");
        if (dataMin && cJSON_IsNumber(dataMin) &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
                TSDB_DATA_TYPE_BINARY &&
            taos_convert_string_to_datatype(dataType->valuestring) !=
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
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        if ((tagSize == 1) &&
            (0 == strcasecmp(dataType->valuestring, "JSON"))) {
            superTbls->tags[0].name = calloc(1, TSDB_COL_NAME_LEN);
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
            superTbls->tags[index].name = calloc(1, TSDB_COL_NAME_LEN);
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
                taos_convert_string_to_datatype(dataType->valuestring);
            superTbls->tags[index].length = data_length;
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

    cJSON *threadspool = cJSON_GetObjectItem(json, "thread_pool_size");
    if (threadspool && threadspool->type == cJSON_Number) {
        g_arguments->nthreads_pool = (uint32_t)threadspool->valueint;
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

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (!dbs || dbs->type != cJSON_Array) goto PARSE_OVER;

    int dbSize = cJSON_GetArraySize(dbs);
    if (dbSize > MAX_DB_COUNT) goto PARSE_OVER;
    tmfree(g_arguments->db->superTbls->columns);
    tmfree(g_arguments->db->superTbls->tags);
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
        cJSON *dbinfos = cJSON_GetArrayItem(dbs, i);
        if (dbinfos == NULL) continue;

        // dbinfo
        cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
        if (!dbinfo || dbinfo->type != cJSON_Object) goto PARSE_OVER;

        cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
        if (dbName && dbName->type == cJSON_String) {
            database->dbName = dbName->valuestring;
        }

        cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
        if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
            if (0 == strcasecmp(drop->valuestring, "no")) {
                database->drop = false;
            } else {
                database->drop = true;
            }
        } else {
            database->drop = true;
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

        cJSON *update = cJSON_GetObjectItem(dbinfo, "update");
        if (update && update->type == cJSON_Number) {
            database->dbCfg.update = (int)update->valueint;
        }

        cJSON *replica = cJSON_GetObjectItem(dbinfo, "replica");
        if (replica && replica->type == cJSON_Number) {
            database->dbCfg.replica = (int)replica->valueint;
        }

        cJSON *keep = cJSON_GetObjectItem(dbinfo, "keep");
        if (keep && keep->type == cJSON_Number) {
            database->dbCfg.keep = (int)keep->valueint;
        }

        cJSON *days = cJSON_GetObjectItem(dbinfo, "days");
        if (days && days->type == cJSON_Number) {
            database->dbCfg.days = (int)days->valueint;
        }

        cJSON *cache = cJSON_GetObjectItem(dbinfo, "cache");
        if (cache && cache->type == cJSON_Number) {
            database->dbCfg.cache = (int)cache->valueint;
        }

        cJSON *blocks = cJSON_GetObjectItem(dbinfo, "blocks");
        if (blocks && blocks->type == cJSON_Number) {
            database->dbCfg.blocks = (int)blocks->valueint;
        }

        cJSON *minRows = cJSON_GetObjectItem(dbinfo, "minRows");
        if (minRows && minRows->type == cJSON_Number) {
            database->dbCfg.minRows = minRows->valueint;
        }

        cJSON *maxRows = cJSON_GetObjectItem(dbinfo, "maxRows");
        if (maxRows && maxRows->type == cJSON_Number) {
            database->dbCfg.maxRows = maxRows->valueint;
        }

        cJSON *comp = cJSON_GetObjectItem(dbinfo, "comp");
        if (comp && comp->type == cJSON_Number) {
            database->dbCfg.comp = (int)comp->valueint;
        }

        cJSON *walLevel = cJSON_GetObjectItem(dbinfo, "walLevel");
        if (walLevel && walLevel->type == cJSON_Number) {
            database->dbCfg.walLevel = (int)walLevel->valueint;
        }

        cJSON *cacheLast = cJSON_GetObjectItem(dbinfo, "cachelast");
        if (cacheLast && cacheLast->type == cJSON_Number) {
            database->dbCfg.cacheLast = (int)cacheLast->valueint;
        }

        cJSON *quorum = cJSON_GetObjectItem(dbinfo, "quorum");
        if (quorum && quorum->type == cJSON_Number) {
            database->dbCfg.quorum = (int)quorum->valueint;
        }

        cJSON *fsync = cJSON_GetObjectItem(dbinfo, "fsync");
        if (fsync && fsync->type == cJSON_Number) {
            database->dbCfg.fsync = (int)fsync->valueint;
        }

        // super_tables
        cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
        if (!stables || stables->type != cJSON_Array) goto PARSE_OVER;

        int stbSize = cJSON_GetArraySize(stables);
        if (stbSize > MAX_SUPER_TABLE_COUNT) goto PARSE_OVER;
        database->superTbls = calloc(stbSize, sizeof(SSuperTable));
        g_memoryUsage += stbSize * sizeof(SSuperTable);
        database->superTblCount = stbSize;
        for (int j = 0; j < stbSize; ++j) {
            SSuperTable *superTable = &(database->superTbls[j]);
            cJSON *      stbInfo = cJSON_GetArrayItem(stables, j);
            if (stbInfo == NULL) continue;

            // dbinfo
            cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
            if (stbName && stbName->type == cJSON_String) {
                superTable->stbName = stbName->valuestring;
            }

            cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
            if (prefix && prefix->type == cJSON_String) {
                superTable->childTblPrefix = prefix->valuestring;
            }

            cJSON *escapeChar =
                cJSON_GetObjectItem(stbInfo, "escape_character");
            if (escapeChar && escapeChar->type == cJSON_String &&
                escapeChar->valuestring != NULL) {
                if ((0 == strncasecmp(escapeChar->valuestring, "yes", 3))) {
                    superTable->escape_character = true;
                } else {
                    superTable->escape_character = false;
                }
            } else {
                superTable->escape_character = false;
            }

            cJSON *autoCreateTbl =
                cJSON_GetObjectItem(stbInfo, "auto_create_table");
            if (autoCreateTbl && autoCreateTbl->type == cJSON_String &&
                autoCreateTbl->valuestring != NULL) {
                if ((0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3)) &&
                    (!superTable->childTblExists)) {
                    superTable->autoCreateTable = true;
                } else {
                    superTable->autoCreateTable = false;
                }
            } else {
                superTable->autoCreateTable = false;
            }

            cJSON *batchCreateTbl =
                cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
            if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
                superTable->batchCreateTableNum = batchCreateTbl->valueint;
            } else {
                superTable->batchCreateTableNum = DEFAULT_CREATE_BATCH;
            }

            cJSON *childTblExists =
                cJSON_GetObjectItem(stbInfo, "child_table_exists");  // yes, no
            if (childTblExists && childTblExists->type == cJSON_String &&
                childTblExists->valuestring != NULL) {
                if ((0 == strncasecmp(childTblExists->valuestring, "yes", 3)) &&
                    (database->drop == false)) {
                    superTable->childTblExists = true;
                    superTable->autoCreateTable = false;
                } else {
                    superTable->childTblExists = false;
                }
            } else {
                superTable->childTblExists = false;
            }

            cJSON *count = cJSON_GetObjectItem(stbInfo, "childtable_count");
            if (count && count->type == cJSON_Number) {
                superTable->childTblCount = count->valueint;
                g_arguments->g_totalChildTables += superTable->childTblCount;
            } else {
                superTable->childTblCount = 10;
                g_arguments->g_totalChildTables += superTable->childTblCount;
            }

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
                    if (g_arguments->reqPerReq > g_arguments->prepared_rand) {
                        g_arguments->prepared_rand = g_arguments->reqPerReq;
                    }
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

            cJSON *transferProtocol =
                cJSON_GetObjectItem(stbInfo, "tcp_transfer");
            if (transferProtocol && transferProtocol->type == cJSON_String &&
                transferProtocol->valuestring != NULL) {
                if (0 == strcasecmp(transferProtocol->valuestring, "yes")) {
                    superTable->tcpTransfer = true;
                }
            } else {
                superTable->tcpTransfer = false;
            }

            cJSON *childTbl_limit =
                cJSON_GetObjectItem(stbInfo, "childtable_limit");
            if (childTbl_limit && childTbl_limit->type == cJSON_Number) {
                if (childTbl_limit->valueint < 0) {
                    infoPrint("childTbl_limit(%" PRId64
                              ") less than 0, ignore it and set to "
                              "%" PRId64 "\n",
                              childTbl_limit->valueint,
                              superTable->childTblCount);
                    superTable->childTblLimit = superTable->childTblCount;
                } else {
                    superTable->childTblLimit = childTbl_limit->valueint;
                }
            } else {
                superTable->childTblLimit = superTable->childTblCount;
            }

            cJSON *childTbl_offset =
                cJSON_GetObjectItem(stbInfo, "childtable_offset");
            if (childTbl_offset && childTbl_offset->type == cJSON_Number) {
                superTable->childTblOffset = childTbl_offset->valueint;
            } else {
                superTable->childTblOffset = 0;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                if (0 == strcasecmp(ts->valuestring, "now")) {
                    superTable->startTimestamp =
                        toolsGetTimestamp(database->dbCfg.precision);
                } else {
                    if (toolsParseTime(ts->valuestring,
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
                    toolsGetTimestamp(database->dbCfg.precision);
            }

            cJSON *timestampStep =
                cJSON_GetObjectItem(stbInfo, "timestamp_step");
            if (timestampStep && timestampStep->type == cJSON_Number) {
                superTable->timestamp_step = timestampStep->valueint;
            } else {
                superTable->timestamp_step = 1;
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

            cJSON *useSampleTs = cJSON_GetObjectItem(stbInfo, "use_sample_ts");
            if (useSampleTs && useSampleTs->type == cJSON_String &&
                useSampleTs->valuestring != NULL) {
                if (0 == strncasecmp(useSampleTs->valuestring, "yes", 3)) {
                    superTable->useSampleTs = true;
                } else {
                    superTable->useSampleTs = false;
                }
            } else {
                superTable->useSampleTs = false;
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String) &&
                (tagsFile->valuestring != NULL)) {
                tstrncpy(superTable->tagsFile, tagsFile->valuestring,
                         MAX_FILE_NAME_LEN);
            } else {
                memset(superTable->tagsFile, 0, MAX_FILE_NAME_LEN);
            }

            cJSON *insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
            if (insertRows && insertRows->type == cJSON_Number) {
                superTable->insertRows = insertRows->valueint;
            } else {
                superTable->insertRows = 10;
            }

            cJSON *stbInterlaceRows =
                cJSON_GetObjectItem(stbInfo, "interlace_rows");
            if (stbInterlaceRows && stbInterlaceRows->type == cJSON_Number) {
                superTable->interlaceRows =
                    (uint32_t)stbInterlaceRows->valueint;
            } else {
                superTable->interlaceRows = 0;
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

            cJSON *disorderRange =
                cJSON_GetObjectItem(stbInfo, "disorder_range");
            if (disorderRange && disorderRange->type == cJSON_Number) {
                superTable->disorderRange = (int)disorderRange->valueint;
            } else {
                superTable->disorderRange = DEFAULT_DISORDER_RANGE;
            }

            cJSON *insertInterval =
                cJSON_GetObjectItem(stbInfo, "insert_interval");
            if (insertInterval && insertInterval->type == cJSON_Number) {
                superTable->insert_interval = insertInterval->valueint;
            } else {
                superTable->insert_interval = g_arguments->insert_interval;
            }

            cJSON *pCoumnNum = cJSON_GetObjectItem(stbInfo, "partial_col_num");
            if (pCoumnNum && pCoumnNum->type == cJSON_Number) {
                superTable->partialColumnNum = pCoumnNum->valueint;
            } else {
                superTable->partialColumnNum = 0;
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
    g_queryInfo.specifiedQueryInfo.concurrent = 1;
    if (!specifiedQuery || specifiedQuery->type != cJSON_Object) {
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
        }

        cJSON *threads = cJSON_GetObjectItem(specifiedQuery, "threads");
        if (threads && threads->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)threads->valueint;
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
        errorPrint("failed to read %s, content is null", file);
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
