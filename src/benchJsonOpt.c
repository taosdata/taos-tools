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

int getColumnAndTagTypeFromInsertJsonFile(cJSON *      stbInfo,
                                          SSuperTable *superTbls,
                                          SArguments * arguments) {
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
            if (length > TSDB_MAX_BINARY_LEN) {
                errorPrint("data length (%d) > TSDB_MAX_BINARY_LEN(%" PRIu64
                           ")\n",
                           length, TSDB_MAX_BINARY_LEN);
                goto PARSE_OVER;
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
                    length = arguments->binwidth;
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

    if ((index + 1 /* ts */) > MAX_NUM_COLUMNS) {
        errorPrint(
            "failed to read json, column size overflow, allowed max column "
            "size is %d\n",
            MAX_NUM_COLUMNS);
        goto PARSE_OVER;
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
            if (data_length > TSDB_MAX_BINARY_LEN) {
                errorPrint("data length (%d) > TSDB_MAX_BINARY_LEN(%" PRIu64
                           ")\n",
                           data_length, TSDB_MAX_BINARY_LEN);
                goto PARSE_OVER;
            }
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
                    data_length = arguments->binwidth;
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

    if (index > TSDB_MAX_TAGS) {
        errorPrint(
            "failed to read json, tags size overflow, allowed max tag count is "
            "%d\n",
            TSDB_MAX_TAGS);
        goto PARSE_OVER;
    }

    superTbls->tagCount = index;

    if ((superTbls->columnCount + superTbls->tagCount + 1 /* ts */) >
        TSDB_MAX_COLUMNS) {
        errorPrint(
            "columns + tags is more than allowed max columns count: %d\n",
            TSDB_MAX_COLUMNS);
        goto PARSE_OVER;
    }
    code = 0;

PARSE_OVER:
    return code;
}

int getMetaFromInsertJsonFile(cJSON *json, SArguments *arguments) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(json, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        arguments->host = host->valuestring;
    }

    cJSON *port = cJSON_GetObjectItem(json, "port");
    if (port && port->type == cJSON_Number) {
        arguments->port = (uint16_t)port->valueint;
    }

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        arguments->user = user->valuestring;
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        arguments->password = password->valuestring;
    }

    cJSON *resultfile = cJSON_GetObjectItem(json, "result_file");
    if (resultfile && resultfile->type == cJSON_String &&
        resultfile->valuestring != NULL) {
        arguments->output_file = resultfile->valuestring;
    }

    cJSON *threads = cJSON_GetObjectItem(json, "thread_count");
    if (threads && threads->type == cJSON_Number) {
        arguments->nthreads = (uint32_t)threads->valueint;
    } else {
        arguments->nthreads = DEFAULT_NTHREADS;
    }

    cJSON *threadspool = cJSON_GetObjectItem(json, "thread_pool_size");
    if (threadspool && threadspool->type == cJSON_Number) {
        arguments->nthreads_pool = (uint32_t)threadspool->valueint;
    } else {
        arguments->nthreads_pool = arguments->nthreads + 5;
    }

    if (init_taos_list(arguments)) goto PARSE_OVER;

    cJSON *numRecPerReq = cJSON_GetObjectItem(json, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == cJSON_Number) {
        arguments->reqPerReq = (uint32_t)numRecPerReq->valueint;
    } else {
        arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    }

    cJSON *prepareRand = cJSON_GetObjectItem(json, "prepared_rand");
    if (prepareRand && prepareRand->type == cJSON_Number) {
        arguments->prepared_rand = prepareRand->valueint;
    } else {
        arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    }

    cJSON *chineseOpt = cJSON_GetObjectItem(json, "chinese");  // yes, no,
    if (chineseOpt && chineseOpt->type == cJSON_String &&
        chineseOpt->valuestring != NULL) {
        if (0 == strncasecmp(chineseOpt->valuestring, "yes", 3)) {
            arguments->chinese = true;
        } else {
            arguments->chinese = false;
        }
    } else {
        arguments->chinese = false;
    }

    cJSON *top_insertInterval = cJSON_GetObjectItem(json, "insert_interval");
    if (top_insertInterval && top_insertInterval->type == cJSON_Number) {
        arguments->insert_interval = top_insertInterval->valueint;
    } else {
        arguments->insert_interval = 0;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            arguments->answer_yes = true;
        } else {
            arguments->answer_yes = false;
        }
    } else {
        arguments->answer_yes = true;
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (!dbs || dbs->type != cJSON_Array) goto PARSE_OVER;

    int dbSize = cJSON_GetArraySize(dbs);
    if (dbSize > MAX_DB_COUNT) goto PARSE_OVER;
    tmfree(arguments->db->superTbls->col_length);
    tmfree(arguments->db->superTbls->col_type);
    tmfree(arguments->db->superTbls->tag_type);
    tmfree(arguments->db->superTbls->tag_length);
    tmfree(arguments->db->superTbls);
    tmfree(arguments->db);
    arguments->db = calloc(dbSize, sizeof(SDataBase));
    arguments->dbCount = dbSize;
    for (int i = 0; i < dbSize; ++i) {
        cJSON *dbinfos = cJSON_GetArrayItem(dbs, i);
        if (dbinfos == NULL) continue;

        // dbinfo
        cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
        if (!dbinfo || dbinfo->type != cJSON_Object) goto PARSE_OVER;

        cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
        if (dbName && dbName->type == cJSON_String) {
            arguments->db[i].dbName = dbName->valuestring;
        }

        cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
        if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
            if (0 == strncasecmp(drop->valuestring, "yes", strlen("yes"))) {
                arguments->db[i].drop = true;
            } else {
                arguments->db[i].drop = false;
            }
        } else {
            arguments->db[i].drop = true;
        }

        cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
        if (precision && precision->type == cJSON_String &&
            precision->valuestring != NULL) {
            if (0 == strcasecmp(precision->valuestring, "us")) {
                arguments->db[i].dbCfg.precision = TSDB_TIME_PRECISION_MICRO;
                arguments->db[i].dbCfg.sml_precision =
                    TSDB_SML_TIMESTAMP_MICRO_SECONDS;
            } else if (0 == strcasecmp(precision->valuestring, "ns")) {
                arguments->db[i].dbCfg.precision = TSDB_TIME_PRECISION_NANO;
                arguments->db[i].dbCfg.sml_precision =
                    TSDB_SML_TIMESTAMP_NANO_SECONDS;
            } else {
                arguments->db[i].dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
                arguments->db[i].dbCfg.sml_precision =
                    TSDB_SML_TIMESTAMP_MILLI_SECONDS;
            }
        } else {
            arguments->db[i].dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
            arguments->db[i].dbCfg.sml_precision =
                TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        }

        cJSON *update = cJSON_GetObjectItem(dbinfo, "update");
        if (update && update->type == cJSON_Number) {
            arguments->db[i].dbCfg.update = (int)update->valueint;
        } else {
            arguments->db[i].dbCfg.update = -1;
        }

        cJSON *replica = cJSON_GetObjectItem(dbinfo, "replica");
        if (replica && replica->type == cJSON_Number) {
            arguments->db[i].dbCfg.replica = (int)replica->valueint;
        } else {
            arguments->db[i].dbCfg.replica = -1;
        }

        cJSON *keep = cJSON_GetObjectItem(dbinfo, "keep");
        if (keep && keep->type == cJSON_Number) {
            arguments->db[i].dbCfg.keep = (int)keep->valueint;
        } else {
            arguments->db[i].dbCfg.keep = -1;
        }

        cJSON *days = cJSON_GetObjectItem(dbinfo, "days");
        if (days && days->type == cJSON_Number) {
            arguments->db[i].dbCfg.days = (int)days->valueint;
        } else {
            arguments->db[i].dbCfg.days = -1;
        }

        cJSON *cache = cJSON_GetObjectItem(dbinfo, "cache");
        if (cache && cache->type == cJSON_Number) {
            arguments->db[i].dbCfg.cache = (int)cache->valueint;
        } else {
            arguments->db[i].dbCfg.cache = -1;
        }

        cJSON *blocks = cJSON_GetObjectItem(dbinfo, "blocks");
        if (blocks && blocks->type == cJSON_Number) {
            arguments->db[i].dbCfg.blocks = (int)blocks->valueint;
        } else {
            arguments->db[i].dbCfg.blocks = -1;
        }

        cJSON *minRows = cJSON_GetObjectItem(dbinfo, "minRows");
        if (minRows && minRows->type == cJSON_Number) {
            arguments->db[i].dbCfg.minRows = (uint32_t)minRows->valueint;
        } else {
            arguments->db[i].dbCfg.minRows = 0;
        }

        cJSON *maxRows = cJSON_GetObjectItem(dbinfo, "maxRows");
        if (maxRows && maxRows->type == cJSON_Number) {
            arguments->db[i].dbCfg.maxRows = (uint32_t)maxRows->valueint;
        } else {
            arguments->db[i].dbCfg.maxRows = 0;
        }

        cJSON *comp = cJSON_GetObjectItem(dbinfo, "comp");
        if (comp && comp->type == cJSON_Number) {
            arguments->db[i].dbCfg.comp = (int)comp->valueint;
        } else {
            arguments->db[i].dbCfg.comp = -1;
        }

        cJSON *walLevel = cJSON_GetObjectItem(dbinfo, "walLevel");
        if (walLevel && walLevel->type == cJSON_Number) {
            arguments->db[i].dbCfg.walLevel = (int)walLevel->valueint;
        } else {
            arguments->db[i].dbCfg.walLevel = -1;
        }

        cJSON *cacheLast = cJSON_GetObjectItem(dbinfo, "cachelast");
        if (cacheLast && cacheLast->type == cJSON_Number) {
            arguments->db[i].dbCfg.cacheLast = (int)cacheLast->valueint;
        } else {
            arguments->db[i].dbCfg.cacheLast = -1;
        }

        cJSON *quorum = cJSON_GetObjectItem(dbinfo, "quorum");
        if (quorum && quorum->type == cJSON_Number) {
            arguments->db[i].dbCfg.quorum = (int)quorum->valueint;
        } else {
            arguments->db[i].dbCfg.quorum = -1;
        }

        cJSON *fsync = cJSON_GetObjectItem(dbinfo, "fsync");
        if (fsync && fsync->type == cJSON_Number) {
            arguments->db[i].dbCfg.fsync = (int)fsync->valueint;
        } else {
            arguments->db[i].dbCfg.fsync = -1;
        }

        // super_tables
        cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
        if (!stables || stables->type != cJSON_Array) goto PARSE_OVER;

        int stbSize = cJSON_GetArraySize(stables);
        if (stbSize > MAX_SUPER_TABLE_COUNT) goto PARSE_OVER;
        arguments->db[i].superTbls = calloc(1, stbSize * sizeof(SSuperTable));
        assert(arguments->db[i].superTbls);
        arguments->db[i].superTblCount = stbSize;
        for (int j = 0; j < stbSize; ++j) {
            cJSON *stbInfo = cJSON_GetArrayItem(stables, j);
            if (stbInfo == NULL) continue;

            // dbinfo
            cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
            if (stbName && stbName->type == cJSON_String) {
                arguments->db[i].superTbls[j].stbName = stbName->valuestring;
            }

            cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
            if (prefix && prefix->type == cJSON_String) {
                arguments->db[i].superTbls[j].childTblPrefix =
                    prefix->valuestring;
            }

            cJSON *escapeChar =
                cJSON_GetObjectItem(stbInfo, "escape_character");
            if (escapeChar && escapeChar->type == cJSON_String &&
                escapeChar->valuestring != NULL) {
                if ((0 == strncasecmp(escapeChar->valuestring, "yes", 3))) {
                    arguments->db[i].superTbls[j].escape_character = true;
                } else {
                    arguments->db[i].superTbls[j].escape_character = false;
                }
            } else {
                arguments->db[i].superTbls[j].escape_character = false;
            }

            cJSON *autoCreateTbl =
                cJSON_GetObjectItem(stbInfo, "auto_create_table");
            if (autoCreateTbl && autoCreateTbl->type == cJSON_String &&
                autoCreateTbl->valuestring != NULL) {
                if ((0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3)) &&
                    (!arguments->db[i].superTbls[j].childTblExists)) {
                    arguments->db[i].superTbls[j].autoCreateTable = true;
                } else {
                    arguments->db[i].superTbls[j].autoCreateTable = false;
                }
            } else {
                arguments->db[i].superTbls[j].autoCreateTable = false;
            }

            cJSON *batchCreateTbl =
                cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
            if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
                arguments->db[i].superTbls[j].batchCreateTableNum =
                    batchCreateTbl->valueint;
            } else {
                arguments->db[i].superTbls[j].batchCreateTableNum =
                    DEFAULT_CREATE_BATCH;
            }

            cJSON *childTblExists =
                cJSON_GetObjectItem(stbInfo, "child_table_exists");  // yes, no
            if (childTblExists && childTblExists->type == cJSON_String &&
                childTblExists->valuestring != NULL) {
                if ((0 == strncasecmp(childTblExists->valuestring, "yes", 3)) &&
                    (arguments->db[i].drop == false)) {
                    arguments->db[i].superTbls[j].childTblExists = true;
                    arguments->db[i].superTbls[j].autoCreateTable = false;
                } else {
                    arguments->db[i].superTbls[j].childTblExists = false;
                }
            } else {
                arguments->db[i].superTbls[j].childTblExists = false;
            }

            cJSON *count = cJSON_GetObjectItem(stbInfo, "childtable_count");
            if (count && count->type == cJSON_Number) {
                arguments->db[i].superTbls[j].childTblCount = count->valueint;
                arguments->g_totalChildTables +=
                    arguments->db[i].superTbls[j].childTblCount;
            }

            cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
            if (dataSource && dataSource->type == cJSON_String &&
                dataSource->valuestring != NULL) {
                if (0 == strcasecmp(dataSource->valuestring, "sample")) {
                    arguments->db[i].superTbls[j].random_data_source = false;
                } else {
                    arguments->db[i].superTbls[j].random_data_source = true;
                }
            } else {
                arguments->db[i].superTbls[j].random_data_source = true;
            }

            cJSON *stbIface = cJSON_GetObjectItem(
                stbInfo, "insert_mode");  // taosc , rest, stmt
            if (stbIface && stbIface->type == cJSON_String &&
                stbIface->valuestring != NULL) {
                if (0 == strcasecmp(stbIface->valuestring, "rest")) {
                    arguments->db[i].superTbls[j].iface = REST_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
                    arguments->db[i].superTbls[j].iface = STMT_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "sml")) {
                    if (!arguments->db[i].superTbls[j].random_data_source) {
                        errorPrint("%s",
                                   "sml insert mode currently does not support "
                                   "sample as data source\n");
                        goto PARSE_OVER;
                    }
                    arguments->db[i].superTbls[j].iface = SML_IFACE;
                } else {
                    arguments->db[i].superTbls[j].iface = TAOSC_IFACE;
                }
            } else {
                arguments->db[i].superTbls[j].iface = TAOSC_IFACE;
            }

            cJSON *stbLineProtocol =
                cJSON_GetObjectItem(stbInfo, "line_protocol");
            if (stbLineProtocol && stbLineProtocol->type == cJSON_String &&
                stbLineProtocol->valuestring != NULL) {
                if (0 == strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                    arguments->db[i].superTbls[j].lineProtocol =
                        TSDB_SML_TELNET_PROTOCOL;
                } else if (0 ==
                           strcasecmp(stbLineProtocol->valuestring, "json")) {
                    arguments->db[i].superTbls[j].lineProtocol =
                        TSDB_SML_JSON_PROTOCOL;
                } else {
                    arguments->db[i].superTbls[j].lineProtocol =
                        TSDB_SML_LINE_PROTOCOL;
                }
            } else {
                arguments->db[i].superTbls[j].lineProtocol =
                    TSDB_SML_LINE_PROTOCOL;
            }

            cJSON *childTbl_limit =
                cJSON_GetObjectItem(stbInfo, "childtable_limit");
            if (childTbl_limit && childTbl_limit->type == cJSON_Number) {
                if (childTbl_limit->valueint < 0) {
                    infoPrint("childTbl_limit(%" PRId64
                              ") less than 0, ignore it and set to "
                              "%" PRId64 "\n",
                              childTbl_limit->valueint,
                              arguments->db[i].superTbls[j].childTblCount);
                    arguments->db[i].superTbls[j].childTblLimit =
                        arguments->db[i].superTbls[j].childTblCount;
                } else {
                    arguments->db[i].superTbls[j].childTblLimit =
                        childTbl_limit->valueint;
                }
            } else {
                arguments->db[i].superTbls[j].childTblLimit =
                    arguments->db[i].superTbls[j].childTblCount;
            }

            cJSON *childTbl_offset =
                cJSON_GetObjectItem(stbInfo, "childtable_offset");
            if (childTbl_offset && childTbl_offset->type == cJSON_Number) {
                arguments->db[i].superTbls[j].childTblOffset =
                    childTbl_offset->valueint;
            } else {
                arguments->db[i].superTbls[j].childTblOffset = 0;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                if (0 == strcasecmp(ts->valuestring, "now")) {
                    arguments->db[i].superTbls[j].startTimestamp =
                        taosGetTimestamp(arguments->db[i].dbCfg.precision);
                } else {
                    if (taos_parse_time(
                            ts->valuestring,
                            &(arguments->db[i].superTbls[j].startTimestamp),
                            (int32_t)strlen(ts->valuestring),
                            arguments->db[i].dbCfg.precision, 0)) {
                        errorPrint("failed to parse time %s\n",
                                   ts->valuestring);
                        return -1;
                    }
                }
            } else {
                arguments->db[i].superTbls[j].startTimestamp =
                    taosGetTimestamp(arguments->db[i].dbCfg.precision);
            }

            cJSON *timestampStep =
                cJSON_GetObjectItem(stbInfo, "timestamp_step");
            if (timestampStep && timestampStep->type == cJSON_Number) {
                arguments->db[i].superTbls[j].timestamp_step =
                    timestampStep->valueint;
            } else {
                arguments->db[i].superTbls[j].timestamp_step = 1;
            }

            cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
            if (sampleFile && sampleFile->type == cJSON_String &&
                sampleFile->valuestring != NULL) {
                tstrncpy(arguments->db[i].superTbls[j].sampleFile,
                         sampleFile->valuestring,
                         min(MAX_FILE_NAME_LEN,
                             strlen(sampleFile->valuestring) + 1));
            } else {
                memset(arguments->db[i].superTbls[j].sampleFile, 0,
                       MAX_FILE_NAME_LEN);
            }

            cJSON *useSampleTs = cJSON_GetObjectItem(stbInfo, "use_sample_ts");
            if (useSampleTs && useSampleTs->type == cJSON_String &&
                useSampleTs->valuestring != NULL) {
                if (0 == strncasecmp(useSampleTs->valuestring, "yes", 3)) {
                    arguments->db[i].superTbls[j].useSampleTs = true;
                } else {
                    arguments->db[i].superTbls[j].useSampleTs = false;
                }
            } else {
                arguments->db[i].superTbls[j].useSampleTs = false;
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String) &&
                (tagsFile->valuestring != NULL)) {
                tstrncpy(arguments->db[i].superTbls[j].tagsFile,
                         tagsFile->valuestring, MAX_FILE_NAME_LEN);
            } else {
                memset(arguments->db[i].superTbls[j].tagsFile, 0,
                       MAX_FILE_NAME_LEN);
            }

            cJSON *insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
            if (insertRows && insertRows->type == cJSON_Number) {
                arguments->db[i].superTbls[j].insertRows = insertRows->valueint;
            } else {
                arguments->db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
            }

            cJSON *stbInterlaceRows =
                cJSON_GetObjectItem(stbInfo, "interlace_rows");
            if (stbInterlaceRows && stbInterlaceRows->type == cJSON_Number) {
                arguments->db[i].superTbls[j].interlaceRows =
                    (uint32_t)stbInterlaceRows->valueint;
            } else {
                arguments->db[i].superTbls[j].interlaceRows = 0;
            }

            cJSON *disorderRatio =
                cJSON_GetObjectItem(stbInfo, "disorder_ratio");
            if (disorderRatio && disorderRatio->type == cJSON_Number) {
                if (disorderRatio->valueint > 50) disorderRatio->valueint = 50;
                if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

                arguments->db[i].superTbls[j].disorderRatio =
                    (int)disorderRatio->valueint;
            } else {
                arguments->db[i].superTbls[j].disorderRatio = 0;
            }

            cJSON *disorderRange =
                cJSON_GetObjectItem(stbInfo, "disorder_range");
            if (disorderRange && disorderRange->type == cJSON_Number) {
                arguments->db[i].superTbls[j].disorderRange =
                    (int)disorderRange->valueint;
            } else {
                arguments->db[i].superTbls[j].disorderRange =
                    DEFAULT_DISORDER_RANGE;
            }

            cJSON *insertInterval =
                cJSON_GetObjectItem(stbInfo, "insert_interval");
            if (insertInterval && insertInterval->type == cJSON_Number) {
                arguments->db[i].superTbls[j].insert_interval =
                    insertInterval->valueint;
            } else {
                arguments->db[i].superTbls[j].insert_interval =
                    arguments->insert_interval;
            }

            if (getColumnAndTagTypeFromInsertJsonFile(
                    stbInfo, &arguments->db[i].superTbls[j], arguments)) {
                goto PARSE_OVER;
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}
int getMetaFromQueryJsonFile(cJSON *json, SArguments *arguments) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(g_queryInfo.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(json, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_queryInfo.host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else {
        tstrncpy(g_queryInfo.host, DEFAULT_HOST, MAX_HOSTNAME_SIZE);
    }

    cJSON *port = cJSON_GetObjectItem(json, "port");
    if (port && port->type == cJSON_Number) {
        g_queryInfo.port = (uint16_t)port->valueint;
    } else {
        g_queryInfo.port = DEFAULT_PORT;
    }

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_queryInfo.user, user->valuestring, MAX_USERNAME_SIZE);
    } else {
        tstrncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        tstrncpy(g_queryInfo.password, password->valuestring,
                 SHELL_MAX_PASSWORD_LEN);
    } else {
        tstrncpy(g_queryInfo.password, TSDB_DEFAULT_PASS,
                 SHELL_MAX_PASSWORD_LEN);
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            arguments->answer_yes = true;
        } else {
            arguments->answer_yes = false;
        }
    } else {
        arguments->answer_yes = false;
    }

    cJSON *gQueryTimes = cJSON_GetObjectItem(json, "query_times");
    if (gQueryTimes && gQueryTimes->type == cJSON_Number) {
        g_queryInfo.query_times = gQueryTimes->valueint;
    } else {
        g_queryInfo.query_times = DEFAULT_QUERY_TIME;
    }

    cJSON *threadspool = cJSON_GetObjectItem(json, "thread_pool_size");
    if (threadspool && threadspool->type == cJSON_Number) {
        arguments->nthreads_pool = (uint32_t)threadspool->valueint;
    } else {
        arguments->nthreads_pool = arguments->nthreads + 5;
    }

    cJSON *respBuffer = cJSON_GetObjectItem(json, "response_buffer");
    if (respBuffer && respBuffer->type == cJSON_Number) {
        g_queryInfo.response_buffer = respBuffer->valueint;
    } else {
        g_queryInfo.response_buffer = RESP_BUF_LEN;
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
        tstrncpy(g_queryInfo.dbName, dbs->valuestring, TSDB_DB_NAME_LEN);
    }

    cJSON *queryMode = cJSON_GetObjectItem(json, "query_mode");
    if (queryMode && queryMode->type == cJSON_String &&
        queryMode->valuestring != NULL) {
        tstrncpy(g_queryInfo.queryMode, queryMode->valuestring,
                 min(SMALL_BUFF_LEN, strlen(queryMode->valuestring) + 1));
    } else {
        tstrncpy(g_queryInfo.queryMode, "taosc",
                 min(SMALL_BUFF_LEN, strlen("taosc") + 1));
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
            g_queryInfo.superQueryInfo.threadCnt = DEFAULT_NTHREADS;
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

int getInfoFromJsonFile(char *file, SArguments *argument) {
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
            argument->test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            argument->test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            argument->test_mode = SUBSCRIBE_TEST;
        } else {
            errorPrint("%s", "failed to read json, filetype not support\n");
            goto PARSE_OVER;
        }
    } else {
        argument->test_mode = INSERT_TEST;
    }

    if (INSERT_TEST == argument->test_mode) {
        code = getMetaFromInsertJsonFile(root, argument);
    } else {
        memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
        code = getMetaFromQueryJsonFile(root, argument);
    }
PARSE_OVER:
    free(content);
    fclose(fp);
    return code;
}
