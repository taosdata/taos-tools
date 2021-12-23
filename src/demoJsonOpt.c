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

#include "demo.h"

int getColumnAndTagTypeFromInsertJsonFile(cJSON *      stbInfo,
                                          SSuperTable *superTbls) {
    int32_t code = -1;

    // columns
    cJSON *columns = cJSON_GetObjectItem(stbInfo, "columns");
    if (!columns || columns->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, columns not found\n");
        goto PARSE_OVER;
    }

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
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
        } else {
            count = 1;
        }

        // column info
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!dataType || dataType->type != cJSON_String ||
            dataType->valuestring == NULL) {
            errorPrint("%s", "failed to read json, column type not found\n");
            goto PARSE_OVER;
        }

        cJSON * dataLen = cJSON_GetObjectItem(column, "len");
        int32_t length;
        if (dataLen && dataLen->type == cJSON_Number) {
            length = (int32_t)dataLen->valueint;
        } else if (dataLen && dataLen->type != cJSON_Number) {
            errorPrint("%s() LN%d: failed to read json, column len not found\n",
                       __func__, __LINE__);
            goto PARSE_OVER;
        } else {
            length = SMALL_BUFF_LEN;
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
    if (!tags || tags->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, tags not found\n");
        goto PARSE_OVER;
    }
    int tag_count = 0;
    int tagSize = cJSON_GetArraySize(tags);

    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        cJSON *countObj = cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == cJSON_Number &&
            (int)countObj->valueint != 0) {
            tag_count += (int)countObj->valueint;
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
        } else {
            tag_count += 1;
        }
    }

    superTbls->tag_type = calloc(tag_count, sizeof(char));
    superTbls->tag_length = calloc(tag_count, sizeof(int32_t));

    // superTbls->tagCount = tagSize;
    for (int k = 0; k < tagSize; ++k) {
        cJSON *tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;

        cJSON *dataType = cJSON_GetObjectItem(tag, "type");
        if (!dataType || dataType->type != cJSON_String ||
            dataType->valuestring == NULL) {
            errorPrint("%s", "failed to read json, tag type not found\n");
            goto PARSE_OVER;
        }
        int    data_length = SMALL_BUFF_LEN;
        cJSON *dataLen = cJSON_GetObjectItem(tag, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
            data_length = (uint32_t)dataLen->valueint;
        } else if (dataLen && dataLen->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column len not found\n");
            goto PARSE_OVER;
        }

        cJSON *countObj = cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = (int)countObj->valueint;
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
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

int getMetaFromInsertJsonFile(cJSON *json) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(json, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        g_args.host = host->valuestring;
    }

    cJSON *port = cJSON_GetObjectItem(json, "port");
    if (port && port->type == cJSON_Number) {
        g_args.port = (uint16_t)port->valueint;
    }

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        g_args.user = user->valuestring;
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        g_args.password = password->valuestring;
    }

    cJSON *resultfile = cJSON_GetObjectItem(json, "result_file");
    if (resultfile && resultfile->type == cJSON_String &&
        resultfile->valuestring != NULL) {
        g_args.output_file = resultfile->valuestring;
    }

    cJSON *threads = cJSON_GetObjectItem(json, "thread_count");
    if (threads && threads->type == cJSON_Number) {
        g_args.nthreads = (uint32_t)threads->valueint;
    } else if (!threads) {
        g_args.nthreads = DEFAULT_NTHREADS;
    } else {
        errorPrint("%s", "failed to read json, threads not found\n");
        goto PARSE_OVER;
    }

    cJSON *gInsertInterval = cJSON_GetObjectItem(json, "insert_interval");
    if (gInsertInterval && gInsertInterval->type == cJSON_Number) {
        if (gInsertInterval->valueint < 0) {
            errorPrint("%s",
                       "failed to read json, insert interval input mistake\n");
            goto PARSE_OVER;
        }
        g_args.insert_interval = gInsertInterval->valueint;
    } else if (!gInsertInterval) {
        g_args.insert_interval = DEFAULT_INSERT_INTERVAL;
    } else {
        errorPrint("%s",
                   "failed to read json, insert_interval input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *interlaceRows = cJSON_GetObjectItem(json, "interlace_rows");
    if (interlaceRows && interlaceRows->type == cJSON_Number) {
        if (interlaceRows->valueint < 0) {
            errorPrint("%s",
                       "failed to read json, interlaceRows input mistake\n");
            goto PARSE_OVER;
        }
        g_args.interlaceRows = (uint32_t)interlaceRows->valueint;
    } else if (!interlaceRows) {
        g_args.interlaceRows =
            DEFAULT_INTERLACE_ROWS;  // 0 means progressive mode, > 0 mean
                                     // interlace mode. max value is less or equ
                                     // num_of_records_per_req
    } else {
        errorPrint("%s", "failed to read json, interlaceRows input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *numRecPerReq = cJSON_GetObjectItem(json, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == cJSON_Number) {
        if (numRecPerReq->valueint <= 0) {
            errorPrint(
                "%s() LN%d, failed to read json, num_of_records_per_req input "
                "mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        } else if (numRecPerReq->valueint > MAX_RECORDS_PER_REQ) {
            printf("NOTICE: number of records per request value %" PRIu64
                   " > %d\n\n",
                   numRecPerReq->valueint, MAX_RECORDS_PER_REQ);
            printf(
                "        number of records per request value will be set to "
                "%d\n\n",
                MAX_RECORDS_PER_REQ);
            prompt();
            numRecPerReq->valueint = MAX_RECORDS_PER_REQ;
        }
        g_args.reqPerReq = (uint32_t)numRecPerReq->valueint;
    } else if (!numRecPerReq) {
        g_args.reqPerReq = MAX_RECORDS_PER_REQ;
    } else {
        errorPrint(
            "%s() LN%d, failed to read json, num_of_records_per_req not "
            "found\n",
            __func__, __LINE__);
        goto PARSE_OVER;
    }

    cJSON *prepareRand = cJSON_GetObjectItem(json, "prepared_rand");
    if (prepareRand && prepareRand->type == cJSON_Number) {
        if (prepareRand->valueint <= 0) {
            errorPrint(
                "%s() LN%d, failed to read json, prepared_rand input mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        }
        g_args.prepared_rand = prepareRand->valueint;
    } else if (!prepareRand) {
        g_args.prepared_rand = DEFAULT_PREPARED_RAND;
    } else {
        errorPrint("%s", "failed to read json, prepared_rand not found\n");
        goto PARSE_OVER;
    }

    cJSON *chineseOpt = cJSON_GetObjectItem(json, "chinese");  // yes, no,
    if (chineseOpt && chineseOpt->type == cJSON_String &&
        chineseOpt->valuestring != NULL) {
        if (0 == strncasecmp(chineseOpt->valuestring, "yes", 3)) {
            g_args.chinese = true;
        } else if (0 == strncasecmp(chineseOpt->valuestring, "no", 2)) {
            g_args.chinese = false;
        } else {
            g_args.chinese = DEFAULT_CHINESE_OPT;
        }
    } else if (!chineseOpt) {
        g_args.chinese = DEFAULT_CHINESE_OPT;
    } else {
        errorPrint("%s", "failed to read json, chinese input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strncasecmp(answerPrompt->valuestring, "yes", 3)) {
            g_args.answer_yes = false;
        } else if (0 == strncasecmp(answerPrompt->valuestring, "no", 2)) {
            g_args.answer_yes = true;
        } else {
            g_args.answer_yes = DEFAULT_ANS_YES;
        }
    } else if (!answerPrompt) {
        g_args.answer_yes = true;  // default is no, mean answer_yes.
    } else {
        errorPrint(
            "%s",
            "failed to read json, confirm_parameter_prompt input mistake\n");
        goto PARSE_OVER;
    }

    // rows per table need be less than insert batch
    if (g_args.interlaceRows > g_args.reqPerReq) {
        printf(
            "NOTICE: interlace rows value %u > num_of_records_per_req %u\n\n",
            g_args.interlaceRows, g_args.reqPerReq);
        printf(
            "        interlace rows value will be set to "
            "num_of_records_per_req %u\n\n",
            g_args.reqPerReq);
        prompt();
        g_args.interlaceRows = g_args.reqPerReq;
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (!dbs || dbs->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, databases not found\n");
        goto PARSE_OVER;
    }

    int dbSize = cJSON_GetArraySize(dbs);
    if (dbSize > MAX_DB_COUNT) {
        errorPrint(
            "failed to read json, databases size overflow, max database is "
            "%d\n",
            MAX_DB_COUNT);
        goto PARSE_OVER;
    }
    db = calloc(dbSize, sizeof(SDataBase));
    g_args.dbCount = dbSize;
    for (int i = 0; i < dbSize; ++i) {
        cJSON *dbinfos = cJSON_GetArrayItem(dbs, i);
        if (dbinfos == NULL) continue;

        // dbinfo
        cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
        if (!dbinfo || dbinfo->type != cJSON_Object) {
            errorPrint("%s", "failed to read json, dbinfo not found\n");
            goto PARSE_OVER;
        }

        cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
        if (!dbName || dbName->type != cJSON_String ||
            dbName->valuestring == NULL) {
            errorPrint("%s", "failed to read json, db name not found\n");
            goto PARSE_OVER;
        }
        tstrncpy(db[i].dbName, dbName->valuestring, TSDB_DB_NAME_LEN);

        cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
        if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
            if (0 == strncasecmp(drop->valuestring, "yes", strlen("yes"))) {
                db[i].drop = true;
            } else {
                db[i].drop = false;
            }
        } else if (!drop) {
            db[i].drop = g_args.drop_database;
        } else {
            errorPrint("%s", "failed to read json, drop input mistake\n");
            goto PARSE_OVER;
        }

        cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
        if (precision && precision->type == cJSON_String &&
            precision->valuestring != NULL) {
            tstrncpy(db[i].dbCfg.precision, precision->valuestring,
                     SMALL_BUFF_LEN);
        } else if (!precision) {
            memset(db[i].dbCfg.precision, 0, SMALL_BUFF_LEN);
        } else {
            errorPrint("%s", "failed to read json, precision not found\n");
            goto PARSE_OVER;
        }

        cJSON *update = cJSON_GetObjectItem(dbinfo, "update");
        if (update && update->type == cJSON_Number) {
            db[i].dbCfg.update = (int)update->valueint;
        } else if (!update) {
            db[i].dbCfg.update = -1;
        } else {
            errorPrint("%s", "failed to read json, update not found\n");
            goto PARSE_OVER;
        }

        cJSON *replica = cJSON_GetObjectItem(dbinfo, "replica");
        if (replica && replica->type == cJSON_Number) {
            db[i].dbCfg.replica = (int)replica->valueint;
        } else if (!replica) {
            db[i].dbCfg.replica = -1;
        } else {
            errorPrint("%s", "failed to read json, replica not found\n");
            goto PARSE_OVER;
        }

        cJSON *keep = cJSON_GetObjectItem(dbinfo, "keep");
        if (keep && keep->type == cJSON_Number) {
            db[i].dbCfg.keep = (int)keep->valueint;
        } else if (!keep) {
            db[i].dbCfg.keep = -1;
        } else {
            errorPrint("%s", "failed to read json, keep not found\n");
            goto PARSE_OVER;
        }

        cJSON *days = cJSON_GetObjectItem(dbinfo, "days");
        if (days && days->type == cJSON_Number) {
            db[i].dbCfg.days = (int)days->valueint;
        } else if (!days) {
            db[i].dbCfg.days = -1;
        } else {
            errorPrint("%s", "failed to read json, days not found\n");
            goto PARSE_OVER;
        }

        cJSON *cache = cJSON_GetObjectItem(dbinfo, "cache");
        if (cache && cache->type == cJSON_Number) {
            db[i].dbCfg.cache = (int)cache->valueint;
        } else if (!cache) {
            db[i].dbCfg.cache = -1;
        } else {
            errorPrint("%s", "failed to read json, cache not found\n");
            goto PARSE_OVER;
        }

        cJSON *blocks = cJSON_GetObjectItem(dbinfo, "blocks");
        if (blocks && blocks->type == cJSON_Number) {
            db[i].dbCfg.blocks = (int)blocks->valueint;
        } else if (!blocks) {
            db[i].dbCfg.blocks = -1;
        } else {
            errorPrint("%s", "failed to read json, block not found\n");
            goto PARSE_OVER;
        }

        // cJSON* maxtablesPerVnode= cJSON_GetObjectItem(dbinfo,
        // "maxtablesPerVnode"); if (maxtablesPerVnode &&
        // maxtablesPerVnode->type
        // == cJSON_Number) {
        //  db[i].dbCfg.maxtablesPerVnode = maxtablesPerVnode->valueint;
        //} else if (!maxtablesPerVnode) {
        //  db[i].dbCfg.maxtablesPerVnode = TSDB_DEFAULT_TABLES;
        //} else {
        // printf("failed to read json, maxtablesPerVnode not found");
        // goto PARSE_OVER;
        //}

        cJSON *minRows = cJSON_GetObjectItem(dbinfo, "minRows");
        if (minRows && minRows->type == cJSON_Number) {
            db[i].dbCfg.minRows = (uint32_t)minRows->valueint;
        } else if (!minRows) {
            db[i].dbCfg.minRows = 0;  // 0 means default
        } else {
            errorPrint("%s", "failed to read json, minRows not found\n");
            goto PARSE_OVER;
        }

        cJSON *maxRows = cJSON_GetObjectItem(dbinfo, "maxRows");
        if (maxRows && maxRows->type == cJSON_Number) {
            db[i].dbCfg.maxRows = (uint32_t)maxRows->valueint;
        } else if (!maxRows) {
            db[i].dbCfg.maxRows = 0;  // 0 means default
        } else {
            errorPrint("%s", "failed to read json, maxRows not found\n");
            goto PARSE_OVER;
        }

        cJSON *comp = cJSON_GetObjectItem(dbinfo, "comp");
        if (comp && comp->type == cJSON_Number) {
            db[i].dbCfg.comp = (int)comp->valueint;
        } else if (!comp) {
            db[i].dbCfg.comp = -1;
        } else {
            errorPrint("%s", "failed to read json, comp not found\n");
            goto PARSE_OVER;
        }

        cJSON *walLevel = cJSON_GetObjectItem(dbinfo, "walLevel");
        if (walLevel && walLevel->type == cJSON_Number) {
            db[i].dbCfg.walLevel = (int)walLevel->valueint;
        } else if (!walLevel) {
            db[i].dbCfg.walLevel = -1;
        } else {
            errorPrint("%s", "failed to read json, walLevel not found\n");
            goto PARSE_OVER;
        }

        cJSON *cacheLast = cJSON_GetObjectItem(dbinfo, "cachelast");
        if (cacheLast && cacheLast->type == cJSON_Number) {
            db[i].dbCfg.cacheLast = (int)cacheLast->valueint;
        } else if (!cacheLast) {
            db[i].dbCfg.cacheLast = -1;
        } else {
            errorPrint("%s", "failed to read json, cacheLast not found\n");
            goto PARSE_OVER;
        }

        cJSON *quorum = cJSON_GetObjectItem(dbinfo, "quorum");
        if (quorum && quorum->type == cJSON_Number) {
            db[i].dbCfg.quorum = (int)quorum->valueint;
        } else if (!quorum) {
            db[i].dbCfg.quorum = 1;
        } else {
            errorPrint("%s", "failed to read json, quorum input mistake");
            goto PARSE_OVER;
        }

        cJSON *fsync = cJSON_GetObjectItem(dbinfo, "fsync");
        if (fsync && fsync->type == cJSON_Number) {
            db[i].dbCfg.fsync = (int)fsync->valueint;
        } else if (!fsync) {
            db[i].dbCfg.fsync = -1;
        } else {
            errorPrint("%s", "failed to read json, fsync input mistake\n");
            goto PARSE_OVER;
        }

        // super_tables
        cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
        if (!stables || stables->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super_tables not found\n");
            goto PARSE_OVER;
        }

        int stbSize = cJSON_GetArraySize(stables);
        if (stbSize > MAX_SUPER_TABLE_COUNT) {
            errorPrint(
                "failed to read json, supertable size overflow, max supertable "
                "is %d\n",
                MAX_SUPER_TABLE_COUNT);
            goto PARSE_OVER;
        }
        db[i].superTbls = calloc(1, stbSize * sizeof(SSuperTable));
        assert(db[i].superTbls);
        db[i].superTblCount = stbSize;
        for (int j = 0; j < stbSize; ++j) {
            cJSON *stbInfo = cJSON_GetArrayItem(stables, j);
            if (stbInfo == NULL) continue;

            // dbinfo
            cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
            if (!stbName || stbName->type != cJSON_String ||
                stbName->valuestring == NULL) {
                errorPrint("%s", "failed to read json, stb name not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(db[i].superTbls[j].stbName, stbName->valuestring,
                     TSDB_TABLE_NAME_LEN);

            cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
            if (!prefix || prefix->type != cJSON_String ||
                prefix->valuestring == NULL) {
                errorPrint(
                    "%s", "failed to read json, childtable_prefix not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(db[i].superTbls[j].childTblPrefix, prefix->valuestring,
                     TBNAME_PREFIX_LEN);

            cJSON *escapeChar =
                cJSON_GetObjectItem(stbInfo, "escape_character");
            if (escapeChar && escapeChar->type == cJSON_String &&
                escapeChar->valuestring != NULL) {
                if ((0 == strncasecmp(escapeChar->valuestring, "yes", 3))) {
                    db[i].superTbls[j].escapeChar = true;
                } else if (0 == strncasecmp(escapeChar->valuestring, "no", 2)) {
                    db[i].superTbls[j].escapeChar = false;
                } else {
                    db[i].superTbls[j].escapeChar = false;
                }
            } else if (!escapeChar) {
                db[i].superTbls[j].escapeChar = false;
            } else {
                errorPrint("%s",
                           "failed to read json, escape_character not found\n");
                goto PARSE_OVER;
            }

            cJSON *autoCreateTbl =
                cJSON_GetObjectItem(stbInfo, "auto_create_table");
            if (autoCreateTbl && autoCreateTbl->type == cJSON_String &&
                autoCreateTbl->valuestring != NULL) {
                if ((0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3)) &&
                    (TBL_ALREADY_EXISTS != db[i].superTbls[j].childTblExists)) {
                    db[i].superTbls[j].autoCreateTable = AUTO_CREATE_SUBTBL;
                } else if (0 ==
                           strncasecmp(autoCreateTbl->valuestring, "no", 2)) {
                    db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
                } else {
                    db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
                }
            } else if (!autoCreateTbl) {
                db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
            } else {
                errorPrint(
                    "%s", "failed to read json, auto_create_table not found\n");
                goto PARSE_OVER;
            }

            cJSON *batchCreateTbl =
                cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
            if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
                db[i].superTbls[j].batchCreateTableNum =
                    batchCreateTbl->valueint;
            } else if (!batchCreateTbl) {
                db[i].superTbls[j].batchCreateTableNum = DEFAULT_CREATE_BATCH;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, batch_create_tbl_num not found\n");
                goto PARSE_OVER;
            }

            cJSON *childTblExists =
                cJSON_GetObjectItem(stbInfo, "child_table_exists");  // yes, no
            if (childTblExists && childTblExists->type == cJSON_String &&
                childTblExists->valuestring != NULL) {
                if ((0 == strncasecmp(childTblExists->valuestring, "yes", 3)) &&
                    (db[i].drop == false)) {
                    db[i].superTbls[j].childTblExists = TBL_ALREADY_EXISTS;
                } else if ((0 == strncasecmp(childTblExists->valuestring, "no",
                                             2) ||
                            (db[i].drop == true))) {
                    db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
                } else {
                    db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
                }
            } else if (!childTblExists) {
                db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, child_table_exists not found\n");
                goto PARSE_OVER;
            }

            if (TBL_ALREADY_EXISTS == db[i].superTbls[j].childTblExists) {
                db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
            }

            cJSON *count = cJSON_GetObjectItem(stbInfo, "childtable_count");
            if (!count || count->type != cJSON_Number || 0 >= count->valueint) {
                errorPrint(
                    "%s",
                    "failed to read json, childtable_count input mistake\n");
                goto PARSE_OVER;
            }
            db[i].superTbls[j].childTblCount = count->valueint;
            g_totalChildTables += db[i].superTbls[j].childTblCount;

            cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
            if (dataSource && dataSource->type == cJSON_String &&
                dataSource->valuestring != NULL) {
                tstrncpy(
                    db[i].superTbls[j].dataSource, dataSource->valuestring,
                    min(SMALL_BUFF_LEN, strlen(dataSource->valuestring) + 1));
            } else if (!dataSource) {
                tstrncpy(db[i].superTbls[j].dataSource, "rand",
                         min(SMALL_BUFF_LEN, strlen("rand") + 1));
            } else {
                errorPrint("%s",
                           "failed to read json, data_source not found\n");
                goto PARSE_OVER;
            }

            cJSON *stbIface = cJSON_GetObjectItem(
                stbInfo, "insert_mode");  // taosc , rest, stmt
            if (stbIface && stbIface->type == cJSON_String &&
                stbIface->valuestring != NULL) {
                if (0 == strcasecmp(stbIface->valuestring, "taosc")) {
                    db[i].superTbls[j].iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "rest")) {
                    db[i].superTbls[j].iface = REST_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
                    db[i].superTbls[j].iface = STMT_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "sml")) {
                    if (strcasecmp(db[i].superTbls[j].dataSource, "sample") ==
                        0) {
                        errorPrint("%s",
                                   "sml insert mode currently does not support "
                                   "sample as data source\n");
                        goto PARSE_OVER;
                    }
                    db[i].superTbls[j].iface = SML_IFACE;
                    g_args.iface = SML_IFACE;
                } else {
                    errorPrint(
                        "failed to read json, insert_mode %s not recognized\n",
                        stbIface->valuestring);
                    goto PARSE_OVER;
                }
            } else if (!stbIface) {
                db[i].superTbls[j].iface = TAOSC_IFACE;
            } else {
                errorPrint("%s",
                           "failed to read json, insert_mode not found\n");
                goto PARSE_OVER;
            }

            cJSON *stbLineProtocol =
                cJSON_GetObjectItem(stbInfo, "line_protocol");
            if (stbLineProtocol && stbLineProtocol->type == cJSON_String &&
                stbLineProtocol->valuestring != NULL) {
                if (0 == strcasecmp(stbLineProtocol->valuestring, "line")) {
                    db[i].superTbls[j].lineProtocol = TSDB_SML_LINE_PROTOCOL;
                } else if (0 ==
                           strcasecmp(stbLineProtocol->valuestring, "telnet")) {
                    db[i].superTbls[j].lineProtocol = TSDB_SML_TELNET_PROTOCOL;
                } else if (0 ==
                           strcasecmp(stbLineProtocol->valuestring, "json")) {
                    db[i].superTbls[j].lineProtocol = TSDB_SML_JSON_PROTOCOL;
                } else {
                    errorPrint(
                        "failed to read json, line_protocol %s not "
                        "recognized\n",
                        stbLineProtocol->valuestring);
                    goto PARSE_OVER;
                }
            } else if (!stbLineProtocol) {
                db[i].superTbls[j].lineProtocol = TSDB_SML_LINE_PROTOCOL;
            } else {
                errorPrint("%s",
                           "failed to read json, line_protocol not found\n");
                goto PARSE_OVER;
            }

            cJSON *childTbl_limit =
                cJSON_GetObjectItem(stbInfo, "childtable_limit");
            if ((childTbl_limit) && (db[i].drop != true) &&
                (db[i].superTbls[j].childTblExists == TBL_ALREADY_EXISTS)) {
                if (childTbl_limit->type != cJSON_Number) {
                    errorPrint("%s", "failed to read json, childtable_limit\n");
                    goto PARSE_OVER;
                }
                db[i].superTbls[j].childTblLimit = childTbl_limit->valueint;
            } else {
                db[i].superTbls[j].childTblLimit =
                    -1;  // select ... limit -1 means all query result, drop =
                         // yes mean all table need recreate, limit value is
                         // invalid.
            }

            cJSON *childTbl_offset =
                cJSON_GetObjectItem(stbInfo, "childtable_offset");
            if ((childTbl_offset) && (db[i].drop != true) &&
                (db[i].superTbls[j].childTblExists == TBL_ALREADY_EXISTS)) {
                if ((childTbl_offset->type != cJSON_Number) ||
                    (0 > childTbl_offset->valueint)) {
                    errorPrint("%s",
                               "failed to read json, childtable_offset\n");
                    goto PARSE_OVER;
                }
                db[i].superTbls[j].childTblOffset = childTbl_offset->valueint;
            } else {
                db[i].superTbls[j].childTblOffset = 0;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                tstrncpy(db[i].superTbls[j].startTimestamp, ts->valuestring,
                         TSDB_DB_NAME_LEN);
            } else if (!ts) {
                tstrncpy(db[i].superTbls[j].startTimestamp, "now",
                         TSDB_DB_NAME_LEN);
            } else {
                errorPrint("%s",
                           "failed to read json, start_timestamp not found\n");
                goto PARSE_OVER;
            }

            cJSON *timestampStep =
                cJSON_GetObjectItem(stbInfo, "timestamp_step");
            if (timestampStep && timestampStep->type == cJSON_Number) {
                db[i].superTbls[j].timeStampStep = timestampStep->valueint;
            } else if (!timestampStep) {
                db[i].superTbls[j].timeStampStep = g_args.timestamp_step;
            } else {
                errorPrint("%s",
                           "failed to read json, timestamp_step not found\n");
                goto PARSE_OVER;
            }

            cJSON *sampleFormat = cJSON_GetObjectItem(stbInfo, "sample_format");
            if (sampleFormat && sampleFormat->type == cJSON_String &&
                sampleFormat->valuestring != NULL) {
                tstrncpy(
                    db[i].superTbls[j].sampleFormat, sampleFormat->valuestring,
                    min(SMALL_BUFF_LEN, strlen(sampleFormat->valuestring) + 1));
            } else if (!sampleFormat) {
                tstrncpy(db[i].superTbls[j].sampleFormat, "csv",
                         SMALL_BUFF_LEN);
            } else {
                errorPrint("%s",
                           "failed to read json, sample_format not found\n");
                goto PARSE_OVER;
            }

            cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
            if (sampleFile && sampleFile->type == cJSON_String &&
                sampleFile->valuestring != NULL) {
                tstrncpy(db[i].superTbls[j].sampleFile, sampleFile->valuestring,
                         min(MAX_FILE_NAME_LEN,
                             strlen(sampleFile->valuestring) + 1));
            } else if (!sampleFile) {
                memset(db[i].superTbls[j].sampleFile, 0, MAX_FILE_NAME_LEN);
            } else {
                errorPrint("%s",
                           "failed to read json, sample_file not found\n");
                goto PARSE_OVER;
            }

            cJSON *useSampleTs = cJSON_GetObjectItem(stbInfo, "use_sample_ts");
            if (useSampleTs && useSampleTs->type == cJSON_String &&
                useSampleTs->valuestring != NULL) {
                if (0 == strncasecmp(useSampleTs->valuestring, "yes", 3)) {
                    db[i].superTbls[j].useSampleTs = true;
                } else if (0 ==
                           strncasecmp(useSampleTs->valuestring, "no", 2)) {
                    db[i].superTbls[j].useSampleTs = false;
                } else {
                    db[i].superTbls[j].useSampleTs = false;
                }
            } else if (!useSampleTs) {
                db[i].superTbls[j].useSampleTs = false;
            } else {
                errorPrint("%s",
                           "failed to read json, use_sample_ts not found\n");
                goto PARSE_OVER;
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String) &&
                (tagsFile->valuestring != NULL)) {
                tstrncpy(db[i].superTbls[j].tagsFile, tagsFile->valuestring,
                         MAX_FILE_NAME_LEN);
                if (0 == db[i].superTbls[j].tagsFile[0]) {
                    db[i].superTbls[j].tagSource = 0;
                } else {
                    db[i].superTbls[j].tagSource = 1;
                }
            } else if (!tagsFile) {
                memset(db[i].superTbls[j].tagsFile, 0, MAX_FILE_NAME_LEN);
                db[i].superTbls[j].tagSource = 0;
            } else {
                errorPrint("%s", "failed to read json, tags_file not found\n");
                goto PARSE_OVER;
            }

            cJSON *insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
            if (insertRows && insertRows->type == cJSON_Number) {
                if (insertRows->valueint < 0) {
                    errorPrint(
                        "%s",
                        "failed to read json, insert_rows input mistake\n");
                    goto PARSE_OVER;
                }
                db[i].superTbls[j].insertRows = insertRows->valueint;
            } else if (!insertRows) {
                db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
            } else {
                errorPrint("%s",
                           "failed to read json, insert_rows input mistake\n");
                goto PARSE_OVER;
            }

            cJSON *stbInterlaceRows =
                cJSON_GetObjectItem(stbInfo, "interlace_rows");
            if (stbInterlaceRows && stbInterlaceRows->type == cJSON_Number) {
                if (stbInterlaceRows->valueint < 0) {
                    errorPrint(
                        "%s",
                        "failed to read json, interlace rows input mistake\n");
                    goto PARSE_OVER;
                }
                db[i].superTbls[j].interlaceRows =
                    (uint32_t)stbInterlaceRows->valueint;

                if (db[i].superTbls[j].interlaceRows >
                    db[i].superTbls[j].insertRows) {
                    printf(
                        "NOTICE: db[%d].superTbl[%d]'s interlace rows value %u "
                        "> insert_rows %" PRId64 "\n\n",
                        i, j, db[i].superTbls[j].interlaceRows,
                        db[i].superTbls[j].insertRows);
                    printf(
                        "        interlace rows value will be set to "
                        "insert_rows %" PRId64 "\n\n",
                        db[i].superTbls[j].insertRows);
                    prompt();
                    db[i].superTbls[j].interlaceRows =
                        (uint32_t)db[i].superTbls[j].insertRows;
                }
            } else if (!stbInterlaceRows) {
                db[i].superTbls[j].interlaceRows =
                    g_args.interlaceRows;  // 0 means progressive mode, > 0 mean
                                           // interlace mode. max value is less
                                           // or equ num_of_records_per_req
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, interlace rows input mistake\n");
                goto PARSE_OVER;
            }

            cJSON *disorderRatio =
                cJSON_GetObjectItem(stbInfo, "disorder_ratio");
            if (disorderRatio && disorderRatio->type == cJSON_Number) {
                if (disorderRatio->valueint > 50) disorderRatio->valueint = 50;

                if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

                db[i].superTbls[j].disorderRatio = (int)disorderRatio->valueint;
            } else if (!disorderRatio) {
                db[i].superTbls[j].disorderRatio = 0;
            } else {
                errorPrint("%s",
                           "failed to read json, disorderRatio not found\n");
                goto PARSE_OVER;
            }

            cJSON *disorderRange =
                cJSON_GetObjectItem(stbInfo, "disorder_range");
            if (disorderRange && disorderRange->type == cJSON_Number) {
                db[i].superTbls[j].disorderRange = (int)disorderRange->valueint;
            } else if (!disorderRange) {
                db[i].superTbls[j].disorderRange = DEFAULT_DISORDER_RANGE;
            } else {
                errorPrint("%s",
                           "failed to read json, disorderRange not found\n");
                goto PARSE_OVER;
            }

            cJSON *insertInterval =
                cJSON_GetObjectItem(stbInfo, "insert_interval");
            if (insertInterval && insertInterval->type == cJSON_Number) {
                db[i].superTbls[j].insertInterval = insertInterval->valueint;
                if (insertInterval->valueint < 0) {
                    errorPrint(
                        "%s",
                        "failed to read json, insert_interval input mistake\n");
                    goto PARSE_OVER;
                }
            } else if (!insertInterval) {
                verbosePrint(
                    "%s() LN%d: stable insert interval be overrode by global "
                    "%" PRIu64 ".\n",
                    __func__, __LINE__, g_args.insert_interval);
                db[i].superTbls[j].insertInterval = g_args.insert_interval;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, insert_interval input mistake\n");
                goto PARSE_OVER;
            }

            if (getColumnAndTagTypeFromInsertJsonFile(stbInfo,
                                                      &db[i].superTbls[j])) {
                goto PARSE_OVER;
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}
int getMetaFromQueryJsonFile(cJSON *json) {
    int32_t code = -1;

    cJSON *cfgdir = cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(g_queryInfo.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON *host = cJSON_GetObjectItem(json, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_queryInfo.host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else if (!host) {
        tstrncpy(g_queryInfo.host, DEFAULT_HOST, MAX_HOSTNAME_SIZE);
    } else {
        errorPrint("%s", "failed to read json, host not found\n");
        goto PARSE_OVER;
    }

    cJSON *port = cJSON_GetObjectItem(json, "port");
    if (port && port->type == cJSON_Number) {
        g_queryInfo.port = (uint16_t)port->valueint;
    } else if (!port) {
        g_queryInfo.port = DEFAULT_PORT;
    }

    cJSON *user = cJSON_GetObjectItem(json, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_queryInfo.user, user->valuestring, MAX_USERNAME_SIZE);
    } else if (!user) {
        tstrncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
        ;
    }

    cJSON *password = cJSON_GetObjectItem(json, "password");
    if (password && password->type == cJSON_String &&
        password->valuestring != NULL) {
        tstrncpy(g_queryInfo.password, password->valuestring,
                 SHELL_MAX_PASSWORD_LEN);
    } else if (!password) {
        tstrncpy(g_queryInfo.password, TSDB_DEFAULT_PASS,
                 SHELL_MAX_PASSWORD_LEN);
        ;
    }

    cJSON *answerPrompt =
        cJSON_GetObjectItem(json, "confirm_parameter_prompt");  // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String &&
        answerPrompt->valuestring != NULL) {
        if (0 == strncasecmp(answerPrompt->valuestring, "yes", 3)) {
            g_args.answer_yes = false;
        } else if (0 == strncasecmp(answerPrompt->valuestring, "no", 2)) {
            g_args.answer_yes = true;
        } else {
            g_args.answer_yes = false;
        }
    } else if (!answerPrompt) {
        g_args.answer_yes = false;
    } else {
        errorPrint("%s",
                   "failed to read json, confirm_parameter_prompt not found\n");
        goto PARSE_OVER;
    }

    cJSON *gQueryTimes = cJSON_GetObjectItem(json, "query_times");
    if (gQueryTimes && gQueryTimes->type == cJSON_Number) {
        if (gQueryTimes->valueint <= 0) {
            errorPrint("%s",
                       "failed to read json, query_times input mistake\n");
            goto PARSE_OVER;
        }
        g_args.query_times = gQueryTimes->valueint;
    } else if (!gQueryTimes) {
        g_args.query_times = DEFAULT_QUERY_TIME;
    } else {
        errorPrint("%s", "failed to read json, query_times input mistake\n");
        goto PARSE_OVER;
    }

    cJSON *dbs = cJSON_GetObjectItem(json, "databases");
    if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
        tstrncpy(g_queryInfo.dbName, dbs->valuestring, TSDB_DB_NAME_LEN);
    } else if (!dbs) {
        errorPrint("%s", "failed to read json, databases not found\n");
        goto PARSE_OVER;
    }

    cJSON *queryMode = cJSON_GetObjectItem(json, "query_mode");
    if (queryMode && queryMode->type == cJSON_String &&
        queryMode->valuestring != NULL) {
        tstrncpy(g_queryInfo.queryMode, queryMode->valuestring,
                 min(SMALL_BUFF_LEN, strlen(queryMode->valuestring) + 1));
    } else if (!queryMode) {
        tstrncpy(g_queryInfo.queryMode, "taosc",
                 min(SMALL_BUFF_LEN, strlen("taosc") + 1));
    } else {
        errorPrint("%s", "failed to read json, query_mode not found\n");
        goto PARSE_OVER;
    }

    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(json, "specified_table_query");
    if (!specifiedQuery) {
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.sqlCount = 0;
    } else if (specifiedQuery->type != cJSON_Object) {
        errorPrint("%s", "failed to read json, super_table_query not found\n");
        goto PARSE_OVER;
    } else {
        cJSON *queryInterval =
            cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (queryInterval && queryInterval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.queryInterval =
                queryInterval->valueint;
        } else if (!queryInterval) {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        cJSON *specifiedQueryTimes =
            cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (specifiedQueryTimes && specifiedQueryTimes->type == cJSON_Number) {
            if (specifiedQueryTimes->valueint <= 0) {
                errorPrint("failed to read json, query_times: %" PRId64
                           ", need be a valid (>0) number\n",
                           specifiedQueryTimes->valueint);
                goto PARSE_OVER;
            }
            g_queryInfo.specifiedQueryInfo.queryTimes =
                specifiedQueryTimes->valueint;
        } else if (!specifiedQueryTimes) {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_args.query_times;
        } else {
            errorPrint(
                "%s() LN%d, failed to read json, query_times input mistake\n",
                __func__, __LINE__);
            goto PARSE_OVER;
        }

        cJSON *concurrent = cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (concurrent && concurrent->type == cJSON_Number) {
            if (concurrent->valueint <= 0) {
                errorPrint(
                    "query sqlCount %d or concurrent %d is not correct.\n",
                    g_queryInfo.specifiedQueryInfo.sqlCount,
                    g_queryInfo.specifiedQueryInfo.concurrent);
                goto PARSE_OVER;
            }
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)concurrent->valueint;
        } else if (!concurrent) {
            g_queryInfo.specifiedQueryInfo.concurrent = 1;
        }

        cJSON *specifiedAsyncMode = cJSON_GetObjectItem(specifiedQuery, "mode");
        if (specifiedAsyncMode && specifiedAsyncMode->type == cJSON_String &&
            specifiedAsyncMode->valuestring != NULL) {
            if (0 == strcmp("sync", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            } else if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                errorPrint("%s",
                           "failed to read json, async mode input error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *interval = cJSON_GetObjectItem(specifiedQuery, "interval");
        if (interval && interval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                interval->valueint;
        } else if (!interval) {
            // printf("failed to read json, subscribe interval no found\n");
            // goto PARSE_OVER;
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                DEFAULT_SUB_INTERVAL;
        }

        cJSON *restart = cJSON_GetObjectItem(specifiedQuery, "restart");
        if (restart && restart->type == cJSON_String &&
            restart->valuestring != NULL) {
            if (0 == strcmp("yes", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
            } else if (0 == strcmp("no", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = false;
            } else {
                errorPrint("%s",
                           "failed to read json, subscribe restart error\n");
                goto PARSE_OVER;
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
            } else if (0 == strcmp("no", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            } else {
                errorPrint(
                    "%s",
                    "failed to read json, subscribe keepProgress error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // sqls
        cJSON *specifiedSqls = cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (!specifiedSqls) {
            g_queryInfo.specifiedQueryInfo.sqlCount = 0;
        } else if (specifiedSqls->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super sqls not found\n");
            goto PARSE_OVER;
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
                if (!sqlStr || sqlStr->type != cJSON_String ||
                    sqlStr->valuestring == NULL) {
                    errorPrint("%s", "failed to read json, sql not found\n");
                    goto PARSE_OVER;
                }
                tstrncpy(g_queryInfo.specifiedQueryInfo.sql[j],
                         sqlStr->valuestring, BUFFER_SIZE);

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
                } else if (NULL == result) {
                    memset(g_queryInfo.specifiedQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                } else {
                    errorPrint("%s",
                               "failed to read json, super query result file "
                               "not found\n");
                    goto PARSE_OVER;
                }
            }
        }
    }

    // super_table_query
    cJSON *superQuery = cJSON_GetObjectItem(json, "super_table_query");
    if (!superQuery) {
        g_queryInfo.superQueryInfo.threadCnt = 1;
        g_queryInfo.superQueryInfo.sqlCount = 0;
    } else if (superQuery->type != cJSON_Object) {
        errorPrint("%s", "failed to read json, sub_table_query not found\n");
        code = 0;
        goto PARSE_OVER;
    } else {
        cJSON *subrate = cJSON_GetObjectItem(superQuery, "query_interval");
        if (subrate && subrate->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.queryInterval = subrate->valueint;
        } else if (!subrate) {
            g_queryInfo.superQueryInfo.queryInterval = 0;
        }

        cJSON *superQueryTimes = cJSON_GetObjectItem(superQuery, "query_times");
        if (superQueryTimes && superQueryTimes->type == cJSON_Number) {
            if (superQueryTimes->valueint <= 0) {
                errorPrint("failed to read json, query_times: %" PRId64
                           ", need be a valid (>0) number\n",
                           superQueryTimes->valueint);
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
        } else if (!superQueryTimes) {
            g_queryInfo.superQueryInfo.queryTimes = g_args.query_times;
        } else {
            errorPrint("%s",
                       "failed to read json, query_times input mistake\n");
            goto PARSE_OVER;
        }

        cJSON *threads = cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == cJSON_Number) {
            if (threads->valueint <= 0) {
                errorPrint("%s",
                           "failed to read json, threads input mistake\n");
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.threadCnt = (uint32_t)threads->valueint;
        } else if (!threads) {
            g_queryInfo.superQueryInfo.threadCnt = DEFAULT_NTHREADS;
        }

        // cJSON* subTblCnt = cJSON_GetObjectItem(superQuery,
        // "childtable_count"); if (subTblCnt && subTblCnt->type ==
        // cJSON_Number)
        // {
        //  g_queryInfo.superQueryInfo.childTblCount = subTblCnt->valueint;
        //} else if (!subTblCnt) {
        //  g_queryInfo.superQueryInfo.childTblCount = 0;
        //}

        cJSON *stblname = cJSON_GetObjectItem(superQuery, "stblname");
        if (stblname && stblname->type == cJSON_String &&
            stblname->valuestring != NULL) {
            tstrncpy(g_queryInfo.superQueryInfo.stbName, stblname->valuestring,
                     TSDB_TABLE_NAME_LEN);
        } else {
            errorPrint("%s",
                       "failed to read json, super table name input error\n");
            goto PARSE_OVER;
        }

        cJSON *superAsyncMode = cJSON_GetObjectItem(superQuery, "mode");
        if (superAsyncMode && superAsyncMode->type == cJSON_String &&
            superAsyncMode->valuestring != NULL) {
            if (0 == strcmp("sync", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            } else if (0 == strcmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                errorPrint("%s",
                           "failed to read json, async mode input error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON *superInterval = cJSON_GetObjectItem(superQuery, "interval");
        if (superInterval && superInterval->type == cJSON_Number) {
            if (superInterval->valueint < 0) {
                errorPrint("%s",
                           "failed to read json, interval input mistake\n");
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.subscribeInterval =
                superInterval->valueint;
        } else if (!superInterval) {
            // printf("failed to read json, subscribe interval no found\n");
            // goto PARSE_OVER;
            g_queryInfo.superQueryInfo.subscribeInterval =
                DEFAULT_QUERY_INTERVAL;
        }

        cJSON *subrestart = cJSON_GetObjectItem(superQuery, "restart");
        if (subrestart && subrestart->type == cJSON_String &&
            subrestart->valuestring != NULL) {
            if (0 == strcmp("yes", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = true;
            } else if (0 == strcmp("no", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = false;
            } else {
                errorPrint("%s",
                           "failed to read json, subscribe restart error\n");
                goto PARSE_OVER;
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
            } else if (0 == strcmp("no", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
            } else {
                errorPrint("%s",
                           "failed to read json, subscribe super table "
                           "keepProgress error\n");
                goto PARSE_OVER;
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
        if (!superSqls) {
            g_queryInfo.superQueryInfo.sqlCount = 0;
        } else if (superSqls->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super sqls not found\n");
            goto PARSE_OVER;
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
                if (!sqlStr || sqlStr->type != cJSON_String ||
                    sqlStr->valuestring == NULL) {
                    errorPrint("%s", "failed to read json, sql not found\n");
                    goto PARSE_OVER;
                }
                tstrncpy(g_queryInfo.superQueryInfo.sql[j], sqlStr->valuestring,
                         BUFFER_SIZE);

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if (result != NULL && result->type == cJSON_String &&
                    result->valuestring != NULL) {
                    tstrncpy(g_queryInfo.superQueryInfo.result[j],
                             result->valuestring, MAX_FILE_NAME_LEN);
                } else if (NULL == result) {
                    memset(g_queryInfo.superQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                } else {
                    errorPrint("%s",
                               "failed to read json, sub query result file not "
                               "found\n");
                    goto PARSE_OVER;
                }
            }
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}

int getInfoFromJsonFile(char *file) {
    debugPrint("%s %d %s\n", __func__, __LINE__, file);
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
        free(content);
        fclose(fp);
        errorPrint("failed to read %s, content is null", file);
        return code;
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
            g_args.test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            g_args.test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            g_args.test_mode = SUBSCRIBE_TEST;
        } else {
            errorPrint("%s", "failed to read json, filetype not support\n");
            goto PARSE_OVER;
        }
    } else if (!filetype) {
        g_args.test_mode = INSERT_TEST;
    } else {
        errorPrint("%s", "failed to read json, filetype not found\n");
        goto PARSE_OVER;
    }

    if (INSERT_TEST == g_args.test_mode) {
        g_args.use_metric = g_args.use_metric;
        code = getMetaFromInsertJsonFile(root);
    } else if ((QUERY_TEST == g_args.test_mode) ||
               (SUBSCRIBE_TEST == g_args.test_mode)) {
        memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
        code = getMetaFromQueryJsonFile(root);
    } else {
        errorPrint("%s",
                   "input json file type error! please input correct file "
                   "type: insert or query or subscribe\n");
        goto PARSE_OVER;
    }
PARSE_OVER:
    free(content);
    fclose(fp);
    return code;
}
