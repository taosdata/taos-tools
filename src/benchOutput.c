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

#define SHOW_PARSE_RESULT_START_TO_FILE(fp, argument)                     \
    do {                                                                  \
        if (argument->metaFile)                                           \
            fprintf(fp,                                                   \
                    "\033[1m\033[40;32m================ %s parse result " \
                    "START ================\033[0m\n",                    \
                    argument->metaFile);                                  \
    } while (0)

#define SHOW_PARSE_RESULT_END_TO_FILE(fp, argument)                       \
    do {                                                                  \
        if (argument->metaFile)                                           \
            fprintf(fp,                                                   \
                    "\033[1m\033[40;32m================ %s parse result " \
                    "END================\033[0m\n",                       \
                    argument->metaFile);                                  \
    } while (0)

int getDbFromServer(TAOS *taos, SDbInfo **dbInfos) {
    TAOS_RES *res;
    TAOS_ROW  row = NULL;
    int       count = 0;

    res = taos_query(taos, "show databases;");
    int32_t code = taos_errno(res);

    if (code != 0) {
        errorPrint("failed to run <show databases>, reason: %s\n",
                   taos_errstr(res));
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    while ((row = taos_fetch_row(res)) != NULL) {
        // sys database name : 'log'
        if (strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log",
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0) {
            continue;
        }

        dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));

        tstrncpy(dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                 fields[TSDB_SHOW_DB_NAME_INDEX].bytes);
        formatTimestamp(dbInfos[count]->create_time,
                        *(int64_t *)row[TSDB_SHOW_DB_CREATED_TIME_INDEX],
                        TSDB_TIME_PRECISION_MILLI);
        dbInfos[count]->ntables = *((int64_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
        dbInfos[count]->vgroups = *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);
        dbInfos[count]->replica = *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
        dbInfos[count]->quorum = *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
        dbInfos[count]->days = *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);

        tstrncpy(dbInfos[count]->keeplist, (char *)row[TSDB_SHOW_DB_KEEP_INDEX],
                 fields[TSDB_SHOW_DB_KEEP_INDEX].bytes);
        dbInfos[count]->cache = *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
        dbInfos[count]->blocks = *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
        dbInfos[count]->minrows = *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
        dbInfos[count]->maxrows = *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
        dbInfos[count]->wallevel =
            *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
        dbInfos[count]->fsync = *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
        dbInfos[count]->comp =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
        dbInfos[count]->cachelast =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_CACHELAST_INDEX]));

        tstrncpy(dbInfos[count]->precision,
                 (char *)row[TSDB_SHOW_DB_PRECISION_INDEX],
                 fields[TSDB_SHOW_DB_PRECISION_INDEX].bytes);
        dbInfos[count]->update = *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
        tstrncpy(dbInfos[count]->status, (char *)row[TSDB_SHOW_DB_STATUS_INDEX],
                 fields[TSDB_SHOW_DB_STATUS_INDEX].bytes);

        count++;
        if (count > MAX_DATABASE_COUNT) {
            errorPrint("The database count overflow than %d\n",
                       MAX_DATABASE_COUNT);
            break;
        }
    }

    return count;
}

void xDumpFieldToFile(FILE *fp, const char *val, TAOS_FIELD *field,
                      int32_t length, int precision) {
    if (val == NULL) {
        fprintf(fp, "%s", TSDB_DATA_NULL_STR);
        return;
    }

    char buf[TSDB_MAX_BYTES_PER_ROW];
    switch (field->type) {
        case TSDB_DATA_TYPE_BOOL:
            fprintf(fp, "%d", ((((int32_t)(*((int8_t *)val))) == 1) ? 1 : 0));
            break;

        case TSDB_DATA_TYPE_TINYINT:
            fprintf(fp, "%d", *((int8_t *)val));
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            fprintf(fp, "%d", *((uint8_t *)val));
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            fprintf(fp, "%d", *((int16_t *)val));
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            fprintf(fp, "%d", *((uint16_t *)val));
            break;

        case TSDB_DATA_TYPE_INT:
            fprintf(fp, "%d", *((int32_t *)val));
            break;

        case TSDB_DATA_TYPE_UINT:
            fprintf(fp, "%d", *((uint32_t *)val));
            break;

        case TSDB_DATA_TYPE_BIGINT:
            fprintf(fp, "%" PRId64 "", *((int64_t *)val));
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            fprintf(fp, "%" PRId64 "", *((uint64_t *)val));
            break;

        case TSDB_DATA_TYPE_FLOAT:
            fprintf(fp, "%.5f", GET_FLOAT_VAL(val));
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            fprintf(fp, "%.9f", GET_DOUBLE_VAL(val));
            break;

        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
            memcpy(buf, val, length);
            buf[length] = 0;
            fprintf(fp, "\'%s\'", buf);
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(buf, *(int64_t *)val, precision);
            fprintf(fp, "'%s'", buf);
            break;

        default:
            break;
    }
}

int xDumpResultToFile(const char *fname, TAOS_RES *tres) {
    TAOS_ROW row = taos_fetch_row(tres);
    if (row == NULL) {
        return 0;
    }

    FILE *fp = fopen(fname, "at");
    if (fp == NULL) {
        errorPrint("failed to open file: %s\n", fname);
        return -1;
    }

    int         num_fields = taos_num_fields(tres);
    TAOS_FIELD *fields = taos_fetch_fields(tres);
    int         precision = taos_result_precision(tres);

    for (int col = 0; col < num_fields; col++) {
        if (col > 0) {
            fprintf(fp, ",");
        }
        fprintf(fp, "%s", fields[col].name);
    }
    fputc('\n', fp);

    int numOfRows = 0;
    do {
        int32_t *length = taos_fetch_lengths(tres);
        for (int i = 0; i < num_fields; i++) {
            if (i > 0) {
                fputc(',', fp);
            }
            xDumpFieldToFile(fp, (const char *)row[i], fields + i, length[i],
                             precision);
        }
        fputc('\n', fp);

        numOfRows++;
        row = taos_fetch_row(tres);
    } while (row != NULL);

    fclose(fp);

    return numOfRows;
}

void printfInsertMetaToFileStream(FILE *fp, SArguments *arguments,
                                  SDataBase *database) {
    setupForAnsiEscape();
    SHOW_PARSE_RESULT_START_TO_FILE(fp, arguments);

    if (arguments->demo_mode) {
        fprintf(
            fp,
            "\ntaosBenchmark is simulating data generated by power equipment "
            "monitoring...\n\n");
    } else {
        fprintf(
            fp,
            "\ntaosBenchmark is simulating random data as you request..\n\n");
    }

    fprintf(fp, "host:                       \033[33m%s:%u\033[0m\n",
            arguments->host, arguments->port);
    fprintf(fp, "user:                       \033[33m%s\033[0m\n",
            arguments->user);
    fprintf(fp, "password:                   \033[33m%s\033[0m\n",
            arguments->password);
    fprintf(fp, "configDir:                  \033[33m%s\033[0m\n", configDir);
    fprintf(fp, "resultFile:                 \033[33m%s\033[0m\n",
            arguments->output_file);
    fprintf(fp, "thread num of insert data:  \033[33m%d\033[0m\n",
            arguments->nthreads);
    fprintf(fp, "thread num of create table: \033[33m%d\033[0m\n",
            arguments->nthreads);
    fprintf(fp, "number of records per req:  \033[33m%u\033[0m\n",
            arguments->reqPerReq);
    fprintf(fp, "random prepare data size:   \033[33m%" PRId64 "\033[0m\n",
            arguments->prepared_rand);
    fprintf(fp, "chinese:                    \033[33m%s\033[0m\n",
            arguments->chinese ? "yes" : "no");

    fprintf(fp, "database count:             \033[33m%d\033[0m\n",
            arguments->dbCount);

    for (int i = 0; i < arguments->dbCount; i++) {
        fprintf(fp, "database[\033[33m%d\033[0m]:\n", i);
        fprintf(fp, "  database[%d] name:      \033[33m%s\033[0m\n", i,
                database[i].dbName);
        if (0 == database[i].drop) {
            fprintf(fp, "  drop:                 \033[33m no\033[0m\n");
        } else {
            fprintf(fp, "  drop:                 \033[33m yes\033[0m\n");
        }

        if (database[i].dbCfg.blocks > 0) {
            fprintf(fp, "  blocks:                \033[33m%d\033[0m\n",
                    database[i].dbCfg.blocks);
        }
        if (database[i].dbCfg.cache > 0) {
            fprintf(fp, "  cache:                 \033[33m%d\033[0m\n",
                    database[i].dbCfg.cache);
        }
        if (database[i].dbCfg.days > 0) {
            fprintf(fp, "  days:                  \033[33m%d\033[0m\n",
                    database[i].dbCfg.days);
        }
        if (database[i].dbCfg.keep > 0) {
            fprintf(fp, "  keep:                  \033[33m%d\033[0m\n",
                    database[i].dbCfg.keep);
        }
        if (database[i].dbCfg.replica > 0) {
            fprintf(fp, "  replica:               \033[33m%d\033[0m\n",
                    database[i].dbCfg.replica);
        }
        if (database[i].dbCfg.update > 0) {
            fprintf(fp, "  update:                \033[33m%d\033[0m\n",
                    database[i].dbCfg.update);
        }
        if (database[i].dbCfg.minRows > 0) {
            fprintf(fp, "  minRows:               \033[33m%d\033[0m\n",
                    database[i].dbCfg.minRows);
        }
        if (database[i].dbCfg.maxRows > 0) {
            fprintf(fp, "  maxRows:               \033[33m%d\033[0m\n",
                    database[i].dbCfg.maxRows);
        }
        if (database[i].dbCfg.comp > 0) {
            fprintf(fp, "  comp:                  \033[33m%d\033[0m\n",
                    database[i].dbCfg.comp);
        }
        if (database[i].dbCfg.walLevel > 0) {
            fprintf(fp, "  walLevel:              \033[33m%d\033[0m\n",
                    database[i].dbCfg.walLevel);
        }
        if (database[i].dbCfg.fsync > 0) {
            fprintf(fp, "  fsync:                 \033[33m%d\033[0m\n",
                    database[i].dbCfg.fsync);
        }
        if (database[i].dbCfg.quorum > 0) {
            fprintf(fp, "  quorum:                \033[33m%d\033[0m\n",
                    database[i].dbCfg.quorum);
        }
        fprintf(fp, "  precision:             \033[33m%s\033[0m\n",
                database[i].dbCfg.precision == TSDB_TIME_PRECISION_MILLI
                    ? "ms"
                    : database[i].dbCfg.precision == TSDB_TIME_PRECISION_MICRO
                          ? "us"
                          : "ns");

        fprintf(fp, "  super table count:     \033[33m%" PRIu64 "\033[0m\n",
                database[i].superTblCount);
        for (uint64_t j = 0; j < database[i].superTblCount; j++) {
            fprintf(fp, "  super table[\033[33m%" PRIu64 "\033[0m]:\n", j);

            fprintf(fp, "      stbName:           \033[33m%s\033[0m\n",
                    database[i].superTbls[j].stbName);

            fprintf(fp, "      interface:         \033[33m%s\033[0m\n",
                    (database[i].superTbls[j].iface == TAOSC_IFACE)
                        ? "taosc"
                        : (database[i].superTbls[j].iface == REST_IFACE)
                              ? "rest"
                              : (database[i].superTbls[j].iface == STMT_IFACE)
                                    ? "stmt"
                                    : "sml");

            if (database[i].superTbls[j].autoCreateTable) {
                fprintf(fp, "      autoCreateTable:   \033[33m%s\033[0m\n",
                        "yes");
            } else {
                fprintf(fp, "      autoCreateTable:   \033[33m%s\033[0m\n",
                        "no");
            }

            if (database[i].superTbls[j].childTblExists) {
                fprintf(fp, "      childTblExists:    \033[33m%s\033[0m\n",
                        "yes");
            } else {
                fprintf(fp, "      childTblExists:    \033[33m%s\033[0m\n",
                        "no");
            }

            fprintf(fp, "      childTblCount:     \033[33m%" PRId64 "\033[0m\n",
                    database[i].superTbls[j].childTblCount);
            fprintf(fp, "      childTblLimit:     \033[33m%" PRIu64 "\033[0m\n",
                    database[i].superTbls[j].childTblLimit);
            fprintf(fp, "      childTblOffset:    \033[33m%" PRIu64 "\033[0m\n",
                    database[i].superTbls[j].childTblOffset);
            fprintf(fp, "      childTblPrefix:    \033[33m%s\033[0m\n",
                    database[i].superTbls[j].childTblPrefix);
            fprintf(fp, "      escapeCharacter:   \033[33m%s\033[0m\n",
                    database[i].superTbls[j].escape_character ? "yes" : "no");
            if (database[i].superTbls[j].random_data_source) {
                fprintf(fp, "      dataSource:        \033[33m%s\033[0m\n",
                        "random");
            } else {
                fprintf(fp, "      dataSource:        \033[33m%s\033[0m\n",
                        "sample");
            }

            if (database[i].superTbls[j].iface == SML_IFACE) {
                fprintf(fp, "      lineProtocol:      \033[33m%s\033[0m\n",
                        (database[i].superTbls[j].lineProtocol ==
                         TSDB_SML_LINE_PROTOCOL)
                            ? "line"
                            : (database[i].superTbls[j].lineProtocol ==
                               TSDB_SML_TELNET_PROTOCOL)
                                  ? "telnet"
                                  : "json");
            }
            if (database[i].superTbls[j].childTblOffset > 0) {
                fprintf(fp,
                        "      childTblOffset:    \033[33m%" PRIu64 "\033[0m\n",
                        database[i].superTbls[j].childTblOffset);
            }
            fprintf(fp, "      insertRows:        \033[33m%" PRId64 "\033[0m\n",
                    database[i].superTbls[j].insertRows);
            fprintf(fp, "      interlaceRows:     \033[33m%u\033[0m\n",
                    database[i].superTbls[j].interlaceRows);

            if (database[i].superTbls[j].interlaceRows > 0) {
                fprintf(fp,
                        "      stable insert interval:   \033[33m%" PRIu64
                        "\033[0m\n",
                        database[i].superTbls[j].insert_interval);
            }

            fprintf(fp, "      disorderRange:     \033[33m%d\033[0m\n",
                    database[i].superTbls[j].disorderRange);
            fprintf(fp, "      disorderRatio:     \033[33m%d\033[0m\n",
                    database[i].superTbls[j].disorderRatio);
            fprintf(fp, "      timeStampStep:     \033[33m%" PRId64 "\033[0m\n",
                    database[i].superTbls[j].timestamp_step);
            fprintf(fp, "      startTimestamp:    \033[33m%" PRIu64 "\033[0m\n",
                    database[i].superTbls[j].startTimestamp);
            fprintf(fp, "      sampleFile:        \033[33m%s\033[0m\n",
                    database[i].superTbls[j].sampleFile);
            fprintf(fp, "      useSampleTs:       \033[33m%s\033[0m\n",
                    database[i].superTbls[j].useSampleTs
                        ? "yes (warning: disorderRange/disorderRatio is "
                          "disabled)"
                        : "no");
            fprintf(fp, "      tagsFile:          \033[33m%s\033[0m\n",
                    database[i].superTbls[j].tagsFile);
            fprintf(fp, "      columnCount:       \033[33m%d\033[0m\n        ",
                    database[i].superTbls[j].columnCount);
            for (int k = 0; k < database[i].superTbls[j].columnCount; k++) {
                if ((database[i].superTbls[j].col_type[k] ==
                     TSDB_DATA_TYPE_NCHAR) ||
                    (database[i].superTbls[j].col_type[k] ==
                     TSDB_DATA_TYPE_BINARY)) {
                    fprintf(fp, "column[%d]:\033[33m%s(%d)\033[0m ", k,
                            taos_convert_datatype_to_string(
                                database[i].superTbls[j].col_type[k]),
                            database[i].superTbls[j].col_length[k]);
                } else {
                    fprintf(fp, "column[%d]:\033[33m%s\033[0m ", k,
                            taos_convert_datatype_to_string(
                                database[i].superTbls[j].col_type[k]));
                }
            }
            fprintf(fp, "\n");

            fprintf(fp,
                    "      tagCount:            \033[33m%d\033[0m\n        ",
                    database[i].superTbls[j].tagCount);
            for (int k = 0; k < database[i].superTbls[j].tagCount; k++) {
                if ((database[i].superTbls[j].tag_type[k] ==
                     TSDB_DATA_TYPE_BINARY) ||
                    (database[i].superTbls[j].tag_type[k] ==
                     TSDB_DATA_TYPE_NCHAR)) {
                    fprintf(fp, "tag[%d]:\033[33m%s(%d)\033[0m ", k,
                            taos_convert_datatype_to_string(
                                database[i].superTbls[j].tag_type[k]),
                            database[i].superTbls[j].tag_length[k]);
                } else if (database[i].superTbls[j].tag_type[k] ==
                           TSDB_DATA_TYPE_JSON) {
                    fprintf(fp,
                            "tag[%d]:\033[33mjson{key(%d):value(%d)}\033[0m ",
                            k, database[i].superTbls[j].tagCount,
                            database[i].superTbls[j].tag_length[k]);
                    break;
                } else {
                    fprintf(fp, "tag[%d]:\033[33m%s\033[0m ", k,
                            taos_convert_datatype_to_string(
                                database[i].superTbls[j].tag_type[k]));
                }
            }
            fprintf(fp, "\n");
        }

        fprintf(fp, "\n");
    }

    SHOW_PARSE_RESULT_END_TO_FILE(fp, arguments);
}

void printfQueryMeta(SArguments *argument) {
    setupForAnsiEscape();
    SHOW_PARSE_RESULT_START_TO_FILE(stdout, argument);

    printf("host:                           \033[33m%s:%u\033[0m\n",
           g_queryInfo.host, g_queryInfo.port);
    printf("user:                           \033[33m%s\033[0m\n",
           g_queryInfo.user);
    printf("database name:                  \033[33m%s\033[0m\n",
           g_queryInfo.dbName);
    printf("query Mode:                     \033[33m%s\033[0m\n",
           g_queryInfo.queryMode);
    printf("response buffer for restful:    \033[33m%" PRIu64 "\033[0m\n",
           g_queryInfo.response_buffer);
    printf("\n");

    if ((SUBSCRIBE_TEST == argument->test_mode) ||
        (QUERY_TEST == argument->test_mode)) {
        printf("specified table query info:                   \n");
        printf("    sqlCount:       \033[33m%d\033[0m\n",
               g_queryInfo.specifiedQueryInfo.sqlCount);
        if (g_queryInfo.specifiedQueryInfo.sqlCount > 0) {
            printf("    specified tbl query times: \033[33m%" PRIu64
                   "\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.queryTimes);
            printf("    query interval: \033[33m%" PRIu64 " ms\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.queryInterval);
            printf("    concurrent:     \033[33m%d\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.concurrent);
            printf("    interval:       \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.subscribeInterval);
            printf("    restart:        \033[33m%d\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.subscribeRestart);
            printf("    keepProgress:   \033[33m%d\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.subscribeKeepProgress);

            for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
                printf("    sql[%d]:    \033[33m%s\033[0m\n", i,
                       g_queryInfo.specifiedQueryInfo.sql[i]);
                printf("    result[%d]: \033[33m%s\033[0m\n", i,
                       g_queryInfo.specifiedQueryInfo.result[i]);
            }
            printf("\n");
        }

        printf("super table query info:\n");
        printf("    sqlCount:       \033[33m%d\033[0m\n",
               g_queryInfo.superQueryInfo.sqlCount);

        if (g_queryInfo.superQueryInfo.sqlCount > 0) {
            printf("    query interval: \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.queryInterval);
            printf("    threadCnt:      \033[33m%d\033[0m\n",
                   g_queryInfo.superQueryInfo.threadCnt);
            printf("    childTblCount:  \033[33m%" PRId64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.childTblCount);
            printf("    stable name:    \033[33m%s\033[0m\n",
                   g_queryInfo.superQueryInfo.stbName);
            printf("    stb query times:\033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.queryTimes);
            printf("    interval:       \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.subscribeInterval);
            printf("    restart:        \033[33m%d\033[0m\n",
                   g_queryInfo.superQueryInfo.subscribeRestart);
            printf("    keepProgress:   \033[33m%d\033[0m\n",
                   g_queryInfo.superQueryInfo.subscribeKeepProgress);

            for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                printf("    sql[%d]:    \033[33m%s\033[0m\n", i,
                       g_queryInfo.superQueryInfo.sql[i]);
                printf("    result[%d]: \033[33m%s\033[0m\n", i,
                       g_queryInfo.superQueryInfo.result[i]);
            }
            printf("\n");
        }
    }

    SHOW_PARSE_RESULT_END_TO_FILE(stdout, argument);
}

void printStatPerThread(threadInfo *pThreadInfo) {
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;

    infoPrint("thread[%d] completed total inserted rows: %" PRIu64
              ", total affected rows: %" PRIu64 ". %.2f records/second\n",
              pThreadInfo->threadID, pThreadInfo->totalInsertRows,
              pThreadInfo->totalAffectedRows,
              (double)(pThreadInfo->totalAffectedRows /
                       ((double)pThreadInfo->totalDelay / 1000000.0)));
}
