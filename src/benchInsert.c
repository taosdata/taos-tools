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
#include "benchData.h"

static int getSuperTableFromServer(int db_index, int stb_index) {
    char         command[SQL_BUFF_LEN] = "\0";
    TAOS_RES *   res;
    TAOS_ROW     row = NULL;
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    TAOS *       taos = select_one_from_pool(NULL);
    snprintf(command, SQL_BUFF_LEN, "describe %s.`%s`", database->dbName,
             stbInfo->stbName);
    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        debugPrint("failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        infoPrint("stable %s does not exist\n", stbInfo->stbName);
        taos_free_result(res);
        return -1;
    }
    int tagIndex = 0;
    int columnIndex = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (strcmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "TAG") == 0) {
            tagIndex++;
        } else {
            columnIndex++;
        }
    }
    int32_t *tmp_length_list = calloc(columnIndex, sizeof(int32_t));
    columnIndex -= 1;
    if (columnIndex == stbInfo->columnCount) {
        for (int i = 0; i < stbInfo->columnCount; ++i) {
            if (stbInfo->columns[i].length == 0) {
                tmp_length_list[i] = 0;
            } else {
                tmp_length_list[i] = 1;
            }
        }
    } else {
        for (int i = 0; i < columnIndex; ++i) {
            tmp_length_list[i] = 1;
        }
    }
    for (int i = 0; i < stbInfo->tagCount; ++i) {
        tmfree(stbInfo->tags[i].name);
    }
    for (int i = 0; i < stbInfo->columnCount; ++i) {
        tmfree(stbInfo->columns[i].name);
    }
    stbInfo->tagCount = tagIndex;
    tmfree(stbInfo->tags);
    stbInfo->tags = calloc(tagIndex, sizeof(Column));
    tmfree(stbInfo->columns);
    stbInfo->columnCount = columnIndex;
    stbInfo->columns = calloc(columnIndex, sizeof(Column));
    taos_free_result(res);
    res = taos_query(taos, command);
    code = taos_errno(res);
    if (code != 0) {
        errorPrint("failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        taos_free_result(res);
        return -1;
    }
    tagIndex = 0;
    columnIndex = 0;
    int count = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (count == 0) {
            count++;
            continue;
        }
        if (strcmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "TAG") == 0) {
            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                 "INT", strlen("INT"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_INT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT", strlen("TINYINT"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_TINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT", strlen("SMALLINT"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BIGINT", strlen("BIGINT"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_BIGINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "FLOAT", strlen("FLOAT"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_FLOAT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "DOUBLE", strlen("DOUBLE"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BINARY", strlen("BINARY"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_BINARY;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "NCHAR", strlen("NCHAR"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_NCHAR;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BOOL", strlen("BOOL"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_BOOL;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TIMESTAMP", strlen("TIMESTAMP"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT UNSIGNED",
                                   strlen("TINYINT UNSIGNED"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_UTINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT UNSIGNED",
                                   strlen("SMALLINT UNSIGNED"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_USMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_UINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_UBIGINT;
            } else {
                stbInfo->tags[tagIndex].type = TSDB_DATA_TYPE_NULL;
            }
            stbInfo->tags[tagIndex].length =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            stbInfo->tags[tagIndex].max = RAND_MAX >> 1;
            stbInfo->tags[tagIndex].min = (RAND_MAX >> 1) * -1;

            tagIndex++;
        } else {
            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                 "INT", strlen("INT")) &&
                strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                       "UNSIGNED") == NULL) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_INT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "TINYINT", strlen("TINYINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_TINYINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "SMALLINT", strlen("SMALLINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT", strlen("BIGINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_BIGINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "FLOAT", strlen("FLOAT"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_FLOAT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "DOUBLE", strlen("DOUBLE"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BINARY", strlen("BINARY"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_BINARY;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "NCHAR", strlen("NCHAR"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_NCHAR;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BOOL", strlen("BOOL"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_BOOL;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TIMESTAMP", strlen("TIMESTAMP"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT UNSIGNED",
                                   strlen("TINYINT UNSIGNED"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_UTINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT UNSIGNED",
                                   strlen("SMALLINT UNSIGNED"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_USMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_UINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_UBIGINT;
            } else {
                stbInfo->columns[columnIndex].type = TSDB_DATA_TYPE_NULL;
            }
            if (tmp_length_list[columnIndex] != 0) {
                stbInfo->columns[columnIndex].length =
                    *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            } else {
                stbInfo->columns[columnIndex].length = 0;
            }
            stbInfo->columns[columnIndex].max = RAND_MAX >> 1;
            stbInfo->columns[columnIndex].min = (RAND_MAX >> 1) * -1;
            stbInfo->columns[columnIndex].name = calloc(
                1,
                sizeof(strlen((char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX])) +
                    1);
            tstrncpy(stbInfo->columns[columnIndex].name,
                     (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                     strlen((char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX]) + 1);
            columnIndex++;
        }
    }
    tmfree(tmp_length_list);
    taos_free_result(res);

    return 0;
}

static int createSuperTable(int db_index, int stb_index) {
    TAOS *       taos = select_one_from_pool(NULL);
    char         cols[COL_BUFFER_LEN] = "\0";
    char         command[BUFFER_SIZE] = "\0";
    int          len = 0;
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);

    for (int colIndex = 0; colIndex < stbInfo->columnCount; colIndex++) {
        switch (stbInfo->columns[colIndex].type) {
            case TSDB_DATA_TYPE_BINARY:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s(%d)",
                                stbInfo->columns[colIndex].name, "BINARY",
                                stbInfo->columns[colIndex].length);
                break;

            case TSDB_DATA_TYPE_NCHAR:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s(%d)",
                                stbInfo->columns[colIndex].name, "NCHAR",
                                stbInfo->columns[colIndex].length);
                break;

            case TSDB_DATA_TYPE_INT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "INT");
                break;

            case TSDB_DATA_TYPE_BIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "BIGINT");
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "SMALLINT");
                break;

            case TSDB_DATA_TYPE_TINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "TINYINT");
                break;

            case TSDB_DATA_TYPE_BOOL:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "BOOL");
                break;

            case TSDB_DATA_TYPE_FLOAT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "FLOAT");
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "DOUBLE");
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name, "TIMESTAMP");
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name,
                                "TINYINT UNSIGNED");
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name,
                                "SMALLINT UNSIGNED");
                break;

            case TSDB_DATA_TYPE_UINT:
                len +=
                    snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                             stbInfo->columns[colIndex].name, "INT UNSIGNED");
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",%s %s",
                                stbInfo->columns[colIndex].name,
                                "BIGINT UNSIGNED");
                break;

            default:

                errorPrint("unknown data type : %d\n",
                           stbInfo->columns[colIndex].type);
                return -1;
        }
    }

    // save for creating child table
    stbInfo->colsOfCreateChildTable =
        (char *)calloc(len + TIMESTAMP_BUFF_LEN, 1);

    snprintf(stbInfo->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(ts timestamp%s)", cols);

    if (stbInfo->tagCount == 0) {
        return 0;
    }

    char tags[TSDB_MAX_TAGS_LEN] = "\0";
    int  tagIndex;
    len = 0;

    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "(");
    for (tagIndex = 0; tagIndex < stbInfo->tagCount; tagIndex++) {
        switch (stbInfo->tags[tagIndex].type) {
            case TSDB_DATA_TYPE_BINARY:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                "%s %s(%d),", stbInfo->tags[tagIndex].name,
                                "BINARY", stbInfo->tags[tagIndex].length);

                break;
            case TSDB_DATA_TYPE_NCHAR:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                "%s %s(%d),", stbInfo->tags[tagIndex].name,
                                "NCHAR", stbInfo->tags[tagIndex].length);
                break;
            case TSDB_DATA_TYPE_INT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "INT");
                break;
            case TSDB_DATA_TYPE_BIGINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "BIGINT");
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "SMALLINT");
                break;
            case TSDB_DATA_TYPE_TINYINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "TINYINT");
                break;
            case TSDB_DATA_TYPE_BOOL:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "BOOL");
                break;
            case TSDB_DATA_TYPE_FLOAT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "FLOAT");
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "DOUBLE");
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                len +=
                    snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                             stbInfo->tags[tagIndex].name, "TINYINT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                len +=
                    snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                             stbInfo->tags[tagIndex].name, "SMALLINT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_UINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "INT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                len +=
                    snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                             stbInfo->tags[tagIndex].name, "BIGINT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s %s,",
                                stbInfo->tags[tagIndex].name, "TIMESTAMP");
                break;
            case TSDB_DATA_TYPE_JSON:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "%s json",
                                stbInfo->tags[tagIndex].name);
                goto skip;
            default:

                errorPrint("unknown data type : %d\n",
                           stbInfo->tags[tagIndex].type);
                return -1;
        }
    }

    len -= 1;
skip:
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, ")");

    snprintf(command, BUFFER_SIZE,
             stbInfo->escape_character
                 ? "CREATE TABLE IF NOT EXISTS %s.`%s` (ts TIMESTAMP%s) TAGS %s"
                 : "CREATE TABLE IF NOT EXISTS %s.%s (ts TIMESTAMP%s) TAGS %s",
             database->dbName, stbInfo->stbName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint("create supertable %s failed!\n\n", stbInfo->stbName);
        return -1;
    }

    infoPrint("create stable %s success!\n", stbInfo->stbName);
    return 0;
}

static int createDatabase(int db_index) {
    char       command[BUFFER_SIZE] = "\0";
    SDataBase *database = &(g_arguments->db[db_index]);
    TAOS *     taos = NULL;
    taos = select_one_from_pool(NULL);
    sprintf(command, "drop database if exists %s;", database->dbName);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        return -1;
    }

    int dataLen = 0;
    dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                        "CREATE DATABASE IF NOT EXISTS %s", database->dbName);

    if (database->dbCfg.blocks >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " BLOCKS %d", database->dbCfg.blocks);
    }
    if (database->dbCfg.cache >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " CACHE %d", database->dbCfg.cache);
    }
    if (database->dbCfg.days >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " DAYS %d", database->dbCfg.days);
    }
    if (database->dbCfg.keep >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " KEEP %d", database->dbCfg.keep);
    }
    if (database->dbCfg.quorum > 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " QUORUM %d", database->dbCfg.quorum);
    }
    if (database->dbCfg.replica > 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " REPLICA %d", database->dbCfg.replica);
    }
    if (database->dbCfg.update >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " UPDATE %d", database->dbCfg.update);
    }
    if (database->dbCfg.minRows >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " MINROWS %d", database->dbCfg.minRows);
    }
    if (database->dbCfg.maxRows >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " MAXROWS %d", database->dbCfg.maxRows);
    }
    if (database->dbCfg.comp >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " COMP %d", database->dbCfg.comp);
    }
    if (database->dbCfg.walLevel >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " wal %d",
                            database->dbCfg.walLevel);
    }
    if (database->dbCfg.cacheLast >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " CACHELAST %d", database->dbCfg.cacheLast);
    }
    if (database->dbCfg.fsync >= 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                            " FSYNC %d", database->dbCfg.fsync);
    }
    switch (database->dbCfg.precision) {
        case TSDB_TIME_PRECISION_MILLI:
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " precision \'ms\';");
            break;
        case TSDB_TIME_PRECISION_MICRO:
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " precision \'us\';");
            break;
        case TSDB_TIME_PRECISION_NANO:
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                " precision \'ns\';");
            break;
    }

    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint("\ncreate database %s failed!\n\n", database->dbName);
        return -1;
    }
    infoPrint("create database %s success!\n", database->dbName);
    return 0;
}

static void *createTable(void *sarg) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    prctl(PR_SET_NAME, "createTable");
    uint64_t lastPrintTime = toolsGetTimestampMs();
    pThreadInfo->buffer = calloc(1, TSDB_MAX_SQL_LEN);
    int len = 0;
    int batchNum = 0;
    debugPrint("thread[%d]: start creating table from %" PRIu64 " to %" PRIu64
               "\n",
               pThreadInfo->threadID, pThreadInfo->start_table_from,
               pThreadInfo->end_table_to);

    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        if (!stbInfo->use_metric) {
            snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                     stbInfo->escape_character
                         ? "CREATE TABLE IF NOT EXISTS %s.`%s%" PRIu64 "` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s%" PRIu64 " %s;",
                     database->dbName, stbInfo->childTblPrefix, i,
                     stbInfo->colsOfCreateChildTable);
            batchNum++;
        } else {
            if (0 == len) {
                batchNum = 0;
                memset(pThreadInfo->buffer, 0, TSDB_MAX_SQL_LEN);
                len += snprintf(pThreadInfo->buffer + len,
                                TSDB_MAX_SQL_LEN - len, "CREATE TABLE ");
            }

            len += snprintf(
                pThreadInfo->buffer + len, TSDB_MAX_SQL_LEN - len,
                stbInfo->escape_character ? "if not exists %s.`%s%" PRIu64
                                            "` using %s.`%s` tags (%s) "
                                          : "if not exists %s.%s%" PRIu64
                                            " using %s.%s tags (%s) ",
                database->dbName, stbInfo->childTblPrefix, i, database->dbName,
                stbInfo->stbName, stbInfo->tagDataBuf + i * stbInfo->lenOfTags);
            batchNum++;
            if ((batchNum < stbInfo->batchCreateTableNum) &&
                ((TSDB_MAX_SQL_LEN - len) >=
                 (stbInfo->lenOfTags + EXTRA_SQL_LEN))) {
                continue;
            }
        }

        len = 0;

        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        batchNum = 0;
        uint64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > PRINT_STAT_INTERVAL) {
            infoPrint("thread[%d] already created %" PRId64 " tables\n",
                      pThreadInfo->threadID, pThreadInfo->tables_created);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        infoPrint("thread[%d] already created %" PRId64 " tables\n",
                  pThreadInfo->threadID, pThreadInfo->tables_created);
    }
    *code = 0;
create_table_end:
    tmfree(pThreadInfo->buffer);
    return code;
}

static int startMultiThreadCreateChildTable(int db_index, int stb_index) {
    int          threads = g_arguments->nthreads;
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    int64_t      ntables = stbInfo->childTblCount;
    pthread_t *  pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo * infos = calloc(1, threads * sizeof(threadInfo));
    uint64_t     tableFrom = 0;
    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }

    int64_t b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = (int)i;
        pThreadInfo->stb_index = stb_index;
        pThreadInfo->db_index = db_index;
        pThreadInfo->taos = select_one_from_pool(database->dbName);
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->tables_created = 0;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
    }

    for (int i = 0; i < threads; i++) {
        void *result;
        pthread_join(pids[i], &result);
        if (*(int32_t *)result) {
            g_fail = true;
        }
        tmfree(result);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        g_arguments->g_actualChildTables += pThreadInfo->tables_created;
    }

    free(pids);
    free(infos);
    if (g_fail) {
        return -1;
    }
    return 0;
}

static int createChildTables() {
    int32_t    code;
    SDataBase *database = g_arguments->db;
    infoPrint("start creating %" PRId64 " table(s) with %d thread(s)\n",
              g_arguments->g_totalChildTables, g_arguments->nthreads);
    if (g_arguments->fpOfInsertResult) {
        fprintf(g_arguments->fpOfInsertResult,
                "creating %" PRId64 " table(s) with %d thread(s)\n",
                g_arguments->g_totalChildTables, g_arguments->nthreads);
    }
    double start = (double)toolsGetTimestampMs();

    for (int i = 0; i < g_arguments->dbCount; i++) {
        for (int j = 0; j < database[i].superTblCount; j++) {
            if (database[i].superTbls[j].autoCreateTable ||
                database[i].superTbls[j].iface == SML_IFACE ||
                database[i].superTbls[j].iface == SML_REST_IFACE) {
                g_arguments->g_autoCreatedChildTables +=
                    database[i].superTbls[j].childTblCount;
                continue;
            }
            if (database[i].superTbls[j].childTblExists) {
                g_arguments->g_existedChildTables +=
                    database[i].superTbls[j].childTblCount;
                continue;
            }
            debugPrint("colsOfCreateChildTable: %s\n",
                       database[i].superTbls[j].colsOfCreateChildTable);

            code = startMultiThreadCreateChildTable(i, j);
            if (code) {
                errorPrint(
                    "startMultiThreadCreateChildTable() "
                    "failed for db %d stable %d\n",
                    i, j);
                return code;
            }
        }
    }

    double end = (double)toolsGetTimestampMs();
    infoPrint("Spent %.4f seconds to create %" PRId64
              " table(s) with %d thread(s), already exist %" PRId64
              " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
              " table(s) will be auto created\n",
              (end - start) / 1000.0, g_arguments->g_totalChildTables,
              g_arguments->nthreads, g_arguments->g_existedChildTables,
              g_arguments->g_actualChildTables,
              g_arguments->g_autoCreatedChildTables);
    if (g_arguments->fpOfInsertResult) {
        fprintf(g_arguments->fpOfInsertResult,
                "Spent %.4f seconds to create %" PRId64
                " table(s) with %d thread(s), already exist %" PRId64
                " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
                " table(s) will be auto created\n",
                (end - start) / 1000.0, g_arguments->g_totalChildTables,
                g_arguments->nthreads, g_arguments->g_existedChildTables,
                g_arguments->g_actualChildTables,
                g_arguments->g_autoCreatedChildTables);
    }
    return 0;
}

void postFreeResource() {
    tmfree(g_arguments->base64_buf);
    tmfclose(g_arguments->fpOfInsertResult);
    SDataBase *database = g_arguments->db;
    for (int i = 0; i < g_arguments->dbCount; i++) {
        for (uint64_t j = 0; j < database[i].superTblCount; j++) {
            tmfree(database[i].superTbls[j].colsOfCreateChildTable);
            tmfree(database[i].superTbls[j].sampleDataBuf);
            tmfree(database[i].superTbls[j].tagDataBuf);
            tmfree(database[i].superTbls[j].stmt_buffer);
            tmfree(database[i].superTbls[j].partialColumnNameBuf);
            for (int k = 0; k < database[i].superTbls[j].tagCount; ++k) {
                tmfree(database[i].superTbls[j].tags[k].name);
                tmfree(database[i].superTbls[j].tags[k].data);
            }
            tmfree(database[i].superTbls[j].tags);

            for (int k = 0; k < database[i].superTbls[j].columnCount; ++k) {
                tmfree(database[i].superTbls[j].columns[k].name);
                tmfree(database[i].superTbls[j].columns[k].data);
            }
            tmfree(database[i].superTbls[j].columns);
            if (g_arguments->test_mode == INSERT_TEST &&
                database[i].superTbls[j].insertRows != 0) {
                for (int64_t k = 0; k < database[i].superTbls[j].childTblCount;
                     ++k) {
                    tmfree(database[i].superTbls[j].childTblName[k]);
                }
            }
            tmfree(database[i].superTbls[j].childTblName);
            if (database[i].superTbls[j].iface == STMT_IFACE) {
                if (database[i].superTbls[j].autoCreateTable) {
                    for (int k = 0; k < database[i].superTbls[j].childTblCount;
                         ++k) {
                        tmfree(database[i].superTbls[j].tag_bind_array[k]);
                    }
                    tmfree(database[i].superTbls[j].tag_bind_array);
                }
            }
        }
        tmfree(database[i].superTbls);
    }
    tmfree(database);
    cJSON_Delete(root);
    cleanup_taos_list();
    tmfree(g_arguments->pool);
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    int32_t      affectedRows;
    TAOS_RES *   res;
    int32_t      code;
    uint16_t     iface = stbInfo->iface;

    switch (iface) {
        case TAOSC_IFACE:

            affectedRows = queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                                       INSERT_TYPE, false);
            break;

        case REST_IFACE:

            if (0 != postProceSql(pThreadInfo->buffer, pThreadInfo)) {
                affectedRows = -1;
            } else {
                affectedRows = k;
            }
            break;

        case STMT_IFACE:
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint("failed to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->stmt));
                affectedRows = -1;
            } else {
                affectedRows = taos_stmt_affected_rows(pThreadInfo->stmt);
            }
            break;
        case SML_IFACE:
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(pThreadInfo->json_array);
            }
            res = taos_schemaless_insert(
                pThreadInfo->taos, pThreadInfo->lines,
                stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                stbInfo->lineProtocol,
                stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL
                    ? database->dbCfg.sml_precision
                    : TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
            code = taos_errno(res);
            affectedRows = taos_affected_rows(res);
            if (code != TSDB_CODE_SUCCESS) {
                errorPrint(
                    "failed to execute schemaless insert. content: %s, reason: "
                    "%s\n",
                    pThreadInfo->lines[0], taos_errstr(res));
                affectedRows = -1;
            }
            break;
        case SML_REST_IFACE: {
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(pThreadInfo->json_array);
                if (0 != postProceSql(pThreadInfo->lines[0], pThreadInfo)) {
                    affectedRows = -1;
                } else {
                    affectedRows = k;
                }
            } else {
                int len = 0;
                for (int i = 0; i < k; ++i) {
                    if (strlen(pThreadInfo->lines[i]) != 0) {
                        if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL &&
                            stbInfo->tcpTransfer) {
                            len += sprintf(pThreadInfo->buffer + len,
                                           "put %s\n", pThreadInfo->lines[i]);
                        } else {
                            len += sprintf(pThreadInfo->buffer + len, "%s\n",
                                           pThreadInfo->lines[i]);
                        }
                    } else {
                        break;
                    }
                }
                if (0 != postProceSql(pThreadInfo->buffer, pThreadInfo)) {
                    affectedRows = -1;
                } else {
                    affectedRows = k;
                }
            }
            break;
        }
    }
    return affectedRows;
}

static void *syncWriteInterlace(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    debugPrint(
        "thread[%d]: start interlace inserting into table from "
        "%" PRIu64 " to %" PRIu64 "\n",
        pThreadInfo->threadID, pThreadInfo->start_table_from,
        pThreadInfo->end_table_to);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;

    int64_t insertRows = stbInfo->insertRows;
    int32_t interlaceRows = stbInfo->interlaceRows;
    int64_t pos = 0;

    uint32_t batchPerTblTimes = g_arguments->reqPerReq / interlaceRows;

    uint64_t   lastPrintTime = toolsGetTimestampMs();
    uint64_t   startTs = toolsGetTimestampMs();
    uint64_t   endTs;
    delayNode *current_delay_node;
    int32_t    generated = 0;
    int        len = 0;
    uint64_t   tableSeq = pThreadInfo->start_table_from;
    while (insertRows > 0) {
        generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }
        for (int i = 0; i < batchPerTblTimes; ++i) {
            int64_t timestamp = pThreadInfo->start_time;
            char *  tableName = stbInfo->childTblName[tableSeq];
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE: {
                    if (i == 0) {
                        len = snprintf(pThreadInfo->buffer,
                                       strlen(STR_INSERT_INTO) + 1, "%s",
                                       STR_INSERT_INTO);
                    }
                    if (stbInfo->partialColumnNum == stbInfo->columnCount) {
                        if (stbInfo->autoCreateTable) {
                            len += snprintf(
                                pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s.%s using `%s` tags (%s) values ",
                                database->dbName, tableName, stbInfo->stbName,
                                stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq);
                        } else {
                            len += snprintf(pThreadInfo->buffer + len,
                                            pThreadInfo->max_sql_len - len,
                                            "%s.%s values", database->dbName,
                                            tableName);
                        }
                    } else {
                        if (stbInfo->autoCreateTable) {
                            len += snprintf(
                                pThreadInfo->buffer + len,
                                pThreadInfo->max_sql_len - len,
                                "%s.%s (%s) using `%s` tags (%s) values ",
                                database->dbName, tableName,
                                stbInfo->partialColumnNameBuf, stbInfo->stbName,
                                stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq);
                        } else {
                            len += snprintf(pThreadInfo->buffer + len,
                                            pThreadInfo->max_sql_len - len,
                                            "%s.%s (%s) values",
                                            database->dbName, tableName,
                                            stbInfo->partialColumnNameBuf);
                        }
                    }

                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        len += snprintf(
                            pThreadInfo->buffer + len,
                            pThreadInfo->max_sql_len - len, "(%" PRId64 ",%s)",
                            timestamp,
                            stbInfo->sampleDataBuf + pos * stbInfo->lenOfCols);
                        generated++;
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (stbInfo->autoCreateTable) {
                        if (taos_stmt_set_tbname_tags(
                                pThreadInfo->stmt, tableName,
                                stbInfo->tag_bind_array[tableSeq])) {
                            errorPrint(
                                "taos_stmt_set_tbname_tags(%s) failed, reason: "
                                "%s\n",
                                tableName, taos_stmt_errstr(pThreadInfo->stmt));
                            goto free_of_interlace;
                        }
                    } else {
                        if (taos_stmt_set_tbname(pThreadInfo->stmt,
                                                 tableName)) {
                            errorPrint(
                                "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                                tableName, taos_stmt_errstr(pThreadInfo->stmt));
                            goto free_of_interlace;
                        }
                    }
                    generated =
                        bindParamBatch(pThreadInfo, interlaceRows, timestamp);
                    break;
                }
                case SML_REST_IFACE:
                case SML_IFACE: {
                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                            cJSON *tag = cJSON_Duplicate(
                                cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->dbCfg.sml_precision, timestamp);
                        } else if (stbInfo->lineProtocol ==
                                   TSDB_SML_LINE_PROTOCOL) {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %s %" PRId64 "",
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                timestamp);
                        } else {
                            snprintf(
                                pThreadInfo->lines[generated],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                timestamp,
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                        }
                        generated++;
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                    }
                    break;
                }
            }
            tableSeq++;
            if (tableSeq > pThreadInfo->end_table_to) {
                tableSeq = pThreadInfo->start_table_from;
                pThreadInfo->start_time +=
                    interlaceRows * stbInfo->timestamp_step;
                pThreadInfo->totalInsertRows +=
                    pThreadInfo->ntables * interlaceRows;
                insertRows -= interlaceRows;
                if (stbInfo->insert_interval > 0) {
                    performancePrint("sleep %" PRIu64 " ms\n",
                                     stbInfo->insert_interval);
                    taosMsleep((int32_t)stbInfo->insert_interval);
                }
                break;
            }
        }

        startTs = toolsGetTimestampUs();

        int64_t affectedRows = execInsert(pThreadInfo, generated);

        endTs = toolsGetTimestampUs();
        switch (stbInfo->iface) {
            case TAOSC_IFACE:
            case REST_IFACE:
                debugPrint("pThreadInfo->buffer: %s\n", pThreadInfo->buffer);
                memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                pThreadInfo->totalAffectedRows += affectedRows;
                break;
            case SML_REST_IFACE:
                memset(pThreadInfo->buffer, 0,
                       g_arguments->reqPerReq * (pThreadInfo->max_sql_len + 1));
            case SML_IFACE:
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    debugPrint("pThreadInfo->lines[0]: %s\n",
                               pThreadInfo->lines[0]);
                    cJSON_Delete(pThreadInfo->json_array);
                    pThreadInfo->json_array = cJSON_CreateArray();
                    tmfree(pThreadInfo->lines[0]);
                } else {
                    for (int j = 0; j < generated; ++j) {
                        debugPrint("pThreadInfo->lines[%d]: %s\n", j,
                                   pThreadInfo->lines[j]);
                        memset(pThreadInfo->lines[j], 0,
                               pThreadInfo->max_sql_len);
                    }
                }
                pThreadInfo->totalAffectedRows += affectedRows;
                break;
            case STMT_IFACE:
                pThreadInfo->totalAffectedRows = affectedRows;
                break;
        }
        if (affectedRows < 0) {
            goto free_of_interlace;
        }
        uint64_t delay = endTs - startTs;
        performancePrint("insert execution time is %10.2f ms\n",
                         delay / 1000.0);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        current_delay_node = calloc(1, sizeof(delayNode));
        current_delay_node->value = delay;
        if (pThreadInfo->delayList.size == 0) {
            pThreadInfo->delayList.head = current_delay_node;
            pThreadInfo->delayList.tail = current_delay_node;
            pThreadInfo->delayList.size++;
        } else {
            pThreadInfo->delayList.tail->next = current_delay_node;
            pThreadInfo->delayList.tail = current_delay_node;
            pThreadInfo->delayList.size++;
        }
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            infoPrint("thread[%d] has currently inserted rows: %" PRIu64
                      ", affected rows: %" PRIu64 "\n",
                      pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                      pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }
        debugPrint("thread[%d] has currently inserted rows: %" PRIu64
                   ", affected rows: %" PRIu64 "\n",
                   pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
    }

    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_interlace:
    return code;
}

void *syncWriteProgressive(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    debugPrint(
        "thread[%d]: start progressive inserting into table from "
        "%" PRIu64 " to %" PRIu64 "\n",
        pThreadInfo->threadID, pThreadInfo->start_table_from,
        pThreadInfo->end_table_to);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    uint64_t   lastPrintTime = toolsGetTimestampMs();
    uint64_t   startTs = toolsGetTimestampMs();
    uint64_t   endTs;
    delayNode *current_delay_node;

    char *  pstr = pThreadInfo->buffer;
    int32_t pos = 0;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *   tableName = stbInfo->childTblName[tableSeq];
        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;

        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            int32_t generated = 0;
            switch (stbInfo->iface) {
                case TAOSC_IFACE:
                case REST_IFACE: {
                    if (stbInfo->partialColumnNum == stbInfo->columnCount) {
                        if (stbInfo->autoCreateTable) {
                            len =
                                snprintf(pstr, pThreadInfo->max_sql_len,
                                         "%s %s.%s using %s tags (%s) values ",
                                         STR_INSERT_INTO, database->dbName,
                                         tableName, stbInfo->stbName,
                                         stbInfo->tagDataBuf +
                                             stbInfo->lenOfTags * tableSeq);
                        } else {
                            len = snprintf(pstr, pThreadInfo->max_sql_len,
                                           "%s %s.%s values ", STR_INSERT_INTO,
                                           database->dbName, tableName);
                        }
                    } else {
                        if (stbInfo->autoCreateTable) {
                            len = snprintf(
                                pstr, pThreadInfo->max_sql_len,
                                "%s %s.%s (%s) using %s tags (%s) values ",
                                STR_INSERT_INTO, database->dbName, tableName,
                                stbInfo->partialColumnNameBuf, stbInfo->stbName,
                                stbInfo->tagDataBuf +
                                    stbInfo->lenOfTags * tableSeq);
                        } else {
                            len = snprintf(pstr, pThreadInfo->max_sql_len,
                                           "%s %s.%s (%s) values ",
                                           STR_INSERT_INTO, database->dbName,
                                           tableName,
                                           stbInfo->partialColumnNameBuf);
                        }
                    }

                    for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                        if (stbInfo && stbInfo->useSampleTs &&
                            !stbInfo->random_data_source) {
                            len +=
                                snprintf(pstr + len,
                                         pThreadInfo->max_sql_len - len, "(%s)",
                                         stbInfo->sampleDataBuf +
                                             pos * stbInfo->lenOfCols);
                        } else {
                            len += snprintf(pstr + len,
                                            pThreadInfo->max_sql_len - len,
                                            "(%" PRId64 ",%s)", timestamp,
                                            stbInfo->sampleDataBuf +
                                                pos * stbInfo->lenOfCols);
                        }
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += stbInfo->timestamp_step;
                        if (stbInfo->disorderRatio > 0) {
                            int rand_num = taosRandom() % 100;
                            if (rand_num < stbInfo->disorderRatio) {
                                timestamp -=
                                    (taosRandom() % stbInfo->disorderRange);
                            }
                        }
                        generated++;
                        if (len > (BUFFER_SIZE - stbInfo->lenOfCols)) {
                            debugPrint("%d", j);
                            break;
                        }
                        if (i + generated >= stbInfo->insertRows) {
                            break;
                        }
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (stbInfo->autoCreateTable) {
                        if (taos_stmt_set_tbname_tags(
                                pThreadInfo->stmt, tableName,
                                stbInfo->tag_bind_array[tableSeq])) {
                            errorPrint(
                                "taos_stmt_set_tbname_tags(%s) failed, reason: "
                                "%s\n",
                                tableName, taos_stmt_errstr(pThreadInfo->stmt));
                            goto free_of_progressive;
                        }
                    } else {
                        if (taos_stmt_set_tbname(pThreadInfo->stmt,
                                                 tableName)) {
                            errorPrint(
                                "taos_stmt_set_tbname(%s) failed, reason: %s\n",
                                tableName, taos_stmt_errstr(pThreadInfo->stmt));
                            goto free_of_progressive;
                        }
                    }
                    generated = bindParamBatch(
                        pThreadInfo,
                        (g_arguments->reqPerReq > (stbInfo->insertRows - i))
                            ? (stbInfo->insertRows - i)
                            : g_arguments->reqPerReq,
                        timestamp);
                    timestamp += generated * stbInfo->timestamp_step;
                    break;
                }
                case SML_REST_IFACE:
                case SML_IFACE: {
                    for (int j = 0; j < g_arguments->reqPerReq; ++j) {
                        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                            cJSON *tag = cJSON_Duplicate(
                                cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(
                                pThreadInfo->json_array, tag, stbInfo,
                                database->dbCfg.sml_precision, timestamp);
                        } else if (stbInfo->lineProtocol ==
                                   TSDB_SML_LINE_PROTOCOL) {
                            snprintf(
                                pThreadInfo->lines[j],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %s %" PRId64 "",
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                timestamp);
                        } else {
                            snprintf(
                                pThreadInfo->lines[j],
                                stbInfo->lenOfCols + stbInfo->lenOfTags,
                                "%s %" PRId64 " %s %s", stbInfo->stbName,
                                timestamp,
                                stbInfo->sampleDataBuf +
                                    pos * stbInfo->lenOfCols,
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from]);
                        }
                        pos++;
                        if (pos >= g_arguments->prepared_rand) {
                            pos = 0;
                        }
                        timestamp += getTSRandTail(
                            stbInfo->timestamp_step, i + 1,
                            stbInfo->disorderRatio, stbInfo->disorderRange);
                        generated++;
                        if (i + generated >= stbInfo->insertRows) {
                            break;
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            i += generated;
            pThreadInfo->totalInsertRows += generated;
            // only measure insert
            startTs = toolsGetTimestampUs();
            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = toolsGetTimestampUs();
            if (affectedRows < 0) {
                goto free_of_progressive;
            }
            switch (stbInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE:
                    memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                    pThreadInfo->totalAffectedRows += affectedRows;
                    break;
                case SML_REST_IFACE:
                    memset(pThreadInfo->buffer, 0,
                           g_arguments->reqPerReq *
                               (pThreadInfo->max_sql_len + 1));
                case SML_IFACE:
                    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                        cJSON_Delete(pThreadInfo->json_array);
                        pThreadInfo->json_array = cJSON_CreateArray();
                        tmfree(pThreadInfo->lines[0]);
                    } else {
                        for (int j = 0; j < generated; ++j) {
                            debugPrint("pThreadInfo->lines[%d]: %s\n", j,
                                       pThreadInfo->lines[j]);
                            memset(pThreadInfo->lines[j], 0,
                                   pThreadInfo->max_sql_len);
                        }
                    }
                    pThreadInfo->totalAffectedRows += affectedRows;
                    break;
                case STMT_IFACE:
                    pThreadInfo->totalAffectedRows = affectedRows;
                    break;
            }

            uint64_t delay = endTs - startTs;
            performancePrint("insert execution time is %10.f ms\n",
                             delay / 1000.0);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            current_delay_node = calloc(1, sizeof(delayNode));
            current_delay_node->value = delay;
            if (pThreadInfo->delayList.size == 0) {
                pThreadInfo->delayList.head = current_delay_node;
                pThreadInfo->delayList.tail = current_delay_node;
                pThreadInfo->delayList.size++;
            } else {
                pThreadInfo->delayList.tail->next = current_delay_node;
                pThreadInfo->delayList.tail = current_delay_node;
                pThreadInfo->delayList.size++;
            }
            pThreadInfo->totalDelay += delay;

            int64_t currentPrintTime = toolsGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint(
                    "thread[%d] has currently inserted rows: "
                    "%" PRId64 ", affected rows: %" PRId64 "\n",
                    pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                    pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }
            debugPrint("thread[%d] has currently inserted rows: %" PRId64
                       ", affected rows: %" PRId64 "\n",
                       pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                       pThreadInfo->totalAffectedRows);
            if (i >= stbInfo->insertRows) {
                break;
            }
        }  // insertRows
    }      // tableSeq
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_progressive:
    return code;
}

static int startMultiThreadInsertData(int db_index, int stb_index) {
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    if ((stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) &&
        !stbInfo->use_metric) {
        errorPrint("%s", "schemaless cannot work without stable\n");
        return -1;
    }

    if (stbInfo->interlaceRows > g_arguments->reqPerReq) {
        infoPrint(
            "interlaceRows(%d) is larger than record per request(%u), which "
            "will be set to %u\n",
            stbInfo->interlaceRows, g_arguments->reqPerReq,
            g_arguments->reqPerReq);
        stbInfo->interlaceRows = g_arguments->reqPerReq;
    }

    if (stbInfo->interlaceRows > stbInfo->insertRows) {
        infoPrint("interlaceRows larger than insertRows %d > %" PRId64 "\n\n",
                  stbInfo->interlaceRows, stbInfo->insertRows);
        infoPrint("%s", "interlaceRows will be set to 0\n\n");
        stbInfo->interlaceRows = 0;
    }

    uint64_t tableFrom = 0;
    uint64_t ntables = stbInfo->childTblCount;
    stbInfo->childTblName = calloc(stbInfo->childTblCount, sizeof(char *));
    for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
        stbInfo->childTblName[i] = calloc(1, TSDB_TABLE_NAME_LEN);
    }

    if ((stbInfo->iface != SML_IFACE || stbInfo->iface != SML_REST_IFACE) &&
        stbInfo->childTblExists) {
        TAOS *taos = select_one_from_pool(database->dbName);
        if (taos == NULL) {
            return -1;
        }
        char cmd[SQL_BUFF_LEN] = "\0";
        if (stbInfo->escape_character) {
            snprintf(cmd, SQL_BUFF_LEN,
                     "select tbname from %s.`%s` limit %" PRId64
                     " offset %" PRIu64 "",
                     database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                     stbInfo->childTblOffset);
        } else {
            snprintf(cmd, SQL_BUFF_LEN,
                     "select tbname from %s.%s limit %" PRId64
                     " offset %" PRIu64 "",
                     database->dbName, stbInfo->stbName, stbInfo->childTblLimit,
                     stbInfo->childTblOffset);
        }
        debugPrint("cmd: %s\n", cmd);
        TAOS_RES *res = taos_query(taos, cmd);
        int32_t   code = taos_errno(res);
        int64_t   count = 0;
        if (code) {
            errorPrint("failed to get child table name: %s. reason: %s", cmd,
                       taos_errstr(res));
            taos_free_result(res);

            return -1;
        }
        TAOS_ROW row = NULL;
        while ((row = taos_fetch_row(res)) != NULL) {
            if (0 == strlen((char *)(row[0]))) {
                errorPrint("No.%" PRId64 " table return empty name\n", count);
                return -1;
            }
            if (stbInfo->escape_character) {
                snprintf(stbInfo->childTblName[count], TSDB_TABLE_NAME_LEN,
                         "`%s`", (char *)row[0]);
            } else {
                snprintf(stbInfo->childTblName[count], TSDB_TABLE_NAME_LEN,
                         "`%s`", (char *)row[0]);
            }
            debugPrint("stbInfo->childTblName[%" PRId64 "]: %s\n", count,
                       stbInfo->childTblName[count]);
            count++;
        }
        ntables = count;
        taos_free_result(res);
    } else {
        for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
            if (stbInfo->escape_character) {
                snprintf(stbInfo->childTblName[i], TSDB_TABLE_NAME_LEN,
                         "`%s%" PRIu64 "`", stbInfo->childTblPrefix, i);
            } else {
                snprintf(stbInfo->childTblName[i], TSDB_TABLE_NAME_LEN,
                         "%s%" PRIu64 "", stbInfo->childTblPrefix, i);
            }
        }
        ntables = stbInfo->childTblCount;
    }
    int     threads = g_arguments->nthreads;
    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }
    int64_t b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }

    pthread_t * pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->db_index = db_index;
        pThreadInfo->stb_index = stb_index;
        pThreadInfo->start_time = stbInfo->startTimestamp;
        pThreadInfo->totalInsertRows = 0;
        pThreadInfo->totalAffectedRows = 0;
        pThreadInfo->samplePos = 0;
        pThreadInfo->maxDelay = 0;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        delay_list_init(&(pThreadInfo->delayList));
        switch (stbInfo->iface) {
            case REST_IFACE: {
                if (stbInfo->autoCreateTable) {
                    pThreadInfo->max_sql_len =
                        (stbInfo->lenOfCols + stbInfo->lenOfTags) *
                            g_arguments->reqPerReq +
                        1024;
                } else {
                    pThreadInfo->max_sql_len =
                        stbInfo->lenOfCols * g_arguments->reqPerReq + 1024;
                }
                pThreadInfo->buffer = calloc(1, pThreadInfo->max_sql_len);
#ifdef WINDOWS
                WSADATA wsaData;
                WSAStartup(MAKEWORD(2, 2), &wsaData);
                SOCKET sockfd;
#else
                int sockfd;
#endif
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd < 0) {
#ifdef WINDOWS
                    errorPrint("Could not create socket : %d",
                               WSAGetLastError());
#endif
                    debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__,
                               sockfd);
                    errorPrint("%s\n", "failed to create socket");
                    return -1;
                }

                int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
                if (retConn < 0) {
                    errorPrint("%s\n", "failed to connect");
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
                break;
            }
            case STMT_IFACE: {
                pThreadInfo->taos = select_one_from_pool(database->dbName);
                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    tmfree(pids);
                    tmfree(infos);
                    errorPrint("taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
                    return -1;
                }

                if (0 != taos_stmt_prepare(pThreadInfo->stmt,
                                           stbInfo->stmt_buffer, 0)) {
                    errorPrint(
                        "failed to execute taos_stmt_prepare. "
                        "reason: %s\n",
                        taos_stmt_errstr(pThreadInfo->stmt));
                    tmfree(pids);
                    tmfree(infos);
                    return -1;
                }
                pThreadInfo->bind_ts = calloc(1, sizeof(int64_t));
                pThreadInfo->bind_ts_array =
                    calloc(1, sizeof(int64_t) * g_arguments->reqPerReq);
                pThreadInfo->bindParams = calloc(
                    1, sizeof(TAOS_MULTI_BIND) * (stbInfo->columnCount + 1));
                pThreadInfo->is_null = calloc(1, g_arguments->reqPerReq);

                break;
            }
            case SML_REST_IFACE: {
#ifdef WINDOWS
                WSADATA wsaData;
                WSAStartup(MAKEWORD(2, 2), &wsaData);
                SOCKET sockfd;
#else
                int sockfd;
#endif
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                debugPrint("sockfd=%d\n", sockfd);
                if (sockfd < 0) {
#ifdef WINDOWS
                    errorPrint("Could not create socket : %d",
                               WSAGetLastError());
#endif

                    errorPrint("%s\n", "failed to create socket");
                    return -1;
                }
                int retConn = connect(
                    sockfd, (struct sockaddr *)&(g_arguments->serv_addr),
                    sizeof(struct sockaddr));
                if (retConn < 0) {
                    errorPrint("%s\n", "failed to connect");
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                    free(pids);
                    free(infos);
                    return -1;
                }
                pThreadInfo->sockfd = sockfd;
            }
            case SML_IFACE: {
                if (stbInfo->iface == SML_IFACE) {
                    pThreadInfo->taos = select_one_from_pool(database->dbName);
                }
                pThreadInfo->max_sql_len =
                    stbInfo->lenOfCols + stbInfo->lenOfTags;
                if (stbInfo->iface == SML_REST_IFACE) {
                    pThreadInfo->buffer =
                        calloc(1, g_arguments->reqPerReq *
                                      (1 + pThreadInfo->max_sql_len));
                }
                if (stbInfo->lineProtocol != TSDB_SML_JSON_PROTOCOL) {
                    pThreadInfo->sml_tags =
                        (char **)calloc(pThreadInfo->ntables, sizeof(char *));
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        pThreadInfo->sml_tags[t] =
                            calloc(1, stbInfo->lenOfTags);
                    }

                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        generateRandData(
                            stbInfo, pThreadInfo->sml_tags[t],
                            stbInfo->lenOfCols + stbInfo->lenOfTags,
                            stbInfo->tags, stbInfo->tagCount, 1, true);
                        debugPrint("pThreadInfo->sml_tags[%d]: %s\n", t,
                                   pThreadInfo->sml_tags[t]);
                    }
                    pThreadInfo->lines =
                        calloc(g_arguments->reqPerReq, sizeof(char *));
                    for (int j = 0; j < g_arguments->reqPerReq; j++) {
                        pThreadInfo->lines[j] =
                            calloc(1, pThreadInfo->max_sql_len);
                    }
                } else {
                    pThreadInfo->json_array = cJSON_CreateArray();
                    pThreadInfo->sml_json_tags = cJSON_CreateArray();
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        if (generateSmlJsonTags(
                                pThreadInfo->sml_json_tags, stbInfo,
                                pThreadInfo->start_table_from, t)) {
                            return -1;
                        }
                    }
                    pThreadInfo->lines = (char **)calloc(1, sizeof(char *));
                }
                break;
            }
            case TAOSC_IFACE: {
                if (stbInfo->autoCreateTable) {
                    pThreadInfo->max_sql_len =
                        (stbInfo->lenOfCols + stbInfo->lenOfTags) *
                            g_arguments->reqPerReq +
                        1024;
                } else {
                    pThreadInfo->max_sql_len =
                        stbInfo->lenOfCols * g_arguments->reqPerReq + 1024;
                }

                pThreadInfo->taos = select_one_from_pool(database->dbName);
                pThreadInfo->buffer = calloc(1, pThreadInfo->max_sql_len);
                break;
            }
            default:
                break;
        }
        if (stbInfo->interlaceRows > 0) {
            pthread_create(pids + i, NULL, syncWriteInterlace, pThreadInfo);
        } else {
            pthread_create(pids + i, NULL, syncWriteProgressive, pThreadInfo);
        }
    }
    int64_t start = toolsGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        void *result;
        pthread_join(pids[i], &result);
        if (*(int32_t *)result) {
            g_fail = true;
        }
        tmfree(result);
    }

    int64_t end = toolsGetTimestampUs();

    uint64_t  totalDelay = 0;
    uint64_t  maxDelay = 0;
    uint64_t  minDelay = UINT64_MAX;
    uint64_t  cntDelay = 0;
    uint64_t *total_delay_list;
    double    avgDelay = 0;
    uint64_t  totalInsertRows = 0;
    uint64_t  totalAffectedRows = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        switch (stbInfo->iface) {
            case REST_IFACE:
#ifdef WINDOWS
                closesocket(pThreadInfo->sockfd);
                WSACleanup();
#else
                close(pThreadInfo->sockfd);
#endif
                tmfree(pThreadInfo->buffer);
                break;
            case SML_REST_IFACE:
                tmfree(pThreadInfo->buffer);
            case SML_IFACE:
                if (stbInfo->lineProtocol != TSDB_SML_JSON_PROTOCOL) {
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        tmfree(pThreadInfo->sml_tags[t]);
                    }
                    for (int j = 0; j < g_arguments->reqPerReq; j++) {
                        tmfree(pThreadInfo->lines[j]);
                    }
                    tmfree(pThreadInfo->sml_tags);

                } else {
                    cJSON_Delete(pThreadInfo->sml_json_tags);
                    cJSON_Delete(pThreadInfo->json_array);
                }
                tmfree(pThreadInfo->lines);
                break;
            case STMT_IFACE:
                taos_stmt_close(pThreadInfo->stmt);
                tmfree(pThreadInfo->bind_ts);
                tmfree(pThreadInfo->bind_ts_array);
                tmfree(pThreadInfo->bindParams);
                tmfree(pThreadInfo->is_null);
                break;
            case TAOSC_IFACE:
                tmfree(pThreadInfo->buffer);
                break;
            default:
                break;
        }
        totalAffectedRows += pThreadInfo->totalAffectedRows;
        totalInsertRows += pThreadInfo->totalInsertRows;
        totalDelay += pThreadInfo->totalDelay;
        cntDelay += pThreadInfo->cntDelay;
        if (g_arguments->debug_print) {
            display_delay_list(&(pThreadInfo->delayList));
        }

        if (pThreadInfo->maxDelay > maxDelay) {
            maxDelay = pThreadInfo->maxDelay;
        }

        if (pThreadInfo->minDelay < minDelay) {
            minDelay = pThreadInfo->minDelay;
        }
    }

    total_delay_list = calloc(cntDelay, sizeof(uint64_t));
    uint64_t index = 0;
    for (int i = 0; i < threads; ++i) {
        threadInfo *pThreadInfo = infos + i;
        delayNode * node = pThreadInfo->delayList.head;
        for (int j = 0; j < pThreadInfo->delayList.size; ++j) {
            total_delay_list[index] = node->value;
            node = node->next;
            index++;
        }
        delay_list_destroy(&(pThreadInfo->delayList));
    }
    qsort(total_delay_list, cntDelay, sizeof(uint64_t), compare);
    if (g_arguments->debug_print) {
        for (int i = 0; i < cntDelay; ++i) {
            debugPrint("total_delay_list[%d]: %" PRIu64 "\n", i,
                       total_delay_list[i]);
        }
    }

    free(pids);
    free(infos);

    if (g_fail) {
        tmfree(total_delay_list);
        return -1;
    }

    if (cntDelay == 0) cntDelay = 1;
    avgDelay = (double)totalDelay / cntDelay;

    int64_t t = end - start;
    if (0 == t) t = 1;

    double tInMs = (double)t / 1000000.0;

    infoPrint("Spent %.4f seconds to insert rows: %" PRIu64
              ", affected rows: %" PRIu64
              " with %d thread(s) into %s %.2f records/second\n\n",
              tInMs, totalInsertRows, totalAffectedRows, threads,
              database->dbName, (double)(totalInsertRows / tInMs));
    if (g_arguments->fpOfInsertResult) {
        fprintf(g_arguments->fpOfInsertResult,
                "Spent %.4f seconds to insert rows: %" PRIu64
                ", affected rows: %" PRIu64
                " with %d thread(s) into %s %.2f "
                "records/second\n\n",
                tInMs, totalInsertRows, totalAffectedRows, threads,
                database->dbName, (double)(totalInsertRows / tInMs));
    }

    if (minDelay != UINT64_MAX) {
        infoPrint(
            "insert delay, min: %5.2fms, avg: %5.2fms, p90: %5.2fms, p95: "
            "%5.2fms, p99: %5.2fms, max: %5.2fms\n\n",
            (double)minDelay / 1000.0, (double)avgDelay / 1000.0,
            (double)total_delay_list[(int32_t)(cntDelay * 0.9)] / 1000.0,
            (double)total_delay_list[(int32_t)(cntDelay * 0.95)] / 1000.0,
            (double)total_delay_list[(int32_t)(cntDelay * 0.99)] / 1000.0,
            (double)maxDelay / 1000.0);

        if (g_arguments->fpOfInsertResult) {
            fprintf(
                g_arguments->fpOfInsertResult,
                "insert delay, min: %5.2fms, avg: %5.2fms, p90: %5.2fms, p95: "
                "%5.2fms, p99: %5.2fms, max: %5.2fms\n\n",
                (double)minDelay / 1000.0, (double)avgDelay / 1000.0,
                (double)total_delay_list[(int32_t)(cntDelay * 0.9)] / 1000.0,
                (double)total_delay_list[(int32_t)(cntDelay * 0.95)] / 1000.0,
                (double)total_delay_list[(int32_t)(cntDelay * 0.99)] / 1000.0,
                (double)maxDelay / 1000.0);
        }
    }
    tmfree(total_delay_list);
    return 0;
}

int insertTestProcess() {
    SDataBase *database = g_arguments->db;
    printfInsertMetaToFileStream(stdout);

    if (g_arguments->fpOfInsertResult) {
        printfInsertMetaToFileStream(g_arguments->fpOfInsertResult);
    }

    prompt();

    encode_base_64();

    for (int i = 0; i < g_arguments->dbCount; ++i) {
        if (database[i].drop) {
            if (createDatabase(i)) return -1;
        }
    }
    for (int i = 0; i < g_arguments->dbCount; ++i) {
        for (int j = 0; j < database[i].superTblCount; ++j) {
            if (database[i].superTbls[j].iface != SML_IFACE &&
                database[i].superTbls[j].iface != SML_REST_IFACE) {
                if (getSuperTableFromServer(i, j)) {
                    if (createSuperTable(i, j)) return -1;
                }
            }
            prepare_sample_data(i, j);
        }
    }
    infoPrint("Estimate memory usage: %.2fMB\n",
              (double)g_memoryUsage / 1048576);
    prompt();

    if (createChildTables()) return -1;

    // create sub threads for inserting data
    for (int i = 0; i < g_arguments->dbCount; i++) {
        for (uint64_t j = 0; j < database[i].superTblCount; j++) {
            if (database[i].superTbls[j].insertRows == 0) {
                continue;
            }
            if (startMultiThreadInsertData(i, j)) {
                return -1;
            }
        }
    }
    return 0;
}
