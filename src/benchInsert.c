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

#include "benchData.h"
#include "bench.h"

int calcRowLen(char *tag_type, char *col_type, int32_t *tag_length,
               int32_t *col_length, int32_t tagCount, int32_t colCount,
               int32_t *plenOfTags, int32_t *plenOfCols, int iface) {
    *plenOfCols = 0;
    *plenOfTags = 0;
    for (int colIndex = 0; colIndex < colCount; colIndex++) {
        switch (col_type[colIndex]) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                *plenOfCols += col_length[colIndex] + 3;
                break;

            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                *plenOfCols += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                *plenOfCols += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                *plenOfCols += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                *plenOfCols += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                *plenOfCols += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                *plenOfCols += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                *plenOfCols += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                *plenOfCols += TIMESTAMP_BUFF_LEN;
                break;

            default:
                errorPrint("unknown data type : %d\n", col_type[colIndex]);
                return -1;
        }
        if (iface == SML_IFACE) {
            *plenOfCols += SML_LINE_SQL_SYNTAX_OFFSET;
        }
    }
    *plenOfCols += TIMESTAMP_BUFF_LEN;

    for (int tagIndex = 0; tagIndex < tagCount; tagIndex++) {
        switch (tag_type[tagIndex]) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                *plenOfTags += tag_length[tagIndex] + 3;
                break;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                *plenOfTags += INT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                *plenOfTags += BIGINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                *plenOfTags += SMALLINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                *plenOfTags += TINYINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_BOOL:
                *plenOfTags += BOOL_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_FLOAT:
                *plenOfTags += FLOAT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                *plenOfTags += DOUBLE_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_JSON:
                break;
            default:
                errorPrint("unknown data type : %d\n", tag_type[tagIndex]);
                return -1;
        }
        if (iface == SML_IFACE) {
            *plenOfTags += SML_LINE_SQL_SYNTAX_OFFSET;
        }
    }

    if (iface == SML_IFACE) {
        *plenOfTags += 2 * TSDB_TABLE_NAME_LEN * 2 + SML_LINE_SQL_SYNTAX_OFFSET;
    }
    return 0;
}

int getSuperTableFromServer(TAOS *taos, char *dbName, SSuperTable *superTbls) {
    char      command[SQL_BUFF_LEN] = "\0";
    TAOS_RES *res;
    TAOS_ROW  row = NULL;

    // get schema use cmd: describe superTblName;
    snprintf(command, SQL_BUFF_LEN, "describe %s.%s", dbName,
             superTbls->stbName);
    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
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
    superTbls->columnCount = columnIndex - 1;
    superTbls->tagCount = tagIndex;
    superTbls->col_type = calloc(columnIndex, sizeof(char));
    superTbls->tag_type = calloc(tagIndex, sizeof(char));
    superTbls->col_length = calloc(columnIndex, sizeof(int32_t));
    superTbls->tag_length = calloc(tagIndex, sizeof(int32_t));
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
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_INT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT", strlen("TINYINT"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_TINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT", strlen("SMALLINT"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BIGINT", strlen("BIGINT"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_BIGINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "FLOAT", strlen("FLOAT"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_FLOAT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "DOUBLE", strlen("DOUBLE"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BINARY", strlen("BINARY"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_BINARY;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "NCHAR", strlen("NCHAR"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_NCHAR;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BOOL", strlen("BOOL"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_BOOL;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TIMESTAMP", strlen("TIMESTAMP"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT UNSIGNED",
                                   strlen("TINYINT UNSIGNED"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_UTINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT UNSIGNED",
                                   strlen("SMALLINT UNSIGNED"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_USMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_UINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_UBIGINT;
            } else {
                superTbls->tag_type[tagIndex] = TSDB_DATA_TYPE_NULL;
            }
            superTbls->tag_length[tagIndex] =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tagIndex++;
        } else {
            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                 "INT", strlen("INT")) &&
                strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                       "UNSIGNED") == NULL) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_INT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "TINYINT", strlen("TINYINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_TINYINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "SMALLINT", strlen("SMALLINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT", strlen("BIGINT")) &&
                       strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                              "UNSIGNED") == NULL) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_BIGINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "FLOAT", strlen("FLOAT"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_FLOAT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "DOUBLE", strlen("DOUBLE"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BINARY", strlen("BINARY"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_BINARY;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "NCHAR", strlen("NCHAR"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_NCHAR;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "BOOL", strlen("BOOL"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_BOOL;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TIMESTAMP", strlen("TIMESTAMP"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "TINYINT UNSIGNED",
                                   strlen("TINYINT UNSIGNED"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_UTINYINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "SMALLINT UNSIGNED",
                                   strlen("SMALLINT UNSIGNED"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_USMALLINT;
            } else if (0 ==
                       strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                   "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_UINT;
            } else if (0 == strncasecmp(
                                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                                "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_UBIGINT;
            } else {
                superTbls->col_type[columnIndex] = TSDB_DATA_TYPE_NULL;
            }
            superTbls->col_length[columnIndex] =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);

            columnIndex++;
        }
    }

    taos_free_result(res);

    return 0;
}

static int createSuperTable(TAOS *taos, char *dbName, SSuperTable *superTbl,
                            char *command) {
    char cols[COL_BUFFER_LEN] = "\0";
    int  len = 0;

    if (superTbl->columnCount == 0) {
        errorPrint("super table column count is %d\n", superTbl->columnCount);
        return -1;
    }

    for (int colIndex = 0; colIndex < superTbl->columnCount; colIndex++) {
        switch (superTbl->col_type[colIndex]) {
            case TSDB_DATA_TYPE_BINARY:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s(%d)",
                                colIndex, "BINARY",
                                superTbl->col_length[colIndex]);
                break;

            case TSDB_DATA_TYPE_NCHAR:
                len +=
                    snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s(%d)",
                             colIndex, "NCHAR", superTbl->col_length[colIndex]);
                break;

            case TSDB_DATA_TYPE_INT:
                if ((g_args.demo_mode) && (colIndex == 1)) {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len,
                                    ", VOLTAGE INT");
                } else {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                    colIndex, "INT");
                }
                break;

            case TSDB_DATA_TYPE_BIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "BIGINT");
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "SMALLINT");
                break;

            case TSDB_DATA_TYPE_TINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "TINYINT");
                break;

            case TSDB_DATA_TYPE_BOOL:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "BOOL");
                break;

            case TSDB_DATA_TYPE_FLOAT:
                if (g_args.demo_mode) {
                    if (colIndex == 0) {
                        len += snprintf(cols + len, COL_BUFFER_LEN - len,
                                        ", CURRENT FLOAT");
                    } else if (colIndex == 2) {
                        len += snprintf(cols + len, COL_BUFFER_LEN - len,
                                        ", PHASE FLOAT");
                    }
                } else {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                    colIndex, "FLOAT");
                }

                break;

            case TSDB_DATA_TYPE_DOUBLE:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "DOUBLE");
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "TIMESTAMP");
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "TINYINT UNSIGNED");
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "SMALLINT UNSIGNED");
                break;

            case TSDB_DATA_TYPE_UINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "INT UNSIGNED");
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                                colIndex, "BIGINT UNSIGNED");
                break;

            default:
                taos_close(taos);
                errorPrint("unknown data type : %d\n",
                           superTbl->col_type[colIndex]);
                return -1;
        }
    }

    // save for creating child table
    superTbl->colsOfCreateChildTable =
        (char *)calloc(len + TIMESTAMP_BUFF_LEN, 1);
    if (NULL == superTbl->colsOfCreateChildTable) {
        taos_close(taos);
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }

    snprintf(superTbl->colsOfCreateChildTable, len + TIMESTAMP_BUFF_LEN,
             "(ts timestamp%s)", cols);
    verbosePrint("%s() LN%d: %s\n", __func__, __LINE__,
                 superTbl->colsOfCreateChildTable);

    if (superTbl->tagCount == 0) {
        errorPrint("super table tag count is %d\n", superTbl->tagCount);
        return -1;
    }

    char tags[TSDB_MAX_TAGS_LEN] = "\0";
    int  tagIndex;
    len = 0;

    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "(");
    for (tagIndex = 0; tagIndex < superTbl->tagCount; tagIndex++) {
        switch (superTbl->tag_type[tagIndex]) {
            case TSDB_DATA_TYPE_BINARY:
                if ((g_args.demo_mode) && (tagIndex == 1)) {
                    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                    "location BINARY(%d),",
                                    superTbl->tag_length[tagIndex]);
                } else {
                    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                    "T%d %s(%d),", tagIndex, "BINARY",
                                    superTbl->tag_length[tagIndex]);
                }
                break;
            case TSDB_DATA_TYPE_NCHAR:
                len +=
                    snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s(%d),",
                             tagIndex, "NCHAR", superTbl->tag_length[tagIndex]);
                break;
            case TSDB_DATA_TYPE_INT:
                if ((g_args.demo_mode) && (tagIndex == 0)) {
                    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                    "groupId INT, ");
                } else {
                    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                                    "T%d %s,", tagIndex, "INT");
                }
                break;
            case TSDB_DATA_TYPE_BIGINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "BIGINT");
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "SMALLINT");
                break;
            case TSDB_DATA_TYPE_TINYINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "TINYINT");
                break;
            case TSDB_DATA_TYPE_BOOL:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "BOOL");
                break;
            case TSDB_DATA_TYPE_FLOAT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "FLOAT");
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "DOUBLE");
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "TINYINT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "SMALLINT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_UINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "INT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "BIGINT UNSIGNED");
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,",
                                tagIndex, "TIMESTAMP");
                break;
            case TSDB_DATA_TYPE_JSON:
                len +=
                    snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "jtag json");
                goto skip;
            default:
                taos_close(taos);
                errorPrint("unknown data type : %d\n",
                           superTbl->tag_type[tagIndex]);
                return -1;
        }
    }

    len -= 1;
skip:
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, ")");

    snprintf(command, BUFFER_SIZE,
             superTbl->escapeChar
                 ? "CREATE TABLE IF NOT EXISTS %s.`%s` (ts TIMESTAMP%s) TAGS %s"
                 : "CREATE TABLE IF NOT EXISTS %s.%s (ts TIMESTAMP%s) TAGS %s",
             dbName, superTbl->stbName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint("create supertable %s failed!\n\n", superTbl->stbName);
        return -1;
    }

    debugPrint("create supertable %s success!\n", superTbl->stbName);
    return 0;
}

int createDatabasesAndStables(char *command) {
    TAOS *taos = NULL;
    int   ret = 0;
    taos = taos_connect(g_args.host, g_args.user, g_args.password, NULL,
                        g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine, reason:%s\n",
                   taos_errstr(NULL));
        return -1;
    }

    for (int i = 0; i < g_args.dbCount; i++) {
        debugPrint("supertbl count:%" PRIu64 "\n", db[i].superTblCount);
        if (db[i].drop) {
            sprintf(command, "drop database if exists %s;", db[i].dbName);
            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                return -1;
            }

            int dataLen = 0;
            dataLen +=
                snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                         "CREATE DATABASE IF NOT EXISTS %s", db[i].dbName);

            if (db[i].dbCfg.blocks > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " BLOCKS %d", db[i].dbCfg.blocks);
            }
            if (db[i].dbCfg.cache > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " CACHE %d", db[i].dbCfg.cache);
            }
            if (db[i].dbCfg.days > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " DAYS %d", db[i].dbCfg.days);
            }
            if (db[i].dbCfg.keep > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " KEEP %d", db[i].dbCfg.keep);
            }
            if (db[i].dbCfg.quorum > 1) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " QUORUM %d", db[i].dbCfg.quorum);
            }
            if (db[i].dbCfg.replica > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " REPLICA %d", db[i].dbCfg.replica);
            }
            if (db[i].dbCfg.update > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " UPDATE %d", db[i].dbCfg.update);
            }
            if (db[i].dbCfg.minRows > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " MINROWS %d", db[i].dbCfg.minRows);
            }
            if (db[i].dbCfg.maxRows > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " MAXROWS %d", db[i].dbCfg.maxRows);
            }
            if (db[i].dbCfg.comp > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " COMP %d", db[i].dbCfg.comp);
            }
            if (db[i].dbCfg.walLevel > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " wal %d", db[i].dbCfg.walLevel);
            }
            if (db[i].dbCfg.cacheLast > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " CACHELAST %d", db[i].dbCfg.cacheLast);
            }
            if (db[i].dbCfg.fsync > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                                    " FSYNC %d", db[i].dbCfg.fsync);
            }
            if ((0 == strncasecmp(db[i].dbCfg.precision, "ms", 2)) ||
                (0 == strncasecmp(db[i].dbCfg.precision, "ns", 2)) ||
                (0 == strncasecmp(db[i].dbCfg.precision, "us", 2))) {
                dataLen +=
                    snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                             " precision \'%s\';", db[i].dbCfg.precision);
            }

            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                errorPrint("\ncreate database %s failed!\n\n", db[i].dbName);
                return -1;
            }
            infoPrint("create database %s success!\n", db[i].dbName);
            for (uint64_t j = 0; j < db[i].superTblCount; j++) {
                if (db[i].superTbls[j].iface == SML_IFACE) {
                    infoPrint(
                        "schemaless insertion will auto create stable: %s\n",
                        db[i].superTbls[j].stbName);
                    continue;
                }
                char *cmd = calloc(1, BUFFER_SIZE);

                ret = createSuperTable(taos, db[i].dbName, &db[i].superTbls[j],
                                       cmd);
                tmfree(cmd);

                if (0 != ret) {
                    errorPrint("create super table %" PRIu64 " failed!\n\n", j);
                    return -1;
                }
                infoPrint("create stable %s success!\n",
                          db[i].superTbls[j].stbName);
            }

        } else {
            for (uint64_t j = 0; j < db[i].superTblCount; j++) {
                if (db[i].superTbls[j].iface == SML_IFACE) {
                    infoPrint(
                        "schemaless insertion will auto create stable: %s\n",
                        db[i].superTbls[j].stbName);
                    continue;
                }
                ret = getSuperTableFromServer(taos, db[i].dbName,
                                              &db[i].superTbls[j]);
                if (0 != ret) {
                    errorPrint("\nget super table %s.%s info failed!\n\n",
                               db[i].dbName, db[i].superTbls[j].stbName);
                    return -1;
                }
                infoPrint("get stable %s success!\n",
                          db[i].superTbls[j].stbName);
            }
        }
    }
    taos_close(taos);
    return 0;
}

static void *createTable(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int32_t *    code = calloc(1, sizeof(int32_t));
    *code = -1;
    prctl(PR_SET_NAME, "createTable");
    uint64_t lastPrintTime = taosGetTimestampMs();

    pThreadInfo->buffer = calloc(1, TSDB_MAX_SQL_LEN);

    int len = 0;
    int batchNum = 0;

    debugPrint("thread[%d]: start creating table from %" PRIu64 " to %" PRIu64
               "\n",
               pThreadInfo->threadID, pThreadInfo->start_table_from,
               pThreadInfo->end_table_to);

    for (uint64_t i = pThreadInfo->start_table_from;
         i <= pThreadInfo->end_table_to; i++) {
        if (0 == g_args.use_metric) {
            snprintf(pThreadInfo->buffer, TSDB_MAX_SQL_LEN,
                     g_args.escapeChar
                         ? "CREATE TABLE IF NOT EXISTS %s.`%s%" PRIu64 "` %s;"
                         : "CREATE TABLE IF NOT EXISTS %s.%s%" PRIu64 " %s;",
                     pThreadInfo->db_name, g_args.tb_prefix, i,
                     pThreadInfo->cols);
            batchNum++;
        } else {
            if (stbInfo == NULL) {
                errorPrint(
                    "%s() LN%d, use metric, but super table info is NULL\n",
                    __func__, __LINE__);
                goto create_table_end;
            } else {
                if (0 == len) {
                    batchNum = 0;
                    memset(pThreadInfo->buffer, 0, TSDB_MAX_SQL_LEN);
                    len += snprintf(pThreadInfo->buffer + len,
                                    TSDB_MAX_SQL_LEN - len, "CREATE TABLE ");
                }

                char *tagsValBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);

                if (0 == stbInfo->tagSource) {
                    if (generateTagValuesForStb(stbInfo, i, tagsValBuf)) {
                        tmfree(tagsValBuf);
                        goto create_table_end;
                    }
                } else {
                    snprintf(
                        tagsValBuf, TSDB_MAX_SQL_LEN, "(%s)",
                        stbInfo->tagDataBuf +
                            stbInfo->lenOfTags * (i % stbInfo->tagSampleCount));
                }
                len += snprintf(
                    pThreadInfo->buffer + len, TSDB_MAX_SQL_LEN - len,
                    stbInfo->escapeChar ? "if not exists %s.`%s%" PRIu64
                                          "` using %s.`%s` tags %s "
                                        : "if not exists %s.%s%" PRIu64
                                          " using %s.%s tags %s ",
                    pThreadInfo->db_name, stbInfo->childTblPrefix, i,
                    pThreadInfo->db_name, stbInfo->stbName, tagsValBuf);
                tmfree(tagsValBuf);
                batchNum++;
                if ((batchNum < stbInfo->batchCreateTableNum) &&
                    ((TSDB_MAX_SQL_LEN - len) >=
                     (stbInfo->lenOfTags + EXTRA_SQL_LEN))) {
                    continue;
                }
            }
        }

        len = 0;

        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                             NO_INSERT_TYPE, false)) {
            goto create_table_end;
        }
        pThreadInfo->tables_created += batchNum;
        batchNum = 0;
        uint64_t currentPrintTime = taosGetTimestampMs();
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

int startMultiThreadCreateChildTable(char *cols, int threads, int64_t ntables,
                                     char *db_name, SSuperTable *stbInfo) {
    pthread_t * pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    uint64_t    tableFrom = 0;
    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }

    int64_t b = 0;
    b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = (int)i;
        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->taos = taos_connect(g_args.host, g_args.user,
                                         g_args.password, db_name, g_args.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint("failed to connect to TDengine, reason:%s\n",
                       taos_errstr(NULL));
            free(pids);
            free(infos);
            return -1;
        }

        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->use_metric = true;
        pThreadInfo->cols = cols;
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
        taos_close(pThreadInfo->taos);
        g_actualChildTables += pThreadInfo->tables_created;
    }

    free(pids);
    free(infos);
    if (g_fail) {
        return -1;
    }

    return 0;
}

int createChildTables() {
    int32_t code = 0;
    infoPrint("start creating %" PRId64 " table(s) with %d thread(s)\n",
              g_totalChildTables, g_args.nthreads);
    if (g_fpOfInsertResult) {
        fprintf(g_fpOfInsertResult,
                "creating %" PRId64 " table(s) with %d thread(s)\n",
                g_totalChildTables, g_args.nthreads);
    }
    double start = (double)taosGetTimestampMs();
    char   tblColsBuf[TSDB_MAX_BYTES_PER_ROW];
    int    len;

    for (int i = 0; i < g_args.dbCount; i++) {
        if (g_args.use_metric) {
            if (db[i].superTblCount > 0) {
                // with super table
                for (int j = 0; j < db[i].superTblCount; j++) {
                    if (AUTO_CREATE_SUBTBL ==
                        db[i].superTbls[j].autoCreateTable) {
                        g_autoCreatedChildTables +=
                            db[i].superTbls[j].childTblCount;
                        continue;
                    }

                    if (TBL_ALREADY_EXISTS ==
                        db[i].superTbls[j].childTblExists) {
                        g_existedChildTables +=
                            db[i].superTbls[j].childTblCount;
                        continue;
                    }
                    debugPrint("colsOfCreateChildTable: %s\n",
                               db[i].superTbls[j].colsOfCreateChildTable);

                    code = startMultiThreadCreateChildTable(
                        db[i].superTbls[j].colsOfCreateChildTable,
                        g_args.nthreads, db[i].superTbls[j].childTblCount,
                        db[i].dbName, &(db[i].superTbls[j]));
                    if (code) {
                        errorPrint(
                            "%s() LN%d, startMultiThreadCreateChildTable() "
                            "failed for db %d stable %d\n",
                            __func__, __LINE__, i, j);
                        return code;
                    }
                }
            }
        } else {
            // normal table
            len = snprintf(tblColsBuf, TSDB_MAX_BYTES_PER_ROW, "(TS TIMESTAMP");
            for (int j = 0; j < g_args.columnCount; j++) {
                switch (g_args.col_type[j]) {
                    case TSDB_DATA_TYPE_BINARY:
                        len += snprintf(
                            tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                            ",C%d binary(%d)", j, g_args.col_length[j]);
                        break;
                    case TSDB_DATA_TYPE_NCHAR:
                        len += snprintf(
                            tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                            ",C%d nchar(%d)", j, g_args.col_length[j]);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d timestamp", j);
                        break;
                    case TSDB_DATA_TYPE_INT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d int", j);
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d bigint", j);
                        break;
                    case TSDB_DATA_TYPE_BOOL:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d bool", j);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d smallint", j);
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d tinyint", j);
                        break;
                    case TSDB_DATA_TYPE_UINT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d unsigned int", j);
                        break;
                    case TSDB_DATA_TYPE_USMALLINT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d unsigned smallint", j);
                        break;
                    case TSDB_DATA_TYPE_UTINYINT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d unsigned tinyint", j);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d float", j);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        len += snprintf(tblColsBuf + len,
                                        TSDB_MAX_BYTES_PER_ROW - len,
                                        ",C%d double", j);
                        break;
                }
            }

            snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len, ")");

            debugPrint("tblColsBuf: %s\n", tblColsBuf);
            code = startMultiThreadCreateChildTable(tblColsBuf, g_args.nthreads,
                                                    g_args.ntables,
                                                    db[i].dbName, NULL);
            if (code) {
                errorPrint(
                    "%s() LN%d, startMultiThreadCreateChildTable() "
                    "failed\n",
                    __func__, __LINE__);
                return code;
            }
        }
    }
    double end = (double)taosGetTimestampMs();
    infoPrint("Spent %.4f seconds to create %" PRId64
              " table(s) with %d thread(s), already exist %" PRId64
              " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
              " table(s) will be auto created\n",
              (end - start) / 1000.0, g_totalChildTables, g_args.nthreads,
              g_existedChildTables, g_actualChildTables,
              g_autoCreatedChildTables);
    if (g_fpOfInsertResult) {
        fprintf(g_fpOfInsertResult,
                "Spent %.4f seconds to create %" PRId64
                " table(s) with %d thread(s), already exist %" PRId64
                " table(s), actual %" PRId64 " table(s) pre created, %" PRId64
                " table(s) will be auto created\n",
                (end - start) / 1000.0, g_totalChildTables, g_args.nthreads,
                g_existedChildTables, g_actualChildTables,
                g_autoCreatedChildTables);
    }
    return code;
}

void postFreeResource() {
    tmfclose(g_fpOfInsertResult);
    for (int i = 0; i < g_args.dbCount; i++) {
        if (g_args.test_mode != INSERT_TEST) {
            continue;
        }
        for (uint64_t j = 0; j < db[i].superTblCount; j++) {
            tmfree(db[i].superTbls[j].colsOfCreateChildTable);
            tmfree(db[i].superTbls[j].buffer);
            tmfree(db[i].superTbls[j].sampleDataBuf);
            tmfree(db[i].superTbls[j].col_type);
            tmfree(db[i].superTbls[j].col_length);
            tmfree(db[i].superTbls[j].tag_type);
            tmfree(db[i].superTbls[j].tag_length);
            tmfree(db[i].superTbls[j].tagDataBuf);
            for (int64_t k = 0; k < db[i].superTbls[j].childTblCount; ++k) {
                tmfree(db[i].superTbls[j].childTblName[k]);
            }
            tmfree(db[i].superTbls[j].childTblName);
        }
        tmfree(db[i].superTbls);
    }
    tmfree(db);
    tmfree(g_randbool_buff);
    tmfree(g_randint_buff);
    tmfree(g_rand_voltage_buff);
    tmfree(g_randbigint_buff);
    tmfree(g_randsmallint_buff);
    tmfree(g_randtinyint_buff);
    tmfree(g_randfloat_buff);
    tmfree(g_rand_current_buff);
    tmfree(g_rand_phase_buff);
    tmfree(g_randdouble_buff);
    tmfree(g_randuint_buff);
    tmfree(g_randutinyint_buff);
    tmfree(g_randusmallint_buff);
    tmfree(g_randubigint_buff);
    tmfree(g_randbool);
    tmfree(g_randtinyint);
    tmfree(g_randutinyint);
    tmfree(g_randsmallint);
    tmfree(g_randusmallint);
    tmfree(g_randint);
    tmfree(g_randuint);
    tmfree(g_randbigint);
    tmfree(g_randubigint);
    tmfree(g_randfloat);
    tmfree(g_randdouble);
    tmfree(g_sampleDataBuf);
    cJSON_Delete(root);
    if (g_args.childTblName != NULL) {
        for (int i = 0; i < g_args.ntables; ++i) {
            tmfree(g_args.childTblName[i]);
        }
        tmfree(g_args.childTblName);
    }
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    int32_t   affectedRows;
    TAOS_RES *res;
    int32_t   code;
    uint16_t  iface = pThreadInfo->iface;

    switch (iface) {
        case TAOSC_IFACE:

            affectedRows = queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                                       INSERT_TYPE, false);
            break;

        case REST_IFACE:

            if (0 != postProceSql(g_args.host, g_args.port, pThreadInfo->buffer,
                                  pThreadInfo)) {
                affectedRows = -1;
            } else {
                affectedRows = g_args.reqPerReq;
            }
            break;

        case STMT_IFACE:
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint("failied to execute insert statement. reason: %s\n",
                           taos_stmt_errstr(pThreadInfo->stmt));
                affectedRows = -1;
            } else {
                affectedRows = taos_stmt_affected_rows(pThreadInfo->stmt);
            }
            break;
        case SML_IFACE:
            if (pThreadInfo->line_protocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(pThreadInfo->json_array);
            }
            res = taos_schemaless_insert(
                pThreadInfo->taos, pThreadInfo->lines,
                pThreadInfo->line_protocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                pThreadInfo->line_protocol, pThreadInfo->smlTimePrec);
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
        default:
            errorPrint("Unknown insert mode: %d\n", pThreadInfo->iface);
            affectedRows = -1;
    }
    return affectedRows;
}

void *syncWriteInterlace(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    debugPrint(
        "thread[%d]: start interlace inserting into table from "
        "%" PRIu64 " to %" PRIu64 "\n",
        pThreadInfo->threadID, pThreadInfo->start_table_from,
        pThreadInfo->end_table_to);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    int64_t insertRows = pThreadInfo->insertRows;
    int32_t interlaceRows = pThreadInfo->interlaceRows;
    int64_t pos = 0;

    uint32_t batchPerTblTimes = g_args.reqPerReq / pThreadInfo->interlaceRows;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;
    int32_t  generated = 0;
    int      len = 0;
    uint64_t tableSeq = pThreadInfo->start_table_from;
    while (insertRows > 0) {
        taosMsleep((int32_t)pThreadInfo->insert_interval);
        generated = 0;
        if (insertRows <= interlaceRows) {
            interlaceRows = insertRows;
        }
        for (int i = 0; i < batchPerTblTimes; ++i) {
            int64_t timestamp = pThreadInfo->start_time;
            char *  tableName = stbInfo ? stbInfo->childTblName[tableSeq]
                                      : g_args.childTblName[tableSeq];
            switch (pThreadInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE: {
                    if (i == 0) {
                        len = snprintf(pThreadInfo->buffer,
                                       strlen(STR_INSERT_INTO) + 1, "%s",
                                       STR_INSERT_INTO);
                    }
                    len +=
                        snprintf(pThreadInfo->buffer + len,
                                 pThreadInfo->max_sql_len - len, "%s.%s values",
                                 pThreadInfo->db_name, tableName);
                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        len += snprintf(pThreadInfo->buffer + len,
                                        pThreadInfo->max_sql_len - len,
                                        "(%" PRId64 ",%s)", timestamp,
                                        (stbInfo ? stbInfo->sampleDataBuf
                                                 : g_sampleDataBuf) +
                                            pos * (stbInfo ? stbInfo->lenOfCols
                                                           : g_args.lenOfCols));
                        generated++;
                        pos++;
                        if (pos >= g_args.prepared_rand) {
                            pos = 0;
                        }
                        timestamp += pThreadInfo->time_step;
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (taos_stmt_set_tbname(pThreadInfo->stmt, tableName)) {
                        errorPrint(
                            "taos_stmt_set_tbname(%s) failed! reason: %s\n",
                            tableName, taos_stmt_errstr(pThreadInfo->stmt));
                        goto free_of_interlace;
                    }
                    if (stbInfo) {
                        generated = bindParamBatch(pThreadInfo, interlaceRows,
                                                   timestamp);
                    } else {
                        generated = prepareStmtWithoutStb(
                            pThreadInfo, tableName, interlaceRows, timestamp);
                    }
                    break;
                }
                case SML_IFACE: {
                    for (int64_t j = 0; j < interlaceRows; ++j) {
                        if (pThreadInfo->line_protocol ==
                            TSDB_SML_JSON_PROTOCOL) {
                            cJSON *tag = cJSON_Duplicate(
                                cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(pThreadInfo->json_array, tag,
                                                stbInfo, pThreadInfo,
                                                timestamp);
                        } else {
                            generateSmlMutablePart(
                                pThreadInfo->lines[generated],
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                stbInfo, pThreadInfo, timestamp);
                        }
                        generated++;
                        timestamp += pThreadInfo->time_step;
                    }
                    break;
                }
            }
            tableSeq++;
            if (tableSeq > pThreadInfo->end_table_to) {
                tableSeq = pThreadInfo->start_table_from;
                pThreadInfo->start_time +=
                    interlaceRows * pThreadInfo->time_step;
                pThreadInfo->totalInsertRows +=
                    pThreadInfo->ntables * interlaceRows;
                insertRows -= interlaceRows;
                break;
            }
        }

        startTs = taosGetTimestampUs();

        int64_t affectedRows = execInsert(pThreadInfo, generated);

        endTs = taosGetTimestampUs();
        switch (pThreadInfo->iface) {
            case TAOSC_IFACE:
            case REST_IFACE:
                debugPrint("pThreadInfo->buffer: %s\n", pThreadInfo->buffer);
                memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                pThreadInfo->totalAffectedRows += affectedRows;
                break;
            case SML_IFACE:
                if (pThreadInfo->line_protocol == TSDB_SML_JSON_PROTOCOL) {
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
        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                         __func__, __LINE__, delay / 1000.0);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        int64_t currentPrintTime = taosGetTimestampMs();
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
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    debugPrint(
        "thread[%d]: start progressive inserting into table from "
        "%" PRIu64 " to %" PRIu64 "\n",
        pThreadInfo->threadID, pThreadInfo->start_table_from,
        pThreadInfo->end_table_to);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint64_t     lastPrintTime = taosGetTimestampMs();
    uint64_t     startTs = taosGetTimestampMs();
    uint64_t     endTs;

    char *  pstr = pThreadInfo->buffer;
    int32_t pos = 0;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        char *tableName = stbInfo ? stbInfo->childTblName[tableSeq]
                                  : g_args.childTblName[tableSeq];
        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;

        for (uint64_t i = 0; i < pThreadInfo->insertRows;) {
            taosMsleep((int32_t)pThreadInfo->insert_interval);
            int32_t generated = 0;
            switch (pThreadInfo->iface) {
                case TAOSC_IFACE:
                case REST_IFACE: {
                    len = snprintf(pstr, pThreadInfo->max_sql_len,
                                   "%s %s.%s values ", STR_INSERT_INTO,
                                   pThreadInfo->db_name, tableName);

                    for (int j = 0; j < g_args.reqPerReq; ++j) {
                        len +=
                            snprintf(pstr + len, pThreadInfo->max_sql_len - len,
                                     "(%" PRId64 ",%s)", timestamp,
                                     (stbInfo ? stbInfo->sampleDataBuf
                                              : g_sampleDataBuf) +
                                         pos * (stbInfo ? stbInfo->lenOfCols
                                                        : g_args.lenOfCols));
                        pos++;
                        if (pos >= g_args.prepared_rand) {
                            pos = 0;
                        }
                        timestamp += pThreadInfo->time_step;
                        generated++;
                        if (i + generated >= pThreadInfo->insertRows) {
                            break;
                        }
                    }
                    break;
                }
                case STMT_IFACE: {
                    if (taos_stmt_set_tbname(pThreadInfo->stmt, tableName)) {
                        errorPrint(
                            "taos_stmt_set_tbname(%s) failed! reason: %s\n",
                            tableName, taos_stmt_errstr(pThreadInfo->stmt));
                        goto free_of_progressive;
                    }
                    if (stbInfo) {
                        generated = bindParamBatch(
                            pThreadInfo,
                            (g_args.reqPerReq > (pThreadInfo->insertRows - i))
                                ? (pThreadInfo->insertRows - i)
                                : g_args.reqPerReq,
                            timestamp);
                    } else {
                        generated = prepareStmtWithoutStb(
                            pThreadInfo, tableName,
                            (g_args.reqPerReq > (pThreadInfo->insertRows - i))
                                ? (pThreadInfo->insertRows - i)
                                : g_args.reqPerReq,
                            timestamp);
                    }
                    timestamp += generated * pThreadInfo->time_step;
                    break;
                }
                case SML_IFACE: {
                    for (int j = 0; j < g_args.reqPerReq; ++j) {
                        if (pThreadInfo->line_protocol ==
                            TSDB_SML_JSON_PROTOCOL) {
                            cJSON *tag = cJSON_Duplicate(
                                cJSON_GetArrayItem(
                                    pThreadInfo->sml_json_tags,
                                    (int)tableSeq -
                                        pThreadInfo->start_table_from),
                                true);
                            generateSmlJsonCols(pThreadInfo->json_array, tag,
                                                stbInfo, pThreadInfo,
                                                timestamp);
                        } else {
                            generateSmlMutablePart(
                                pThreadInfo->lines[j],
                                pThreadInfo
                                    ->sml_tags[(int)tableSeq -
                                               pThreadInfo->start_table_from],
                                stbInfo, pThreadInfo, timestamp);
                        }
                        timestamp += pThreadInfo->time_step;
                        generated++;
                        if (i + generated >= pThreadInfo->insertRows) {
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
            startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            if (affectedRows < 0) {
                goto free_of_progressive;
            }
            switch (pThreadInfo->iface) {
                case REST_IFACE:
                case TAOSC_IFACE:
                    memset(pThreadInfo->buffer, 0, pThreadInfo->max_sql_len);
                    pThreadInfo->totalAffectedRows += affectedRows;
                    break;
                case SML_IFACE:
                    if (pThreadInfo->line_protocol == TSDB_SML_JSON_PROTOCOL) {
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
            pThreadInfo->totalDelay += delay;

            int64_t currentPrintTime = taosGetTimestampMs();
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
            if (i >= pThreadInfo->insertRows) {
                break;
            }
        }  // insertRows
    }      // tableSeq
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_progressive:
    return code;
}

int startMultiThreadInsertData(int threads, char *db_name, char *precision,
                               SSuperTable *stbInfo) {
    int iface = stbInfo ? stbInfo->iface : g_args.iface;
    int line_protocol =
        stbInfo ? stbInfo->lineProtocol : TSDB_SML_LINE_PROTOCOL;
    int32_t  timePrec = TSDB_TIME_PRECISION_MILLI;
    int32_t  smlTimePrec = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    int64_t  startTime = DEFAULT_START_TIME;
    int64_t  ntables = stbInfo ? stbInfo->childTblCount : g_args.ntables;
    int64_t  insertRows = stbInfo ? stbInfo->insertRows : g_args.insertRows;
    uint64_t insert_interval = stbInfo ? stbInfo->insertInterval : 0;
    int32_t  interlaceRows =
        stbInfo ? stbInfo->interlaceRows : g_args.interlaceRows;
    int64_t time_step =
        stbInfo ? stbInfo->timeStampStep : g_args.timestamp_step;
    char *childTbls_prefix =
        stbInfo ? stbInfo->childTblPrefix : g_args.tb_prefix;
    uint64_t tableFrom = 0;
    if (stbInfo) {
        if (stbInfo->lineProtocol != TSDB_SML_LINE_PROTOCOL) {
            smlTimePrec = TSDB_SML_TIMESTAMP_NOT_CONFIGURED;
        } else if (0 != precision[0]) {
            if (0 == strncasecmp(precision, "ms", 2)) {
                timePrec = TSDB_TIME_PRECISION_MILLI;
                smlTimePrec = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
            } else if (0 == strncasecmp(precision, "us", 2)) {
                timePrec = TSDB_TIME_PRECISION_MICRO;
                smlTimePrec = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
            } else if (0 == strncasecmp(precision, "ns", 2)) {
                timePrec = TSDB_TIME_PRECISION_NANO;
                smlTimePrec = TSDB_SML_TIMESTAMP_NANO_SECONDS;
            } else {
                errorPrint("unknown precision: %s\n", precision);
                return -1;
            }
        } else {
            smlTimePrec = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
        }

        if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
            startTime = taosGetTimestamp(timePrec);
        } else {
            if (TSDB_CODE_SUCCESS !=
                taos_parse_time(stbInfo->startTimestamp, &startTime,
                                (int32_t)strlen(stbInfo->startTimestamp),
                                timePrec, 0)) {
                errorPrint("failed to parse time %s\n",
                           stbInfo->startTimestamp);
                return -1;
            }
        }

        stbInfo->childTblName = calloc(stbInfo->childTblCount, sizeof(char *));
        for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
            stbInfo->childTblName[i] = calloc(1, TSDB_TABLE_NAME_LEN);
        }

        if (iface != SML_IFACE) {
            TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
                                      db_name, g_args.port);
            if (taos == NULL) {
                errorPrint("Failed to connect to TDengine, reason: %s\n",
                           taos_errstr(NULL));
                return -1;
            }
            char cmd[SQL_BUFF_LEN] = "\0";
            if (g_args.escapeChar) {
                snprintf(cmd, SQL_BUFF_LEN,
                         "select tbname from %s.`%s` limit %" PRId64
                         " offset %" PRIu64 "",
                         db_name, stbInfo->stbName, stbInfo->childTblCount,
                         stbInfo->childTblOffset);
            } else {
                snprintf(cmd, SQL_BUFF_LEN,
                         "select tbname from %s.%s limit %" PRId64
                         " offset %" PRIu64 "",
                         db_name, stbInfo->stbName, stbInfo->childTblCount,
                         stbInfo->childTblOffset);
            }
            TAOS_RES *res = taos_query(taos, cmd);
            int32_t   code = taos_errno(res);
            int64_t   count = 0;
            if (code) {
                errorPrint("failed to get child table name: %s. reason: %s",
                           cmd, taos_errstr(res));
                taos_free_result(res);
                taos_close(taos);
                return -1;
            }
            TAOS_ROW row = NULL;
            while ((row = taos_fetch_row(res)) != NULL) {
                if (0 == strlen((char *)(row[0]))) {
                    errorPrint("No.%" PRId64 " table return empty name\n",
                               count);
                    return -1;
                }
                if (g_args.escapeChar) {
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
            taos_close(taos);

        } else {
            for (int64_t i = 0; i < stbInfo->childTblCount; ++i) {
                if (g_args.escapeChar) {
                    snprintf(stbInfo->childTblName[i], TSDB_TABLE_NAME_LEN,
                             "`%s%" PRIu64 "`", childTbls_prefix, i);
                } else {
                    snprintf(stbInfo->childTblName[i], TSDB_TABLE_NAME_LEN,
                             "%s%" PRIu64 "", childTbls_prefix, i);
                }
            }
        }
    } else {
        g_args.childTblName = calloc(g_args.ntables, sizeof(char *));
        for (int64_t i = 0; i < g_args.ntables; ++i) {
            g_args.childTblName[i] = calloc(1, TSDB_TABLE_NAME_LEN);
        }
        for (int64_t i = 0; i < g_args.ntables; ++i) {
            if (g_args.escapeChar) {
                snprintf(g_args.childTblName[i], TSDB_TABLE_NAME_LEN,
                         "`%s%" PRIu64 "`", childTbls_prefix, i);
            } else {
                snprintf(g_args.childTblName[i], TSDB_TABLE_NAME_LEN,
                         "%s%" PRIu64 "", childTbls_prefix, i);
            }
        }
    }
    debugPrint("iface: %d\n", iface);
    debugPrint("line_protocol: %d\n", line_protocol);
    debugPrint("timePrec: %d;\n", timePrec);
    debugPrint("smlTimePrec: %d\n", smlTimePrec);
    debugPrint("startTime: %" PRId64 "\n", startTime);
    debugPrint("ntables: %" PRId64 "\n", ntables);
    debugPrint("insertRows: %" PRId64 "\n", insertRows);
    debugPrint("timestep: %" PRId64 "\n", time_step);
    debugPrint("interlaceRows: %d\n", interlaceRows);
    debugPrint("insertInterval: %" PRIu64 "\n", insert_interval);
    debugPrint("childTbls_prefix: %s\n", childTbls_prefix);
    char *  stmtBuffer = calloc(1, BUFFER_SIZE);
    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }
    int64_t b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }
    if (iface == REST_IFACE) {
        if (convertHostToServAddr(g_args.host, g_args.port,
                                  &(g_args.serv_addr)) != 0) {
            errorPrint("%s\n", "convert host to server address");
            return -1;
        }
    } else if (iface == STMT_IFACE) {
        int len = 0;
        len += sprintf(stmtBuffer + len, "INSERT INTO ? VALUES(?");

        int columnCount = (stbInfo) ? stbInfo->columnCount : g_args.columnCount;
        char *   col_type = (stbInfo) ? stbInfo->col_type : g_args.col_type;
        int32_t *col_length =
            (stbInfo) ? stbInfo->col_length : g_args.col_length;
        g_string_grid = calloc(columnCount, sizeof(char *));
        for (int col = 0; col < columnCount; col++) {
            len += sprintf(stmtBuffer + len, ",?");
            if (col_type[col] == TSDB_DATA_TYPE_NCHAR ||
                col_type[col] == TSDB_DATA_TYPE_BINARY) {
                g_string_grid[col] =
                    calloc(1, g_args.reqPerReq * (col_length[col] + 1));
                for (int i = 0; i < g_args.reqPerReq; ++i) {
                    rand_string(g_string_grid[col] + i * col_length[col],
                                col_length[col]);
                }
            }
        }
        len += sprintf(stmtBuffer + len, ")");
        debugPrint("stmtBuffer: %s\n", stmtBuffer);
        if (g_args.prepared_rand < g_args.reqPerReq) {
            infoPrint(
                "in stmt mode, batch size(%u) can not larger than prepared "
                "sample data size(%" PRId64
                "), restart with larger prepared_rand or batch size will be "
                "auto set to %" PRId64 "\n",
                g_args.reqPerReq, g_args.prepared_rand, g_args.prepared_rand);
            g_args.reqPerReq = g_args.prepared_rand;
        }
    }

    if (interlaceRows > g_args.reqPerReq) {
        infoPrint(
            "interlaceRows(%d) is larger than record per request(%u), which "
            "will be set to %u\n",
            interlaceRows, g_args.reqPerReq, g_args.reqPerReq);
        interlaceRows = g_args.reqPerReq;
    }

    pthread_t * pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        pThreadInfo->iface = iface;
        pThreadInfo->line_protocol = line_protocol;
        pThreadInfo->totalInsertRows = 0;
        pThreadInfo->totalAffectedRows = 0;
        pThreadInfo->samplePos = 0;
        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->time_precision = timePrec;
        pThreadInfo->stbInfo = stbInfo;
        pThreadInfo->start_time = startTime;
        pThreadInfo->maxDelay = 0;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->insert_interval = insert_interval;
        tstrncpy(pThreadInfo->tb_prefix,
                 stbInfo ? stbInfo->childTblPrefix : g_args.tb_prefix,
                 TBNAME_PREFIX_LEN);
        pThreadInfo->insertRows = insertRows;
        pThreadInfo->interlaceRows = interlaceRows;
        pThreadInfo->time_step = time_step;
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        switch (iface) {
            case REST_IFACE: {
                pThreadInfo->max_sql_len =
                    (stbInfo ? stbInfo->lenOfCols : g_args.lenOfCols) *
                        g_args.reqPerReq +
                    1024;
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

                int retConn =
                    connect(sockfd, (struct sockaddr *)&(g_args.serv_addr),
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
                pThreadInfo->taos =
                    taos_connect(g_args.host, g_args.user, g_args.password,
                                 db_name, g_args.port);
                if (NULL == pThreadInfo->taos) {
                    tmfree(infos);
                    errorPrint(
                        "connect to taosd server failed "
                        "reason:%s\n ",
                        taos_errstr(NULL));
                    return -1;
                }
                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    tmfree(pids);
                    tmfree(infos);
                    tmfree(stmtBuffer);
                    errorPrint("taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
                    return -1;
                }

                if (0 != taos_stmt_prepare(pThreadInfo->stmt, stmtBuffer, 0)) {
                    tmfree(pids);
                    tmfree(infos);
                    tmfree(stmtBuffer);
                    errorPrint(
                        "failed to execute taos_stmt_prepare. "
                        "reason: %s\n",
                        taos_stmt_errstr(pThreadInfo->stmt));
                    return -1;
                }
                pThreadInfo->bind_ts = malloc(sizeof(int64_t));
                pThreadInfo->bind_ts_array =
                    calloc(1, sizeof(int64_t) * g_args.reqPerReq);
                pThreadInfo->bindParams = calloc(
                    1,
                    sizeof(TAOS_MULTI_BIND) *
                        ((stbInfo ? stbInfo->columnCount : g_args.columnCount) +
                         1));
                pThreadInfo->is_null = calloc(1, g_args.reqPerReq);

                break;
            }
            case SML_IFACE: {
                pThreadInfo->taos =
                    taos_connect(g_args.host, g_args.user, g_args.password,
                                 db_name, g_args.port);
                if (NULL == pThreadInfo->taos) {
                    tmfree(infos);
                    errorPrint(
                        "connect to taosd server failed "
                        "reason:%s\n ",
                        taos_errstr(NULL));
                    return -1;
                }
                pThreadInfo->smlTimePrec = smlTimePrec;
                pThreadInfo->max_sql_len =
                    stbInfo ? (stbInfo->lenOfCols + stbInfo->lenOfTags)
                            : (g_args.lenOfCols + g_args.lenOfTags);
                if (line_protocol != TSDB_SML_JSON_PROTOCOL) {
                    pThreadInfo->sml_tags =
                        (char **)calloc(pThreadInfo->ntables, sizeof(char *));
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        pThreadInfo->sml_tags[t] = calloc(
                            1, stbInfo ? stbInfo->lenOfTags : g_args.lenOfTags);
                    }

                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        generateSmlConstPart(pThreadInfo->sml_tags[t], stbInfo,
                                             pThreadInfo, t);
                        debugPrint("pThreadInfo->sml_tags[%d]: %s\n", t,
                                   pThreadInfo->sml_tags[t]);
                    }
                    pThreadInfo->lines =
                        calloc(g_args.reqPerReq, sizeof(char *));
                    for (int j = 0; j < g_args.reqPerReq; j++) {
                        pThreadInfo->lines[j] =
                            calloc(1, pThreadInfo->max_sql_len);
                    }
                } else {
                    pThreadInfo->json_array = cJSON_CreateArray();
                    pThreadInfo->sml_json_tags = cJSON_CreateArray();
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        if (generateSmlJsonTags(pThreadInfo->sml_json_tags,
                                                stbInfo, pThreadInfo, t)) {
                            return -1;
                        }
                    }
                    pThreadInfo->lines = (char **)calloc(1, sizeof(char *));
                }
                break;
            }
            case TAOSC_IFACE: {
                pThreadInfo->max_sql_len =
                    (stbInfo ? stbInfo->lenOfCols : g_args.lenOfCols) *
                        g_args.reqPerReq +
                    1024;
                pThreadInfo->taos =
                    taos_connect(g_args.host, g_args.user, g_args.password,
                                 db_name, g_args.port);
                if (NULL == pThreadInfo->taos) {
                    tmfree(infos);
                    errorPrint(
                        "connect to taosd server failed "
                        "reason:%s\n ",
                        taos_errstr(NULL));
                    return -1;
                }
                pThreadInfo->buffer = calloc(1, pThreadInfo->max_sql_len);
                break;
            }
            default:
                break;
        }
        if (interlaceRows > 0) {
            pthread_create(pids + i, NULL, syncWriteInterlace, pThreadInfo);
        } else {
            pthread_create(pids + i, NULL, syncWriteProgressive, pThreadInfo);
        }
    }
    tmfree(stmtBuffer);
    int64_t start = taosGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        void *result;
        pthread_join(pids[i], &result);
        if (*(int32_t *)result) {
            g_fail = true;
        }
        tmfree(result);
    }

    int64_t end = taosGetTimestampUs();

    uint64_t totalDelay = 0;
    uint64_t maxDelay = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t cntDelay = 0;
    double   avgDelay = 0;
    uint64_t totalInsertRows = 0;
    uint64_t totalAffectedRows = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        switch (pThreadInfo->iface) {
            case REST_IFACE:
#ifdef WINDOWS
                closesocket(pThreadInfo->sockfd);
                WSACleanup();
#else
                close(pThreadInfo->sockfd);
#endif
                tmfree(pThreadInfo->buffer);
                break;
            case SML_IFACE:
                taos_close(pThreadInfo->taos);
                if (pThreadInfo->line_protocol != TSDB_SML_JSON_PROTOCOL) {
                    for (int t = 0; t < pThreadInfo->ntables; t++) {
                        tmfree(pThreadInfo->sml_tags[t]);
                    }
                    for (int j = 0; j < g_args.reqPerReq; j++) {
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
                taos_close(pThreadInfo->taos);
                taos_stmt_close(pThreadInfo->stmt);
                tmfree(pThreadInfo->bind_ts);
                tmfree(pThreadInfo->bind_ts_array);
                tmfree(pThreadInfo->bindParams);
                tmfree(pThreadInfo->is_null);
                break;
            case TAOSC_IFACE:
                taos_close(pThreadInfo->taos);
                tmfree(pThreadInfo->buffer);
                break;
            default:
                break;
        }
        totalAffectedRows += pThreadInfo->totalAffectedRows;
        totalInsertRows += pThreadInfo->totalInsertRows;
        totalDelay += pThreadInfo->totalDelay;
        cntDelay += pThreadInfo->cntDelay;
        if (pThreadInfo->maxDelay > maxDelay) {
            maxDelay = pThreadInfo->maxDelay;
        }

        if (pThreadInfo->minDelay < minDelay) {
            minDelay = pThreadInfo->minDelay;
        }
    }
    if (iface == STMT_IFACE) {
        for (int i = 0;
             i < (stbInfo ? stbInfo->columnCount : g_args.columnCount); ++i) {
            tmfree(g_string_grid[i]);
        }
    }
    tmfree(g_string_grid);

    free(pids);
    free(infos);

    if (g_fail) {
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
              tInMs, totalInsertRows, totalAffectedRows, threads, db_name,
              (double)(totalInsertRows / tInMs));
    if (g_fpOfInsertResult) {
        fprintf(g_fpOfInsertResult,
                "Spent %.4f seconds to insert rows: %" PRIu64
                ", affected rows: %" PRIu64
                " with %d thread(s) into %s %.2f "
                "records/second\n\n",
                tInMs, totalInsertRows, totalAffectedRows, threads, db_name,
                (double)(totalInsertRows / tInMs));
    }

    if (minDelay != UINT64_MAX) {
        infoPrint(
            "insert delay, avg: %10.2fms, max: %10.2fms, min: "
            "%10.2fms\n\n",
            (double)avgDelay / 1000.0, (double)maxDelay / 1000.0,
            (double)minDelay / 1000.0);

        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "insert delay, avg:%10.2fms, max: %10.2fms, min: "
                    "%10.2fms\n\n",
                    (double)avgDelay / 1000.0, (double)maxDelay / 1000.0,
                    (double)minDelay / 1000.0);
        }
    }

    return 0;
}

int insertTestProcess() {
    int32_t code = -1;
    char *  cmdBuffer = calloc(1, BUFFER_SIZE);

    printfInsertMetaToFileStream(stdout);

    g_fpOfInsertResult = fopen(g_args.output_file, "a");
    if (NULL == g_fpOfInsertResult) {
        errorPrint("failed to open %s for save result\n", g_args.output_file);
    }

    if (g_fpOfInsertResult) {
        printfInsertMetaToFileStream(g_fpOfInsertResult);
    }

    prompt();

    if (init_rand_data()) {
        goto end_insert_process;
    }

    // create database and super tables

    if (createDatabasesAndStables(cmdBuffer)) {
        goto end_insert_process;
    }

    // pretreatment
    if (prepareSampleData()) {
        goto end_insert_process;
    }

    if (g_args.iface != SML_IFACE && g_totalChildTables > 0) {
        if (createChildTables()) {
            goto end_insert_process;
        }
    }

    // create sub threads for inserting data
    // start = taosGetTimestampMs();
    if (g_args.use_metric) {
        for (int i = 0; i < g_args.dbCount; i++) {
            for (uint64_t j = 0; j < db[i].superTblCount; j++) {
                if (startMultiThreadInsertData(g_args.nthreads, db[i].dbName,
                                               db[i].dbCfg.precision,
                                               &db[i].superTbls[j])) {
                    goto end_insert_process;
                }
            }
        }
    } else {
        if (startMultiThreadInsertData(g_args.nthreads, g_args.database, "ms",
                                       NULL)) {
            goto end_insert_process;
        }
    }

    code = 0;
end_insert_process:
    tmfree(cmdBuffer);
    return code;
}