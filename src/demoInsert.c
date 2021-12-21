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
#include "demoData.h"

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

static int getSuperTableFromServer(TAOS *taos, char *dbName,
                                   SSuperTable *superTbls) {
    char      command[SQL_BUFF_LEN] = "\0";
    TAOS_RES *res;
    TAOS_ROW  row = NULL;
    int       count = 0;

    // get schema use cmd: describe superTblName;
    snprintf(command, SQL_BUFF_LEN, "describe %s.%s", dbName,
             superTbls->stbName);
    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        printf("failed to run command %s, reason: %s\n", command,
               taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    int tagIndex = 0;
    int columnIndex = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == count) {
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
        count++;
    }

    superTbls->columnCount = columnIndex;
    superTbls->tagCount = tagIndex;
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
            // if (db[i].dbCfg.maxtablesPerVnode > 0) {
            //  dataLen += snprintf(command + dataLen,
            //  BUFFER_SIZE - dataLen, "tables %d ",
            //  db[i].dbCfg.maxtablesPerVnode);
            //}
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
        }

        debugPrint("supertbl count:%" PRIu64 "\n", db[i].superTblCount);

        int validStbCount = 0;

        for (uint64_t j = 0; j < db[i].superTblCount; j++) {
            if (db[i].superTbls[j].iface == SML_IFACE) {
                goto skip;
            }

            sprintf(command, "describe %s.%s;", db[i].dbName,
                    db[i].superTbls[j].stbName);
            ret = queryDbExec(taos, command, NO_INSERT_TYPE, true);

            if ((ret != 0) || (db[i].drop)) {
                char *cmd = calloc(1, BUFFER_SIZE);
                if (NULL == cmd) {
                    errorPrint("%s", "failed to allocate memory\n");
                    return -1;
                }

                ret = createSuperTable(taos, db[i].dbName, &db[i].superTbls[j],
                                       cmd);
                tmfree(cmd);

                if (0 != ret) {
                    errorPrint("create super table %" PRIu64 " failed!\n\n", j);
                    return -1;
                }
            } else {
                ret = getSuperTableFromServer(taos, db[i].dbName,
                                              &db[i].superTbls[j]);
                if (0 != ret) {
                    errorPrint("\nget super table %s.%s info failed!\n\n",
                               db[i].dbName, db[i].superTbls[j].stbName);
                    return -1;
                }
            }
        skip:
            validStbCount++;
        }
        db[i].superTblCount = validStbCount;
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
    tmfree(g_dupstr);
    for (int i = 0; i < g_args.dbCount; i++) {
        for (uint64_t j = 0; j < db[i].superTblCount; j++) {
            tmfree(db[i].superTbls[j].colsOfCreateChildTable);
            tmfree(db[i].superTbls[j].buffer);
            tmfree(db[i].superTbls[j].sampleDataBuf);
            for (int c = 0; c < db[i].superTbls[j].columnCount; c++) {
                if (db[i].superTbls[j].sampleBindBatchArray) {
                    tmfree((char *)((uintptr_t) *
                                    (uintptr_t *)(db[i]
                                                      .superTbls[j]
                                                      .sampleBindBatchArray +
                                                  sizeof(char *) * c)));
                }
            }
            tmfree(db[i].superTbls[j].sampleBindBatchArray);

            if (0 != db[i].superTbls[j].tagDataBuf) {
                tmfree(db[i].superTbls[j].tagDataBuf);
                db[i].superTbls[j].tagDataBuf = NULL;
            }
            if (0 != db[i].superTbls[j].childTblName) {
                tmfree(db[i].superTbls[j].childTblName);
                db[i].superTbls[j].childTblName = NULL;
            }
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
    tmfree(g_randint);
    tmfree(g_randuint);
    tmfree(g_randbigint);
    tmfree(g_randubigint);
    tmfree(g_randfloat);
    tmfree(g_randdouble);
    tmfree(g_sampleDataBuf);

    for (int l = 0; l < g_args.columnCount; l++) {
        if (g_sampleBindBatchArray) {
            tmfree((char *)((uintptr_t) * (uintptr_t *)(g_sampleBindBatchArray +
                                                        sizeof(char *) * l)));
        }
    }
    tmfree(g_sampleBindBatchArray);
    cJSON_Delete(root);
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k) {
    int32_t      affectedRows;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_RES *   res;
    int32_t      code;
    uint16_t     iface;
    if (stbInfo)
        iface = stbInfo->iface;
    else {
        if (g_args.iface == INTERFACE_BUT)
            iface = TAOSC_IFACE;
        else
            iface = g_args.iface;
    }

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
                affectedRows = k;
            }
            break;

        case STMT_IFACE:
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint(
                    "%s() LN%d, failied to execute insert statement. reason: "
                    "%s\n",
                    __func__, __LINE__, taos_stmt_errstr(pThreadInfo->stmt));
                affectedRows = -1;
            } else {
                affectedRows = k;
            }
            break;
        case SML_IFACE:
            res = taos_schemaless_insert(
                pThreadInfo->taos, pThreadInfo->lines,
                stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL ? 0 : k,
                stbInfo->lineProtocol, stbInfo->tsPrecision);
            code = taos_errno(res);
            affectedRows = taos_affected_rows(res);
            if (code != TSDB_CODE_SUCCESS) {
                errorPrint(
                    "%s() LN%d, failed to execute schemaless insert. content: "
                    "%s, reason: "
                    "%s\n",
                    __func__, __LINE__, pThreadInfo->lines[0],
                    taos_errstr(res));
                affectedRows = -1;
            }
            break;
        default:
            errorPrint("Unknown insert mode: %d\n", stbInfo->iface);
            affectedRows = -1;
    }
    return affectedRows;
}

static void getTableName(char *pTblName, threadInfo *pThreadInfo,
                         uint64_t tableSeq) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    if (stbInfo) {
        if (AUTO_CREATE_SUBTBL != stbInfo->autoCreateTable) {
            if (stbInfo->childTblLimit > 0) {
                snprintf(pTblName, TSDB_TABLE_NAME_LEN,
                         stbInfo->escapeChar ? "`%s`" : "%s",
                         stbInfo->childTblName +
                             (tableSeq - stbInfo->childTblOffset) *
                                 TSDB_TABLE_NAME_LEN);
            } else {
                verbosePrint("[%d] %s() LN%d: from=%" PRIu64 " count=%" PRId64
                             " seq=%" PRIu64 "\n",
                             pThreadInfo->threadID, __func__, __LINE__,
                             pThreadInfo->start_table_from,
                             pThreadInfo->ntables, tableSeq);
                snprintf(
                    pTblName, TSDB_TABLE_NAME_LEN,
                    stbInfo->escapeChar ? "`%s`" : "%s",
                    stbInfo->childTblName + tableSeq * TSDB_TABLE_NAME_LEN);
            }
        } else {
            snprintf(pTblName, TSDB_TABLE_NAME_LEN,
                     stbInfo->escapeChar ? "`%s%" PRIu64 "`" : "%s%" PRIu64 "",
                     stbInfo->childTblPrefix, tableSeq);
        }
    } else {
        snprintf(pTblName, TSDB_TABLE_NAME_LEN,
                 g_args.escapeChar ? "`%s%" PRIu64 "`" : "%s%" PRIu64 "",
                 g_args.tb_prefix, tableSeq);
    }
}

static int execStbBindParamBatch(threadInfo *pThreadInfo, char *tableName,
                                 int64_t tableSeq, uint32_t batch,
                                 uint64_t insertRows, uint64_t recordFrom,
                                 int64_t startTime, int64_t *pSamplePos) {
    TAOS_STMT *stmt = pThreadInfo->stmt;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    uint32_t columnCount = pThreadInfo->stbInfo->columnCount;

    uint32_t thisBatch = (uint32_t)(g_args.prepared_rand - (*pSamplePos));

    if (thisBatch > batch) {
        thisBatch = batch;
    }

    memset(pThreadInfo->bindParams, 0,
           (sizeof(TAOS_MULTI_BIND) * (columnCount + 1)));
    memset(pThreadInfo->is_null, 0, thisBatch);

    for (int c = 0; c < columnCount + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);

        char data_type;

        if (c == 0) {
            data_type = TSDB_DATA_TYPE_TIMESTAMP;
            param->buffer_length = sizeof(int64_t);
            param->buffer = pThreadInfo->bind_ts_array;

        } else {
            data_type = stbInfo->col_type[c - 1];

            char *tmpP;

            switch (data_type) {
                case TSDB_DATA_TYPE_BINARY:
                    param->buffer_length = stbInfo->col_length[c - 1];

                    tmpP =
                        (char *)((uintptr_t) *
                                 (uintptr_t *)(stbInfo->sampleBindBatchArray +
                                               sizeof(char *) * (c - 1)));

                    param->buffer =
                        (void *)(tmpP + *pSamplePos * param->buffer_length);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    param->buffer_length = stbInfo->col_length[c - 1];

                    tmpP =
                        (char *)((uintptr_t) *
                                 (uintptr_t *)(stbInfo->sampleBindBatchArray +
                                               sizeof(char *) * (c - 1)));

                    param->buffer =
                        (void *)(tmpP + *pSamplePos * param->buffer_length);
                    break;

                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    param->buffer_length = sizeof(int32_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    param->buffer_length = sizeof(int8_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    param->buffer_length = sizeof(int16_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    param->buffer_length = sizeof(int64_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    param->buffer_length = sizeof(int8_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    param->buffer_length = sizeof(float);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    param->buffer_length = sizeof(double);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    param->buffer_length = sizeof(int64_t);
                    param->buffer =
                        (void *)((uintptr_t) *
                                     (uintptr_t *)(stbInfo
                                                       ->sampleBindBatchArray +
                                                   sizeof(char *) * (c - 1)) +
                                 stbInfo->col_length[c - 1] * (*pSamplePos));
                    break;

                default:
                    errorPrint("wrong data type: %d\n", data_type);
                    return -1;
            }
        }

        param->buffer_type = data_type;
        param->length = calloc(thisBatch, sizeof(int32_t));

        for (int b = 0; b < thisBatch; b++) {
            /*
            if (param->buffer_type == TSDB_DATA_TYPE_NCHAR) {
                param->length[b] = (int32_t)strlen((char *)param->buffer +
                                                   b * sizeof(int32_t));
            } else {
                param->length[b] = (int32_t)param->buffer_length;
            }
            */
            param->length[b] = (int32_t)param->buffer_length;
        }
        param->is_null = pThreadInfo->is_null;
        param->num = thisBatch;
    }

    uint32_t k;
    for (k = 0; k < thisBatch;) {
        /* columnCount + 1 (ts) */
        if (stbInfo->disorderRatio) {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + getTSRandTail(stbInfo->timeStampStep, k,
                                          stbInfo->disorderRatio,
                                          stbInfo->disorderRange);
        } else {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + stbInfo->timeStampStep * k;
        }

        k++;
        recordFrom++;

        (*pSamplePos)++;
        if ((*pSamplePos) == g_args.prepared_rand) {
            *pSamplePos = 0;
        }

        if (recordFrom >= insertRows) {
            break;
        }
    }

    if (taos_stmt_bind_param_batch(
            stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams)) {
        errorPrint("taos_stmt_bind_param_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }

    for (int c = 0; c < stbInfo->columnCount + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);
        tmfree(param->length);
    }

    // if msg > 3MB, break
    if (taos_stmt_add_batch(stmt)) {
        errorPrint("taos_stmt_add_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }
    return k;
}

int32_t prepareStbStmt(threadInfo *pThreadInfo, char *tableName,
                       int64_t tableSeq, uint32_t batch, uint64_t insertRows,
                       uint64_t recordFrom, int64_t startTime,
                       int64_t *pSamplePos) {
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *  stmt = pThreadInfo->stmt;

    char *tagsArray = calloc(1, sizeof(TAOS_BIND) * stbInfo->tagCount);
    if (NULL == tagsArray) {
        errorPrint("%s", "failed to allocate memory\n");
        return -1;
    }
    char *tagsValBuf = (char *)calloc(TSDB_MAX_SQL_LEN + 1, 1);

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        if (0 == stbInfo->tagSource) {
            if (generateTagValuesForStb(stbInfo, tableSeq, tagsValBuf)) {
                tmfree(tagsValBuf);
                tmfree(tagsArray);
                return -1;
            }
        } else {
            snprintf(
                tagsValBuf, TSDB_MAX_SQL_LEN, "(%s)",
                stbInfo->tagDataBuf +
                    stbInfo->lenOfTags * (tableSeq % stbInfo->tagSampleCount));
        }

        if (prepareStbStmtBindTag(tagsArray, stbInfo, tagsValBuf,
                                  pThreadInfo->time_precision)) {
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

        if (taos_stmt_set_tbname_tags(stmt, tableName,
                                      (TAOS_BIND *)tagsArray)) {
            errorPrint("taos_stmt_set_tbname_tags() failed! reason: %s\n",
                       taos_stmt_errstr(stmt));
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

    } else {
        if (taos_stmt_set_tbname(stmt, tableName)) {
            errorPrint("taos_stmt_set_tbname() failed! reason: %s\n",
                       taos_stmt_errstr(stmt));
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }
    }
    tmfree(tagsValBuf);
    tmfree(tagsArray);
    return execStbBindParamBatch(pThreadInfo, tableName, tableSeq, batch,
                                 insertRows, recordFrom, startTime, pSamplePos);
}

// stmt sync write interlace data
static void *syncWriteInterlaceStmtBatch(threadInfo *pThreadInfo,
                                         uint32_t    interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### stmt interlace write\n",
               pThreadInfo->threadID, __func__, __LINE__);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    int64_t  insertRows;
    int64_t  timeStampStep;
    uint64_t insert_interval;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%" PRIu64 " ntables=%" PRId64
               " insertRows=%" PRIu64 "\n",
               pThreadInfo->threadID, __func__, __LINE__,
               pThreadInfo->start_table_from, pThreadInfo->ntables, insertRows);

    uint64_t timesInterlace = (insertRows / interlaceRows) + 1;
    uint32_t precalcBatch = interlaceRows;

    if (precalcBatch > g_args.reqPerReq) precalcBatch = g_args.reqPerReq;

    if (precalcBatch > g_args.prepared_rand)
        precalcBatch = g_args.prepared_rand;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t  startTime;

    bool     flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    pThreadInfo->samplePos = 0;

    for (int64_t interlace = 0; interlace < timesInterlace; interlace++) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        int64_t generated = 0;
        int64_t samplePos;

        for (; tableSeq < pThreadInfo->start_table_from + pThreadInfo->ntables;
             tableSeq++) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                goto free_of_interlace_stmt;
            }

            samplePos = pThreadInfo->samplePos;
            startTime = pThreadInfo->start_time +
                        interlace * interlaceRows * timeStampStep;
            uint64_t remainRecPerTbl = insertRows - interlaceRows * interlace;
            uint64_t recPerTbl = 0;

            uint64_t remainPerInterlace;
            if (remainRecPerTbl > interlaceRows) {
                remainPerInterlace = interlaceRows;
            } else {
                remainPerInterlace = remainRecPerTbl;
            }

            while (remainPerInterlace > 0) {
                uint32_t batch;
                if (remainPerInterlace > precalcBatch) {
                    batch = precalcBatch;
                } else {
                    batch = (uint32_t)remainPerInterlace;
                }
                debugPrint(
                    "[%d] %s() LN%d, tableName:%s, batch:%d "
                    "startTime:%" PRId64 "\n",
                    pThreadInfo->threadID, __func__, __LINE__, tableName, batch,
                    startTime);

                if (stbInfo) {
                    generated =
                        prepareStbStmt(pThreadInfo, tableName, tableSeq, batch,
                                       insertRows, 0, startTime, &samplePos);
                } else {
                    generated = prepareStmtWithoutStb(
                        pThreadInfo, tableName, batch, insertRows,
                        interlaceRows * interlace + recPerTbl, startTime);
                }

                debugPrint("[%d] %s() LN%d, generated records is %" PRId64 "\n",
                           pThreadInfo->threadID, __func__, __LINE__,
                           generated);
                if (generated < 0) {
                    errorPrint(
                        "[%d] %s() LN%d, generated records is %" PRId64 "\n",
                        pThreadInfo->threadID, __func__, __LINE__, generated);
                    goto free_of_interlace_stmt;
                } else if (generated == 0) {
                    break;
                }

                recPerTbl += generated;
                remainPerInterlace -= generated;
                pThreadInfo->totalInsertRows += generated;

                verbosePrint("[%d] %s() LN%d totalInsertRows=%" PRIu64 "\n",
                             pThreadInfo->threadID, __func__, __LINE__,
                             pThreadInfo->totalInsertRows);

                startTs = taosGetTimestampUs();

                int64_t affectedRows =
                    execInsert(pThreadInfo, (uint32_t)generated);

                endTs = taosGetTimestampUs();
                uint64_t delay = endTs - startTs;
                performancePrint(
                    "%s() LN%d, insert execution time is %10.2f ms\n", __func__,
                    __LINE__, delay / 1000.0);
                verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                             pThreadInfo->threadID, __func__, __LINE__,
                             affectedRows);

                if (delay > pThreadInfo->maxDelay)
                    pThreadInfo->maxDelay = delay;
                if (delay < pThreadInfo->minDelay)
                    pThreadInfo->minDelay = delay;
                pThreadInfo->cntDelay++;
                pThreadInfo->totalDelay += delay;

                if (generated != affectedRows) {
                    errorPrint("[%d] %s() LN%d execInsert() insert %" PRId64
                               ", affected rows: %" PRId64 "\n\n",
                               pThreadInfo->threadID, __func__, __LINE__,
                               generated, affectedRows);
                    goto free_of_interlace_stmt;
                }

                pThreadInfo->totalAffectedRows += affectedRows;

                int64_t currentPrintTime = taosGetTimestampMs();
                if (currentPrintTime - lastPrintTime > 30 * 1000) {
                    printf("thread[%d] has currently inserted rows: %" PRIu64
                           ", affected rows: %" PRIu64 "\n",
                           pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                           pThreadInfo->totalAffectedRows);
                    lastPrintTime = currentPrintTime;
                }

                startTime += (generated * timeStampStep);
            }
        }
        pThreadInfo->samplePos = samplePos;

        if (tableSeq == pThreadInfo->start_table_from + pThreadInfo->ntables) {
            // turn to first table
            tableSeq = pThreadInfo->start_table_from;

            flagSleep = true;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st)) {
                uint64_t sleepTime = insert_interval - (et - st);
                performancePrint("%s() LN%d sleep: %" PRId64
                                 " ms for insert interval\n",
                                 __func__, __LINE__, sleepTime);
                taosMsleep((int32_t)sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }

    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_interlace_stmt:
    return code;
}
/*
void *syncWriteInterlace(threadInfo *pThreadInfo, uint32_t interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### interlace write\n",
pThreadInfo->threadID,
               __func__, __LINE__);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    int64_t  insertRows;
    int64_t  timeStampStep;
    uint64_t insert_interval;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%" PRIu64 " ntables=%"
PRId64 " insertRows=%" PRIu64 "\n", pThreadInfo->threadID, __func__,
__LINE__, pThreadInfo->start_table_from, pThreadInfo->ntables, insertRows);

    if (interlaceRows > g_args.reqPerReq) interlaceRows = g_args.reqPerReq;

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }
    pThreadInfo->buffer = calloc(TSDB_MAX_SQL_LEN, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint("%s", "failed to allocate memory\n");
        goto free_of_interlace;
    }

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t  startTime = pThreadInfo->start_time;

    uint64_t generatedRecPerTbl = 0;
    bool     flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    while (pThreadInfo->totalInsertRows < pThreadInfo->ntables * insertRows)
{ if ((flagSleep) && (insert_interval)) { st = taosGetTimestampMs();
            flagSleep = false;
        }

        // generate data
        memset(pThreadInfo->buffer, 0, TSDB_MAX_SQL_LEN);
        uint64_t remainderBufLen = TSDB_MAX_SQL_LEN;

        char *pstr = pThreadInfo->buffer;

        int len =
            snprintf(pstr, strlen(STR_INSERT_INTO) + 1, "%s",
STR_INSERT_INTO); pstr += len; remainderBufLen -= len;

        uint32_t recOfBatch = 0;

        int32_t generated;
        for (uint64_t i = 0; i < batchPerTblTimes; i++) {
            char tableName[TSDB_TABLE_NAME_LEN];

            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                goto free_of_interlace;
            }

            uint64_t oldRemainderLen = remainderBufLen;

            if (stbInfo) {
                generated = generateStbInterlaceData(
                    pThreadInfo, tableName, batchPerTbl, i,
batchPerTblTimes, tableSeq, pstr, insertRows, startTime, &remainderBufLen);
            } else {
                generated = (int32_t)generateInterlaceDataWithoutStb(
                    tableName, batchPerTbl, tableSeq, pThreadInfo->db_name,
                    pstr, insertRows, startTime, &remainderBufLen);
            }

            debugPrint("[%d] %s() LN%d, generated records is %d\n",
                       pThreadInfo->threadID, __func__, __LINE__,
generated); if (generated < 0) { errorPrint("[%d] %s() LN%d, generated
records is %d\n", pThreadInfo->threadID, __func__, __LINE__, generated);
                goto free_of_interlace;
            } else if (generated == 0) {
                break;
            }

            tableSeq++;
            recOfBatch += batchPerTbl;

            pstr += (oldRemainderLen - remainderBufLen);
            pThreadInfo->totalInsertRows += batchPerTbl;

            verbosePrint("[%d] %s() LN%d batchPerTbl=%d recOfBatch=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__,
batchPerTbl, recOfBatch);

            if (tableSeq ==
                pThreadInfo->start_table_from + pThreadInfo->ntables) {
                // turn to first table
                tableSeq = pThreadInfo->start_table_from;
                generatedRecPerTbl += batchPerTbl;

                startTime = pThreadInfo->start_time +
                            generatedRecPerTbl * timeStampStep;

                flagSleep = true;
                if (generatedRecPerTbl >= insertRows) break;

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows))
                    batchPerTbl = (uint32_t)remainRows;

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq)
                    break;
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%" PRId64
                         " insertRows=%" PRId64 "\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl) break;
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%" PRIu64
                     "\n",
                     pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                     pThreadInfo->totalInsertRows);
        verbosePrint("[%d] %s() LN%d, buffer=%s\n", pThreadInfo->threadID,
                     __func__, __LINE__, pThreadInfo->buffer);

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint("[%d] %s() LN%d Failed to insert records of batch
%d\n", pThreadInfo->threadID, __func__, __LINE__, batchPerTbl); if
(batchPerTbl > 0) { errorPrint(
                    "\tIf the batch is %d, the length of the SQL to insert a
" "row must be less then %" PRId64 "\n", batchPerTbl,
(uint64_t)TSDB_MAX_SQL_LEN / batchPerTbl);
            }
            errorPrint("\tPlease check if the buffer length(%" PRId64
                       ") or batch(%d) is set with proper value!\n",
                       (uint64_t)TSDB_MAX_SQL_LEN, batchPerTbl);
            goto free_of_interlace;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;
        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                         __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                     pThreadInfo->threadID, __func__, __LINE__,
affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint(
                "[%d] %s() LN%d execInsert insert %d, affected rows: %"
PRId64
                "\n%s\n",
                pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                affectedRows, pThreadInfo->buffer);
            goto free_of_interlace;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int64_t currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            printf("thread[%d] has currently inserted rows: %" PRIu64
                   ", affected rows: %" PRIu64 "\n",
                   pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st)) {
                uint64_t sleepTime = insert_interval - (et - st);
                performancePrint("%s() LN%d sleep: %" PRId64
                                 " ms for insert interval\n",
                                 __func__, __LINE__, sleepTime);
                taosMsleep((int32_t)sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_interlace:
    tmfree(pThreadInfo->buffer);
    return code;
}
*/
static void *syncWriteInterlaceSml(threadInfo *pThreadInfo,
                                   uint32_t    interlaceRows) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    debugPrint("[%d] %s() LN%d: ### interlace schemaless write\n",
               pThreadInfo->threadID, __func__, __LINE__);
    int64_t  insertRows;
    int64_t  timeStampStep;
    uint64_t insert_interval;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%" PRIu64 " ntables=%" PRId64
               " insertRows=%" PRIu64 "\n",
               pThreadInfo->threadID, __func__, __LINE__,
               pThreadInfo->start_table_from, pThreadInfo->ntables, insertRows);

    if (interlaceRows > g_args.reqPerReq) interlaceRows = g_args.reqPerReq;

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes = g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }

    char **smlList;
    cJSON *tagsList;
    cJSON *jsonArray;
    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL ||
        stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        smlList = (char **)calloc(pThreadInfo->ntables, sizeof(char *));
        if (NULL == smlList) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_of_interlace_sml;
        }

        for (int t = 0; t < pThreadInfo->ntables; t++) {
            char *sml =
                (char *)calloc(1, (stbInfo->lenOfTags + stbInfo->lenOfCols));
            if (NULL == sml) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_smlheadlist_interlace_sml;
            }
            if (generateSmlConstPart(sml, stbInfo, pThreadInfo, t)) {
                goto free_smlheadlist_interlace_sml;
            }
            smlList[t] = sml;
        }

        pThreadInfo->lines = calloc(g_args.reqPerReq, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_smlheadlist_interlace_sml;
        }

        for (int i = 0; i < g_args.reqPerReq; i++) {
            pThreadInfo->lines[i] =
                calloc(1, (stbInfo->lenOfTags + stbInfo->lenOfCols));
            if (NULL == pThreadInfo->lines[i]) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_lines_interlace_sml;
            }
        }
    } else {
        jsonArray = cJSON_CreateArray();
        tagsList = cJSON_CreateArray();
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            if (generateSmlJsonTags(tagsList, stbInfo, pThreadInfo, t)) {
                goto free_json_interlace_sml;
            }
        }

        pThreadInfo->lines = (char **)calloc(1, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_json_interlace_sml;
        }
    }

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t  startTime = pThreadInfo->start_time;

    uint64_t generatedRecPerTbl = 0;
    bool     flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    while (pThreadInfo->totalInsertRows < pThreadInfo->ntables * insertRows) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }
        // generate data

        uint32_t recOfBatch = 0;

        for (uint64_t i = 0; i < batchPerTblTimes; i++) {
            int64_t timestamp = startTime;
            for (int j = recOfBatch; j < recOfBatch + batchPerTbl; j++) {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    cJSON *tag = cJSON_Duplicate(
                        cJSON_GetArrayItem(
                            tagsList,
                            (int)(tableSeq - pThreadInfo->start_table_from)),
                        true);
                    if (generateSmlJsonCols(jsonArray, tag, stbInfo,
                                            pThreadInfo, timestamp)) {
                        goto free_json_interlace_sml;
                    }
                } else {
                    if (generateSmlMutablePart(
                            pThreadInfo->lines[j],
                            smlList[tableSeq - pThreadInfo->start_table_from],
                            stbInfo, pThreadInfo, timestamp)) {
                        goto free_lines_interlace_sml;
                    }
                }

                timestamp += timeStampStep;
            }
            tableSeq++;
            recOfBatch += batchPerTbl;

            pThreadInfo->totalInsertRows += batchPerTbl;

            verbosePrint("[%d] %s() LN%d batchPerTbl=%d recOfBatch=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__, batchPerTbl,
                         recOfBatch);

            if (tableSeq ==
                pThreadInfo->start_table_from + pThreadInfo->ntables) {
                // turn to first table
                tableSeq = pThreadInfo->start_table_from;
                generatedRecPerTbl += batchPerTbl;

                startTime = pThreadInfo->start_time +
                            generatedRecPerTbl * timeStampStep;

                flagSleep = true;
                if (generatedRecPerTbl >= insertRows) {
                    break;
                }

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows)) {
                    batchPerTbl = (uint32_t)remainRows;
                }

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq) {
                    break;
                }
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%" PRId64
                         " insertRows=%" PRId64 "\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl) {
                break;
            }
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%" PRIu64
                     "\n",
                     pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                     pThreadInfo->totalInsertRows);
        verbosePrint("[%d] %s() LN%d, buffer=%s\n", pThreadInfo->threadID,
                     __func__, __LINE__, pThreadInfo->buffer);

        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
            pThreadInfo->lines[0] = cJSON_Print(jsonArray);
        }

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint("Failed to insert records of batch %d\n", batchPerTbl);
            if (batchPerTbl > 0) {
                errorPrint(
                    "\tIf the batch is %d, the length of the SQL to insert "
                    "a "
                    "row must be less then %" PRId64 "\n",
                    batchPerTbl, (int64_t)TSDB_MAX_SQL_LEN / batchPerTbl);
            }
            errorPrint("\tPlease check if the buffer length(%" PRId64
                       ") or batch(%d) is set with proper value!\n",
                       (int64_t)TSDB_MAX_SQL_LEN, batchPerTbl);
            goto free_lines_interlace_sml;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;

        if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
            tmfree(pThreadInfo->lines[0]);
            cJSON_Delete(jsonArray);
            jsonArray = cJSON_CreateArray();
        }

        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                         __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%" PRId64 "\n",
                     pThreadInfo->threadID, __func__, __LINE__, affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint("execInsert insert %d, affected rows: %" PRId64 "\n%s\n",
                       recOfBatch, affectedRows, pThreadInfo->buffer);
            goto free_lines_interlace_sml;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int64_t currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
            printf("thread[%d] has currently inserted rows: %" PRIu64
                   ", affected rows: %" PRIu64 "\n",
                   pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                   pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st)) {
                uint64_t sleepTime = insert_interval - (et - st);
                performancePrint("%s() LN%d sleep: %" PRId64
                                 " ms for insert interval\n",
                                 __func__, __LINE__, sleepTime);
                taosMsleep((int32_t)sleepTime);  // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }

    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_interlace_sml:
    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
        tmfree(pThreadInfo->lines);
    free_json_interlace_sml:
        if (jsonArray != NULL) {
            cJSON_Delete(jsonArray);
        }
        if (tagsList != NULL) {
            cJSON_Delete(tagsList);
        }
    } else {
    free_lines_interlace_sml:
        for (int index = 0; index < g_args.reqPerReq; index++) {
            tmfree(pThreadInfo->lines[index]);
        }
        tmfree(pThreadInfo->lines);
    free_smlheadlist_interlace_sml:
        for (int index = 0; index < pThreadInfo->ntables; index++) {
            tmfree(smlList[index]);
        }
        tmfree(smlList);
    }
    return code;
}

void *syncWriteProgressiveStmt(threadInfo *pThreadInfo) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int64_t      timeStampStep =
        stbInfo ? stbInfo->timeStampStep : g_args.timestamp_step;
    int64_t insertRows = (stbInfo) ? stbInfo->insertRows : g_args.insertRows;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;
    debugPrint("thread[%d]: start inserting from table %" PRIu64 " to %" PRIu64
               "\n",
               pThreadInfo->threadID, pThreadInfo->start_table_from,
               pThreadInfo->end_table_to);
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        int64_t start_time = pThreadInfo->start_time;

        for (uint64_t i = 0; i < insertRows;) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                goto free_of_stmt_progressive;
            }

            // measure prepare + insert
            startTs = taosGetTimestampUs();

            int32_t generated;
            if (stbInfo) {
                generated = prepareStbStmt(
                    pThreadInfo, tableName, tableSeq,
                    (uint32_t)((g_args.reqPerReq > stbInfo->insertRows)
                                   ? stbInfo->insertRows
                                   : g_args.reqPerReq),
                    insertRows, i, start_time, &(pThreadInfo->samplePos));
            } else {
                generated = prepareStmtWithoutStb(pThreadInfo, tableName,
                                                  g_args.reqPerReq, insertRows,
                                                  i, start_time);
            }

            verbosePrint("[%d] %s() LN%d generated=%d\n", pThreadInfo->threadID,
                         __func__, __LINE__, generated);

            if (generated > 0)
                i += generated;
            else
                goto free_of_stmt_progressive;

            start_time += generated * timeStampStep;
            pThreadInfo->totalInsertRows += generated;

            // only measure insert
            // startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                             __func__, __LINE__, delay / 1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         affectedRows);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (affectedRows < 0) {
                errorPrint("affected rows: %d\n", affectedRows);
                goto free_of_stmt_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int64_t currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                printf("thread[%d] has currently inserted rows: %" PRId64
                       ", affected rows: %" PRId64 "\n",
                       pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                       pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (i >= insertRows) break;
        }  // insertRows

        if ((g_args.verbose_print) && (tableSeq == pThreadInfo->ntables - 1) &&
            (stbInfo) &&
            (0 ==
             strncasecmp(stbInfo->dataSource, "sample", strlen("sample")))) {
            verbosePrint("%s() LN%d samplePos=%" PRId64 "\n", __func__,
                         __LINE__, pThreadInfo->samplePos);
        }
    }  // tableSeq

    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_stmt_progressive:
    tmfree(pThreadInfo->buffer);
    return code;
}

void *syncWriteProgressive(threadInfo *pThreadInfo) {
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int64_t      timeStampStep =
        stbInfo ? stbInfo->timeStampStep : g_args.timestamp_step;
    int64_t  insertRows = (stbInfo) ? stbInfo->insertRows : g_args.insertRows;
    uint64_t max_sql_len =
        (stbInfo ? stbInfo->lenOfCols : g_args.lenOfCols) * g_args.reqPerReq +
        1024;
    pThreadInfo->buffer = calloc(1, max_sql_len);

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    debugPrint("thread[%d]: start inserting into table from %" PRIu64
               " to %" PRIu64 "\n",
               pThreadInfo->threadID, pThreadInfo->start_table_from,
               pThreadInfo->end_table_to);
    int32_t pos = 0;
    for (uint64_t tableSeq = pThreadInfo->start_table_from;
         tableSeq <= pThreadInfo->end_table_to; tableSeq++) {
        memset(pThreadInfo->buffer, 0, max_sql_len);
        int64_t  timestamp = pThreadInfo->start_time;
        uint64_t len = 0;
        int32_t  generated = 0;
        for (uint64_t i = 0; i < insertRows;) {
            char *pstr = pThreadInfo->buffer;
            char  tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint("[%d] %s() LN%d, getTableName return null\n",
                           pThreadInfo->threadID, __func__, __LINE__);
                goto free_of_progressive;
            }

            len = snprintf(pstr, max_sql_len, "%s %s.%s values ",
                           STR_INSERT_INTO, pThreadInfo->db_name, tableName);

            for (int j = 0; j < g_args.reqPerReq; ++j) {
                len += snprintf(
                    pstr + len, max_sql_len - len, "(%" PRId64 ",%s)",
                    timestamp,
                    (stbInfo ? stbInfo->sampleDataBuf : g_sampleDataBuf) +
                        pos *
                            (stbInfo ? stbInfo->lenOfCols : g_args.lenOfCols));
                pos++;
                if (pos >= g_args.prepared_rand) {
                    pos = 0;
                }
                timestamp += timeStampStep;
                generated++;
                if (i + generated >= insertRows) {
                    break;
                }
            }
            i += generated;
            pThreadInfo->totalInsertRows += generated;
            // only measure insert
            startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            performancePrint("insert execution time is %10.f ms\n",
                             delay / 1000.0);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (affectedRows < 0) {
                errorPrint("affected rows: %d\n", affectedRows);
                goto free_of_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int64_t currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                infoPrint("thread[%d] has currently inserted rows: %" PRId64
                          ", affected rows: %" PRId64 "\n",
                          pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                          pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (i >= insertRows) break;

        }  // insertRows

    }  // tableSeq
    *code = 0;
    printStatPerThread(pThreadInfo);
free_of_progressive:
    tmfree(pThreadInfo->buffer);
    return code;
}

void *syncWriteProgressiveSml(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### sml progressive write\n", __func__, __LINE__);
    int32_t *code = calloc(1, sizeof(int32_t));
    *code = -1;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int64_t      timeStampStep = stbInfo->timeStampStep;
    int64_t      insertRows = stbInfo->insertRows;
    verbosePrint("%s() LN%d insertRows=%" PRId64 "\n", __func__, __LINE__,
                 insertRows);

    uint64_t lastPrintTime = taosGetTimestampMs();

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    char **smlList;
    cJSON *tagsList;
    cJSON *jsonArray;

    if (insertRows < g_args.reqPerReq) {
        g_args.reqPerReq = (uint32_t)insertRows;
    }

    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL ||
        stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        smlList = (char **)calloc(pThreadInfo->ntables, sizeof(char *));
        if (NULL == smlList) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_of_progressive_sml;
        }
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            char *sml =
                (char *)calloc(1, (stbInfo->lenOfTags + stbInfo->lenOfCols));
            if (NULL == sml) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_smlheadlist_progressive_sml;
            }
            if (generateSmlConstPart(sml, stbInfo, pThreadInfo, t)) {
                tmfree(sml);
                goto free_smlheadlist_progressive_sml;
            }
            smlList[t] = sml;
        }

        pThreadInfo->lines = (char **)calloc(g_args.reqPerReq, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_smlheadlist_progressive_sml;
        }

        for (int i = 0; i < g_args.reqPerReq; i++) {
            pThreadInfo->lines[i] =
                (char *)calloc(1, (stbInfo->lenOfTags + stbInfo->lenOfCols));
            if (NULL == pThreadInfo->lines[i]) {
                errorPrint("%s", "failed to allocate memory\n");
                goto free_lines_progressive_sml;
            }
        }
    } else {
        jsonArray = cJSON_CreateArray();
        tagsList = cJSON_CreateArray();
        for (int t = 0; t < pThreadInfo->ntables; t++) {
            if (generateSmlJsonTags(tagsList, stbInfo, pThreadInfo, t)) {
                goto free_json_progressive_sml;
            }
        }

        pThreadInfo->lines = (char **)calloc(1, sizeof(char *));
        if (NULL == pThreadInfo->lines) {
            errorPrint("%s", "failed to allocate memory\n");
            goto free_json_progressive_sml;
        }
    }

    for (uint64_t i = 0; i < pThreadInfo->ntables; i++) {
        int64_t timestamp = pThreadInfo->start_time;
        for (uint64_t j = 0; j < insertRows;) {
            for (int k = 0; k < g_args.reqPerReq; k++) {
                if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                    cJSON *tag = cJSON_Duplicate(
                        cJSON_GetArrayItem(tagsList, (int)i), true);
                    if (generateSmlJsonCols(jsonArray, tag, stbInfo,
                                            pThreadInfo, timestamp)) {
                        goto free_json_progressive_sml;
                    }
                } else {
                    if (generateSmlMutablePart(pThreadInfo->lines[k],
                                               smlList[i], stbInfo, pThreadInfo,
                                               timestamp)) {
                        goto free_lines_progressive_sml;
                    }
                }
                timestamp += timeStampStep;
                j++;
                if (j == insertRows) {
                    break;
                }
            }
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                pThreadInfo->lines[0] = cJSON_Print(jsonArray);
            }
            uint64_t startTs = taosGetTimestampUs();
            int32_t  affectedRows = execInsert(pThreadInfo, g_args.reqPerReq);
            uint64_t endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
                tmfree(pThreadInfo->lines[0]);
                cJSON_Delete(jsonArray);
                jsonArray = cJSON_CreateArray();
            }

            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                             __func__, __LINE__, delay / 1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                         pThreadInfo->threadID, __func__, __LINE__,
                         affectedRows);

            if (delay > pThreadInfo->maxDelay) {
                pThreadInfo->maxDelay = delay;
            }
            if (delay < pThreadInfo->minDelay) {
                pThreadInfo->minDelay = delay;
            }
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            pThreadInfo->totalAffectedRows += affectedRows;
            pThreadInfo->totalInsertRows += g_args.reqPerReq;

            int64_t currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30 * 1000) {
                printf("thread[%d] has currently inserted rows: %" PRId64
                       ", affected rows: %" PRId64 "\n",
                       pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                       pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (j == insertRows) {
                break;
            }
        }
    }

    *code = 0;
free_of_progressive_sml:
    if (stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
        tmfree(pThreadInfo->lines);
    free_json_progressive_sml:
        if (jsonArray != NULL) {
            cJSON_Delete(jsonArray);
        }
        if (tagsList != NULL) {
            cJSON_Delete(tagsList);
        }
    } else {
    free_lines_progressive_sml:
        for (int index = 0; index < g_args.reqPerReq; index++) {
            tmfree(pThreadInfo->lines[index]);
        }
        tmfree(pThreadInfo->lines);
    free_smlheadlist_progressive_sml:
        for (int index = 0; index < pThreadInfo->ntables; index++) {
            tmfree(smlList[index]);
        }
        tmfree(smlList);
    }
    return code;
}

void *syncWrite(void *sarg) {
    threadInfo * pThreadInfo = (threadInfo *)sarg;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    prctl(PR_SET_NAME, "syncWrite");
    uint32_t interlaceRows = 0;

    if (stbInfo) {
        if (stbInfo->interlaceRows < stbInfo->insertRows)
            interlaceRows = stbInfo->interlaceRows;
    } else {
        if (g_args.interlaceRows < g_args.insertRows)
            interlaceRows = g_args.interlaceRows;
    }

    if (interlaceRows > 0) {  // interlace mode
        if (stbInfo) {
            if (STMT_IFACE == stbInfo->iface) {
                return syncWriteInterlaceStmtBatch(pThreadInfo, interlaceRows);
            } else if (SML_IFACE == stbInfo->iface) {
                return syncWriteInterlaceSml(pThreadInfo, interlaceRows);
            } else {
                /*return syncWriteInterlace(pThreadInfo, interlaceRows);*/
            }
        }
    } else {
        // progressive mode
        if (((stbInfo) && (STMT_IFACE == stbInfo->iface)) ||
            (STMT_IFACE == g_args.iface)) {
            return syncWriteProgressiveStmt(pThreadInfo);
        } else if (((stbInfo) && (SML_IFACE == stbInfo->iface)) ||
                   (SML_IFACE == g_args.iface)) {
            return syncWriteProgressiveSml(pThreadInfo);
        } else {
            return syncWriteProgressive(pThreadInfo);
        }
    }

    return NULL;
}

int startMultiThreadInsertData(int threads, char *db_name, char *precision,
                               SSuperTable *stbInfo) {
    if (stbInfo && g_args.pressure_mode) {
        stbInfo->buffer = calloc(1, stbInfo->columnCount * 2 + 2);
        int pos = 0;
        for (int i = 0; i < stbInfo->columnCount; ++i) {
            strncpy(stbInfo->buffer + pos, ",1", 3);
            pos += 2;
        }
        strncpy(stbInfo->buffer + pos, ")", 2);
    }
    int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
    if (stbInfo) {
        stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    }

    if (0 != precision[0]) {
        if (0 == strncasecmp(precision, "ms", 2)) {
            timePrec = TSDB_TIME_PRECISION_MILLI;
            if (stbInfo) {
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
            }
        } else if (0 == strncasecmp(precision, "us", 2)) {
            timePrec = TSDB_TIME_PRECISION_MICRO;
            if (stbInfo) {
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
            }
        } else if (0 == strncasecmp(precision, "ns", 2)) {
            timePrec = TSDB_TIME_PRECISION_NANO;
            if (stbInfo) {
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
            }
        } else {
            errorPrint("Not support precision: %s\n", precision);
            return -1;
        }
    }
    if (stbInfo) {
        if (stbInfo->iface == SML_IFACE) {
            if (stbInfo->lineProtocol != TSDB_SML_LINE_PROTOCOL) {
                if (stbInfo->columnCount != 1) {
                    errorPrint(
                        "Schemaless telnet/json protocol can only have 1 "
                        "column "
                        "instead of %d\n",
                        stbInfo->columnCount);
                    return -1;
                }
                stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_NOT_CONFIGURED;
            }
        }
    }

    int64_t startTime;
    if (stbInfo) {
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
    } else {
        startTime = DEFAULT_START_TIME;
    }
    debugPrint("startTime: %" PRId64 "\n", startTime);

    TAOS *taos0 = taos_connect(g_args.host, g_args.user, g_args.password,
                               db_name, g_args.port);
    if (NULL == taos0) {
        errorPrint("connect to taosd fail , reason: %s\n", taos_errstr(NULL));
        return -1;
    }

    int64_t  ntables = 0;
    uint64_t tableFrom = 0;

    if (stbInfo) {
        if (stbInfo->iface != SML_IFACE) {
            int64_t  limit;
            uint64_t offset;

            if ((NULL != g_args.sqlFile) &&
                (stbInfo->childTblExists == TBL_NO_EXISTS) &&
                ((stbInfo->childTblOffset != 0) ||
                 (stbInfo->childTblLimit >= 0))) {
                printf(
                    "WARNING: offset and limit will not be used since the "
                    "child tables not exists!\n");
            }

            if (stbInfo->childTblExists == TBL_ALREADY_EXISTS) {
                if ((stbInfo->childTblLimit < 0) ||
                    ((stbInfo->childTblOffset + stbInfo->childTblLimit) >
                     (stbInfo->childTblCount))) {
                    if (stbInfo->childTblCount < stbInfo->childTblOffset) {
                        printf(
                            "WARNING: offset will not be used since the "
                            "child "
                            "tables count is less then offset!\n");

                        stbInfo->childTblOffset = 0;
                    }
                    stbInfo->childTblLimit =
                        stbInfo->childTblCount - stbInfo->childTblOffset;
                }

                offset = stbInfo->childTblOffset;
                limit = stbInfo->childTblLimit;
            } else {
                limit = stbInfo->childTblCount;
                offset = 0;
            }

            ntables = limit;
            tableFrom = offset;

            if ((stbInfo->childTblExists != TBL_NO_EXISTS) &&
                ((stbInfo->childTblOffset + stbInfo->childTblLimit) >
                 stbInfo->childTblCount)) {
                printf(
                    "WARNING: specified offset + limit > child table "
                    "count!\n");
                prompt();
            }

            if ((stbInfo->childTblExists != TBL_NO_EXISTS) &&
                (0 == stbInfo->childTblLimit)) {
                printf(
                    "WARNING: specified limit = 0, which cannot find table "
                    "name to insert or query! \n");
                prompt();
            }

            stbInfo->childTblName =
                (char *)calloc(1, limit * TSDB_TABLE_NAME_LEN);
            if (NULL == stbInfo->childTblName) {
                errorPrint("%s", "failed to allocate memory\n");
                return -1;
            }

            if (stbInfo->autoCreateTable != AUTO_CREATE_SUBTBL) {
                int64_t childTblCount;
                if (getChildNameOfSuperTableWithLimitAndOffset(
                        taos0, db_name, stbInfo->stbName,
                        &stbInfo->childTblName, &childTblCount, limit, offset,
                        stbInfo->escapeChar)) {
                    return -1;
                }
                ntables = childTblCount;
            }
        } else {
            ntables = stbInfo->childTblCount;
        }
    } else {
        ntables = g_args.ntables;
        tableFrom = 0;
    }

    taos_close(taos0);

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = (int)ntables;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }

    if (g_args.iface == REST_IFACE ||
        ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
        if (convertHostToServAddr(g_args.host, g_args.port,
                                  &(g_args.serv_addr)) != 0) {
            errorPrint("%s\n", "convert host to server address");
            return -1;
        }
    }

    pthread_t * pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    char *      stmtBuffer = calloc(1, BUFFER_SIZE);

    uint32_t interlaceRows = 0;
    uint32_t batch;

    if (stbInfo) {
        if (stbInfo->interlaceRows < stbInfo->insertRows)
            interlaceRows = stbInfo->interlaceRows;
    } else {
        if (g_args.interlaceRows < g_args.insertRows)
            interlaceRows = g_args.interlaceRows;
    }

    if (interlaceRows > 0) {
        batch = interlaceRows;
    } else {
        batch = (uint32_t)((g_args.reqPerReq > g_args.insertRows)
                               ? g_args.insertRows
                               : g_args.reqPerReq);
    }

    if ((g_args.iface == STMT_IFACE) ||
        ((stbInfo) && (stbInfo->iface == STMT_IFACE))) {
        char *pstr = stmtBuffer;

        if ((stbInfo) && (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable)) {
            pstr += sprintf(pstr, "INSERT INTO ? USING %s TAGS(?",
                            stbInfo->stbName);
            for (int tag = 0; tag < (stbInfo->tagCount - 1); tag++) {
                pstr += sprintf(pstr, ",?");
            }
            pstr += sprintf(pstr, ") VALUES(?");
        } else {
            pstr += sprintf(pstr, "INSERT INTO ? VALUES(?");
        }

        int columnCount = (stbInfo) ? stbInfo->columnCount : g_args.columnCount;

        for (int col = 0; col < columnCount; col++) {
            pstr += sprintf(pstr, ",?");
        }
        pstr += sprintf(pstr, ")");

        debugPrint("stmtBuffer: %s\n", stmtBuffer);
        parseSamplefileToStmtBatch(stbInfo);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;

        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->time_precision = timePrec;
        pThreadInfo->stbInfo = stbInfo;

        pThreadInfo->start_time = startTime;
        pThreadInfo->minDelay = UINT64_MAX;

        if ((NULL == stbInfo) || (stbInfo->iface != REST_IFACE)) {
            // t_info->taos = taos;
            pThreadInfo->taos =
                taos_connect(g_args.host, g_args.user, g_args.password, db_name,
                             g_args.port);
            if (NULL == pThreadInfo->taos) {
                free(infos);
                errorPrint(
                    "connect to server fail from insert sub "
                    "thread,reason:%s\n ",
                    taos_errstr(NULL));
                return -1;
            }

            if ((g_args.iface == STMT_IFACE) ||
                ((stbInfo) && (stbInfo->iface == STMT_IFACE))) {
                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    free(pids);
                    free(infos);
                    errorPrint("taos_stmt_init() failed, reason: %s\n",
                               taos_errstr(NULL));
                    return -1;
                }

                if (0 != taos_stmt_prepare(pThreadInfo->stmt, stmtBuffer, 0)) {
                    free(pids);
                    free(infos);
                    free(stmtBuffer);
                    errorPrint(
                        "failed to execute taos_stmt_prepare. reason: %s\n",
                        taos_stmt_errstr(pThreadInfo->stmt));
                    return -1;
                }
                pThreadInfo->bind_ts = malloc(sizeof(int64_t));

                if (stbInfo) {
                    parseStbSampleToStmtBatchForThread(pThreadInfo, stbInfo,
                                                       timePrec, batch);

                } else {
                    parseNtbSampleToStmtBatchForThread(pThreadInfo, timePrec,
                                                       batch);
                }
            }
        } else {
            pThreadInfo->taos = NULL;
        }

        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i < b ? a + 1 : a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        if (g_args.iface == REST_IFACE ||
            ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
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
                errorPrint("Could not create socket : %d", WSAGetLastError());
#endif
                debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__,
                           sockfd);
                errorPrint("%s\n", "failed to create socket");
                return -1;
            }

            int retConn =
                connect(sockfd, (struct sockaddr *)&(g_args.serv_addr),
                        sizeof(struct sockaddr));
            debugPrint("%s() LN%d connect() return %d\n", __func__, __LINE__,
                       retConn);
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
        }
        pthread_create(pids + i, NULL, syncWrite, pThreadInfo);
    }

    free(stmtBuffer);

    int64_t start = taosGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        void *result;
        pthread_join(pids[i], &result);
        if (*(int32_t *)result) {
            g_fail = true;
        }
        tmfree(result);
    }

    uint64_t totalDelay = 0;
    uint64_t maxDelay = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t cntDelay = 0;
    double   avgDelay = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);

        if (pThreadInfo->stmt) {
            taos_stmt_close(pThreadInfo->stmt);
        }

        tmfree((char *)pThreadInfo->bind_ts);

        tmfree((char *)pThreadInfo->bind_ts_array);
        tmfree(pThreadInfo->bindParams);
        tmfree(pThreadInfo->is_null);
        if (g_args.iface == REST_IFACE ||
            ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
#ifdef WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        }

        if (stbInfo) {
            stbInfo->totalAffectedRows += pThreadInfo->totalAffectedRows;
            stbInfo->totalInsertRows += pThreadInfo->totalInsertRows;
        } else {
            g_args.totalAffectedRows += pThreadInfo->totalAffectedRows;
            g_args.totalInsertRows += pThreadInfo->totalInsertRows;
        }

        totalDelay += pThreadInfo->totalDelay;
        cntDelay += pThreadInfo->cntDelay;
        if (pThreadInfo->maxDelay > maxDelay) maxDelay = pThreadInfo->maxDelay;
        if (pThreadInfo->minDelay < minDelay) minDelay = pThreadInfo->minDelay;
    }

    free(pids);
    free(infos);

    if (g_fail) {
        return -1;
    }

    if (cntDelay == 0) cntDelay = 1;
    avgDelay = (double)totalDelay / cntDelay;

    int64_t end = taosGetTimestampUs();
    int64_t t = end - start;
    if (0 == t) t = 1;

    double tInMs = (double)t / 1000000.0;

    if (stbInfo) {
        infoPrint("Spent %.4f seconds to insert rows: %" PRIu64
                  ", affected rows: %" PRIu64
                  " with %d thread(s) into %s.%s. %.2f records/second\n\n",
                  tInMs, stbInfo->totalInsertRows, stbInfo->totalAffectedRows,
                  threads, db_name, stbInfo->stbName,
                  (double)(stbInfo->totalInsertRows / tInMs));

        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "Spent %.4f seconds to insert rows: %" PRIu64
                    ", affected rows: %" PRIu64
                    " with %d thread(s) into %s.%s. %.2f records/second\n\n",
                    tInMs, stbInfo->totalInsertRows, stbInfo->totalAffectedRows,
                    threads, db_name, stbInfo->stbName,
                    (double)(stbInfo->totalInsertRows / tInMs));
        }
    } else {
        infoPrint("Spent %.4f seconds to insert rows: %" PRIu64
                  ", affected rows: %" PRIu64
                  " with %d thread(s) into %s %.2f records/second\n\n",
                  tInMs, g_args.totalInsertRows, g_args.totalAffectedRows,
                  threads, db_name, (double)(g_args.totalInsertRows / tInMs));
        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "Spent %.4f seconds to insert rows: %" PRIu64
                    ", affected rows: %" PRIu64
                    " with %d thread(s) into %s %.2f records/second\n\n",
                    tInMs, g_args.totalInsertRows, g_args.totalAffectedRows,
                    threads, db_name, (double)(g_args.totalInsertRows / tInMs));
        }
    }

    if (minDelay != UINT64_MAX) {
        infoPrint(
            "insert delay, avg: %10.2fms, max: %10.2fms, min: %10.2fms\n\n",
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
    for (int i = 0; i < g_args.dbCount; i++) {
        if (g_args.use_metric) {
            if (db[i].superTblCount > 0) {
                for (uint64_t j = 0; j < db[i].superTblCount; j++) {
                    SSuperTable *stbInfo = &db[i].superTbls[j];

                    if (stbInfo && (stbInfo->insertRows > 0)) {
                        if (startMultiThreadInsertData(
                                g_args.nthreads, db[i].dbName,
                                db[i].dbCfg.precision, stbInfo)) {
                            goto end_insert_process;
                        }
                    }
                }
            }
        } else {
            if (SML_IFACE == g_args.iface) {
                code = -1;
                errorPrint("%s\n", "Schemaless insertion must include stable");
                goto end_insert_process;
            } else {
                if (startMultiThreadInsertData(g_args.nthreads, db[i].dbName,
                                               db[i].dbCfg.precision, NULL)) {
                    goto end_insert_process;
                }
            }
        }
    }
    code = 0;
end_insert_process:
    tmfree(cmdBuffer);
    return code;
}