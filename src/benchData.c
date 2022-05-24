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

const char charset[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

static int usc2utf8(char *p, int unic) {
    if (unic <= 0x0000007F) {
        *p = (unic & 0x7F);
        return 1;
    } else if (unic >= 0x00000080 && unic <= 0x000007FF) {
        *(p + 1) = (unic & 0x3F) | 0x80;
        *p = ((unic >> 6) & 0x1F) | 0xC0;
        return 2;
    } else if (unic >= 0x00000800 && unic <= 0x0000FFFF) {
        *(p + 2) = (unic & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 6) & 0x3F) | 0x80;
        *p = ((unic >> 12) & 0x0F) | 0xE0;
        return 3;
    } else if (unic >= 0x00010000 && unic <= 0x001FFFFF) {
        *(p + 3) = (unic & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 12) & 0x3F) | 0x80;
        *p = ((unic >> 18) & 0x07) | 0xF0;
        return 4;
    } else if (unic >= 0x00200000 && unic <= 0x03FFFFFF) {
        *(p + 4) = (unic & 0x3F) | 0x80;
        *(p + 3) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 12) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 18) & 0x3F) | 0x80;
        *p = ((unic >> 24) & 0x03) | 0xF8;
        return 5;
    } else if (unic >= 0x04000000) {
        *(p + 5) = (unic & 0x3F) | 0x80;
        *(p + 4) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 3) = ((unic >> 12) & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 18) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 24) & 0x3F) | 0x80;
        *p = ((unic >> 30) & 0x01) | 0xFC;
        return 6;
    }
    return 0;
}

static void rand_string(char *str, int size, bool chinese) {
    if (chinese) {
        char *pstr = str;
        int   move = 0;
        while (size > 0) {
            // Chinese Character need 3 bytes space
            if (size < 3) {
                break;
            }
            // Basic Chinese Character's Unicode is from 0x4e00 to 0x9fa5
            int unic = 0x4e00 + taosRandom() % (0x9fa5 - 0x4e00);
            move = usc2utf8(pstr, unic);
            pstr += move;
            size -= move;
        }
    } else {
        str[0] = 0;
        if (size > 0) {
            //--size;
            int n;
            for (n = 0; n < size; n++) {
                int key = abs(taosRandom()) % (int)(sizeof(charset) - 1);
                str[n] = charset[key];
            }
            str[n] = 0;
        }
    }
}

int stmt_prepare(SSuperTable *stbInfo, TAOS_STMT *stmt, uint64_t tableSeq) {
    int   len = 0;
    char *prepare = calloc(1, BUFFER_SIZE);
    if (prepare == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    
    g_memoryUsage += BUFFER_SIZE;
    if (stbInfo->autoCreateTable) {
        len += sprintf(prepare + len,
                       "INSERT INTO ? USING `%s` TAGS (%s) VALUES(?",
                       stbInfo->stbName,
                       stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq);
    } else {
        len += sprintf(prepare + len, "INSERT INTO ? VALUES(?");
    }

    int columnCount = stbInfo->columnCount;
    for (int col = 0; col < columnCount; col++) {
        len += sprintf(prepare + len, ",?");
    }
    sprintf(prepare + len, ")");
    if (g_arguments->prepared_rand < g_arguments->reqPerReq) {
        infoPrint(stdout,
                  "in stmt mode, batch size(%u) can not larger than prepared "
                  "sample data size(%" PRId64
                  "), restart with larger prepared_rand or batch size will be "
                  "auto set to %" PRId64 "\n",
                  g_arguments->reqPerReq, g_arguments->prepared_rand,
                  g_arguments->prepared_rand);
        g_arguments->reqPerReq = g_arguments->prepared_rand;
    }
    if (taos_stmt_prepare(stmt, prepare, strlen(prepare))) {
        errorPrint(stderr, "taos_stmt_prepare(%s) failed\n", prepare);
        tmfree(prepare);
        return -1;
    }
    tmfree(prepare);
    return 0;
}

static int generateSampleFromCsvForStb(char *buffer, char *file, int32_t length,
                                       int64_t size) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;
    int     getRows = 0;

    FILE *fp = fopen(file, "r");
    if (fp == NULL) {
        errorPrint(stderr, "Failed to open sample file: %s, reason:%s\n", file,
                   strerror(errno));
        return -1;
    }
    while (1) {
        readLen = getline(&line, &n, fp);
        if (-1 == readLen) {
            if (0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint(stderr, "Failed to fseek file: %s, reason:%s\n",
                           file, strerror(errno));
                fclose(fp);
                return -1;
            }
            continue;
        }

        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        if (readLen > length) {
            infoPrint(
                stdout,
                "sample row len[%d] overflow define schema len[%d], so discard "
                "this row\n",
                (int32_t)readLen, length);
            continue;
        }

        memcpy(buffer + getRows * length, line, readLen);
        getRows++;

        if (getRows == size) {
            break;
        }
    }

    fclose(fp);
    tmfree(line);
    return 0;
}

static int getAndSetRowsFromCsvFile(SSuperTable *stbInfo) {
    int32_t code = -1;
    FILE *  fp = fopen(stbInfo->sampleFile, "r");
    int     line_count = 0;
    char *  buf;
    if (fp == NULL) {
        errorPrint(stderr, "Failed to open sample file: %s, reason:%s\n",
                   stbInfo->sampleFile, strerror(errno));
        goto free_of_get_set_rows_from_csv;
    }
    buf = calloc(1, TSDB_MAX_SQL_LEN);
    if (buf == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    while (fgets(buf, TSDB_MAX_SQL_LEN, fp)) {
        line_count++;
    }
    stbInfo->insertRows = line_count;
    code = 0;
free_of_get_set_rows_from_csv:
    fclose(fp);
    tmfree(buf);
    return code;
}

static void calcRowLen(SSuperTable *stbInfo) {
    stbInfo->lenOfCols = 0;
    stbInfo->lenOfTags = 0;
    for (int colIndex = 0; colIndex < stbInfo->columnCount; colIndex++) {
        switch (stbInfo->columns[colIndex].type) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                stbInfo->lenOfCols += stbInfo->columns[colIndex].length + 3;
                break;

            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                stbInfo->lenOfCols += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                stbInfo->lenOfCols += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                stbInfo->lenOfCols += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                stbInfo->lenOfCols += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                stbInfo->lenOfCols += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                stbInfo->lenOfCols += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                stbInfo->lenOfCols += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                stbInfo->lenOfCols += TIMESTAMP_BUFF_LEN;
                break;
        }
        stbInfo->lenOfCols += 1;
        if (stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) {
            stbInfo->lenOfCols += SML_LINE_SQL_SYNTAX_OFFSET +
                                  strlen(stbInfo->columns[colIndex].name);
        }
    }
    stbInfo->lenOfCols += TIMESTAMP_BUFF_LEN;

    for (int tagIndex = 0; tagIndex < stbInfo->tagCount; tagIndex++) {
        switch (stbInfo->tags[tagIndex].type) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                stbInfo->lenOfTags += stbInfo->tags[tagIndex].length + 4;
                break;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                stbInfo->lenOfTags += INT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                stbInfo->lenOfTags += BIGINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                stbInfo->lenOfTags += SMALLINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                stbInfo->lenOfTags += TINYINT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_BOOL:
                stbInfo->lenOfTags += BOOL_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_FLOAT:
                stbInfo->lenOfTags += FLOAT_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                stbInfo->lenOfTags += DOUBLE_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_JSON:
                stbInfo->lenOfTags +=
                    (JSON_BUFF_LEN + stbInfo->tags[tagIndex].length) *
                    stbInfo->tagCount;
                return;
        }
        stbInfo->lenOfTags += 1;
        if (stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) {
            stbInfo->lenOfTags += SML_LINE_SQL_SYNTAX_OFFSET +
                                  strlen(stbInfo->tags[tagIndex].name);
        }
    }

    if (stbInfo->iface == SML_IFACE || stbInfo->iface == SML_REST_IFACE) {
        stbInfo->lenOfTags +=
            2 * TSDB_TABLE_NAME_LEN * 2 + SML_LINE_SQL_SYNTAX_OFFSET;
    }
    return;
}

void generateRandData(SSuperTable *stbInfo, char *sampleDataBuf,
                      int lenOfOneRow, Column *columns, int count, int loop,
                      bool tag) {
    int     iface = stbInfo->iface;
    int     line_protocol = stbInfo->lineProtocol;
    int32_t pos = 0;
    if (iface == STMT_IFACE) {
        for (int i = 0; i < count; ++i) {
            if (columns[i].type == TSDB_DATA_TYPE_BINARY ||
                columns[i].type == TSDB_DATA_TYPE_NCHAR) {
                columns[i].data = calloc(1, loop * (columns[i].length + 1));
                g_memoryUsage += loop * (columns[i].length + 1);
            } else {
                columns[i].data = calloc(1, loop * columns[i].length);
                g_memoryUsage += loop * columns[i].length;
            }
            if (NULL == columns[i].data) {
                errorPrint(stderr, "%s", "memory allocation failed\n");
                exit(EXIT_FAILURE);
            }
        }
    }
    for (int k = 0; k < loop; ++k) {
        pos = k * lenOfOneRow;
        if (line_protocol == TSDB_SML_LINE_PROTOCOL &&
            (iface == SML_IFACE || iface == SML_REST_IFACE) && tag) {
            pos += sprintf(sampleDataBuf + pos, "%s,", stbInfo->stbName);
        }
        for (int i = 0; i < count; ++i) {
            if (iface == TAOSC_IFACE || iface == REST_IFACE) {
                if (columns[i].null) {
                    continue;
                }
                if (columns[i].length == 0) {
                    pos += sprintf(sampleDataBuf + pos, "null,");
                    continue;
                }
            }
            switch (columns[i].type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool rand_bool = (taosRandom() % 2) & 1;
                    if (iface == STMT_IFACE) {
                        *(bool *)(columns[i].data + k * columns[i].length) =
                            rand_bool;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%s,",
                                       columns[i].name,
                                       rand_bool ? "true" : "false");
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%s ",
                                           columns[i].name,
                                           rand_bool ? "true" : "false");
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "%s ",
                                           rand_bool ? "true" : "false");
                        }
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_bool ? "true" : "false");
                    }
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    if (columns[i].min < -127) {
                        columns[i].min = -127;
                    }
                    if (columns[i].max > 128) {
                        columns[i].max = 128;
                    }
                    int8_t tinyint =
                        columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(int8_t *)(columns[i].data + k * columns[i].length) =
                            tinyint;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%di8,",
                                       columns[i].name, tinyint);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%di8 ",
                                           columns[i].name, tinyint);
                        } else {
                            pos +=
                                sprintf(sampleDataBuf + pos, "%di8 ", tinyint);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%d,", tinyint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    if (columns[i].min < 0) {
                        columns[i].min = 0;
                    }
                    if (columns[i].max > 254) {
                        columns[i].max = 254;
                    }
                    uint8_t utinyint =
                        columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(uint8_t *)(columns[i].data + k * columns[i].length) =
                            utinyint;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%uu8,",
                                       columns[i].name, utinyint);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%uu8 ",
                                           columns[i].name, utinyint);
                        } else {
                            pos +=
                                sprintf(sampleDataBuf + pos, "%uu8 ", utinyint);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%u,", utinyint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    if (columns[i].min < -32767) {
                        columns[i].min = -32767;
                    }
                    if (columns[i].max > 32767) {
                        columns[i].max = 32767;
                    }
                    int16_t smallint =
                        columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(int16_t *)(columns[i].data + k * columns[i].length) =
                            smallint;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%di16,",
                                       columns[i].name, smallint);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%di16 ",
                                           columns[i].name, smallint);
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "%di16 ",
                                           smallint);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%d,", smallint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    if (columns[i].min < 0) {
                        columns[i].min = 0;
                    }
                    if (columns[i].max > 65534) {
                        columns[i].max = 65534;
                    }
                    uint16_t usmallint =
                        columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(uint16_t *)(columns[i].data + k * columns[i].length) =
                            usmallint;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%uu16,",
                                       columns[i].name, usmallint);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%uu16 ",
                                           columns[i].name, usmallint);
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "%uu16 ",
                                           usmallint);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%u,", usmallint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t int_;
                    if ((g_arguments->demo_mode) && (i == 0)) {
                        int_ = taosRandom() % 10 + 1;
                    } else if ((g_arguments->demo_mode) && (i == 1)) {
                        int_ = 215 + taosRandom() % 10;
                    } else {
                        if (columns[i].min < (-1 * (RAND_MAX >> 1))) {
                            columns[i].min = -1 * (RAND_MAX >> 1);
                        }
                        if (columns[i].max > (RAND_MAX >> 1)) {
                            columns[i].max = RAND_MAX >> 1;
                        }
                        int_ =
                            columns[i].min +
                            (taosRandom() % (columns[i].max - columns[i].min));
                    }
                    if (iface == STMT_IFACE) {
                        *(int32_t *)(columns[i].data + k * columns[i].length) =
                            int_;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%di32,",
                                       columns[i].name, int_);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%di32 ",
                                           columns[i].name, int_);
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "%di32 ", int_);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%d,", int_);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int32_t int_;
                    if (columns[i].min < (-1 * (RAND_MAX >> 1))) {
                        columns[i].min = -1 * (RAND_MAX >> 1);
                    }
                    if (columns[i].max > (RAND_MAX >> 1)) {
                        columns[i].max = RAND_MAX >> 1;
                    }
                    int_ = columns[i].min +
                           (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(int64_t *)(columns[i].data + k * columns[i].length) =
                            (int64_t)int_;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%di64,",
                                       columns[i].name, int_);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%di64 ",
                                           columns[i].name, int_);
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "%di64 ", int_);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%d,", int_);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    if (columns[i].min < 0) {
                        columns[i].min = 0;
                    }
                    if (columns[i].max > RAND_MAX) {
                        columns[i].max = RAND_MAX;
                    }
                    uint32_t uint =
                        columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(uint32_t *)(columns[i].data + k * columns[i].length) =
                            uint;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%uu32,",
                                       columns[i].name, uint);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%uu32 ",
                                           columns[i].name, uint);
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "%uu32 ", uint);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%u,", uint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    if (columns[i].min < 0) {
                        columns[i].min = 0;
                    }
                    if (columns[i].max > RAND_MAX) {
                        columns[i].max = RAND_MAX;
                    }
                    uint32_t ubigint =
                        columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min));
                    if (iface == STMT_IFACE) {
                        *(uint64_t *)(columns[i].data + k * columns[i].length) =
                            (uint64_t)ubigint;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        (line_protocol == TSDB_SML_LINE_PROTOCOL ||
                         (tag && line_protocol == TSDB_SML_TELNET_PROTOCOL))) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%uu64,",
                                       columns[i].name, ubigint);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%uu64 ",
                                           columns[i].name, ubigint);
                        } else {
                            pos +=
                                sprintf(sampleDataBuf + pos, "%uu64 ", ubigint);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%u,", ubigint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    if (columns[i].min < ((RAND_MAX >> 1) * -1)) {
                        columns[i].min = (RAND_MAX >> 1) * -1;
                    }
                    if (columns[i].max > RAND_MAX >> 1) {
                        columns[i].max = RAND_MAX >> 1;
                    }
                    float float_ = (float)(columns[i].min +
                                           (taosRandom() %
                                            (columns[i].max - columns[i].min)) +
                                           (taosRandom() % 1000) / 1000.0);
                    if (g_arguments->demo_mode && i == 0) {
                        float_ = (float)(9.8 + 0.04 * (taosRandom() % 10) +
                                         float_ / 1000000000);
                    } else if (g_arguments->demo_mode && i == 2) {
                        float_ = (float)((115 + taosRandom() % 10 +
                                          float_ / 1000000000) /
                                         360);
                    }
                    if (iface == STMT_IFACE) {
                        *(float *)(columns[i].data + k * columns[i].length) =
                            float_;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%ff32,",
                                       columns[i].name, float_);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%ff32 ",
                                           columns[i].name, float_);
                        } else {
                            pos +=
                                sprintf(sampleDataBuf + pos, "%ff32 ", float_);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%f,", float_);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    if (columns[i].min < ((RAND_MAX >> 1) * -1)) {
                        columns[i].min = (RAND_MAX >> 1) * -1;
                    }
                    if (columns[i].max > RAND_MAX >> 1) {
                        columns[i].max = RAND_MAX >> 1;
                    }
                    double double_ =
                        (double)(columns[i].min +
                                 (taosRandom() %
                                  (columns[i].max - columns[i].min)) +
                                 taosRandom() % 1000000 / 1000000.0);
                    if (iface == STMT_IFACE) {
                        *(double *)(columns[i].data + k * columns[i].length) =
                            double_;
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=%ff64,",
                                       columns[i].name, double_);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=%ff64 ",
                                           columns[i].name, double_);
                        } else {
                            pos +=
                                sprintf(sampleDataBuf + pos, "%ff64 ", double_);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%f,", double_);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = calloc(1, columns[i].length + 1);
                    if (tmp == NULL) {
                        errorPrint(stderr, "%s", "memory allocation failed\n");
                        exit(EXIT_FAILURE);
                    }
                    if (g_arguments->demo_mode) {
                        if (taosRandom() % 2 == 1) {
                            if (g_arguments->chinese) {
                                sprintf(tmp, "上海");
                            } else {
                                sprintf(tmp, "shanghai");
                            }
                        } else {
                            if (g_arguments->chinese) {
                                sprintf(tmp, "北京");
                            } else {
                                sprintf(tmp, "beijing");
                            }
                        }
                    } else if (columns[i].values) {
                        cJSON *buf = cJSON_GetArrayItem(
                            columns[i].values,
                            taosRandom() %
                                cJSON_GetArraySize(columns[i].values));
                        sprintf(tmp, "%s", buf->valuestring);
                    } else {
                        rand_string(tmp, columns[i].length,
                                    g_arguments->chinese);
                    }
                    if (iface == STMT_IFACE) {
                        sprintf((char *)columns[i].data + k * columns[i].length,
                                "%s", tmp);
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        columns[i].type == TSDB_DATA_TYPE_BINARY &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=\"%s\",",
                                       columns[i].name, tmp);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               columns[i].type == TSDB_DATA_TYPE_NCHAR &&
                               line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s=L\"%s\",",
                                       columns[i].name, tmp);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               columns[i].type == TSDB_DATA_TYPE_BINARY &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=L\"%s\" ",
                                           columns[i].name, tmp);
                        } else {
                            pos += sprintf(sampleDataBuf + pos, "\"%s\" ", tmp);
                        }

                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               columns[i].type == TSDB_DATA_TYPE_NCHAR &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        if (tag) {
                            pos += sprintf(sampleDataBuf + pos, "%s=L\"%s\" ",
                                           columns[i].name, tmp);
                        } else {
                            pos +=
                                sprintf(sampleDataBuf + pos, "L\"%s\" ", tmp);
                        }

                    } else {
                        pos += sprintf(sampleDataBuf + pos, "'%s',", tmp);
                    }
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    pos += sprintf(sampleDataBuf + pos, "'{");
                    for (int j = 0; j < count; ++j) {
                        pos += sprintf(sampleDataBuf + pos, "\"k%d\":", j);
                        char *buf = calloc(1, columns[j].length + 1);
                        if (buf == NULL) {
                            errorPrint(stderr, "%s", "memory allocation failed\n");
                            exit(EXIT_FAILURE);
                        }
                        rand_string(buf, columns[j].length,
                                    g_arguments->chinese);
                        pos += sprintf(sampleDataBuf + pos, "\"%s\",", buf);
                        tmfree(buf);
                    }
                    pos += sprintf(sampleDataBuf + pos - 1, "}'");
                    goto skip;
                }
            }
        }
    skip:
        *(sampleDataBuf + pos - 1) = 0;
    }
}

int prepare_sample_data(int db_index, int stb_index) {
    SDataBase *  database = &(g_arguments->db[db_index]);
    SSuperTable *stbInfo = &(database->superTbls[stb_index]);
    calcRowLen(stbInfo);
    if (stbInfo->partialColumnNum != 0 &&
        (stbInfo->iface == TAOSC_IFACE || stbInfo->iface == REST_IFACE)) {
        if (stbInfo->partialColumnNum > stbInfo->columnCount) {
            stbInfo->partialColumnNum = stbInfo->columnCount;
        } else {
            stbInfo->partialColumnNameBuf = calloc(1, BUFFER_SIZE);
            if (NULL == stbInfo->partialColumnNameBuf) {
                errorPrint(stderr, "%s", "memory allocation failed\n");
                exit(EXIT_FAILURE);
            }
            g_memoryUsage += BUFFER_SIZE;
            int pos = 0;
            pos += sprintf(stbInfo->partialColumnNameBuf + pos, "ts");
            for (int i = 0; i < stbInfo->partialColumnNum; ++i) {
                pos += sprintf(stbInfo->partialColumnNameBuf + pos, ",%s",
                               stbInfo->columns[i].name);
            }
            for (int i = stbInfo->partialColumnNum; i < stbInfo->columnCount;
                 ++i) {
                stbInfo->columns[i].null = true;
            }
            debugPrint(stdout, "partialColumnNameBuf: %s\n",
                       stbInfo->partialColumnNameBuf);
        }
    } else {
        stbInfo->partialColumnNum = stbInfo->columnCount;
    }
    stbInfo->sampleDataBuf =
        calloc(1, stbInfo->lenOfCols * g_arguments->prepared_rand);
    if (stbInfo->sampleDataBuf == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }    
    infoPrint(stdout,
              "generate stable<%s> columns data with lenOfCols<%u> * "
              "prepared_rand<%" PRIu64 ">\n",
              stbInfo->stbName, stbInfo->lenOfCols, g_arguments->prepared_rand);
    g_memoryUsage += stbInfo->lenOfCols * g_arguments->prepared_rand;
    if (stbInfo->random_data_source) {
        generateRandData(stbInfo, stbInfo->sampleDataBuf, stbInfo->lenOfCols,
                         stbInfo->columns, stbInfo->columnCount,
                         g_arguments->prepared_rand, false);
    } else {
        if (stbInfo->useSampleTs) {
            if (getAndSetRowsFromCsvFile(stbInfo)) return -1;
        }
        if (generateSampleFromCsvForStb(stbInfo->sampleDataBuf,
                                        stbInfo->sampleFile, stbInfo->lenOfCols,
                                        g_arguments->prepared_rand)) {
            return -1;
        }
    }
    debugPrint(stdout, "sampleDataBuf: %s\n", stbInfo->sampleDataBuf);

    if (!stbInfo->childTblExists && stbInfo->tagCount != 0) {
        stbInfo->tagDataBuf =
            calloc(1, stbInfo->childTblCount * stbInfo->lenOfTags);
        if (NULL == stbInfo->tagDataBuf) {
            errorPrint(stderr, "%s() LN%d, failed to alloc memory!\n", __func__, __LINE__);
            return -1;
        }
        infoPrint(stdout,
                  "generate stable<%s> tags data with lenOfTags<%u> * "
                  "childTblCount<%" PRIu64 ">\n",
                  stbInfo->stbName, stbInfo->lenOfTags, stbInfo->childTblCount);
        g_memoryUsage += stbInfo->childTblCount * stbInfo->lenOfTags;
        if (stbInfo->tagsFile[0] != 0) {
            if (generateSampleFromCsvForStb(
                    stbInfo->tagDataBuf, stbInfo->tagsFile, stbInfo->lenOfTags,
                    stbInfo->childTblCount)) {
                return -1;
            }
        } else {
            generateRandData(stbInfo, stbInfo->tagDataBuf, stbInfo->lenOfTags,
                             stbInfo->tags, stbInfo->tagCount,
                             stbInfo->childTblCount, true);
        }
        debugPrint(stdout, "tagDataBuf: %s\n", stbInfo->tagDataBuf);
    }

    if (stbInfo->iface == REST_IFACE || stbInfo->iface == SML_REST_IFACE) {
        if (stbInfo->tcpTransfer && stbInfo->iface == SML_REST_IFACE) {
            if (convertHostToServAddr(g_arguments->host,
                                      g_arguments->telnet_tcp_port,
                                      &(g_arguments->serv_addr))) {
                errorPrint(stderr, "%s\n", "convert host to server address");
                return -1;
            }
        } else {
            if (convertHostToServAddr(g_arguments->host,
                                      g_arguments->port + TSDB_PORT_HTTP,
                                      &(g_arguments->serv_addr))) {
                errorPrint(stderr, "%s\n", "convert host to server address");
                return -1;
            }
        }
    }
    return 0;
}

int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
                      int disorderRange) {
    int64_t randTail = timeStampStep * seq;
    if (disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if (rand_num < disorderRatio) {
            randTail = (randTail + (taosRandom() % disorderRange + 1)) * (-1);
        }
    }
    return randTail;
}

int bindParamBatch(threadInfo *pThreadInfo, uint32_t batch, int64_t startTime) {
    TAOS_STMT *  stmt = pThreadInfo->stmt;
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    uint32_t     columnCount = stbInfo->columnCount;
    memset(pThreadInfo->bindParams, 0,
           (sizeof(TAOS_MULTI_BIND) * (columnCount + 1)));
    memset(pThreadInfo->is_null, 0, batch);

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
            data_type = stbInfo->columns[c - 1].type;
            param->buffer = stbInfo->columns[c - 1].data;
            param->buffer_length = stbInfo->columns[c - 1].length;
            debugPrint(stdout, "col[%d]: type: %s, len: %d\n", c,
                       taos_convert_datatype_to_string(data_type),
                       stbInfo->columns[c - 1].length);
        }
        param->buffer_type = data_type;
        param->length = calloc(batch, sizeof(int32_t));
        if (param->length == NULL) {
            errorPrint(stderr, "%s", "memory allocation failed\n");
            exit(EXIT_FAILURE);
        }
        g_memoryUsage += batch * sizeof(int32_t);

        for (int b = 0; b < batch; b++) {
            param->length[b] = (int32_t)param->buffer_length;
        }
        param->is_null = pThreadInfo->is_null;
        param->num = batch;
    }

    for (uint32_t k = 0; k < batch; k++) {
        /* columnCount + 1 (ts) */
        if (stbInfo->disorderRatio) {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + getTSRandTail(stbInfo->timestamp_step, k,
                                          stbInfo->disorderRatio,
                                          stbInfo->disorderRange);
        } else {
            *(pThreadInfo->bind_ts_array + k) =
                startTime + stbInfo->timestamp_step * k;
        }
    }

    if (taos_stmt_bind_param_batch(
            stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams)) {
        errorPrint(stderr, "taos_stmt_bind_param_batch() failed! reason: %s\n",
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
        errorPrint(stderr, "taos_stmt_add_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return -1;
    }
    return batch;
}

int32_t generateSmlJsonTags(cJSON *tagsList, SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    int32_t code = -1;
    Column *columns = stbInfo->tags;
    cJSON * tags = cJSON_CreateObject();
    char *  tbName = calloc(1, TSDB_TABLE_NAME_LEN);
    if (tbName == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64 "",
             stbInfo->childTblPrefix, tbSeq + start_table_from);
    cJSON_AddStringToObject(tags, "id", tbName);
    char *tagName = calloc(1, TSDB_MAX_TAGS);
    if (tagName == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < stbInfo->tagCount; i++) {
        cJSON *tag = cJSON_CreateObject();
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (stbInfo->tags[i].type) {
            case TSDB_DATA_TYPE_BOOL: {
                cJSON_AddBoolToObject(tag, "value", (taosRandom() % 2) & 1);
                cJSON_AddStringToObject(tag, "type", "bool");
                break;
            }

            case TSDB_DATA_TYPE_TINYINT: {
                cJSON_AddNumberToObject(
                    tag, "value",
                    columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min)));
                cJSON_AddStringToObject(tag, "type", "tinyint");
                break;
            }

            case TSDB_DATA_TYPE_SMALLINT: {
                cJSON_AddNumberToObject(
                    tag, "value",
                    columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min)));
                cJSON_AddStringToObject(tag, "type", "smallint");
                break;
            }

            case TSDB_DATA_TYPE_INT: {
                cJSON_AddNumberToObject(
                    tag, "value",
                    columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min)));
                cJSON_AddStringToObject(tag, "type", "int");
                break;
            }

            case TSDB_DATA_TYPE_BIGINT: {
                cJSON_AddNumberToObject(
                    tag, "value",
                    (double)columns[i].min +
                        (taosRandom() % (columns[i].max - columns[i].min)));
                cJSON_AddStringToObject(tag, "type", "bigint");
                break;
            }

            case TSDB_DATA_TYPE_FLOAT: {
                cJSON_AddNumberToObject(
                    tag, "value",
                    (float)(columns[i].min +
                            (taosRandom() % (columns[i].max - columns[i].min)) +
                            taosRandom() % 1000 / 1000.0));
                cJSON_AddStringToObject(tag, "type", "float");
                break;
            }

            case TSDB_DATA_TYPE_DOUBLE: {
                cJSON_AddNumberToObject(
                    tag, "value",
                    (double)(columns[i].min +
                             (taosRandom() %
                              (columns[i].max - columns[i].min)) +
                             taosRandom() % 1000000 / 1000000.0));
                cJSON_AddStringToObject(tag, "type", "double");
                break;
            }

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)calloc(stbInfo->tags[i].length + 1, 1);
                if (buf == NULL) {
                    errorPrint(stderr, "%s", "memory allocation failed\n");
                    exit(EXIT_FAILURE);
                }
                rand_string(buf, stbInfo->tags[i].length, g_arguments->chinese);
                if (stbInfo->tags[i].type == TSDB_DATA_TYPE_BINARY) {
                    cJSON_AddStringToObject(tag, "value", buf);
                    cJSON_AddStringToObject(tag, "type", "binary");
                } else {
                    cJSON_AddStringToObject(tag, "value", buf);
                    cJSON_AddStringToObject(tag, "type", "nchar");
                }
                tmfree(buf);
                break;
            }
            default:
                errorPrint(
                    stderr,
                    "unknown data type (%d) for schemaless json protocol\n",
                    stbInfo->tags[i].type);
                goto free_of_generate_sml_json_tag;
        }
        cJSON_AddItemToObject(tags, tagName, tag);
    }
    cJSON_AddItemToArray(tagsList, tags);
    code = 0;
free_of_generate_sml_json_tag:
    tmfree(tagName);
    tmfree(tbName);
    return code;
}

int32_t generateSmlJsonCols(cJSON *array, cJSON *tag, SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp) {
    cJSON * record = cJSON_CreateObject();
    cJSON * ts = cJSON_CreateObject();
    Column *columns = stbInfo->columns;
    cJSON_AddNumberToObject(ts, "value", (double)timestamp);
    if (time_precision == TSDB_SML_TIMESTAMP_MILLI_SECONDS) {
        cJSON_AddStringToObject(ts, "type", "ms");
    } else if (time_precision == TSDB_SML_TIMESTAMP_MICRO_SECONDS) {
        cJSON_AddStringToObject(ts, "type", "us");
    } else if (time_precision == TSDB_SML_TIMESTAMP_NANO_SECONDS) {
        cJSON_AddStringToObject(ts, "type", "ns");
    } else {
        errorPrint(stderr, "Unknown time precision %d\n", time_precision);
        return -1;
    }
    cJSON *value = cJSON_CreateObject();
    switch (stbInfo->columns[0].type) {
        case TSDB_DATA_TYPE_BOOL:
            cJSON_AddBoolToObject(value, "value", (taosRandom() % 2) & 1);
            cJSON_AddStringToObject(value, "type", "bool");
            break;
        case TSDB_DATA_TYPE_TINYINT:
            cJSON_AddNumberToObject(
                value, "value",
                columns[0].min +
                    (taosRandom() % (columns[0].max - columns[0].min)));
            cJSON_AddStringToObject(value, "type", "tinyint");
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            cJSON_AddNumberToObject(
                value, "value",
                columns[0].min +
                    (taosRandom() % (columns[0].max - columns[0].min)));
            cJSON_AddStringToObject(value, "type", "smallint");
            break;
        case TSDB_DATA_TYPE_INT:
            cJSON_AddNumberToObject(
                value, "value",
                columns[0].min +
                    (taosRandom() % (columns[0].max - columns[0].min)));
            cJSON_AddStringToObject(value, "type", "int");
            break;
        case TSDB_DATA_TYPE_BIGINT:
            cJSON_AddNumberToObject(
                value, "value",
                (double)columns[0].min +
                    (taosRandom() % (columns[0].max - columns[0].min)));
            cJSON_AddStringToObject(value, "type", "bigint");
            break;
        case TSDB_DATA_TYPE_FLOAT:
            cJSON_AddNumberToObject(
                value, "value",
                (float)(columns[0].min +
                        (taosRandom() % (columns[0].max - columns[0].min)) +
                        taosRandom() % 1000 / 1000.0));
            cJSON_AddStringToObject(value, "type", "float");
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            cJSON_AddNumberToObject(
                value, "value",
                (double)(columns[0].min +
                         (taosRandom() % (columns[0].max - columns[0].min)) +
                         taosRandom() % 1000000 / 1000000.0));
            cJSON_AddStringToObject(value, "type", "double");
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)calloc(stbInfo->columns[0].length + 1, 1);
            if (buf == NULL) {
                errorPrint(stderr, "%s", "memory allocation failed\n");
                exit(EXIT_FAILURE);
            }
            rand_string(buf, stbInfo->columns[0].length, g_arguments->chinese);
            if (stbInfo->columns[0].type == TSDB_DATA_TYPE_BINARY) {
                cJSON_AddStringToObject(value, "value", buf);
                cJSON_AddStringToObject(value, "type", "binary");
            } else {
                cJSON_AddStringToObject(value, "value", buf);
                cJSON_AddStringToObject(value, "type", "nchar");
            }
            tmfree(buf);
            break;
        }
        default:
            errorPrint(stderr,
                       "unknown data type (%d) for schemaless json protocol\n",
                       stbInfo->columns[0].type);
            return -1;
    }
    cJSON_AddItemToObject(record, "timestamp", ts);
    cJSON_AddItemToObject(record, "value", value);
    cJSON_AddItemToObject(record, "tags", tag);
    cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    cJSON_AddItemToArray(array, record);
    return 0;
}
