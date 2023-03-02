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
 */

#include <bench.h>
#include <benchData.h>

const char charset[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

const char* locations[] = {
    "California.SanFrancisco", "California.LosAngles",
    "California.SanDiego", "California.SanJose",
    "California.PaloAlto", "California.Campbell",
    "California.MountainView", "California.Sunnyvale",
    "California.SantaClara", "California.Cupertino"};

const char* locations_sml[] = {
    "California.SanFrancisco", "California.LosAngles",
    "California.SanDiego", "California.SanJose",
    "California.PaloAlto", "California.Campbell",
    "California.MountainView", "California.Sunnyvale",
    "California.SantaClara", "California.Cupertino"};

#ifdef WINDOWS
    #define ssize_t int
    #if _MSC_VER >= 1910
        #include "benchLocations.h"
    #else
        #include "benchLocationsWin.h"
    #endif
#else
    #include "benchLocations.h"
#endif

static int usc2utf8(char *p, int unic) {
    int ret = 0;
    if (unic <= 0x0000007F) {
        *p = (unic & 0x7F);
        ret = 1;
    } else if (unic <= 0x000007FF) {
        *(p + 1) = (unic & 0x3F) | 0x80;
        *p = ((unic >> 6) & 0x1F) | 0xC0;
        ret = 2;
    } else if (unic <= 0x0000FFFF) {
        *(p + 2) = (unic & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 6) & 0x3F) | 0x80;
        *p = ((unic >> 12) & 0x0F) | 0xE0;
        ret = 3;
    } else if (unic <= 0x001FFFFF) {
        *(p + 3) = (unic & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 12) & 0x3F) | 0x80;
        *p = ((unic >> 18) & 0x07) | 0xF0;
        ret = 4;
    } else if (unic <= 0x03FFFFFF) {
        *(p + 4) = (unic & 0x3F) | 0x80;
        *(p + 3) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 12) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 18) & 0x3F) | 0x80;
        *p = ((unic >> 24) & 0x03) | 0xF8;
        ret = 5;
    // } else if (unic >= 0x04000000) {
    } else {
        *(p + 5) = (unic & 0x3F) | 0x80;
        *(p + 4) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 3) = ((unic >> 12) & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 18) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 24) & 0x3F) | 0x80;
        *p = ((unic >> 30) & 0x01) | 0xFC;
        ret = 6;
    }

    return ret;
}

static void rand_string(char *str, int size, bool chinese) {
    if (chinese) {
        char *pstr = str;
        while (size > 0) {
            // Chinese Character need 3 bytes space
            if (size < 3) {
                break;
            }
            // Basic Chinese Character's Unicode is from 0x4e00 to 0x9fa5
            int unic = 0x4e00 + taosRandom() % (0x9fa5 - 0x4e00);
            int move = usc2utf8(pstr, unic);
            pstr += move;
            size -= move;
        }
    } else {
        str[0] = 0;
        if (size > 0) {
            // --size;
            int n;
            for (n = 0; n < size; n++) {
                int key = taosRandom() % (unsigned int)(sizeof(charset) - 1);
                str[n] = charset[key];
            }
            str[n] = 0;
        }
    }
}

int prepareStmt(SSuperTable *stbInfo, TAOS_STMT *stmt, uint64_t tableSeq) {
    int   len = 0;
    char *prepare = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
    int n;
    if (stbInfo->autoCreateTable) {
        char ttl[TTL_BUFF_LEN] = "";
        if (stbInfo->ttl != 0) {
            snprintf(ttl, TTL_BUFF_LEN, "TTL %d", stbInfo->ttl);
        }
        n = snprintf(prepare + len,
                       TSDB_MAX_ALLOWED_SQL_LEN - len,
                       "INSERT INTO ? USING `%s` TAGS (%s) %s VALUES(?",
                       stbInfo->stbName,
                       stbInfo->tagDataBuf + stbInfo->lenOfTags * tableSeq,
                       ttl);
    } else {
        n = snprintf(prepare + len, TSDB_MAX_ALLOWED_SQL_LEN - len,
                        "INSERT INTO ? VALUES(?");
    }

    if (n < 0 || n >= TSDB_MAX_ALLOWED_SQL_LEN - len) {
        errorPrint("%s() LN%d snprintf overflow\n", __func__, __LINE__);
        tmfree(prepare);
        return -1;
    } else {
        len += n;
    }

    for (int col = 0; col < stbInfo->cols->size; col++) {
        n = snprintf(prepare + len, TSDB_MAX_ALLOWED_SQL_LEN - len, ",?");
        if (n < 0 || n >= TSDB_MAX_ALLOWED_SQL_LEN - len) {
            errorPrint("%s() LN%d snprintf overflow on %d\n",
                       __func__, __LINE__, col);
            break;;
        } else {
            len += n;
        }
    }
    snprintf(prepare + len, TSDB_MAX_ALLOWED_SQL_LEN - len, ")");
    if (g_arguments->prepared_rand < g_arguments->reqPerReq) {
        infoPrint(
                  "in stmt mode, batch size(%u) can not larger than prepared "
                  "sample data size(%" PRId64
                  "), restart with larger prepared_rand or batch size will be "
                  "auto set to %" PRId64 "\n",
                  g_arguments->reqPerReq, g_arguments->prepared_rand,
                  g_arguments->prepared_rand);
        g_arguments->reqPerReq = g_arguments->prepared_rand;
    }
    if (taos_stmt_prepare(stmt, prepare, strlen(prepare))) {
        errorPrint("taos_stmt_prepare(%s) failed\n", prepare);
        tmfree(prepare);
        return -1;
    }
    tmfree(prepare);
    return 0;
}

static bool getSampleFileNameByPattern(char *filePath,
                                       SSuperTable *stbInfo,
                                       int64_t child) {
    char *pos = strstr(stbInfo->childTblSample, "XXXX");
    snprintf(filePath, MAX_PATH_LEN, "%s", stbInfo->childTblSample);
    int64_t offset = (int64_t)pos - (int64_t)stbInfo->childTblSample;
    snprintf(filePath + offset,
             MAX_PATH_LEN - offset,
            "%s",
            stbInfo->childTblArray[child]->childTableName);
    size_t len = strlen(stbInfo->childTblArray[child]->childTableName);
    snprintf(filePath + offset + len,
            MAX_PATH_LEN - offset - len,
            "%s", pos +4);
    return true;
}

static int generateSampleFromCsv(char *buffer,
                                 char *file, int32_t length,
                                 int64_t size) {
    size_t  n = 0;
    char *  line = NULL;
    int     getRows = 0;

    FILE *fp = fopen(file, "r");
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n", file,
                   strerror(errno));
        return -1;
    }
    while (1) {
        ssize_t readLen = 0;
#if defined(WIN32) || defined(WIN64)
        toolsGetLineFile(&line, &n, fp);
        readLen = n;
#else
        readLen = getline(&line, &n, fp);
#endif
        if (-1 == readLen) {
            if (0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint("Failed to fseek file: %s, reason:%s\n",
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

static int getAndSetRowsFromCsvFile(char *sampleFile, uint64_t *insertRows) {
    FILE *  fp = fopen(sampleFile, "r");
    if (NULL == fp) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   sampleFile, strerror(errno));
        return -1;
    }

    int     line_count = 0;
    char *  buf = NULL;

    buf = benchCalloc(1, TSDB_MAX_SQL_LEN, false);
    if (NULL == buf) {
        errorPrint("%s() failed to allocate memory!\n", __func__);
        fclose(fp);
        return -1;
    }

    while (fgets(buf, TSDB_MAX_SQL_LEN, fp)) {
        line_count++;
    }
    *insertRows = line_count;
    fclose(fp);
    tmfree(buf);
    return 0;
}

uint32_t accumulateRowLen(BArray *fields, int iface) {
    uint32_t len = 0;
    for (int i = 0; i < fields->size; ++i) {
        Field *field = benchArrayGet(fields, i);
        switch (field->type) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                len += field->length + 3;
                break;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                len += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                len += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                len += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                len += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                len += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                len += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                len += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                len += TIMESTAMP_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_JSON:
                len += (JSON_BUFF_LEN + field->length) * fields->size;
                return len;
        }
        len += 1;
        if (iface == SML_REST_IFACE || iface == SML_IFACE) {
            len += SML_LINE_SQL_SYNTAX_OFFSET + strlen(field->name);
        }
    }
    if (iface == SML_IFACE || iface == SML_REST_IFACE) {
        len += 2 * TSDB_TABLE_NAME_LEN * 2 + SML_LINE_SQL_SYNTAX_OFFSET;
    }
    len += TIMESTAMP_BUFF_LEN;
    return len;
}

static int tmpStr(char *tmp, int iface, Field *field, int i) {
    if (g_arguments->demo_mode) {
        unsigned int tmpRand = taosRandom();
        if (g_arguments->chinese) {
            snprintf(tmp, field->length, "%s",
                     locations_chinese[tmpRand % 10]);
        } else if (SML_IFACE == iface) {
            snprintf(tmp, field->length, "%s",
                     locations_sml[tmpRand % 10]);
        } else {
            snprintf(tmp, field->length, "%s",
                     locations[tmpRand % 10]);
        }
    } else if (field->values) {
        int arraySize = tools_cJSON_GetArraySize(field->values);
        if (arraySize) {
            tools_cJSON *buf = tools_cJSON_GetArrayItem(
                    field->values,
                    taosRandom() % arraySize);
            snprintf(tmp, field->length,
                     "%s", buf->valuestring);
        } else {
            errorPrint("%s() cannot read correct value "
                       "from json file. array size: %d\n",
                       __func__, arraySize);
            return -1;
        }
    } else {
        rand_string(tmp, field->length,
                    g_arguments->chinese);
    }
    return 0;
}

static float tmpFloat(Field *field, int i) {
    float floatTmp = (float)(field->min +
        (taosRandom() %
        (field->max - field->min)) +
        (taosRandom() % 1000) / 1000.0);
    if (g_arguments->demo_mode && i == 0) {
        floatTmp = (float)(9.8 + 0.04 * (taosRandom() % 10) +
            floatTmp / 1000000000);
    } else if (g_arguments->demo_mode && i == 2) {
        floatTmp = (float)((105 + taosRandom() % 10 +
            floatTmp / 1000000000) /
            360);
    }
    return floatTmp;
}

static int tmpInt32(Field *field, int i) {
    int intTmp;
    if ((g_arguments->demo_mode) && (i == 0)) {
        unsigned int tmpRand = taosRandom();
        intTmp = tmpRand % 10 + 1;
    } else if ((g_arguments->demo_mode) && (i == 1)) {
        intTmp = 105 + taosRandom() % 10;
    } else {
        if (field->min < (-1 * (RAND_MAX >> 1))) {
            field->min = -1 * (RAND_MAX >> 1);
        }
        if (field->max > (RAND_MAX >> 1)) {
            field->max = RAND_MAX >> 1;
        }
        intTmp = field->min + (taosRandom() %
            (field->max - field->min));
    }
    return intTmp;
}

static int tmpJson(char *sampleDataBuf,
                   int bufLen, int64_t pos, int fieldsSize, Field *field) {
    int n;
    n = snprintf(sampleDataBuf + pos, bufLen - pos, "'{");
    if (n < 0 || n >= bufLen - pos) {
        errorPrint("%s() LN%d snprintf overflow\n",
                   __func__, __LINE__);
        return -1;
    } else {
        pos += n;
    }
    for (int j = 0; j < fieldsSize; ++j) {
        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                        "\"k%d\":", j);
        if (n < 0 || n >= bufLen - pos) {
            errorPrint("%s() LN%d snprintf overflow\n",
                       __func__, __LINE__);
            return -1;
        } else {
            pos += n;
        }
        char *buf = benchCalloc(1, field->length + 1, false);
        rand_string(buf, field->length,
                    g_arguments->chinese);
        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                        "\"%s\",", buf);
        if (n < 0 || n >= bufLen - pos) {
            errorPrint("%s() LN%d snprintf overflow\n",
                       __func__, __LINE__);
            tmfree(buf);
            return -1;
        } else {
            pos += n;
        }
        tmfree(buf);
    }
    n = snprintf(sampleDataBuf + pos - 1,
                    bufLen - pos, "}'");
    if (n < 0 || n >= bufLen - pos) {
        errorPrint("%s() LN%d snprintf overflow\n",
                   __func__, __LINE__);
        return -1;
    } else {
        pos += n;
    }

    return pos;
}

static int generateRandDataSQL(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag) {
    for (int64_t k = 0; k < loop; ++k) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            if (field->none) {
                continue;
            }

            int n = 0;
            if (field->null) {
                n = snprintf(sampleDataBuf + pos, bufLen - pos, "null,");
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                    continue;
                }
            }
            if (field->type == TSDB_DATA_TYPE_TIMESTAMP && !tag) {
                n = snprintf(sampleDataBuf + pos, bufLen - pos, "now,");
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                    continue;
                }
            }
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool rand_bool = (taosRandom() % 2) & 1;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s,",
                                    rand_bool ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%d,", tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = field->min
                        + (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos,
                                    "%u,", utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = field->min
                        + (taosRandom() % (field->max -field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = field->min
                        + (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%u,", usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32(field, i);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                       "%"PRId64",", bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%u,", uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                       "%"PRIu64",", ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloat(field, i);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ =
                        (double)(field->min +
                                 (taosRandom() %
                                  (field->max - field->min)) +
                                 taosRandom() % 1000000 / 1000000.0);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", double_);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, i)) {
                        free(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    pos += tmpJson(sampleDataBuf, bufLen, pos,
                                   fieldsSize, field);
                    goto skip_sql;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_sql:
        *(sampleDataBuf + pos - 1) = 0;
    }

    return 0;
}

static int generateRandDataStmt(
    SSuperTable *stbInfo,
    char *sampleDataBuf,
    int bufLen,
    int lenOfOneRow, BArray * fields,
    int64_t loop, bool tag) {
    // generateRandDataStmt()
    for (int i = 0; i < fields->size; ++i) {
        Field * field = benchArrayGet(fields, i);
        if (field->type == TSDB_DATA_TYPE_BINARY ||
            field->type == TSDB_DATA_TYPE_NCHAR) {
                field->data = benchCalloc(1, loop * (field->length + 1), true);
            } else {
            field->data = benchCalloc(1, loop * field->length, true);
        }
    }

    for (int64_t k = 0; k < loop; ++k) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            int n = 0;
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool rand_bool = (taosRandom() % 2) & 1;
                    ((bool *)field->data)[k] = rand_bool;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s,",
                                       rand_bool ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    ((int8_t *)field->data)[k] = tinyint;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = field->min
                        + (taosRandom() % (field->max - field->min));
                    ((uint8_t *)field->data)[k] = utinyint;
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%u,", utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = field->min
                        + (taosRandom() % (field->max -field->min));
                    ((int16_t *)field->data)[k] = smallint;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = field->min
                        + (taosRandom() % (field->max - field->min));
                    ((uint16_t *)field->data)[k] = usmallint;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%u,", usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32(field, i);
                    ((int32_t *)field->data)[k] = intTmp;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp;
                    bigintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    ((int64_t *)field->data)[k] = bigintTmp;
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                       "%"PRId64",", bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    ((uint32_t *)field->data)[k] = uintTmp;
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%u,", uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    ((uint64_t *)field->data)[k] = ubigintTmp;
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                       "%"PRIu64",", ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloat(field, i);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ =
                        (double)(field->min +
                                 (taosRandom() %
                                  (field->max - field->min)) +
                                 taosRandom() % 1000000 / 1000000.0);
                    ((double *)field->data)[k] = double_;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", double_);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, i)) {
                        free(tmp);
                        return -1;
                    }
                    snprintf((char *)field->data + k * field->length,
                                 field->length,
                                "%s", tmp);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    pos += tmpJson(sampleDataBuf, bufLen, pos,
                                   fieldsSize, field);
                    goto skip_stmt;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_stmt:
        *(sampleDataBuf + pos - 1) = 0;
    }

    return 0;
}

static int generateRandDataSmlTelnet(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag) {
    for (int64_t k = 0; k < loop; ++k) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        if (!tag) {
            fieldsSize = 1;
        }
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            int n = 0;
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool rand_bool = (taosRandom() % 2) & 1;
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%s ",
                                        field->name,
                                        rand_bool ? "true" : "false");
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s ",
                                        rand_bool ? "true" : "false");
                    }
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%s=%di8 ",
                                        field->name, tinyint);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos, "%di8 ", tinyint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = field->min
                        + (taosRandom() % (field->max - field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%uu8 ",
                                        field->name, utinyint);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%uu8 ", utinyint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = field->min
                        + (taosRandom() % (field->max -field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%di16 ",
                                        field->name, smallint);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%di16 ",
                                        smallint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = field->min
                        + (taosRandom() % (field->max - field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%uu16 ",
                                        field->name, usmallint);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%uu16 ",
                                        usmallint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32(field, i);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%di32 ",
                                        field->name, intTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%di32 ", intTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%s=%"PRId64"i64 ",
                                        field->name, bigintTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%"PRId64"i64 ", bigintTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%s=%uu32 ",
                                        field->name, uintTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%uu32 ", uintTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%s=%"PRIu64"u64 ",
                                        field->name, ubigintTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos,
                                     "%"PRIu64"u64 ", ubigintTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloat(field, i);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%ff32 ",
                                        field->name, floatTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos,
                                     "%ff32 ", floatTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ =
                        (double)(field->min +
                                 (taosRandom() %
                                  (field->max - field->min)) +
                                 taosRandom() % 1000000 / 1000000.0);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%ff64 ",
                                        field->name, double_);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                     "%ff64 ", double_);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, i)) {
                        free(tmp);
                        return -1;
                    }
                    if (field->type == TSDB_DATA_TYPE_BINARY) {
                        if (tag) {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                            "%s=L\"%s\" ",
                                           field->name, tmp);
                        } else {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                            "\"%s\" ", tmp);
                        }
                        if (n < 0 || n >= bufLen - pos) {
                            errorPrint("%s() LN%d snprintf overflow\n",
                                       __func__, __LINE__);
                            tmfree(tmp);
                            return -1;
                        } else {
                            pos += n;
                        }
                    } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
                        if (tag) {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                            "%s=L\"%s\" ",
                                           field->name, tmp);
                        } else {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                         "L\"%s\" ", tmp);
                        }
                        if (n < 0 || n >= bufLen - pos) {
                            errorPrint("%s() LN%d snprintf overflow\n",
                                       __func__, __LINE__);
                            tmfree(tmp);
                            return -1;
                        } else {
                            pos += n;
                        }
                    }
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    pos += tmpJson(sampleDataBuf, bufLen, pos,
                                   fieldsSize, field);
                    goto skip_telnet;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_telnet:
        *(sampleDataBuf + pos - 1) = 0;
    }

    return 0;
}

static int generateRandDataSmlJson(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag) {
    for (int64_t k = 0; k < loop; ++k) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            int n = 0;
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool rand_bool = (taosRandom() % 2) & 1;
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s,",
                                    rand_bool ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = field->min
                        + (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%u,", utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = field->min
                        + (taosRandom() % (field->max -field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = field->min
                        + (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%u,", usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32(field, i);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                       "%"PRId64",", bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%u,", uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                       "%"PRIu64",", ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloat(field, i);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ =
                        (double)(field->min +
                                 (taosRandom() %
                                  (field->max - field->min)) +
                                 taosRandom() % 1000000 / 1000000.0);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", double_);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, i)) {
                        free(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    pos += tmpJson(sampleDataBuf, bufLen, pos,
                                   fieldsSize, field);
                    goto skip_json;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_json:
        *(sampleDataBuf + pos - 1) = 0;
    }

    return 0;
}

static int generateRandDataSmlLine(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag) {
    for (int64_t k = 0; k < loop; ++k) {
        int64_t pos = k * lenOfOneRow;
        int n = 0;
        if (tag) {
            n = snprintf(sampleDataBuf + pos,
                           bufLen - pos,
                           "%s,", stbInfo->stbName);
            if (n < 0 || n >= bufLen - pos) {
                errorPrint("%s() LN%d snprintf overflow\n",
                           __func__, __LINE__);
                return -1;
            } else {
                pos += n;
            }
        }

        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool rand_bool = (taosRandom() % 2) & 1;
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos,
                                    "%s=%s,",
                                    field->name,
                                    rand_bool ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos,
                                    "%s=%di8,",
                                    field->name, tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = field->min
                        + (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%uu8,",
                                    field->name, utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = field->min
                        + (taosRandom() % (field->max -field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%di16,",
                                    field->name, smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = field->min
                        + (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%uu16,",
                                    field->name, usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32(field, i);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%di32,",
                                    field->name, intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%"PRId64"i64,",
                                    field->name, bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = field->min + (taosRandom()
                        % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%uu32,",
                                    field->name, uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp =
                            field->min +
                        (taosRandom() % (field->max - field->min));
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos,
                                    "%s=%"PRIu64"u64,",
                                    field->name, ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloat(field, i);
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos, "%s=%ff32,",
                                    field->name, floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ =
                        (double)(field->min +
                                 (taosRandom() %
                                  (field->max - field->min)) +
                                 taosRandom() % 1000000 / 1000000.0);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%ff64,",
                                    field->name, double_);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, i)) {
                        free(tmp);
                        return -1;
                    }
                    if (field->type == TSDB_DATA_TYPE_BINARY) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=\"%s\",",
                                       field->name, tmp);
                    } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=L\"%s\",",
                                       field->name, tmp);
                    }
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    n = tmpJson(sampleDataBuf, bufLen, pos,
                                   fieldsSize, field);
                    if (n < 0 || n >= bufLen) {
                        errorPrint("%s() LN%d snprintf overflow\n",
                                   __func__, __LINE__);
                        return -1;
                    } else {
                        pos += n;
                    }
                    goto skip_line;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_line:
        *(sampleDataBuf + pos - 1) = 0;
    }

    return 0;
}

static int generateRandDataSml(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag) {
    int     protocol = stbInfo->lineProtocol;

    switch (protocol) {
        case TSDB_SML_LINE_PROTOCOL:
            return generateRandDataSmlLine(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag);
        case TSDB_SML_TELNET_PROTOCOL:
            return generateRandDataSmlTelnet(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag);
        default:
            return generateRandDataSmlJson(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag);
    }

    return -1;
}

int generateRandData(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag) {
    int     iface = stbInfo->iface;
    switch (iface) {
        case TAOSC_IFACE:
        case REST_IFACE:
            return generateRandDataSQL(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag);
        case STMT_IFACE:
            return generateRandDataStmt(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag);
        case SML_IFACE:
        case SML_REST_IFACE:
            return generateRandDataSml(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag);
        default:
            errorPrint("Unknown iface: %d\n", iface);
            break;
    }

    return -1;
}

int prepareChildTblSampleData(SDataBase* database, SSuperTable* stbInfo) {
    return -1;
}

int prepareSampleData(SDataBase* database, SSuperTable* stbInfo) {
    stbInfo->lenOfCols = accumulateRowLen(stbInfo->cols, stbInfo->iface);
    stbInfo->lenOfTags = accumulateRowLen(stbInfo->tags, stbInfo->iface);
    if (stbInfo->partialColNum != 0
            && ((stbInfo->iface == TAOSC_IFACE
                || stbInfo->iface == REST_IFACE))) {
        if (stbInfo->partialColNum > stbInfo->cols->size) {
            stbInfo->partialColNum = stbInfo->cols->size;
        } else {
            stbInfo->partialColNameBuf =
                    benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
            int pos = 0;
            int n;
            n = snprintf(stbInfo->partialColNameBuf + pos,
                            TSDB_MAX_ALLOWED_SQL_LEN - pos,
                            TS_COL_NAME);
            if (n < 0 || n > TSDB_MAX_ALLOWED_SQL_LEN - pos) {
                errorPrint("%s() LN%d snprintf overflow\n",
                           __func__, __LINE__);
            } else {
                pos += n;
            }
            for (int i = 0; i < stbInfo->partialColNum; ++i) {
                Field * col = benchArrayGet(stbInfo->cols, i);
                n = snprintf(stbInfo->partialColNameBuf+pos,
                                TSDB_MAX_ALLOWED_SQL_LEN - pos,
                               ",%s", col->name);
                if (n < 0 || n > TSDB_MAX_ALLOWED_SQL_LEN - pos) {
                    errorPrint("%s() LN%d snprintf overflow at %d\n",
                               __func__, __LINE__, i);
                } else {
                    pos += n;
                }
            }
                for (int i = 0; i < stbInfo->partialColNum; ++i) {
            }
            for (int i = stbInfo->partialColNum; i < stbInfo->cols->size; ++i) {
                Field * col = benchArrayGet(stbInfo->cols, i);
                col->none = true;
            }
            debugPrint("partialColNameBuf: %s\n",
                       stbInfo->partialColNameBuf);
        }
    } else {
        stbInfo->partialColNum = stbInfo->cols->size;
    }
    stbInfo->sampleDataBuf =
            benchCalloc(
                1, stbInfo->lenOfCols*g_arguments->prepared_rand, true);
    infoPrint(
              "generate stable<%s> columns data with lenOfCols<%u> * "
              "prepared_rand<%" PRIu64 ">\n",
              stbInfo->stbName, stbInfo->lenOfCols, g_arguments->prepared_rand);
    if (stbInfo->random_data_source) {
        if (g_arguments->mistMode) {
            for (int64_t child = 0; child < stbInfo->childTblCount; child++) {
                SChildTable *childTbl = stbInfo->childTblArray[child];
                childTbl->sampleDataBuf =
                    benchCalloc(
                        1, stbInfo->lenOfCols*g_arguments->prepared_rand, true);
                if (generateRandData(stbInfo, childTbl->sampleDataBuf,
                             stbInfo->lenOfCols*g_arguments->prepared_rand,
                             stbInfo->lenOfCols,
                             stbInfo->cols,
                             g_arguments->prepared_rand,
                             false)) {
                    return -1;
                }
                childTbl->useOwnSample = true;
            }
        } else {
            if (generateRandData(stbInfo, stbInfo->sampleDataBuf,
                             stbInfo->lenOfCols*g_arguments->prepared_rand,
                             stbInfo->lenOfCols,
                             stbInfo->cols,
                             g_arguments->prepared_rand,
                             false)) {
                return -1;
            }
        }
    } else {
        if (stbInfo->useSampleTs) {
            if (getAndSetRowsFromCsvFile(
                    stbInfo->sampleFile, &stbInfo->insertRows)) {
                return -1;
            }
        }
        if (generateSampleFromCsv(stbInfo->sampleDataBuf,
                                        stbInfo->sampleFile, stbInfo->lenOfCols,
                                        g_arguments->prepared_rand)) {
            errorPrint("Failed to generate sample from csv file %s\n",
                    stbInfo->sampleFile);
            return -1;
        }

        if (stbInfo->childTblSample) {
            if (NULL == strstr(stbInfo->childTblSample, "XXXX")) {
                errorPrint("Child table sample file pattern has no %s\n",
                   "XXXX");
                return -1;
            }
            for (int64_t child = 0; child < stbInfo->childTblCount; child++) {
                char sampleFilePath[MAX_PATH_LEN] = {0};
                getSampleFileNameByPattern(sampleFilePath, stbInfo, child);
                if (0 != access(sampleFilePath, F_OK)) {
                    continue;
                }
                SChildTable *childTbl = stbInfo->childTblArray[child];
                if (getAndSetRowsFromCsvFile(sampleFilePath,
                                             &(childTbl->insertRows))) {
                    return -1;
                }

                childTbl->sampleDataBuf =
                    benchCalloc(
                        1, stbInfo->lenOfCols*g_arguments->prepared_rand, true);
                if (generateSampleFromCsv(
                            childTbl->sampleDataBuf,
                            sampleFilePath,
                            stbInfo->lenOfCols,
                            g_arguments->prepared_rand)) {
                    errorPrint("Failed to generate sample from file "
                                   "for child table %"PRId64"\n",
                                    child);
                    return -1;
                }
                childTbl->useOwnSample = true;
            }
        }
    }
    debugPrint("sampleDataBuf: %s\n", stbInfo->sampleDataBuf);

    if (stbInfo->tags->size != 0) {
        stbInfo->tagDataBuf =
                benchCalloc(
                    1, stbInfo->childTblCount*stbInfo->lenOfTags, true);
        infoPrint(
                  "generate stable<%s> tags data with lenOfTags<%u> * "
                  "childTblCount<%" PRIu64 ">\n",
                  stbInfo->stbName, stbInfo->lenOfTags,
                  stbInfo->childTblCount);
        if (stbInfo->tagsFile[0] != 0) {
            if (generateSampleFromCsv(
                    stbInfo->tagDataBuf, stbInfo->tagsFile,
                    stbInfo->lenOfTags,
                    stbInfo->childTblCount)) {
                return -1;
            }
        } else {
            if (generateRandData(stbInfo,
                                 stbInfo->tagDataBuf,
                                 stbInfo->childTblCount*stbInfo->lenOfTags,
                                 stbInfo->lenOfTags,
                                 stbInfo->tags,
                                 stbInfo->childTblCount, true)) {
                return -1;
            }
        }
        debugPrint("tagDataBuf: %s\n", stbInfo->tagDataBuf);
    }

    if (0 != convertServAddr(
            stbInfo->iface,
            stbInfo->tcpTransfer,
            stbInfo->lineProtocol)) {
        return -1;
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

uint32_t bindParamBatch(threadInfo *pThreadInfo,
                        uint32_t batch, int64_t startTime) {
    TAOS_STMT *  stmt = pThreadInfo->conn->stmt;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint32_t     columnCount = stbInfo->cols->size;
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
            Field * col = benchArrayGet(stbInfo->cols, c - 1);
            data_type = col->type;
            param->buffer = col->data;
            param->buffer_length = col->length;
            debugPrint("col[%d]: type: %s, len: %d\n", c,
                       convertDatatypeToString(data_type),
                       col->length);
            param->is_null = col->is_null;
        }
        param->buffer_type = data_type;
        param->length = benchCalloc(batch, sizeof(int32_t), true);

        for (int b = 0; b < batch; b++) {
            param->length[b] = (int32_t)param->buffer_length;
        }
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
        errorPrint("taos_stmt_bind_param_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return 0;
    }

    for (int c = 0; c < stbInfo->cols->size + 1; c++) {
        TAOS_MULTI_BIND *param =
            (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                sizeof(TAOS_MULTI_BIND) * c);
        tmfree(param->length);
    }

    // if msg > 3MB, break
    if (taos_stmt_add_batch(stmt)) {
        errorPrint("taos_stmt_add_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return 0;
    }
    return batch;
}

void generateSmlJsonTags(tools_cJSON *tagsList,
                         char **sml_tags_json_array,
                         SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    tools_cJSON * tags = tools_cJSON_CreateObject();
    char *  tbName = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64 "",
             stbInfo->childTblPrefix, start_table_from + tbSeq);
    char *tagName = benchCalloc(1, TSDB_MAX_TAGS, true);
    for (int i = 0; i < stbInfo->tags->size; i++) {
        Field * tag = benchArrayGet(stbInfo->tags, i);
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (tag->type) {
            case TSDB_DATA_TYPE_BOOL: {
                tools_cJSON_AddNumberToObject(tags, tagName,
                                              (taosRandom() % 2) & 1);
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                tools_cJSON_AddNumberToObject(
                        tags, tagName,
                        (float)(tag->min +
                            (taosRandom() % (tag->max - tag->min)) +
                            taosRandom() % 1000 / 1000.0));
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                tools_cJSON_AddNumberToObject(
                        tags, tagName,
                        (double)(tag->min
                            + (taosRandom() % (tag->max - tag->min))
                            + taosRandom() % 1000000 / 1000000.0));
                break;
            }

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)benchCalloc(tag->length + 1, 1, false);
                rand_string(buf, tag->length, g_arguments->chinese);
                if (tag->type == TSDB_DATA_TYPE_BINARY) {
                    tools_cJSON_AddStringToObject(tags, tagName, buf);
                } else {
                    tools_cJSON_AddStringToObject(tags, tagName, buf);
                }
                tmfree(buf);
                break;
            }
            default:
                tools_cJSON_AddNumberToObject(
                        tags, tagName,
                        tag->min + (taosRandom() % (tag->max - tag->min)));
                break;
        }
    }
    tools_cJSON_AddItemToArray(tagsList, tags);
    debugPrintJsonNoTime(tags);
    char *tags_text = tools_cJSON_PrintUnformatted(tags);
    debugPrintNoTimestamp("%s() LN%d, No.%"PRIu64" table's tags text: %s\n",
                          __func__, __LINE__,
                          start_table_from + tbSeq, tags_text);
    sml_tags_json_array[tbSeq] = tags_text;
    tmfree(tagName);
    tmfree(tbName);
}

void generateSmlTaosJsonTags(tools_cJSON *tagsList, SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    tools_cJSON * tags = tools_cJSON_CreateObject();
    char *  tbName = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64 "",
             stbInfo->childTblPrefix, tbSeq + start_table_from);
    tools_cJSON_AddStringToObject(tags, "id", tbName);
    char *tagName = benchCalloc(1, TSDB_MAX_TAGS, true);
    for (int i = 0; i < stbInfo->tags->size; i++) {
        Field * tag = benchArrayGet(stbInfo->tags, i);
        tools_cJSON *tagObj = tools_cJSON_CreateObject();
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (tag->type) {
            case TSDB_DATA_TYPE_BOOL: {
                tools_cJSON_AddBoolToObject(tagObj,
                                            "value",
                                            (taosRandom() % 2) & 1);
                tools_cJSON_AddStringToObject(tagObj, "type", "bool");
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                tools_cJSON_AddNumberToObject(
                        tagObj, "value",
                        (float)(tag->min +
                            (taosRandom() % (tag->max - tag->min)) +
                            taosRandom() % 1000 / 1000.0));
                tools_cJSON_AddStringToObject(tagObj, "type", "float");
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                tools_cJSON_AddNumberToObject(
                        tagObj, "value",
                        (double)(tag->min
                                + (taosRandom() % (tag->max - tag->min))
                                + taosRandom() % 1000000 / 1000000.0));
                tools_cJSON_AddStringToObject(tagObj, "type", "double");
                break;
            }

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)benchCalloc(tag->length + 1, 1, false);
                rand_string(buf, tag->length, g_arguments->chinese);
                if (tag->type == TSDB_DATA_TYPE_BINARY) {
                    tools_cJSON_AddStringToObject(tagObj, "value", buf);
                    tools_cJSON_AddStringToObject(tagObj, "type", "binary");
                } else {
                    tools_cJSON_AddStringToObject(tagObj, "value", buf);
                    tools_cJSON_AddStringToObject(tagObj, "type", "nchar");
                }
                tmfree(buf);
                break;
            }
            default:
                tools_cJSON_AddNumberToObject(
                        tagObj, "value",
                        tag->min + (taosRandom() % (tag->max - tag->min)));
                        tools_cJSON_AddStringToObject(tagObj, "type",
                                        convertDatatypeToString(tag->type));
                break;
        }
        tools_cJSON_AddItemToObject(tags, tagName, tagObj);
    }
    tools_cJSON_AddItemToArray(tagsList, tags);
    tmfree(tagName);
    tmfree(tbName);
}

void generateSmlJsonValues(
        char **sml_json_value_array, SSuperTable *stbInfo, int tableSeq) {
    char *value_buf = NULL;
    Field* col = benchArrayGet(stbInfo->cols, 0);
    int len_key = strlen("\"value\":,");
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL:
            value_buf = benchCalloc(len_key + 6, 1, true);
            snprintf(value_buf, len_key + 6,
                     "\"value\":%s,",
                        ((taosRandom()%2)&1)?"true":"false");
            break;
        case TSDB_DATA_TYPE_FLOAT:
            value_buf = benchCalloc(len_key + 20, 1, true);
            snprintf(value_buf, len_key + 20,
                     "\"value\":%f,",
                     (float)(col->min +
                        (taosRandom() % (col->max - col->min)) +
                        taosRandom() % 1000 / 1000.0));
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            value_buf = benchCalloc(len_key + 40, 1, true);
            snprintf(
                value_buf, len_key + 40, "\"value\":%f,",
                (double)(col->min +
                         (taosRandom() % (col->max - col->min)) +
                         taosRandom() % 1000000 / 1000000.0));
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            value_buf = benchCalloc(len_key + col->length + 3, 1, true);
            snprintf(value_buf, len_key + col->length + 3,
                     "\"value\":\"%s\",", buf);
            tmfree(buf);
            break;
        }
        default:
            value_buf = benchCalloc(len_key + 20, 1, true);
            snprintf(
                    value_buf, len_key + 20,
                    "\"value\":%f,",
                    (double)col->min +
                    (taosRandom() % (col->max - col->min)));
            break;
    }

    sml_json_value_array[tableSeq] = value_buf;
}

void generateSmlJsonCols(tools_cJSON *array, tools_cJSON *tag,
                         SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp) {
    tools_cJSON * record = tools_cJSON_CreateObject();
    tools_cJSON_AddNumberToObject(record, "timestamp", (double)timestamp);
    Field* col = benchArrayGet(stbInfo->cols, 0);
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL:
            tools_cJSON_AddBoolToObject(record, "value", (taosRandom()%2)&1);
            break;
        case TSDB_DATA_TYPE_FLOAT:
            tools_cJSON_AddNumberToObject(
                record, "value",
                (float)(col->min +
                        (taosRandom() % (col->max - col->min)) +
                        taosRandom() % 1000 / 1000.0));
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            tools_cJSON_AddNumberToObject(
                record, "value",
                (double)(col->min +
                         (taosRandom() % (col->max - col->min)) +
                         taosRandom() % 1000000 / 1000000.0));
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            if (col->type == TSDB_DATA_TYPE_BINARY) {
                tools_cJSON_AddStringToObject(record, "value", buf);
            } else {
                tools_cJSON_AddStringToObject(record, "value", buf);
            }
            tmfree(buf);
            break;
        }
        default:
            tools_cJSON_AddNumberToObject(
                    record, "value",
                    (double)col->min +
                    (taosRandom() % (col->max - col->min)));
            break;
    }
    tools_cJSON_AddItemToObject(record, "tags", tag);
    tools_cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    tools_cJSON_AddItemToArray(array, record);
}

void generateSmlTaosJsonCols(tools_cJSON *array, tools_cJSON *tag,
                         SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp) {
    tools_cJSON * record = tools_cJSON_CreateObject();
    tools_cJSON * ts = tools_cJSON_CreateObject();
    tools_cJSON_AddNumberToObject(ts, "value", (double)timestamp);
    if (time_precision == TSDB_SML_TIMESTAMP_MILLI_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "ms");
    } else if (time_precision == TSDB_SML_TIMESTAMP_MICRO_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "us");
    } else if (time_precision == TSDB_SML_TIMESTAMP_NANO_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "ns");
    }
    tools_cJSON *value = tools_cJSON_CreateObject();
    Field* col = benchArrayGet(stbInfo->cols, 0);
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL:
            tools_cJSON_AddBoolToObject(value, "value", (taosRandom()%2)&1);
            tools_cJSON_AddStringToObject(value, "type", "bool");
            break;
        case TSDB_DATA_TYPE_FLOAT:
            tools_cJSON_AddNumberToObject(
                value, "value",
                (float)(col->min +
                        (taosRandom() % (col->max - col->min)) +
                        taosRandom() % 1000 / 1000.0));
            tools_cJSON_AddStringToObject(value, "type", "float");
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            tools_cJSON_AddNumberToObject(
                value, "value",
                (double)(col->min +
                         (taosRandom() % (col->max - col->min)) +
                         taosRandom() % 1000000 / 1000000.0));
            tools_cJSON_AddStringToObject(value, "type", "double");
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            if (col->type == TSDB_DATA_TYPE_BINARY) {
                tools_cJSON_AddStringToObject(value, "value", buf);
                tools_cJSON_AddStringToObject(value, "type", "binary");
            } else {
                tools_cJSON_AddStringToObject(value, "value", buf);
                tools_cJSON_AddStringToObject(value, "type", "nchar");
            }
            tmfree(buf);
            break;
        }
        default:
            tools_cJSON_AddNumberToObject(
                    value, "value",
                    (double)col->min +
                    (taosRandom() % (col->max - col->min)));
            tools_cJSON_AddStringToObject(value, "type",
                                          convertDatatypeToString(col->type));
            break;
    }
    tools_cJSON_AddItemToObject(record, "timestamp", ts);
    tools_cJSON_AddItemToObject(record, "value", value);
    tools_cJSON_AddItemToObject(record, "tags", tag);
    tools_cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    tools_cJSON_AddItemToArray(array, record);
}
