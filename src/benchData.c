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

int8_t *  g_randbool = NULL;
int8_t *  g_randtinyint = NULL;
uint8_t * g_randutinyint = NULL;
int16_t * g_randsmallint = NULL;
uint16_t *g_randusmallint = NULL;
int32_t * g_randint = NULL;
uint32_t *g_randuint = NULL;
int64_t * g_randbigint = NULL;
uint64_t *g_randubigint = NULL;
float *   g_randfloat = NULL;
double *  g_randdouble = NULL;
char *    g_randbool_buff = NULL;
char *    g_randint_buff = NULL;
char *    g_randuint_buff = NULL;
char *    g_rand_voltage_buff = NULL;
char *    g_randbigint_buff = NULL;
char *    g_randubigint_buff = NULL;
char *    g_randsmallint_buff = NULL;
char *    g_randusmallint_buff = NULL;
char *    g_randtinyint_buff = NULL;
char *    g_randutinyint_buff = NULL;
char *    g_randfloat_buff = NULL;
char *    g_rand_current_buff = NULL;
char *    g_rand_phase_buff = NULL;
char *    g_randdouble_buff = NULL;

const char charset[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

static char *rand_bool_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randbool_buff + ((cursor)*BOOL_BUFF_LEN);
}

static int32_t rand_bool() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randbool[cursor];
}

static char *rand_tinyint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randtinyint_buff + ((cursor)*TINYINT_BUFF_LEN);
}

static int32_t rand_tinyint() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randtinyint[cursor];
}

static char *rand_utinyint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randutinyint_buff + ((cursor)*TINYINT_BUFF_LEN);
}

static char *rand_smallint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randsmallint_buff + ((cursor)*SMALLINT_BUFF_LEN);
}

static int32_t rand_smallint() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randsmallint[cursor];
}

static char *rand_usmallint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randusmallint_buff + ((cursor)*SMALLINT_BUFF_LEN);
}

static char *rand_int_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randint_buff + ((cursor)*INT_BUFF_LEN);
}

static int32_t rand_int() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randint[cursor];
}

static char *rand_uint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randuint_buff + ((cursor)*INT_BUFF_LEN);
}

static char *rand_bigint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randbigint_buff + ((cursor)*BIGINT_BUFF_LEN);
}

static int64_t rand_bigint() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randbigint[cursor];
}

static char *rand_ubigint_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randubigint_buff + ((cursor)*BIGINT_BUFF_LEN);
}

static char *rand_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randfloat_buff + ((cursor)*FLOAT_BUFF_LEN);
}

static float rand_float() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randfloat[cursor];
}

static char *demo_current_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_rand_current_buff + ((cursor)*FLOAT_BUFF_LEN);
}

static char *demo_voltage_int_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_rand_voltage_buff + ((cursor)*INT_BUFF_LEN);
}

static char *demo_phase_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_rand_phase_buff + ((cursor)*FLOAT_BUFF_LEN);
}

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

static char *rand_double_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randdouble_buff + (cursor * DOUBLE_BUFF_LEN);
}

static double rand_double() {
    static int cursor;
    cursor++;
    if (cursor > (g_arguments->prepared_rand - 1)) cursor = 0;
    return g_randdouble[cursor];
}

static void check_randomness(char *buffer_name, int buffer_length,
                             char *buffer) {
    debugPrint("%s: %s, %s, %s, ...\n", buffer_name, buffer,
               buffer + buffer_length, buffer + 2 * buffer_length);
}

int init_rand_data() {
    g_randint_buff = calloc(1, INT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += INT_BUFF_LEN * g_arguments->prepared_rand;
    g_rand_voltage_buff = calloc(1, INT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += INT_BUFF_LEN * g_arguments->prepared_rand;
    g_randbigint_buff = calloc(1, BIGINT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += BIGINT_BUFF_LEN * g_arguments->prepared_rand;
    g_randsmallint_buff =
        calloc(1, SMALLINT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += SMALLINT_BUFF_LEN * g_arguments->prepared_rand;
    g_randtinyint_buff =
        calloc(1, TINYINT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += TINYINT_BUFF_LEN * g_arguments->prepared_rand;
    g_randbool_buff = calloc(1, BOOL_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += BOOL_BUFF_LEN * g_arguments->prepared_rand;
    g_randfloat_buff = calloc(1, FLOAT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += FLOAT_BUFF_LEN * g_arguments->prepared_rand;
    g_rand_current_buff =
        calloc(1, FLOAT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += FLOAT_BUFF_LEN * g_arguments->prepared_rand;
    g_rand_phase_buff = calloc(1, FLOAT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += FLOAT_BUFF_LEN * g_arguments->prepared_rand;
    g_randdouble_buff = calloc(1, DOUBLE_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += DOUBLE_BUFF_LEN * g_arguments->prepared_rand;
    g_randuint_buff = calloc(1, INT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += INT_BUFF_LEN * g_arguments->prepared_rand;
    g_randutinyint_buff =
        calloc(1, TINYINT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += TINYINT_BUFF_LEN * g_arguments->prepared_rand;
    g_randusmallint_buff =
        calloc(1, SMALLINT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += SMALLINT_BUFF_LEN * g_arguments->prepared_rand;
    g_randubigint_buff =
        calloc(1, BIGINT_BUFF_LEN * g_arguments->prepared_rand);
    g_memoryUsage += BIGINT_BUFF_LEN * g_arguments->prepared_rand;
    g_randbool = calloc(1, sizeof(int8_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(int8_t) * g_arguments->prepared_rand;
    g_randtinyint = calloc(1, sizeof(int8_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(int8_t) * g_arguments->prepared_rand;
    g_randutinyint = calloc(1, sizeof(uint8_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(uint8_t) * g_arguments->prepared_rand;
    g_randsmallint = calloc(1, sizeof(int16_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(int16_t) * g_arguments->prepared_rand;
    g_randusmallint = calloc(1, sizeof(uint16_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(uint16_t) * g_arguments->prepared_rand;
    g_randint = calloc(1, sizeof(int32_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(int32_t) * g_arguments->prepared_rand;
    g_randuint = calloc(1, sizeof(uint32_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(uint32_t) * g_arguments->prepared_rand;
    g_randbigint = calloc(1, sizeof(int64_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(int64_t) * g_arguments->prepared_rand;
    g_randubigint = calloc(1, sizeof(uint64_t) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(uint64_t) * g_arguments->prepared_rand;
    g_randfloat = calloc(1, sizeof(float) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(float) * g_arguments->prepared_rand;
    g_randdouble = calloc(1, sizeof(double) * g_arguments->prepared_rand);
    g_memoryUsage += sizeof(double) * g_arguments->prepared_rand;

    for (int i = 0; i < g_arguments->prepared_rand; i++) {
        g_randint[i] = (int)(taosRandom() % RAND_MAX - (RAND_MAX >> 1));
        g_randuint[i] = (int)(taosRandom());
        g_randbool[i] = (g_randint[i] % 2) & 1;
        g_randtinyint[i] = g_randint[i] % 128;
        g_randutinyint[i] = g_randuint[i] % 255;
        g_randsmallint[i] = g_randint[i] % 32768;
        g_randusmallint[i] = g_randuint[i] % 65535;
        sprintf(g_randint_buff + i * INT_BUFF_LEN, "%d", g_randint[i]);
        sprintf(g_rand_voltage_buff + i * INT_BUFF_LEN, "%d",
                215 + g_randint[i] % 10);

        sprintf(g_randbool_buff + i * BOOL_BUFF_LEN, "%s",
                (g_randbool[i]) ? "true" : "false");
        sprintf(g_randsmallint_buff + i * SMALLINT_BUFF_LEN, "%d",
                g_randsmallint[i]);
        sprintf(g_randtinyint_buff + i * TINYINT_BUFF_LEN, "%d",
                g_randtinyint[i]);
        sprintf(g_randuint_buff + i * INT_BUFF_LEN, "%u", g_randuint[i]);
        sprintf(g_randusmallint_buff + i * SMALLINT_BUFF_LEN, "%u",
                g_randusmallint[i]);
        sprintf(g_randutinyint_buff + i * TINYINT_BUFF_LEN, "%u",
                g_randutinyint[i]);

        g_randbigint[i] = (int64_t)(taosRandom() % RAND_MAX - (RAND_MAX >> 1));
        g_randubigint[i] = (uint64_t)(taosRandom());
        sprintf(g_randbigint_buff + i * BIGINT_BUFF_LEN, "%" PRId64 "",
                g_randbigint[i]);
        sprintf(g_randubigint_buff + i * BIGINT_BUFF_LEN, "%" PRIu64 "",
                g_randubigint[i]);

        g_randfloat[i] =
            (float)(taosRandom() / 1000.0) * (taosRandom() % 2 > 0.5 ? 1 : -1);
        sprintf(g_randfloat_buff + i * FLOAT_BUFF_LEN, "%f", g_randfloat[i]);
        sprintf(g_rand_current_buff + i * FLOAT_BUFF_LEN, "%f",
                (float)(9.8 + 0.04 * (g_randint[i] % 10) +
                        g_randfloat[i] / 1000000000));
        sprintf(
            g_rand_phase_buff + i * FLOAT_BUFF_LEN, "%f",
            (float)((115 + g_randint[i] % 10 + g_randfloat[i] / 1000000000) /
                    360));

        g_randdouble[i] = (double)(taosRandom() / 1000000.0) *
                          (taosRandom() % 2 > 0.5 ? 1 : -1);
        sprintf(g_randdouble_buff + i * DOUBLE_BUFF_LEN, "%f", g_randdouble[i]);
    }
    check_randomness("g_randint_buff", INT_BUFF_LEN, g_randint_buff);
    check_randomness("g_rand_voltage_buff", INT_BUFF_LEN, g_rand_voltage_buff);
    check_randomness("g_randbigint_buff", BIGINT_BUFF_LEN, g_randbigint_buff);
    check_randomness("g_randsmallint_buff", SMALL_BUFF_LEN,
                     g_randsmallint_buff);
    check_randomness("g_randbool_buff", BOOL_BUFF_LEN, g_randbool_buff);
    check_randomness("g_randfloat_buff", FLOAT_BUFF_LEN, g_randfloat_buff);
    check_randomness("g_rand_current_buff", FLOAT_BUFF_LEN,
                     g_rand_current_buff);
    check_randomness("g_rand_phase_buff", FLOAT_BUFF_LEN, g_rand_phase_buff);
    check_randomness("g_randdouble_buff", DOUBLE_BUFF_LEN, g_randdouble_buff);
    check_randomness("g_randuint_buff", INT_BUFF_LEN, g_randuint_buff);
    check_randomness("g_randutinyint_buff", TINYINT_BUFF_LEN,
                     g_randutinyint_buff);
    check_randomness("g_randusmallint_buff", SMALL_BUFF_LEN,
                     g_randusmallint_buff);
    check_randomness("g_randubigint_buff", BIGINT_BUFF_LEN, g_randubigint_buff);
    check_randomness("g_randtinyint_buff", TINYINT_BUFF_LEN,
                     g_randtinyint_buff);

    return 0;
}

static void generateStmtTagArray(SSuperTable *stbInfo) {
    stbInfo->tag_bind_array =
        calloc(stbInfo->childTblCount, sizeof(TAOS_BIND *));
    for (int i = 0; i < stbInfo->childTblCount; ++i) {
        stbInfo->tag_bind_array[i] =
            calloc(stbInfo->tagCount, sizeof(TAOS_BIND));
        for (int j = 0; j < stbInfo->tagCount; ++j) {
            TAOS_BIND *tag = &(stbInfo->tag_bind_array[i][j]);
            tag->buffer_type = stbInfo->tag_type[j];
            tag->buffer_length = stbInfo->tag_length[j];
            tag->length = &tag->buffer_length;
            tag->is_null = NULL;
            switch (tag->buffer_type) {
                case TSDB_DATA_TYPE_BOOL:
                    tag->buffer = g_randbool + i * sizeof(int8_t);
                    break;
                case TSDB_DATA_TYPE_TINYINT:
                    tag->buffer = g_randtinyint + i * sizeof(int8_t);
                    break;
                case TSDB_DATA_TYPE_UTINYINT:
                    tag->buffer = g_randutinyint + i * sizeof(uint8_t);
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    tag->buffer = g_randsmallint + i * sizeof(int16_t);
                    break;
                case TSDB_DATA_TYPE_USMALLINT:
                    tag->buffer = g_randusmallint + i * sizeof(uint16_t);
                    break;
                case TSDB_DATA_TYPE_INT:
                    tag->buffer = g_randint + i * sizeof(int32_t);
                    break;
                case TSDB_DATA_TYPE_UINT:
                    tag->buffer = g_randuint + i * sizeof(uint32_t);
                    break;
                case TSDB_DATA_TYPE_TIMESTAMP:
                case TSDB_DATA_TYPE_BIGINT:
                    tag->buffer = g_randbigint + i * sizeof(int64_t);
                    break;
                case TSDB_DATA_TYPE_UBIGINT:
                    tag->buffer = g_randubigint + i * sizeof(uint64_t);
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    tag->buffer = g_randfloat + i * sizeof(float);
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    tag->buffer = g_randdouble + i * sizeof(double);
                    break;
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    tag->buffer = stbInfo->stmt_tag_string_grid[j] +
                                  i * stbInfo->tag_length[j];
                    break;
                }
            }
        }
    }
}

void generateStmtBuffer(SSuperTable *stbInfo) {
    int      len = 0;
    int      tagCount = stbInfo->tagCount;
    char *   tag_type = stbInfo->tag_type;
    int32_t *tag_length = stbInfo->tag_length;
    stbInfo->stmt_buffer = calloc(1, BUFFER_SIZE);
    g_memoryUsage += BUFFER_SIZE;
    if (stbInfo->autoCreateTable) {
        stbInfo->stmt_tag_string_grid = calloc(tagCount, sizeof(char *));
        g_memoryUsage += tagCount * sizeof(char *);
        len += sprintf(stbInfo->stmt_buffer + len,
                       "INSERT INTO ? USING `%s` TAGS (", stbInfo->stbName);
        for (int i = 0; i < stbInfo->tagCount; ++i) {
            if (i == 0) {
                len += sprintf(stbInfo->stmt_buffer + len, "?");
            } else {
                len += sprintf(stbInfo->stmt_buffer + len, ",?");
            }
            if (tag_type[i] == TSDB_DATA_TYPE_NCHAR ||
                tag_type[i] == TSDB_DATA_TYPE_BINARY) {
                stbInfo->stmt_tag_string_grid[i] =
                    calloc(1, stbInfo->childTblCount * (tag_length[i] + 1));
                g_memoryUsage += stbInfo->childTblCount * (tag_length[i] + 1);
                for (int j = 0; j < stbInfo->childTblCount; ++j) {
                    rand_string(
                        stbInfo->stmt_tag_string_grid[i] + j * tag_length[i],
                        tag_length[i], g_arguments->chinese);
                }
            }
        }
        len += sprintf(stbInfo->stmt_buffer + len, ") VALUES(?");
        generateStmtTagArray(stbInfo);
    } else {
        len += sprintf(stbInfo->stmt_buffer + len, "INSERT INTO ? VALUES(?");
    }

    int      columnCount = stbInfo->columnCount;
    char *   col_type = stbInfo->col_type;
    int32_t *col_length = stbInfo->col_length;
    stbInfo->stmt_col_string_grid = calloc(columnCount, sizeof(char *));
    g_memoryUsage += columnCount * sizeof(char *);
    for (int col = 0; col < columnCount; col++) {
        len += sprintf(stbInfo->stmt_buffer + len, ",?");
        if (col_type[col] == TSDB_DATA_TYPE_NCHAR ||
            col_type[col] == TSDB_DATA_TYPE_BINARY) {
            stbInfo->stmt_col_string_grid[col] =
                calloc(1, g_arguments->reqPerReq * (col_length[col] + 1));
            g_memoryUsage += g_arguments->reqPerReq * (col_length[col] + 1);
            for (int i = 0; i < g_arguments->reqPerReq; ++i) {
                rand_string(
                    stbInfo->stmt_col_string_grid[col] + i * col_length[col],
                    col_length[col], g_arguments->chinese);
            }
        }
    }
    sprintf(stbInfo->stmt_buffer + len, ")");
    debugPrint("stmtBuffer: %s\n", stbInfo->stmt_buffer);
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
}

static int generateSampleFromCsvForStb(char *buffer, char *file, int32_t length,
                                       int64_t size) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;
    int     getRows = 0;

    FILE *fp = fopen(file, "r");
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n", file,
                   strerror(errno));
        return -1;
    }
    while (1) {
        readLen = getline(&line, &n, fp);
        if (-1 == readLen) {
            if (0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint("Failed to fseek file: %s, reason:%s\n", file,
                           strerror(errno));
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

static int getAndSetRowsFromCsvFile(SSuperTable *stbInfo) {
    int32_t code = -1;
    FILE *  fp = fopen(stbInfo->sampleFile, "r");
    int     line_count = 0;
    char *  buf;
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   stbInfo->sampleFile, strerror(errno));
        goto free_of_get_set_rows_from_csv;
    }
    buf = calloc(1, TSDB_MAX_SQL_LEN);
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
        switch (stbInfo->col_type[colIndex]) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                stbInfo->lenOfCols += stbInfo->col_length[colIndex] + 3;
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
        if (stbInfo->iface == SML_IFACE) {
            stbInfo->lenOfCols += SML_LINE_SQL_SYNTAX_OFFSET;
        }
    }
    stbInfo->lenOfCols += TIMESTAMP_BUFF_LEN;

    for (int tagIndex = 0; tagIndex < stbInfo->tagCount; tagIndex++) {
        switch (stbInfo->tag_type[tagIndex]) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                stbInfo->lenOfTags += stbInfo->tag_length[tagIndex] + 4;
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
                    (JSON_BUFF_LEN + stbInfo->tag_length[tagIndex]) *
                    stbInfo->tagCount;
                return;
        }
        stbInfo->lenOfTags += 1;
        if (stbInfo->iface == SML_IFACE) {
            stbInfo->lenOfTags += SML_LINE_SQL_SYNTAX_OFFSET;
        }
    }

    if (stbInfo->iface == SML_IFACE) {
        stbInfo->lenOfTags +=
            2 * TSDB_TABLE_NAME_LEN * 2 + SML_LINE_SQL_SYNTAX_OFFSET;
    }
    return;
}

static void generateSampleFromRand(char *sampleDataBuf, int32_t lenOfOneRow,
                                   int count, char *data_type,
                                   int32_t *data_length, int64_t size,
                                   uint16_t iface, int32_t line_protocol,
                                   bool *data_null) {
    for (int64_t i = 0; i < size; i++) {
        int32_t pos = i * lenOfOneRow;
        for (int c = 0; c < count; c++) {
            char *tmp = NULL;
            if (iface == TAOSC_IFACE || iface == REST_IFACE) {
                if (data_null[c]) {
                    pos += sprintf(sampleDataBuf + pos, "null,");
                    continue;
                }
            }
            switch (data_type[c]) {
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *data = calloc(1, 1 + data_length[c]);
                    if (g_arguments->demo_mode) {
                        if (taosRandom() % 2 == 1) {
                            if (g_arguments->chinese) {
                                sprintf(data, "上海");
                            } else {
                                sprintf(data, "beijing");
                            }
                        } else {
                            if (g_arguments->chinese) {
                                sprintf(data, "北京");
                            } else {
                                sprintf(data, "shanghai");
                            }
                        }
                    } else {
                        rand_string(data, data_length[c], g_arguments->chinese);
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        data_type[c] == TSDB_DATA_TYPE_BINARY &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=\"%s\",", c,
                                       data);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               data_type[c] == TSDB_DATA_TYPE_NCHAR &&
                               line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=L\"%s\",", c,
                                       data);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               data_type[c] == TSDB_DATA_TYPE_BINARY &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "\"%s\" ", data);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               data_type[c] == TSDB_DATA_TYPE_NCHAR &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "L\"%s\" ", data);
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "'%s',", data);
                    }
                    tmfree(data);
                    break;
                }
                case TSDB_DATA_TYPE_INT:
                    if ((g_arguments->demo_mode) && (c == 1)) {
                        tmp = demo_voltage_int_str();
                    } else if ((g_arguments->demo_mode) && (c == 0)) {
                        tmp = rand_utinyint_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos +=
                            sprintf(sampleDataBuf + pos, "c%d=%si32,", c, tmp);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%si32 ", tmp);
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,", tmp);
                    }
                    break;

                case TSDB_DATA_TYPE_UINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%su32,", c,
                                       rand_uint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%su32 ",
                                       rand_uint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_uint_str());
                    }
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%si64,", c,
                                       rand_bigint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%si64 ",
                                       rand_bigint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_bigint_str());
                    }
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                case TSDB_DATA_TYPE_UBIGINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%su64,", c,
                                       rand_ubigint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%su64 ",
                                       rand_ubigint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_ubigint_str());
                    }
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_arguments->demo_mode && c == 0) {
                        tmp = demo_current_float_str();
                    } else if (g_arguments->demo_mode && c == 2) {
                        tmp = demo_phase_float_str();
                    } else {
                        tmp = rand_float_str();
                    }
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos +=
                            sprintf(sampleDataBuf + pos, "c%d=%sf32,", c, tmp);
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%sf32 ", tmp);
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,", tmp);
                    }
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%sf64,", c,
                                       rand_double_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%sf64 ",
                                       rand_double_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_double_str());
                    }
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%si16,", c,
                                       rand_smallint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%si16,",
                                       rand_smallint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_smallint_str());
                    }

                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%su16,", c,
                                       rand_usmallint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%su16 ",
                                       rand_usmallint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_usmallint_str());
                    }
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%si8,", c,
                                       rand_tinyint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%si8 ",
                                       rand_tinyint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_tinyint_str());
                    }

                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%su8,", c,
                                       rand_utinyint_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%su8 ",
                                       rand_utinyint_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_utinyint_str());
                    }
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    if ((iface == SML_IFACE || iface == SML_REST_IFACE) &&
                        line_protocol == TSDB_SML_LINE_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "c%d=%s,", c,
                                       rand_bool_str());
                    } else if ((iface == SML_IFACE ||
                                iface == SML_REST_IFACE) &&
                               line_protocol == TSDB_SML_TELNET_PROTOCOL) {
                        pos += sprintf(sampleDataBuf + pos, "%s ",
                                       rand_bool_str());
                    } else {
                        pos += sprintf(sampleDataBuf + pos, "%s,",
                                       rand_bool_str());
                    }
                    break;

                case TSDB_DATA_TYPE_JSON: {
                    pos += sprintf(sampleDataBuf + pos, "'{");
                    for (int j = 0; j < count; ++j) {
                        pos += sprintf(sampleDataBuf + pos, "\"k%d\":", j);
                        char *buf = calloc(1, data_length[j] + 1);
                        rand_string(buf, data_length[j], g_arguments->chinese);
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
    debugPrint("stable: %s: tagCount: %d; lenOfTags: %d\n", stbInfo->stbName,
               stbInfo->tagCount, stbInfo->lenOfTags);
    debugPrint("stable: %s: columnCount: %d; lenOfCols: %d\n", stbInfo->stbName,
               stbInfo->columnCount, stbInfo->lenOfCols);
    if (stbInfo->partialColumnNum != 0 &&
        (stbInfo->iface == TAOSC_IFACE || stbInfo->iface == REST_IFACE)) {
        if (stbInfo->partialColumnNum > stbInfo->columnCount) {
            stbInfo->partialColumnNum = stbInfo->columnCount;
        } else {
            stbInfo->partialColumnNameBuf = calloc(1, BUFFER_SIZE);
            g_memoryUsage += BUFFER_SIZE;
            int pos = 0;
            pos += sprintf(stbInfo->partialColumnNameBuf + pos, "ts");
            for (int i = 0; i < stbInfo->partialColumnNum; ++i) {
                pos += sprintf(stbInfo->partialColumnNameBuf + pos, ",c%d", i);
            }
            debugPrint("partialColumnNameBuf: %s\n",
                       stbInfo->partialColumnNameBuf);
        }
    } else {
        stbInfo->partialColumnNum = stbInfo->columnCount;
    }
    if (stbInfo->iface != STMT_IFACE) {
        stbInfo->sampleDataBuf =
            calloc(1, stbInfo->lenOfCols * g_arguments->prepared_rand);
        g_memoryUsage += stbInfo->lenOfCols * g_arguments->prepared_rand;
        if (stbInfo->random_data_source) {
            generateSampleFromRand(stbInfo->sampleDataBuf, stbInfo->lenOfCols,
                                   stbInfo->partialColumnNum, stbInfo->col_type,
                                   stbInfo->col_length,
                                   g_arguments->prepared_rand, stbInfo->iface,
                                   stbInfo->lineProtocol, stbInfo->col_null);
        } else {
            if (stbInfo->useSampleTs) {
                if (getAndSetRowsFromCsvFile(stbInfo)) return -1;
            }
            if (generateSampleFromCsvForStb(
                    stbInfo->sampleDataBuf, stbInfo->sampleFile,
                    stbInfo->lenOfCols, g_arguments->prepared_rand)) {
                return -1;
            }
        }
        debugPrint("sampleDataBuf: %s\n", stbInfo->sampleDataBuf);
    } else {
        generateStmtBuffer(stbInfo);
    }
    if (!stbInfo->childTblExists && stbInfo->tagCount != 0) {
        stbInfo->tagDataBuf =
            calloc(1, stbInfo->childTblCount * stbInfo->lenOfTags);
        g_memoryUsage += stbInfo->childTblCount * stbInfo->lenOfTags;
        if (stbInfo->tagsFile[0] != 0) {
            if (generateSampleFromCsvForStb(
                    stbInfo->tagDataBuf, stbInfo->tagsFile, stbInfo->lenOfTags,
                    stbInfo->childTblCount)) {
                return -1;
            }
        } else {
            generateSampleFromRand(stbInfo->tagDataBuf, stbInfo->lenOfTags,
                                   stbInfo->tagCount, stbInfo->tag_type,
                                   stbInfo->tag_length, stbInfo->childTblCount,
                                   stbInfo->iface, 0, stbInfo->tag_null);
        }
        debugPrint("tagDataBuf: %s\n", stbInfo->tagDataBuf);
    }

    if (stbInfo->iface == REST_IFACE || stbInfo->iface == SML_REST_IFACE) {
        if (stbInfo->tcpTransfer && stbInfo->iface == SML_REST_IFACE) {
            if (convertHostToServAddr(g_arguments->host,
                                      g_arguments->telnet_tcp_port,
                                      &(g_arguments->serv_addr))) {
                errorPrint("%s\n", "convert host to server address");
                return -1;
            }
        } else {
            if (convertHostToServAddr(g_arguments->host,
                                      g_arguments->port + TSDB_PORT_HTTP,
                                      &(g_arguments->serv_addr))) {
                errorPrint("%s\n", "convert host to server address");
                return -1;
            }
        }
    }
    switch (stbInfo->iface) {
        case REST_IFACE:
        case TAOSC_IFACE:
            if (stbInfo->autoCreateTable) {
                g_memoryUsage += g_arguments->nthreads *
                                 ((stbInfo->lenOfCols + stbInfo->lenOfTags) *
                                      g_arguments->reqPerReq +
                                  1024);
            } else {
                g_memoryUsage +=
                    g_arguments->nthreads *
                    (stbInfo->lenOfCols * g_arguments->reqPerReq + 1024);
            }
            break;
        case SML_REST_IFACE:
            g_memoryUsage += (stbInfo->lenOfCols + stbInfo->lenOfTags + 1) *
                             g_arguments->reqPerReq * g_arguments->nthreads;
        case SML_IFACE:
            g_memoryUsage += stbInfo->childTblCount * sizeof(char *);
            g_memoryUsage += stbInfo->childTblCount * stbInfo->lenOfTags;
            g_memoryUsage += g_arguments->reqPerReq * sizeof(char *);
            g_memoryUsage += g_arguments->reqPerReq *
                             (stbInfo->lenOfTags + stbInfo->lenOfCols);
            break;
        default:
            break;
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
            data_type = stbInfo->col_type[c - 1];
            switch (data_type) {
                case TSDB_DATA_TYPE_NCHAR:
                case TSDB_DATA_TYPE_BINARY: {
                    param->buffer = stbInfo->stmt_col_string_grid[c - 1];
                    break;
                }
                case TSDB_DATA_TYPE_INT:
                    param->buffer = g_randint;
                    break;
                case TSDB_DATA_TYPE_UINT:
                    param->buffer = g_randuint;
                    break;
                case TSDB_DATA_TYPE_TINYINT:
                    param->buffer = g_randtinyint;
                    break;
                case TSDB_DATA_TYPE_UTINYINT:
                    param->buffer = g_randutinyint;
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    param->buffer = g_randsmallint;
                    break;
                case TSDB_DATA_TYPE_USMALLINT:
                    param->buffer = g_randusmallint;
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                    param->buffer = g_randbigint;
                    break;
                case TSDB_DATA_TYPE_UBIGINT:
                    param->buffer = g_randubigint;
                    break;
                case TSDB_DATA_TYPE_BOOL:
                    param->buffer = g_randbool;
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    param->buffer = g_randfloat;
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    param->buffer = g_randdouble;
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    param->buffer = g_randubigint;
                    break;

                default:
                    errorPrint("wrong data type: %d\n", data_type);
                    return -1;
            }
            param->buffer_length = stbInfo->col_length[c - 1];
            debugPrint("col[%d]: type: %s, len: %d\n", c,
                       taos_convert_datatype_to_string(data_type),
                       stbInfo->col_length[c - 1]);
        }
        param->buffer_type = data_type;
        param->length = calloc(batch, sizeof(int32_t));

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
    return batch;
}

int32_t generateSmlTags(char *sml, SSuperTable *stbInfo) {
    int64_t  dataLen = 0;
    uint64_t length = (stbInfo->lenOfTags + stbInfo->lenOfCols);
    if (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
        dataLen +=
            snprintf(sml + dataLen, length - dataLen, "%s", stbInfo->stbName);
    }

    for (int j = 0; j < stbInfo->tagCount; j++) {
        tstrncpy(sml + dataLen,
                 (stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) ? "," : " ",
                 2);
        dataLen += 1;
        switch (stbInfo->tag_type[j]) {
            case TSDB_DATA_TYPE_TIMESTAMP:
                errorPrint("%s", "Does not support timestamp as tag\n");
                return -1;
            case TSDB_DATA_TYPE_BOOL:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_bool_str());
                break;
            case TSDB_DATA_TYPE_TINYINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_tinyint_str());
                break;
            case TSDB_DATA_TYPE_UTINYINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_utinyint_str());
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_smallint_str());
                break;
            case TSDB_DATA_TYPE_USMALLINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_usmallint_str());
                break;
            case TSDB_DATA_TYPE_INT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_int_str());
                break;
            case TSDB_DATA_TYPE_UINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_uint_str());
                break;
            case TSDB_DATA_TYPE_BIGINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_bigint_str());
                break;
            case TSDB_DATA_TYPE_UBIGINT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_ubigint_str());
                break;
            case TSDB_DATA_TYPE_FLOAT:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_float_str());
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                dataLen += snprintf(sml + dataLen, length - dataLen, "t%d=%s",
                                    j, rand_double_str());
                break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)calloc(stbInfo->tag_length[j] + 1, 1);
                rand_string(buf, stbInfo->tag_length[j], g_arguments->chinese);
                dataLen +=
                    snprintf(sml + dataLen, length - dataLen, "t%d=%s", j, buf);
                tmfree(buf);
                break;
            }
            default:
                errorPrint("unknown data type %d\n", stbInfo->tag_type[j]);
                return -1;
        }
    }
    return 0;
}

int32_t generateSmlJsonTags(cJSON *tagsList, SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    int32_t code = -1;
    cJSON * tags = cJSON_CreateObject();
    char *  tbName = calloc(1, TSDB_TABLE_NAME_LEN);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64 "",
             stbInfo->childTblPrefix, tbSeq + start_table_from);
    cJSON_AddStringToObject(tags, "id", tbName);
    char *tagName = calloc(1, TSDB_MAX_TAGS);
    assert(tagName);
    for (int i = 0; i < stbInfo->tagCount; i++) {
        cJSON *tag = cJSON_CreateObject();
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (stbInfo->tag_type[i]) {
            case TSDB_DATA_TYPE_BOOL:
                cJSON_AddBoolToObject(tag, "value", rand_bool());
                cJSON_AddStringToObject(tag, "type", "bool");
                break;
            case TSDB_DATA_TYPE_TINYINT:
                cJSON_AddNumberToObject(tag, "value", rand_tinyint());
                cJSON_AddStringToObject(tag, "type", "tinyint");
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                cJSON_AddNumberToObject(tag, "value", rand_smallint());
                cJSON_AddStringToObject(tag, "type", "smallint");
                break;
            case TSDB_DATA_TYPE_INT:
                cJSON_AddNumberToObject(tag, "value", rand_int());
                cJSON_AddStringToObject(tag, "type", "int");
                break;
            case TSDB_DATA_TYPE_BIGINT:
                cJSON_AddNumberToObject(tag, "value", (double)rand_bigint());
                cJSON_AddStringToObject(tag, "type", "bigint");
                break;
            case TSDB_DATA_TYPE_FLOAT:
                cJSON_AddNumberToObject(tag, "value", rand_float());
                cJSON_AddStringToObject(tag, "type", "float");
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                cJSON_AddNumberToObject(tag, "value", rand_double());
                cJSON_AddStringToObject(tag, "type", "double");
                break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)calloc(stbInfo->tag_length[i] + 1, 1);
                rand_string(buf, stbInfo->tag_length[i], g_arguments->chinese);
                if (stbInfo->tag_type[i] == TSDB_DATA_TYPE_BINARY) {
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
                    "unknown data type (%d) for schemaless json protocol\n",
                    stbInfo->tag_type[i]);
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
    cJSON *record = cJSON_CreateObject();
    cJSON *ts = cJSON_CreateObject();
    cJSON_AddNumberToObject(ts, "value", (double)timestamp);
    if (time_precision == TSDB_SML_TIMESTAMP_MILLI_SECONDS) {
        cJSON_AddStringToObject(ts, "type", "ms");
    } else if (time_precision == TSDB_SML_TIMESTAMP_MICRO_SECONDS) {
        cJSON_AddStringToObject(ts, "type", "us");
    } else if (time_precision == TSDB_SML_TIMESTAMP_NANO_SECONDS) {
        cJSON_AddStringToObject(ts, "type", "ns");
    } else {
        errorPrint("Unknown time precision %d\n", time_precision);
        return -1;
    }
    cJSON *value = cJSON_CreateObject();
    switch (stbInfo->col_type[0]) {
        case TSDB_DATA_TYPE_BOOL:
            cJSON_AddBoolToObject(value, "value", rand_bool());
            cJSON_AddStringToObject(value, "type", "bool");
            break;
        case TSDB_DATA_TYPE_TINYINT:
            cJSON_AddNumberToObject(value, "value", rand_tinyint());
            cJSON_AddStringToObject(value, "type", "tinyint");
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            cJSON_AddNumberToObject(value, "value", rand_smallint());
            cJSON_AddStringToObject(value, "type", "smallint");
            break;
        case TSDB_DATA_TYPE_INT:
            cJSON_AddNumberToObject(value, "value", rand_int());
            cJSON_AddStringToObject(value, "type", "int");
            break;
        case TSDB_DATA_TYPE_BIGINT:
            cJSON_AddNumberToObject(value, "value", (double)rand_bigint());
            cJSON_AddStringToObject(value, "type", "bigint");
            break;
        case TSDB_DATA_TYPE_FLOAT:
            cJSON_AddNumberToObject(value, "value", rand_float());
            cJSON_AddStringToObject(value, "type", "float");
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            cJSON_AddNumberToObject(value, "value", rand_double());
            cJSON_AddStringToObject(value, "type", "double");
            break;
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)calloc(stbInfo->col_length[0] + 1, 1);
            rand_string(buf, stbInfo->col_length[0], g_arguments->chinese);
            if (stbInfo->col_type[0] == TSDB_DATA_TYPE_BINARY) {
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
            errorPrint("unknown data type (%d) for schemaless json protocol\n",
                       stbInfo->col_type[0]);
            return -1;
    }
    cJSON_AddItemToObject(record, "timestamp", ts);
    cJSON_AddItemToObject(record, "value", value);
    cJSON_AddItemToObject(record, "tags", tag);
    cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    cJSON_AddItemToArray(array, record);
    return 0;
}
