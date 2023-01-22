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

#include "bench.h"
#include "benchDataMix.h"
#include <float.h>

#define VAL_NULL "NULL"

#define VBOOL_CNT 3

int32_t inul = 20; // interval null count

#define SIGNED_RANDOM(type, high, format)  \
      {                                 \
        type max = high/2;              \
        type min = (max-1) * -1;        \
        type mid = RD(max);             \
        if(RD(50) == 0) {               \
            mid = max;                  \
        } else {                        \
            if(RD(50) == 0) mid = min;  \
        }                               \
        sprintf(val, format, mid);      \
      }                                 \

#define UNSIGNED_RANDOM(type, high, format)   \
      {                                 \
        type max = high;                \
        type min = 0;                   \
        type mid = RD(max);             \
        if(RD(50) == 0) {               \
            mid = max;                  \
        } else {                        \
            if(RD(50) == 0) mid = min;  \
        }                               \
        sprintf(val, format, mid);      \
      }                                 \


#define FLOAT_RANDOM(type, min, max)    \
      {                                 \
        type mid =  RD((int32_t)max);   \
        mid += RD(1000000)/800001.1;    \
        if(RD(50) == 0) {               \
            mid = max;                  \
        } else {                        \
            if(RD(50) == 0) mid = min;  \
        }                               \
        sprintf(val, "%f", mid);        \
      }                                 \


// 32 ~ 126 + '\r' '\n' '\t' radom char
uint32_t genRadomString(char* val, uint32_t len) {
  uint32_t size = RD(len) / 2;
  if (size < 3) {
    strcpy(val, VAL_NULL);
    return sizeof(VAL_NULL);
    }

    val[0] = '\'';

    char spe[] = {'\\', '\r', '\n', '\t'};
    for (int32_t i = 1; i < size - 1; i++) {
        if (false) {
          val[i] = spe[RD(sizeof(spe))];
        } else {
          val[i] = 32 + RD(94);
        }
        if(val[i] == '\'' || val[i] == '\"' || val[i] == '%' || val[i] == '\\') {
            val[i] = 'x';
        }
    }

    val[size - 1] = '\'';
    // set string end
    val[size] = 0;
    return size;
}

uint32_t dataGenByField(Field* fd, char* pstr, uint32_t len) {
    uint32_t size = 0;
    char val[512] = VAL_NULL;
    if( RD(inul) == 0 ) {
        size = sprintf(pstr + len, ",%s", VAL_NULL);
        return size;
    }

    switch (fd->type) {    
    case TSDB_DATA_TYPE_BOOL:
        strcpy(val, RD(2) ? "true" : "false");
        break;
    // timestamp    
    case TSDB_DATA_TYPE_TIMESTAMP:
        strcpy(val, "now");
        break;
    // signed    
    case TSDB_DATA_TYPE_TINYINT:
        SIGNED_RANDOM(int8_t, 0xFF, "%d")
        break;        
    case TSDB_DATA_TYPE_SMALLINT:
        SIGNED_RANDOM(int16_t, 0xFFFF, "%d")
        break;
    case TSDB_DATA_TYPE_INT:
        SIGNED_RANDOM(int32_t, 0xFFFFFFFF, "%d")
        break;
    case TSDB_DATA_TYPE_BIGINT:
        SIGNED_RANDOM(int64_t, 0xFFFFFFFFFFFFFFFF, "%"PRId64)
        break;
    // unsigned    
    case TSDB_DATA_TYPE_UTINYINT:
        UNSIGNED_RANDOM(uint8_t, 0xFF,"%u")
        break;
    case TSDB_DATA_TYPE_USMALLINT:
        UNSIGNED_RANDOM(uint16_t, 0xFFFF, "%u")
        break;
    case TSDB_DATA_TYPE_UINT:
        UNSIGNED_RANDOM(uint32_t, 0xFFFFFFFF, "%u")
        break;
    case TSDB_DATA_TYPE_UBIGINT:
        UNSIGNED_RANDOM(uint64_t, 0xFFFFFFFFFFFFFFFF, "%"PRIu64)
        break;
    // float double
    case TSDB_DATA_TYPE_FLOAT:
        FLOAT_RANDOM(float, FLT_MIN, FLT_MAX)
        break;
    case TSDB_DATA_TYPE_DOUBLE:
        FLOAT_RANDOM(double, DBL_MIN, DBL_MAX)
        break;
    // binary nchar
    case TSDB_DATA_TYPE_BINARY:
        genRadomString(val, fd->length > sizeof(val) ? sizeof(val) : fd->length);
        break;
    case TSDB_DATA_TYPE_NCHAR:
        genRadomString(val, fd->length > sizeof(val) ? sizeof(val) : fd->length);
        break;
    default:
        break;
    }

    size += sprintf(pstr + len, ",%s", val);
    return size;
}