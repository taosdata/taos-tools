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


uint32_t dataGenByField(Field* fd, char* pstr, uint32_t len) {
    uint32_t size = 0;

    switch (fd->type) {
    case TSDB_DATA_TYPE_INT:
        break;
    
    default:
        break;
    }

    return size;
}