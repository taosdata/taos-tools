/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#ifdef WINDOWS
#include <time.h>
#include <WinSock2.h>
#endif

#include <toolsdef.h>

int64_t atomic_add_fetch_64(int64_t volatile* ptr, int64_t val) {
#ifdef WINDOWS
    return InterlockedExchangeAdd64((int64_t volatile*)(ptr), (int64_t)(val)) + (int64_t)(val);
#elif defined(_TD_NINGSI_60)
    return __sync_add_and_fetch((ptr), (val));
#else
    return __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

