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
#include "benchData.h"
#include "benchInsertMix.h"

typedef struct SMixRatio {
    uint32_t disorderRatio;
    uint32_t updateRate;
    uint32_t deleteRatio;
} SMixRatio;


uint32_t genBatchSql() {
    uint32_t genRows = 0;
    for (uint32_t i = 0; i < g_arguments->reqPerReq; i++) {
        appendSqlValues();
        genRows++;
    }
}

//
// insert data to db->stb with info
//
bool insertDataMix(threadInfo* info, SDataBase* db, SSuperTable* stb) {

    // check interface
    if(stb->iface != TAOSC_IFACE) {
        return false;
    }

    // loop insert child tables
    for(uint64_t tbIdx = info->start_table_from; tbIdx <= info->end_table_to; tbIdx) {
        char * tbName = stb->childTblName[tbIdx];

        SMixRatio mixRatio;
        mixRatioInit(&mixRatio);


        while (info->totalInsertRows < stb->insertRows) {
          // check terminate
          if (g_arguments->terminate) {
            break;
          }
          
          // generate pre sql like "insert into tbname ( part column names) values  "
          genInsertPreSql(info, db, stb, tbName);

          // batch create sql values
          uint32_t batchRows = genBatchSql(info, db, stb &mixRatio);

          // execute insert sql
          int64_t startTs = toolsGetTimestampUs();
          if (execInsert(info, batchRows)!=0) {
            g_fail = true;
            break;
          }

          // execute delete sql if need
          if(mixRatioNeedDel(&mixRatio)) {
            if(execDelete(info)!=0) {
                g_fail = true;
                break;
            }
          }
          
          // sleep if need
          if (stb->insert_interval > 0) {
            debugPrint("%s() LN%d, insert_interval: %" PRIu64 "\n", __func__, __LINE__, stb->insert_interval);
            perfPrint("sleep %" PRIu64 " ms\n", stb->insert_interval);
            toolsMsleep((int32_t)stb->insert_interval);
          }

          // total
          info->totalInsertRows += batchRows;

        }  // row end

    } // child table end

    // end
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;
    succPrint("thread[%d] %s(), completed total inserted rows: %" PRIu64 ", %.2f records/second\n", info->threadID,
              __func__, info->totalInsertRows, (double)(info->totalInsertRows / ((double)info->totalDelay / 1E6)));

    return true;
}
