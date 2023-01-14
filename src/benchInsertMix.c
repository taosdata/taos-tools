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

#include "benchInsertMix.h"
#include "bench.h"
#include "benchData.h"

//
// ------------------ mix ratio area -----------------------
//

#define MDIS 0
#define MUPD 1
#define MDEL 2

#define MCNT 3

struct SMixRatio {
  int8_t range[MCNT];

  // need generate count , calc from stb->insertRows * ratio
  uint64_t genCnt[MCNT];

  // status aleady done count
  uint64_t doneCnt[MCNT];

  // task out from batch to list buffer
  TSKEY* buf[MCNT];

  // bufer cnt
  uint64_t bMaxCnt[MCNT];  // buffer max count for buf
  uint64_t bCnt[MCNT];     // current valid cnt in buf
};

void mixRatioInit(SMixRatio* mix, SSuperTable* stb) {
  uint64_t rows = stb->insertRows;
  uint32_t batchSize = g_arguments->reqPerReq;

  if (batchSize == 0) batchSize = 1;

  // set range
  mix->range[MDIS] = stb->disRange;
  mix->range[MUPD] = stb->updRange;
  mix->range[MDEL] = stb->delRange;

  // calc count
  mix->genCnt[MDIS] = rows * stb->disRatio / 100;
  mix->genCnt[MUPD] = rows * stb->updRatio / 100;
  mix->genCnt[MDEL] = rows * stb->delRatio / 100;

  // malloc buffer
  for (int32_t i = 0; i < MCNT; i++) {
    // max
    if (mix->getCnt[i] > 0) {
      // buffer max count calc
      mix->bMaxCnt[i] = batchSize * 2 + mix->getCnt[i] / 1000;
      mix->buf[i] = calloc(mix->bMaxCnt[i], sizeof(TSKEY));
    } else {
      mix->bMaxCnt = 0;
      mix->buf[i] = NULL;
    }
    mix->bCnt = 0;
  }
}

void mixRatioExit(SMixRatio* mix) {
  // free buffer
  for (int32_t i = 0; i < MCNT; i++) {
    if (mix->buf[i]) {
      free(mix->buf[i]);
      mix->buf[i] = NULL;
    }
  }
}

//
// ------------------ gen area -----------------------
//

//
// generate head
//
uint32_t genInsertPreSql(threadInfo* info, SDataBase* db, SSuperTable* stb, char* tableName, char* pstr) {
  uint32_t len = 0;
  // ttl
  char ttl[20] = "";
  if (stb->ttl != 0) {
    sprintf(ttl, "TTL %d", stb->ttl);
  }

  if (stb->partialColNum == stb->cols->size) {
    if (stb->autoCreateTable) {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s USING %s.%s TAGS (%s) %s VALUES ", STR_INSERT_INTO, database->dbName,
                     tableName, database->dbName, stb->stbName, stb->tagDataBuf + stb->lenOfTags * tableSeq, ttl);
    } else {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s VALUES ", STR_INSERT_INTO, database->dbName, tableName);
    }
  } else {
    if (stb->autoCreateTable) {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s (%s) USING %s.%s TAGS (%s) %s VALUES ", STR_INSERT_INTO,
                     database->dbName, tableName, stb->partialColNameBuf, database->dbName, stb->stbName,
                     stb->tagDataBuf + stb->lenOfTags * tableSeq, ttl);
    } else {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s (%s) VALUES ", STR_INSERT_INTO, database->dbName, tableName,
                     stb->partialColNameBuf);
    }
  }

  return len;
}

//
// append row to batch buffer
//
uint32_t appendRowRuleOld(SSuperTable* stb, char* pstr, uint32_t len, int64_t timestamp) {
  uint32_t size = 0;
  int32_t  pos = raosRandom() % g_arguments->prepared_rand;
  int      disorderRange = stbInfo->disorderRange;

  if (stb->useSampleTs && !stb->random_data_source) {
    len += snprintf(pstr + len, MAX_SQL_LEN - len, "(%s)", stb->sampleDataBuf + pos * stb->lenOfCols);
  } else {
    int64_t disorderTs = 0;
    if (stb->disorderRatio > 0) {
      int rand_num = taosRandom() % 100;
      if (rand_num < stb->disorderRatio) {
        disorderRange--;
        if (0 == disorderRange) {
          disorderRange = stb->disorderRange;
        }
        disorderTs = stb->startTimestamp - disorderRange;
        debugPrint(
            "rand_num: %d, < disorderRatio:"
            " %d, disorderTs: %" PRId64 "\n",
            rand_num, stb->disorderRatio, disorderTs);
      }
    }
    len += snprintf(pstr + len, MAX_SQL_LEN - len, "(%" PRId64 ",%s)", disorderTs ? disorderTs : timestamp,
                    stb->sampleDataBuf + pos * stb->lenOfCols);
  }

  return size;
}

//
// generate body
//
uint32_t genBatchSql(threadInfo* info, SDataBase* db, SSuperTable* stb, SMixRatio* mix, char* pstr, uint32_t len) {
  uint32_t genRows = 0;
  uint32_t pos = 0;  // for pstr pos
  int64_t  ts = stb->startTimestamp;

  for (uint32_t i = 0; i < g_arguments->reqPerReq; i++) {
    switch (stb->genRowRule) {
      case RULE_OLD:
        len += appendRowRuleOld(stb, mix, pstr, len, ts);
        break;
      case RULE_MIX:
        len += appendRowRuleMix(info, db, stb, mix, pstr, len);
        break;
      default:
        break;
    }

    // move next ts
    ts += stb->timestamp_step;
    genRows++;

    // check over MAX_SQL_LENGTH
    if (len > (MAX_SQL_LEN - stbInfo->lenOfCols)) {
      break;
    }

  }

  return genRows;
}

//
// insert data to db->stb with info
//
bool insertDataMix(threadInfo* info, SDataBase* db, SSuperTable* stb) {
  uint64_t batchCnt = 0;

  // check interface
  if (stb->iface != TAOSC_IFACE) {
    return false;
  }

  // loop insert child tables
  for (uint64_t tbIdx = info->start_table_from; tbIdx <= info->end_table_to; tbIdx) {
    char* tbName = stb->childTblName[tbIdx];

    SMixRatio mixRatio;
    mixRatioInit(&mixRatio);
    uint32_t len = 0;
    batchCnt = 0;

    while (info->totalInsertRows < stb->insertRows) {
      // check terminate
      if (g_arguments->terminate) {
        break;
      }

      // generate pre sql  like "insert into tbname ( part column names) values  "
      len = genInsertPreSql(info, db, stb, tbName, info->buffer);

      // batch create sql values
      uint32_t batchRows = genBatchSql(info, db, stb & mixRatio, info->buffer + len);

      // execute insert sql
      int64_t startTs = toolsGetTimestampUs();
      if (execInsert(info, batchRows) != 0) {
        g_fail = true;
        break;
      }

      // execute delete sql if need
      if (mixRatioNeedDel(&mixRatio)) {
        if (execDelete(info) != 0) {
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
      batchCnt++;
    }  // row end

  }  // child table end

  // end
  if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;
  succPrint("thread[%d] %s(), completed total inserted rows: %" PRIu64 ", %.2f records/second\n", info->threadID,
            __func__, info->totalInsertRows, (double)(info->totalInsertRows / ((double)info->totalDelay / 1E6)));

  return true;
}
