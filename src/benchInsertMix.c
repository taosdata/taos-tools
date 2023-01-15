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

#define  NEED_TAKEOUT_ROW_TOBUF(type)  (mix->doneCnt[type] + mix->bufCnt[type] < mix->genCnt[type] && taosRandom()%100 <= mix->ratio[type])
#define  FORCE_TAKEOUT(type, remain) (remain < mix->genCnt[type] - mix->doneCnt[type] - mix->bufCnt[type] + 1000 )
#define  RD(max) (taosRandom() % max)

typedef struct {
  int8_t ratio[MCNT];
  int64_t range[MCNT];

  // need generate count , calc from stb->insertRows * ratio
  uint64_t genCnt[MCNT];

  // status aleady done count
  uint64_t doneCnt[MCNT];

  // task out from batch to list buffer
  TSKEY* buf[MCNT];

  // bufer cnt
  uint64_t capacity[MCNT]; // capacity size for buf
  uint64_t bufCnt[MCNT];   // current valid cnt in buf

  // calc need value
  int32_t curBatchCnt;

} SMixRatio;

void mixRatioInit(SMixRatio* mix, SSuperTable* stb) {
  uint64_t rows = stb->insertRows;
  uint32_t batchSize = g_arguments->reqPerReq;

  if (batchSize == 0) batchSize = 1;

  // set ratio
  mix->ratio[MDIS] = stb->disRatio;
  mix->ratio[MUPD] = stb->updRatio;
  mix->ratio[MDEL] = stb->delRatio;

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
  int      disorderRange = stb->disorderRange;

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
// create columns data 
//
uint32_t createColsDataRandom(SSuperTable* stb, char* pstr, uint32_t len, int64_t ts,) {
  uint32_t size = 0;
  int32_t  pos = RD(g_arguments->prepared_rand);

  // gen row data
  size = snprintf(pstr + len, MAX_SQL_LEN - len, "(%" PRId64 ",%s)", ts, stb->sampleDataBuf + pos * stb->lenOfCols);
  
  return size;
}

bool takeRowOutToBuf(SMixRatio* mix, uint8_t type, int64_t ts) {
    int64_t* buf = mix->buf[type];
    if(buf == NULL){
        return false;
    }

    if(mix->bufCnt[type] >= mix->capacity[type]) {
        // no space to save
        return false;
    }

    uint64_t bufCnt = mix->bufCnt[type];

    // save
    buf[bufCnt] = ts;
    // move next
    mix->bufCnt[type] += 1;

    return true;
}

//
// row rule mix , global info put into mix
//
#define MIN_COMMIT_ROWS 10000
uint32_t appendRowRuleMix(threadInfo* info, SDataBase* db, SSuperTable* stb, SMixRatio* mix, char* pstr, uint32_t len, int64_t ts, uint32_t* pGenRows) {
    uint32_t size = 0;
    
    // remain need generate rows
    uint64_t remain = stb->insertRows - info->totalInsertRows;
    bool forceDis = FORCE_TAKEOUT(MDIS, remain);
    bool forceUpd = FORCE_TAKEOUT(MUPD, remain);

    // disorder
    if ( forceDis || NEED_TAKEOUT_ROW_TOBUF(MDIS)) {
        // need take out current row to buf
        if(takeRowOutToBuf(mix, MDIS, ts)){
            return 0;
        }
    } else {
        // no cnt to be take out
        mix->takeOutCnt[MDIS] = genRandTakeCnt(MDIS);
    }

    // append row
    size = createColsDataRandom(stb, pstr, len, ts);
    *pGenRows += 1;

    // update
    if (forceUpd || NEED_TAKEOUT_ROW_TOBUF(MUPD)) {
        takeRowOutToBuf(mix, MUPD, ts);
    }

    return size;
}

//
// fill update rows from mix
//
uint32_t fillBatchWithBuf(SSuperTable* stb, SMixRatio* mix, int64_t startTime, char* pstr, uint32_t len, uint32_t* pGenRows, uint8_t type, uint32_t maxFill) {
    uint32_t size = 0;
    uint32_t maxRows = g_arguments->reqPerReq;
    uint32_t cntFill = taosRandom() % maxFill;
    if(cntFill == 0) {
        cntFill = maxFill;
    }

    if(maxRows - *pGenRows < cntFill) {
        cntFill = maxRows - *pGenRows;
    }

    int64_t* buf = mix->buf[type];
    int32_t  bufCnt = mix->bufCnt[type];

    // fill from buf
    int32_t selCnt = 0;
    int32_t findCnt = 0;
    int64_t ts;
    int32_t i;
    while(selCnt < cntFill && bufCnt > 0 && ++findCnt < cntFill * 2) {
        // get ts
        i = RD(bufCnt);
        ts = buf[i];
        if( ts > startTime) {
            // in current batch , ignore
            continue;
        }

        // generate row by ts
        size += createColsDataRandom(stb, pstr, len, ts);
        *pGenRows += 1;

        // remove current item
        mix->bufCnt[type] -= 1;
        buf[i] = buf[bufCnt - 1]; // last set to current
        bufCnt = mix->bufCnt[type]; 
    }

    return size;
}


//
// generate body
//
uint32_t genBatchSql(threadInfo* info, SDataBase* db, SSuperTable* stb, SMixRatio* mix, int64_t startTime, char* pstr, uint32_t slen) {
  uint32_t genRows = 0;
  uint32_t pos = 0;  // for pstr pos
  int64_t  ts = startTime;
  uint32_t len = slen; // slen: start len

  uint64_t remain = stb->insertRows - info->totalInsertRows;
  bool     forceDis = FORCE_TAKEOUT(MDIS, remain);
  bool     forceUpd = FORCE_TAKEOUT(MUPD, remain);

  while ( genRows < g_arguments->reqPerReq) {
    switch (stb->genRowRule) {
      case RULE_OLD:
        len += appendRowRuleOld(stb, mix, pstr, len, ts);
        genRows ++;
        break;
      case RULE_MIX:
        // add new row (maybe del)
        len += appendRowRuleMix(info, db, stb, mix, pstr, len, ts, &genRows);

        if( forceUpd || RD(stb->fillIntervalUpd)) {
            // fill update rows from buffer
            len += fillBatchWithBuf(stb, mix, startTime, pstr, len, &genRows, MUPD, stb->fillIntervalUpd*2);
        }

        if( forceDis || RD(stb->fillIntervalDis)) {
            // fill disorder rows from buffer
            len += fillBatchWithBuf(stb, mix, startTime, pstr, len, &genRows, MDIS, stb->fillIntervalDis*2);
        }
        break;
      default:
        break;
    }

    // move next ts
    ts += stb->timestamp_step;

    // check over MAX_SQL_LENGTH
    if (len > (MAX_SQL_LEN - stb->lenOfCols)) {
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
