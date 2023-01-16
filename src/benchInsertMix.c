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

typedef struct {
  uint64_t insertedRows;
  uint64_t updRows;
  uint64_t disRows;
  uint64_t delRows;
} STotal;


void mixRatioInit(SMixRatio* mix, SSuperTable* stb) {
  memset(mix, 0, sizeof(SMixRatio));
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
    if (mix->genCnt[i] > 0) {
      // buffer max count calc
      mix->capacity[i] = batchSize * 2 + mix->genCnt[i] / 1000;
      mix->buf[i] = calloc(mix->capacity[i], sizeof(TSKEY));
    } else {
      mix->capacity[i] = 0;
      mix->buf[i] = NULL;
    }
    mix->bufCnt[i] = 0;
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
//  --------------------- util ----------------
//

// return true can do execute delelte sql
bool needExecDel(SMixRatio* mix) {
  if (mix->genCnt[MDEL] == 0 || mix->doneCnt[MDEL] >= mix->genCnt[MDEL]) {
    return false;
  }

  return mix->bufCnt[MDEL] > 0;
}

//
// ------------------ gen area -----------------------
//

//
// generate head
//
uint32_t genInsertPreSql(threadInfo* info, SDataBase* db, SSuperTable* stb, char* tableName, uint64_t tableSeq, char* pstr) {
  uint32_t len = 0;
  // ttl
  char ttl[20] = "";
  if (stb->ttl != 0) {
    sprintf(ttl, "TTL %d", stb->ttl);
  }

  if (stb->partialColNum == stb->cols->size) {
    if (stb->autoCreateTable) {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s USING %s.%s TAGS (%s) %s VALUES ", STR_INSERT_INTO, db->dbName,
                     tableName, db->dbName, stb->stbName, stb->tagDataBuf + stb->lenOfTags * tableSeq, ttl);
    } else {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s VALUES ", STR_INSERT_INTO, db->dbName, tableName);
    }
  } else {
    if (stb->autoCreateTable) {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s (%s) USING %s.%s TAGS (%s) %s VALUES ", STR_INSERT_INTO,
                     db->dbName, tableName, stb->partialColNameBuf, db->dbName, stb->stbName,
                     stb->tagDataBuf + stb->lenOfTags * tableSeq, ttl);
    } else {
      len = snprintf(pstr, MAX_SQL_LEN, "%s %s.%s (%s) VALUES ", STR_INSERT_INTO, db->dbName, tableName,
                     stb->partialColNameBuf);
    }
  }

  return len;
}

//
// generate delete pre sql like "delete from st"
//
uint32_t genDelPreSql(SDataBase* db, SSuperTable* stb, char* tableName, char* pstr) {
  uint32_t len = 0;
  // super table name or child table name random select
  char* name = RD(2) ? tableName : stb->stbName;
  len = snprintf(pstr, MAX_SQL_LEN, "delete from %s.%s where ", db->dbName, name);

  return len;
}

//
// append row to batch buffer
//
uint32_t appendRowRuleOld(SSuperTable* stb, char* pstr, uint32_t len, int64_t timestamp) {
  uint32_t size = 0;
  int32_t  pos = RD(g_arguments->prepared_rand);
  int      disorderRange = stb->disorderRange;

  if (stb->useSampleTs && !stb->random_data_source) {
    size = snprintf(pstr + len, MAX_SQL_LEN - len, "(%s)", stb->sampleDataBuf + pos * stb->lenOfCols);
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
    // generate
    size = snprintf(pstr + len, MAX_SQL_LEN - len, "(%" PRId64 ",%s)", disorderTs ? disorderTs : timestamp,
                    stb->sampleDataBuf + pos * stb->lenOfCols);
  }

  return size;
}



// create columns data 
uint32_t createColsDataRandom(SSuperTable* stb, char* pstr, uint32_t len, int64_t ts) {
  uint32_t size = 0;
  int32_t  pos = RD(g_arguments->prepared_rand);

  // gen row data
  size = snprintf(pstr + len, MAX_SQL_LEN - len, "(%" PRId64 ",%s)", ts, stb->sampleDataBuf + pos * stb->lenOfCols);
  
  return size;
}

// take out row
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
uint32_t appendRowRuleMix(threadInfo* info, SSuperTable* stb, SMixRatio* mix, char* pstr, uint32_t len, int64_t ts, uint32_t* pGenRows) {
    uint32_t size = 0;
    
    // remain need generate rows
    uint64_t remain = stb->insertRows - info->totalInsertRows;
    bool forceDis = FORCE_TAKEOUT(MDIS, remain);
    bool forceUpd = FORCE_TAKEOUT(MUPD, remain);
    bool forceDel = FORCE_TAKEOUT(MDEL, remain);

    // delete
    if (forceDel || NEED_TAKEOUT_ROW_TOBUF(MDEL)) {
        takeRowOutToBuf(mix, MDEL, ts);
    }

    // disorder
    if ( forceDis || NEED_TAKEOUT_ROW_TOBUF(MDIS)) {
        // need take out current row to buf
        if(takeRowOutToBuf(mix, MDIS, ts)){
            return 0;
        }
    }

    // gen col data
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
// generate  insert batch body, return rows in batch
//
uint32_t genBatchSql(threadInfo* info, SDataBase* db, SSuperTable* stb, SMixRatio* mix, int64_t* pStartTime, char* pstr, uint32_t slen, STotal* pTotal) {
  uint32_t genRows = 0;
  int64_t  ts = *pStartTime;
  int64_t  startTime = *pStartTime;
  uint32_t len = slen; // slen: start len
  uint32_t insertedRows = 0;

  uint64_t remain = stb->insertRows - info->totalInsertRows;
  bool     forceDis = FORCE_TAKEOUT(MDIS, remain);
  bool     forceUpd = FORCE_TAKEOUT(MUPD, remain);

  while ( genRows < g_arguments->reqPerReq) {
    switch (stb->genRowRule) {
      case RULE_OLD:
        len += appendRowRuleOld(stb, pstr, len, ts);
        genRows ++;
        pTotal->insertedRows ++;
        break;
      case RULE_MIX:
        // add new row (maybe del)
        insertedRows = 0;
        len += appendRowRuleMix(info, stb, mix, pstr, len, ts, &insertedRows);
        if (insertedRows > 0) {
          genRows += insertedRows;
          pTotal->insertedRows += insertedRows;
        }

        if( forceUpd || RD(stb->fillIntervalUpd) == 0) {
            // fill update rows from buffer
            uint32_t updRows = 0;
            len += fillBatchWithBuf(stb, mix, startTime, pstr, len, &updRows, MUPD, stb->fillIntervalUpd*2);
            if (updRows > 0) {
              genRows += updRows;
              pTotal->updRows += updRows;
            }
        }

        if( forceDis || RD(stb->fillIntervalDis) == 0) {
            // fill disorder rows from buffer
            uint32_t disRows = 0;
            len += fillBatchWithBuf(stb, mix, startTime, pstr, len, &disRows, MDIS, stb->fillIntervalDis*2);
            if (disRows > 0) {
              genRows += disRows;
              pTotal->disRows += disRows;
            }
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

  *pStartTime = ts;

  return genRows;
}

//
// generate delete batch body
//
uint32_t genBatchDelSql(threadInfo* info, SSuperTable* stb, SMixRatio* mix, char* pstr, uint32_t slen) {
  uint32_t genRows = 0;
  uint32_t len = slen;  // slen: start len

  uint64_t remain = stb->insertRows - info->totalInsertRows;
  bool     forceDel = FORCE_TAKEOUT(MDEL, remain);
  bool     first = false;
  int64_t* buf = mix->buf[MDEL];
  int32_t  i;

  if (stb->genRowRule != RULE_MIX) {
    return 0;
  }

  // forceDel put all to buffer
  if (forceDel) {
    for (i = mix->bufCnt[MDEL] - 1; i >= 0; i--) {
      int64_t ts = buf[i];

      // draw
      len += snprintf(pstr + len, MAX_SQL_LEN, "%s ts=%" PRId64 "", first ? "" : " or ", ts);
      if (first) first = false;
      genRows++;
      mix->bufCnt[MDEL] -= 1;

      // check over MAX_SQL_LENGTH
      if (len > (MAX_SQL_LEN - stb->lenOfCols - 24)) {
        break;
      }
    }

    return genRows;
  }

  // random select count to delete
  uint32_t bufCnt = mix->bufCnt[MDEL];
  uint32_t count = RD(bufCnt);
  int32_t  loop = 0;
  first = true;
  while (genRows < count && ++loop < count * 2) {
    i = RD(bufCnt);
    int64_t ts = buf[i];

    // takeout
    len += snprintf(pstr + len, MAX_SQL_LEN, "%s ts=%" PRId64 "", first ? "" : " or ", ts);
    if (first) first = false;
    genRows++;
    // replace delete with last
    buf[i] = buf[bufCnt - 1];
    // reduce buffer count
    mix->bufCnt[MDEL] -= 1;
    bufCnt = mix->bufCnt[MDEL];

    // check over MAX_SQL_LENGTH
    if (len > (MAX_SQL_LEN - stb->lenOfCols - 24)) {
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

  STotal total;
  memset(&total, 0, sizeof(STotal));

  // loop insert child tables
  for (uint64_t tbIdx = info->start_table_from; tbIdx <= info->end_table_to; ++tbIdx) {
    char* tbName = stb->childTblName[tbIdx];

    SMixRatio mixRatio;
    mixRatioInit(&mixRatio, stb);
    uint32_t len = 0;
    batchCnt = 0;
    int64_t batchStartTime = stb->startTimestamp;
    STotal tbTotal;
    memset(&tbTotal, 0 , sizeof(STotal));

    while (tbTotal.insertedRows < stb->insertRows) {
      // check terminate
      if (g_arguments->terminate) {
        break;
      }

      // generate pre sql  like "insert into tbname ( part column names) values  "
      len = genInsertPreSql(info, db, stb, tbName, tbIdx, info->buffer);

      // batch create sql values
      STotal batTotal;
      memset(&batTotal, 0 , sizeof(STotal));
      uint32_t batchRows = genBatchSql(info, db, stb , &mixRatio, &batchStartTime, info->buffer, len, &batTotal);

      // execute insert sql
      //int64_t startTs = toolsGetTimestampUs();
      if (execBufSql(info, batchRows) != 0) {
        g_fail = true;
        g_arguments->terminate = true;
        break;
      }

      // update total
      if(batTotal.insertedRows>0) {
        tbTotal.insertedRows += batTotal.insertedRows;  
      }

      if (batTotal.disRows > 0) {
        tbTotal.disRows += batTotal.disRows;
        mixRatio.doneCnt[MDIS] += batTotal.disRows;
      }

      if (batTotal.updRows > 0) {
        tbTotal.updRows += batTotal.updRows;
        mixRatio.doneCnt[MUPD] += batTotal.updRows;
      }

      // delete
      if (needExecDel(&mixRatio)) {
        len = genDelPreSql(db, stb, tbName, info->buffer);
        batchRows = genBatchDelSql(info, stb, &mixRatio, info->buffer, len);
        if (batchRows > 0) {
            if (execBufSql(info, batchRows) != 0) {
              g_fail = true;
              break;
            }
            tbTotal.delRows += batchRows;
            mixRatio.doneCnt[MDEL] += batchRows;
        }
      }

      // sleep if need
      if (stb->insert_interval > 0) {
        debugPrint("%s() LN%d, insert_interval: %" PRIu64 "\n", __func__, __LINE__, stb->insert_interval);
        perfPrint("sleep %" PRIu64 " ms\n", stb->insert_interval);
        toolsMsleep((int32_t)stb->insert_interval);
      }

      // total
      batchCnt++;
    }  // row end

    // total
    total.insertedRows += tbTotal.insertedRows;
    total.delRows += tbTotal.delRows;
    total.disRows += tbTotal.disRows;
    total.updRows += tbTotal.updRows;

    // print
    infoPrint("table:%s inserted ok, rows inserted(%" PRId64 ")  disorder(%" PRId64 ") update(%" PRId64") delete(%" PRId64 ")",
              tbName, tbTotal.insertedRows, tbTotal.disRows, tbTotal.updRows, tbTotal.delRows);

  }  // child table end


  // end
  if (0 == info->totalDelay) info->totalDelay = 1;

  // total
  succPrint("thread[%d] %s(), completed total inserted rows: %" PRIu64 ", %.2f records/second\n", info->threadID,
            __func__, info->totalInsertRows, (double)(info->totalInsertRows / ((double)info->totalDelay / 1E6)));

  // print
  succPrint("inserted finished, rows inserted(%" PRId64 ")  disorder(%" PRId64 ") update(%" PRId64") delete(%" PRId64 ")",
            total.insertedRows, total.disRows, total.updRows, total.delRows);

  return true;
}
