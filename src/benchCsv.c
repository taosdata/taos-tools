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
#include <benchDataMix.h>
#include <benchCsv.h>


//
// main etry
//
int csvTestProcess() {
    pthread_t handle = NULL;
    int ret = pthread_create(handle, NULL, csvWriteThread, NULL);
    if (ret != 0) {
        errorPrint("pthread_create failed. error code =%d", ret);
        return -1;
    }
    pthread_join(handle);
    return 0;
}

void csvWriteThread(void* param) {
    // write thread 
    for (int i = 0; i < g_arguments->databases->size; i++) {
        // database 
        SDataBase * db = benchArrayGet(g_arguments->databases, i);
        for (int j=0; i< db->superTbls->size; j++) {
            // stb
            SSuperTable* stb = benchArrayGet(database->superTbls, j);
            // gen csv
            int ret = genWithSTable(db, stb, g_arguments->csvPath);
            if(ret != 0) {
                errorPrint("failed generate to csv. db=%s stb=%s error code=%d", db->dbName, stb->stbName, ret);
                return ret;
            }
        }
    }
}

int genWithSTable(SDataBase* db, SSuperTable* stb, char* outDir) {
    // filename
    int ret = 0;
    char outFile[MAX_FILE_NAME_LEN] = {0};
    obtainCsvFile(outFile, db, stb, outDir);
    FILE * fs = fopen(outFile, "w");
    if(fs == NULL) {
        errorPrint("failed create csv file. file=%s, last errno=%d strerror=%s", outFile, errno, strerror(errno));
        return -1;
    }

    int rowLen = TSDB_TABLE_NAME_LEN + stb->lenOfTags + stb->lenOfCols + stb->tags->size + stb->cols->size;
    int bufLen = rowLen * g_arguments->reqPerReq;
    char* buf  = benchCalloc(1, bufLen, true);

    if (stb->interlaceRows > 0) {
        // interlace mode
        ret = interlaceWriteCsv(db, stb, fs, buf, bufLen, rowLen * 2);
    } else {
        // batch mode 
        ret = batchWriteCsv(db, stb, fs, buf, bufLen, rowLen * 2);
    }

    tmfree(buf);
    close(fs);

    return ret;
}


void obtainCsvFile(char * outFile, SDataBase* db, SSuperTable* stb, char* outDir) {
    sprintf(outFile, "%s//%s-%s.csv", outDir, db->dbName, stb->stbName);
}



int32_t writeCsvFile(FILE* f, char * buf, int32_t len) {
    size_t size = fwrite(buf, 1, len, f);
    if(size ! = len) {
        errorPrint("failed to write csv file. expect write length:%d real write length:%d", len, size);
        return -1;
    }
    return 0;
}

int batchWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int bufLen, int minRemain) {
    int ret    = 0;
    int cnt    = 0;
    int pos    = 0;
    int64_t n  = 0; // already inserted rows for one child table

    int    tagDataLen = stb->lenOfTags + stb->tags->size + 256;
    char * tagData    = (char *) benchCalloc(1, tagDataLen, true);    
    int    colDataLen = stb->lenOfCols + stb->cols->size + 256;
    char * colData    = (char *) benchCalloc(1, colDataLen, true);

    // gen child name
    for (int64_t i = 0; i < stb->childTblCount; i++) {
        int64_t ts = stb->startTimestamp;
        // tags
        genTagData(tagData, stb, i);
        // insert child column data
        for(int64_t j=0; j< stb->insertRows; j++) {
            genColumnData(colData, ts, i, j);
            // combine
            pos += sprintf(buf + pos, "%s,%s\n", tagData, colData);
            if (bufLen - pos < minRemain ) {
                // submit 
                ret = writeCsvFile(fs, buf, pos);
                if (ret != 0) {
                    goto END;
                }

                pos = 0;
            }

            // ts move next
            ts += stb->timestamp_step;
        }
    }

END:
    // free
    tmfree(tagData);
    tmfree(colData);
    return ret;
}

int interlaceWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int bufLen, int minRemain) {
    int ret    = 0;
    int cnt    = 0;
    int pos    = 0;
    int64_t n  = 0; // already inserted rows for one child table
    int64_t tk = 0;

    char **tagDatas   = (char **)benchCalloc(stb->childTblCount, sizeof(char *), true);
    int    colDataLen = stb->lenOfCols + stb->cols->size + 256;
    char * colData    = (char *) benchCalloc(1, colDataLen, true);
    
    while (n < stb->insertRows ) {
        for (int64_t i = 0; i < stb->childTblCount; i++) {
            // start one table
            int64_t ts = stb->startTimestamp;
            int64_t ck = 0;
            // tags
            tagDatas[i] = genTagData(NULL, stb, i, &tk);
            // calc need insert rows
            int64_t needInserts = stb->interlaceRows;
            if(needInserts > stb->insertRows - n) {
                needInserts = stb->insertRows - n;
            }

            for (int64_t j = 0; j < needInserts; j++) {
                genColumnData(colData, ts, i, j, &ck, db->precision);
                // combine tags,cols
                pos += sprintf(buf + pos, "%s,%s\n", tagDatas[i], colData);
                if (bufLen - pos <  ) {
                    // submit 
                    ret = writeToFile(fs, buf, pos);
                    if (ret != 0) {
                        goto END:
                    }
                    pos = 0;
                }

                // ts move next
                ts += stb->timestamp_step;
            }

            if (i == 0) {
                n += needInserts;
            }
        }
    }

END:
    // free
    for(int64_t m = 0 ; m < stb->childTblCount; m ++) {
        if (tagDatas[m]) {
            tmfree(tagDatas[m]);
        }
    }
    tmfree(colData);
    return ret;
}

// gen tag data 
char * genTagData(char* buf, SSuperTable* stb, int64_t i, int64_t *k) {
    // malloc
    char* tagData;
    if (buf == NULL) {
        int tagDataLen = TSDB_TABLE_NAME_LEN +  stb->lenOfTags + stb->tags->size + 32;
        tagData = benchCalloc(1, tagDataLen, true);
    } else {
        tagData = buf;
    }

    int pos = 0;
    // tbname
    pos += sprintf(tagData + pos, "%s%d", stb->childTblPrefix);
    // tags
    pos += genRowByField(tagData + pos, stb->tags, stb->tags->size, stb->binaryPrefex, stb->ncharPrefex, k);

    return tagData;
}

// gen column data 
char * genColumnData(char* colData, SSuperTable* stb, int64_t ts, int64_t i, int32_t precision, int64_t *k) {
    int  pos = 0;
    char str[128] = "";
    toolsFormatTimestamp(colData, ts, precision);
    pos = strlen(colData);

    // columns
    genRowByField(colData + pos, stb->cols, stb->cols->size, stb->binaryPrefex, stb->ncharPrefex, precision, k);
    return colData;
}


int32_t genRowByField(char* buf, int32_t pos1, BArray* fields, int16_t fieldCnt, char* binanryPrefix, char* ncharPrefix, int64_t *k) {

  // other cols data
  int32_t pos2 = 0;
  for(uint16_t i = 0; i < fieldCnt; i++) {
    Field* fd = benchArrayGet(fields, GET_IDX(i));
    char* prefix = "";
    if(fd->type == TSDB_DATA_TYPE_BINARY || fd->type == TSDB_DATA_TYPE_VARBINARY) {
      if(stb->binaryPrefex) {
        prefix = stb->binaryPrefex;
      }
    } else if(fd->type == TSDB_DATA_TYPE_NCHAR) {
      if(stb->ncharPrefex) {
        prefix = stb->ncharPrefex;
      }
    }

    pos2 += dataGenByField(fd, buf + pos1 + pos2, prefix, k);
  }
  
  return pos2;
}
