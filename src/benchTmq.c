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
#include <time.h>
#include <bench.h>
#include <benchData.h>

typedef struct {
    tmq_t* tmq;
    int64_t  totalMsgs;
    int64_t  totalRows;

    int      calcDelay;
    int      id;
    FILE*    fpOfRowsFile;

	int64_t totalDelay;
	int64_t delayItemCnt;
	float avgDelayOfTmq;
	int32_t maxDelay;
	int32_t minDelay;
	int64_t lastDelayPrintTime;
} tmqThreadInfo;


#define   MAX_DELAY_ITEM_NUM   (1000 * 1000 * 100)
static int running = 1;

void printfTmqConfigIntoFile() {
  if (NULL == g_arguments->fpOfInsertResult) {
      return;
  }

  infoPrintToFile(g_arguments->fpOfInsertResult, "%s\n", "============================================");

  SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
  infoPrintToFile(g_arguments->fpOfInsertResult, "concurrent: %d\n", pConsumerInfo->concurrent);
  infoPrintToFile(g_arguments->fpOfInsertResult, "pollDelay: %d\n", pConsumerInfo->pollDelay);
  infoPrintToFile(g_arguments->fpOfInsertResult, "groupId: %s\n", pConsumerInfo->groupId);
  infoPrintToFile(g_arguments->fpOfInsertResult, "clientId: %s\n", pConsumerInfo->clientId);
  infoPrintToFile(g_arguments->fpOfInsertResult, "autoOffsetReset: %s\n", pConsumerInfo->autoOffsetReset);
  infoPrintToFile(g_arguments->fpOfInsertResult, "enableAutoCommit: %s\n", pConsumerInfo->enableAutoCommit);
  infoPrintToFile(g_arguments->fpOfInsertResult, "autoCommitIntervalMs: %d\n", pConsumerInfo->autoCommitIntervalMs);
  infoPrintToFile(g_arguments->fpOfInsertResult, "enableHeartbeatBackground: %s\n", pConsumerInfo->enableHeartbeatBackground);
  infoPrintToFile(g_arguments->fpOfInsertResult, "snapshotEnable: %s\n", pConsumerInfo->snapshotEnable);
  infoPrintToFile(g_arguments->fpOfInsertResult, "msgWithTableName: %s\n", pConsumerInfo->msgWithTableName);
  infoPrintToFile(g_arguments->fpOfInsertResult, "rowsFile: %s\n", pConsumerInfo->rowsFile);
  infoPrintToFile(g_arguments->fpOfInsertResult, "expectRows: %d\n", pConsumerInfo->expectRows);
  
  for (int i = 0; i < pConsumerInfo->topicCount; ++i) {
      infoPrintToFile(g_arguments->fpOfInsertResult, "topicName[%d]: %s\n", i, pConsumerInfo->topicName[i]);
      infoPrintToFile(g_arguments->fpOfInsertResult, "topicSql[%d]: %s\n", i, pConsumerInfo->topicSql[i]);
  }  
}


static int create_topic() {
    SBenchConn* conn = initBenchConn();
    if (conn == NULL) {
        return -1;
    }
    TAOS* taos = conn->taos;
    char command[SHORT_1K_SQL_BUFF_LEN];
    memset(command, 0, SHORT_1K_SQL_BUFF_LEN);

    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    for (int i = 0; i < pConsumerInfo->topicCount; ++i) {
        char buffer[SHORT_1K_SQL_BUFF_LEN];
        memset(buffer, 0, SHORT_1K_SQL_BUFF_LEN);
        snprintf(buffer, SHORT_1K_SQL_BUFF_LEN, "create topic if not exists %s as %s",
                         pConsumerInfo->topicName[i], pConsumerInfo->topicSql[i]);
        TAOS_RES *res = taos_query(taos, buffer);

        if (taos_errno(res) != 0) {
            errorPrint("failed to create topic: %s, reason: %s\n",
                     pConsumerInfo->topicName[i], taos_errstr(res));
            closeBenchConn(conn);
            return -1;
        }

        infoPrint("successfully create topic: %s\n", pConsumerInfo->topicName[i]);
    }
    closeBenchConn(conn);
    return 0;
}

static tmq_list_t * buildTopicList() {
    tmq_list_t * topic_list = tmq_list_new();
    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    for (int i = 0; i < pConsumerInfo->topicCount; ++i) {
        tmq_list_append(topic_list, pConsumerInfo->topicName[i]);
    }
    infoPrint("%s", "successfully build topic list\n");
    return topic_list;
}

int tmqSubscribe(tmqThreadInfo * pThreadInfo) {
    int ret = 0;
    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;

    char tmpBuff[128] = {0};

    tmq_list_t * topic_list = buildTopicList();

    char groupId[16] = {0};
	if ((NULL == pConsumerInfo->groupId) || (0 == strlen(pConsumerInfo->groupId))) {
		// rand string
		memset(groupId, 0, sizeof(groupId));
		rand_string(groupId, sizeof(groupId) - 1, 0);
		infoPrint("rand generate group id: %s\n", groupId);
	    pConsumerInfo->groupId = groupId;
	}
    
    tmq_conf_t * conf = tmq_conf_new();
    tmq_conf_set(conf, "td.connect.user", g_arguments->user);
    tmq_conf_set(conf, "td.connect.pass", g_arguments->password);
    tmq_conf_set(conf, "td.connect.ip", g_arguments->host);
    
    memset(tmpBuff, 0, sizeof(tmpBuff));
    snprintf(tmpBuff, 16, "%d", g_arguments->port);
    tmq_conf_set(conf, "td.connect.port", tmpBuff);
    
    tmq_conf_set(conf, "group.id", pConsumerInfo->groupId);
    
    memset(tmpBuff, 0, sizeof(tmpBuff));
    snprintf(tmpBuff, 16, "%s_%d", pConsumerInfo->clientId, pThreadInfo->id);
    tmq_conf_set(conf, "client.id", tmpBuff);
    
    tmq_conf_set(conf, "auto.offset.reset", pConsumerInfo->autoOffsetReset);
    tmq_conf_set(conf, "enable.auto.commit", pConsumerInfo->enableAutoCommit);
    
    memset(tmpBuff, 0, sizeof(tmpBuff));
    snprintf(tmpBuff, 16, "%d", pConsumerInfo->autoCommitIntervalMs);
    tmq_conf_set(conf, "auto.commit.interval.ms", tmpBuff);
    
    tmq_conf_set(conf, "enable.heartbeat.background", pConsumerInfo->enableHeartbeatBackground);
    tmq_conf_set(conf, "experimental.snapshot.enable", pConsumerInfo->snapshotEnable);
    tmq_conf_set(conf, "msg.with.table.name", pConsumerInfo->msgWithTableName);
    
    pThreadInfo->tmq = tmq_consumer_new(conf, NULL, 0);
    tmq_conf_destroy(conf);
    if (pThreadInfo->tmq == NULL) {
        errorPrint("%s", "failed to execute tmq_consumer_new\n");
		tmq_list_destroy(topic_list);
        ret = -1;
        return ret;
    }
    infoPrint("thread[%d]: successfully create consumer\n", pThreadInfo->id);
    int32_t code = tmq_subscribe(pThreadInfo->tmq, topic_list);
    if (code) {
        errorPrint("failed to execute tmq_subscribe, reason: %s\n", tmq_err2str(code));
        ret = -1;
	    tmq_list_destroy(topic_list);
        return ret;
    }
    infoPrint("thread[%d]: successfully subscribe topics\n", pThreadInfo->id);
    tmq_list_destroy(topic_list);
	
    return ret;
}

static int32_t data_msg_process(TAOS_RES* msg, tmqThreadInfo* pInfo, int32_t msgIndex) {
  char* buf = (char*)calloc(1, 16*1024);
  if (NULL == buf) {
      errorPrint("consumer id %d calloc memory fail.\n", pInfo->id);
      return 0;
  }

  int32_t totalRows = 0;

  // infoPrint("topic: %s\n", tmq_get_topic_name(msg));
  int32_t     vgroupId = tmq_get_vgroup_id(msg);
  const char* dbName = tmq_get_db_name(msg);
  const char* tblName = tmq_get_table_name(msg);

  if (pInfo->fpOfRowsFile) {
    fprintf(pInfo->fpOfRowsFile, "consumerId: %d, msg index:%d\n", pInfo->id, msgIndex);
    fprintf(pInfo->fpOfRowsFile, "dbName: %s, tblname: %s, topic: %s, vgroupId: %d\n", dbName, tblName != NULL ? tblName : "invalid table",
                  tmq_get_topic_name(msg), vgroupId);
  }

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);

    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    const char* tbName = tmq_get_table_name(msg);

    taos_print_row(buf, row, fields, numOfFields);

    if (pInfo->fpOfRowsFile) {
        fprintf(pInfo->fpOfRowsFile, "tbname:%s, rows[%d]:\n%s\n", (tbName != NULL ? tbName : "null table"), totalRows, buf);        
    }

	if (pInfo->calcDelay) {
		char tsStr[32] = {0};
		strncpy(tsStr,buf,19);
        int64_t insertTs = atoll(tsStr);
		int64_t currentTs = toolsGetTimestampNs();
		/*
		if (currentTs < insertTs) {
		    fprintf(g_arguments->fpOfInsertResult, "tbname:%s, rows[%d]:\n%s\n", (tbName != NULL ? tbName : "null table"), totalRows, buf); 
			fprintf(g_arguments->fpOfInsertResult, "insert ts:%" PRId64 ", current ts: %" PRId64 "\n", insertTs, currentTs); 
		}
		*/
		int32_t delayOfTmq = (int32_t)((currentTs - insertTs)/1000000);
		pInfo->totalDelay += delayOfTmq;
		pInfo->delayItemCnt++;

	    if (delayOfTmq > pInfo->maxDelay) {
			pInfo->maxDelay = delayOfTmq;
	    }

	    if (delayOfTmq < pInfo->minDelay) {
			pInfo->minDelay = delayOfTmq;
	    }

        /*
		if (pInfo->minDelay < 0) {
		    fprintf(g_arguments->fpOfInsertResult, "==tbname:%s, rows[%d]:\n%s\n", (tbName != NULL ? tbName : "null table"), totalRows, buf); 
			fprintf(g_arguments->fpOfInsertResult, "==insert ts:%" PRId64 ", current ts: %" PRId64 "\n", insertTs, currentTs); 
		}
		*/

        uint64_t currentPrintTime = toolsGetTimestampMs();;
        if (currentPrintTime - pInfo->lastDelayPrintTime > 10 * 1000) {
		    if (pInfo->delayItemCnt > 0) {
		        pInfo->avgDelayOfTmq = (float)pInfo->totalDelay / pInfo->delayItemCnt / 1000.0;
		    }
            infoPrint("consumer id: %d, cosnume delay avg: %.3f s, max: %d ms, min: %d ms\n", 
                       pInfo->id, pInfo->avgDelayOfTmq, pInfo->maxDelay, pInfo->minDelay);
            pInfo->lastDelayPrintTime = currentPrintTime;
        }
	}

    totalRows++;
  }
  free(buf);
  return totalRows;
}

static void* tmqConsume(void* arg) {
    tmqThreadInfo *pThreadInfo = (tmqThreadInfo*)arg;

    tmqSubscribe(pThreadInfo);

    int64_t totalMsgs = 0;
    int64_t totalRows = 0;
	int32_t manualCommit = 0;

    infoPrint("consumer id %d start to loop pull msg\n", pThreadInfo->id);

	if ((NULL != g_tmqInfo.consumerInfo.enableManualCommit) && (0 == strncmp("true", g_tmqInfo.consumerInfo.enableManualCommit, 4))) {
        manualCommit = 1;		
        infoPrint("consumer id %d enable manual commit\n", pThreadInfo->id);
	}

    int64_t  lastTotalMsgs = 0;
    int64_t  lastTotalRows = 0;
    int64_t lastPrintTime = toolsGetTimestampMs();
	pThreadInfo->lastDelayPrintTime = lastPrintTime;

    int32_t consumeDelay = g_tmqInfo.consumerInfo.pollDelay == -1 ? -1 : g_tmqInfo.consumerInfo.pollDelay;
    while (running) {
      TAOS_RES* tmqMsg = tmq_consumer_poll(pThreadInfo->tmq, consumeDelay);
      if (tmqMsg) {
        tmq_res_t msgType = tmq_get_res_type(tmqMsg);
        if (msgType == TMQ_RES_DATA) {
          totalRows += data_msg_process(tmqMsg, pThreadInfo, totalMsgs);
        } else {
          errorPrint("consumer id %d get error msg type: %d.\n", pThreadInfo->id, msgType);
          taos_free_result(tmqMsg);
          break;
        }

		if (0 != manualCommit) {
            tmq_commit_sync(pThreadInfo->tmq, tmqMsg);
		}
		
        taos_free_result(tmqMsg);

        totalMsgs++;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30 * 1000) {
          infoPrint("consumer id %d has poll total msgs: %" PRId64 ", period rate: %.3f msgs/s, total rows: %" PRId64 ", period rate: %.3f rows/s\n",
              pThreadInfo->id, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / (currentPrintTime - lastPrintTime), totalRows, (totalRows - lastTotalRows) * 1000.0 / (currentPrintTime - lastPrintTime));
          lastPrintTime = currentPrintTime;
          lastTotalMsgs = totalMsgs;
          lastTotalRows = totalRows;
        }

        if ((g_tmqInfo.consumerInfo.expectRows > 0) && (totalRows > g_tmqInfo.consumerInfo.expectRows)) {
            infoPrint("consumer id %d consumed rows: %" PRId64 " over than expect rows: %d, exit consume\n", pThreadInfo->id, totalRows, g_tmqInfo.consumerInfo.expectRows);
            break;
        }
      } else {
        infoPrint("consumer id %d no poll more msg when time over, break consume\n", pThreadInfo->id);
        break;
      }
    }

    pThreadInfo->totalMsgs = totalMsgs;
    pThreadInfo->totalRows = totalRows;

    int32_t code;
    //code = tmq_unsubscribe(pThreadInfo->tmq);
    //if (code != 0) {
    //  errorPrint("thread %d tmq_unsubscribe() fail, reason: %s\n", i, tmq_err2str(code));
    //}

    code = tmq_consumer_close(pThreadInfo->tmq);
    if (code != 0) {
        errorPrint("thread %d tmq_consumer_close() fail, reason: %s\n",
                   pThreadInfo->id, tmq_err2str(code));
    }
    pThreadInfo->tmq = NULL;

	if (pThreadInfo->delayItemCnt > 0) {
	    pThreadInfo->avgDelayOfTmq = (float)pThreadInfo->totalDelay / pThreadInfo->delayItemCnt / 1000.0;	
	}

    infoPrint("consumerId: %d, consume msgs: %" PRId64 ", consume rows: %" PRId64 ", consume delay avg: %0.3f s, max: %d ms, min: %d ms\n", pThreadInfo->id, totalMsgs, totalRows, pThreadInfo->avgDelayOfTmq, pThreadInfo->maxDelay, pThreadInfo->minDelay);
    infoPrintToFile(g_arguments->fpOfInsertResult,
              "consumerId: %d, consume msgs: %" PRId64 ", consume rows: %" PRId64 ", consume delay avg: %0.3f s, max: %d ms, min: %d ms\n", pThreadInfo->id, totalMsgs, totalRows, pThreadInfo->avgDelayOfTmq, pThreadInfo->maxDelay, pThreadInfo->minDelay);

    return NULL;
}

int subscribeTestProcess() {
    printfTmqConfigIntoFile();
    int ret = 0;
    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    if (pConsumerInfo->topicCount > 0) {
        if (create_topic()) {
            return -1;
        }
    }

    pthread_t * pids = benchCalloc(pConsumerInfo->concurrent, sizeof(pthread_t), true);
    tmqThreadInfo *infos = benchCalloc(pConsumerInfo->concurrent, sizeof(tmqThreadInfo), true);

    for (int i = 0; i < pConsumerInfo->concurrent; ++i) {
        char tmpBuff[128] = {0};

        tmqThreadInfo * pThreadInfo = infos + i;
        pThreadInfo->totalMsgs = 0;
        pThreadInfo->totalRows = 0;
        pThreadInfo->id = i;

        if (strlen(pConsumerInfo->rowsFile)) {
            memset(tmpBuff, 0, sizeof(tmpBuff));
            snprintf(tmpBuff, 64, "%s_%d", pConsumerInfo->rowsFile, i);
            pThreadInfo->fpOfRowsFile = fopen(tmpBuff, "a");
            if (NULL == pThreadInfo->fpOfRowsFile) {
                errorPrint("failed to open %s file for save rows\n", pConsumerInfo->rowsFile);
                ret = -1;
                goto tmq_over;
            }
        }
		
		pThreadInfo->calcDelay = 1;		
		pThreadInfo->totalDelay = 0;
		pThreadInfo->delayItemCnt = 0;
		pThreadInfo->avgDelayOfTmq = 0;
		pThreadInfo->maxDelay = 0;
		pThreadInfo->minDelay = 0x7fff;		
        pthread_create(pids + i, NULL, tmqConsume, pThreadInfo);
    }

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t totalMsgs = 0;
    int64_t totalRows = 0;
	int64_t totalDelay = 0;
	int32_t totalItemCnt = 0;
	int32_t maxDelay = 0;
	int32_t minDelay = 0x7fff;

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        tmqThreadInfo * pThreadInfo = infos + i;

        if (pThreadInfo->fpOfRowsFile) {
            fclose(pThreadInfo->fpOfRowsFile);
            pThreadInfo->fpOfRowsFile = NULL;
        }

        totalMsgs += pThreadInfo->totalMsgs;
        totalRows += pThreadInfo->totalRows;

        totalDelay += pThreadInfo->totalDelay;
		totalItemCnt += pThreadInfo->delayItemCnt;

		if (pThreadInfo->maxDelay > maxDelay) {
			maxDelay = pThreadInfo->maxDelay;
		}

		if (pThreadInfo->minDelay < minDelay) {
			minDelay = pThreadInfo->minDelay;
		}
    }

	float avgDelay = 0;
	if (totalItemCnt > 0) {
        avgDelay = (float)totalDelay / totalItemCnt / 1000.0;
	}
	
    infoPrint("Consumed total msgs: %" PRId64 ", total rows: %" PRId64 ", consume delay avg: %.3f s, max: %d ms, min: %d ms \n",
              totalMsgs, totalRows, avgDelay, maxDelay, minDelay);
    infoPrintToFile(g_arguments->fpOfInsertResult,
                    "Consumed total msgs: %" PRId64 ","
                    "total rows: %" PRId64 ", consume delay avg: %.3f s, max: %d ms, min: %d ms \n", totalMsgs, totalRows, avgDelay, maxDelay, minDelay);

tmq_over:
    free(pids);
    free(infos);
    return ret;
}


int subscribeTestProcess_bak() {
    printfTmqConfigIntoFile();
    int ret = 0;
    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    if (pConsumerInfo->topicCount > 0) {
        if (create_topic()) {
            return -1;
        }
    }

    tmq_list_t * topic_list = buildTopicList();

    char groupId[16] = {0};
	if ((NULL == pConsumerInfo->groupId) || (0 == strlen(pConsumerInfo->groupId))) {
		// rand string
		memset(groupId, 0, sizeof(groupId));
		rand_string(groupId, sizeof(groupId) - 1, 0);
		infoPrint("rand generate group id: %s\n", groupId);
	    pConsumerInfo->groupId = groupId;
	}

    pthread_t * pids = benchCalloc(pConsumerInfo->concurrent, sizeof(pthread_t), true);
    tmqThreadInfo *infos = benchCalloc(pConsumerInfo->concurrent, sizeof(tmqThreadInfo), true);

    for (int i = 0; i < pConsumerInfo->concurrent; ++i) {
        char tmpBuff[128] = {0};

        tmqThreadInfo * pThreadInfo = infos + i;
        pThreadInfo->totalMsgs = 0;
        pThreadInfo->totalRows = 0;
        pThreadInfo->id = i;

        if (strlen(pConsumerInfo->rowsFile)) {
            memset(tmpBuff, 0, sizeof(tmpBuff));
            snprintf(tmpBuff, 64, "%s_%d", pConsumerInfo->rowsFile, i);
            pThreadInfo->fpOfRowsFile = fopen(tmpBuff, "a");
            if (NULL == pThreadInfo->fpOfRowsFile) {
                errorPrint("failed to open %s file for save rows\n", pConsumerInfo->rowsFile);
                ret = -1;
                goto tmq_over;
            }
        }
		
        tmq_conf_t * conf = tmq_conf_new();
        tmq_conf_set(conf, "td.connect.user", g_arguments->user);
        tmq_conf_set(conf, "td.connect.pass", g_arguments->password);
        tmq_conf_set(conf, "td.connect.ip", g_arguments->host);

        memset(tmpBuff, 0, sizeof(tmpBuff));
        snprintf(tmpBuff, 16, "%d", g_arguments->port);
        tmq_conf_set(conf, "td.connect.port", tmpBuff);

        tmq_conf_set(conf, "group.id", pConsumerInfo->groupId);

        memset(tmpBuff, 0, sizeof(tmpBuff));
        snprintf(tmpBuff, 16, "%s_%d", pConsumerInfo->clientId, i);
        tmq_conf_set(conf, "client.id", tmpBuff);

        tmq_conf_set(conf, "auto.offset.reset", pConsumerInfo->autoOffsetReset);
        tmq_conf_set(conf, "enable.auto.commit", pConsumerInfo->enableAutoCommit);

        memset(tmpBuff, 0, sizeof(tmpBuff));
        snprintf(tmpBuff, 16, "%d", pConsumerInfo->autoCommitIntervalMs);
        tmq_conf_set(conf, "auto.commit.interval.ms", tmpBuff);

        tmq_conf_set(conf, "enable.heartbeat.background", pConsumerInfo->enableHeartbeatBackground);
        tmq_conf_set(conf, "experimental.snapshot.enable", pConsumerInfo->snapshotEnable);
        tmq_conf_set(conf, "msg.with.table.name", pConsumerInfo->msgWithTableName);

        pThreadInfo->tmq = tmq_consumer_new(conf, NULL, 0);
        tmq_conf_destroy(conf);
        if (pThreadInfo->tmq == NULL) {
            errorPrint("%s", "failed to execute tmq_consumer_new\n");
            ret = -1;
            goto tmq_over;
        }
        infoPrint("thread[%d]: successfully create consumer\n", i);
        int32_t code = tmq_subscribe(pThreadInfo->tmq, topic_list);
        if (code) {
            errorPrint("failed to execute tmq_subscribe, reason: %s\n", tmq_err2str(code));
            ret = -1;
            goto tmq_over;
        }
        infoPrint("thread[%d]: successfully subscribe topics\n", i);
		
		pThreadInfo->calcDelay = 1;		
		pThreadInfo->totalDelay = 0;
		pThreadInfo->delayItemCnt = 0;
		pThreadInfo->avgDelayOfTmq = 0;
		pThreadInfo->maxDelay = 0;
		pThreadInfo->minDelay = 0x7fff;		
        pthread_create(pids + i, NULL, tmqConsume, pThreadInfo);
    }

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t totalMsgs = 0;
    int64_t totalRows = 0;
	int64_t totalDelay = 0;
	int32_t totalItemCnt = 0;
	int32_t maxDelay = 0;
	int32_t minDelay = 0x7fff;

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        tmqThreadInfo * pThreadInfo = infos + i;

        if (pThreadInfo->fpOfRowsFile) {
            fclose(pThreadInfo->fpOfRowsFile);
            pThreadInfo->fpOfRowsFile = NULL;
        }

        totalMsgs += pThreadInfo->totalMsgs;
        totalRows += pThreadInfo->totalRows;

        totalDelay += pThreadInfo->totalDelay;
		totalItemCnt += pThreadInfo->delayItemCnt;

		if (pThreadInfo->maxDelay > maxDelay) {
			maxDelay = pThreadInfo->maxDelay;
		}

		if (pThreadInfo->minDelay < minDelay) {
			minDelay = pThreadInfo->minDelay;
		}
    }

	float avgDelay = 0;
	if (totalItemCnt > 0) {
        avgDelay = (float)totalDelay / totalItemCnt / 1000.0;
	}

    infoPrint("Consumed total msgs: %" PRId64 ", total rows: %" PRId64 ", consume delay avg: %.3f ms, max: %d ms, min: %d ms \n",
              totalMsgs, totalRows,  avgDelay, maxDelay, minDelay);
    infoPrintToFile(g_arguments->fpOfInsertResult,
                    "Consumed total msgs: %" PRId64 ","
                    "total rows: %" PRId64 ", consume delay avg: %.3f ms, max: %d ms, min: %d ms \n", totalMsgs, totalRows, avgDelay, maxDelay, minDelay);

tmq_over:
    free(pids);
    free(infos);
    tmq_list_destroy(topic_list);
    return ret;
}


