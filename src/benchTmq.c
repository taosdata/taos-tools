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

typedef struct {
    tmq_t* tmq;
	int64_t  totalMsgs;
	int64_t  totalRows;	

    int      id;
	FILE*    fpOfRowsFile;
} tmqThreadInfo;

static int running = 1;

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
                     pConsumerInfo->topicName[i],taos_errstr(res));
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

struct tm *taosLocalTime(const time_t *timep, struct tm *result) {
  if (result == NULL) {
    return localtime(timep);
  }
  
  localtime_r(timep, result);
  return result;
}

char* getCurrentTimeString(char* timeString) {
  time_t    tTime = (int32_t)time(NULL);
  struct tm tm;
  taosLocalTime(&tTime, &tm);
  sprintf(timeString, "%d-%02d-%02d %02d:%02d:%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
          tm.tm_min, tm.tm_sec);

  return timeString;
}

static int32_t data_msg_process(TAOS_RES* msg, tmqThreadInfo* pInfo, int32_t msgIndex) {
  char* buf = (char*)calloc(1, 16*1024);
  if (NULL == buf) {
      errorPrint("consumer id %d calloc memory fail.\n", pInfo->id);
      return 0;
  }
  
  int32_t totalRows = 0;

  // printf("topic: %s\n", tmq_get_topic_name(msg));
  int32_t     vgroupId = tmq_get_vgroup_id(msg);
  const char* dbName = tmq_get_db_name(msg);

  if (pInfo->fpOfRowsFile) {
    fprintf(pInfo->fpOfRowsFile, "consumerId: %d, msg index:%d\n", pInfo->id, msgIndex);  
    fprintf(pInfo->fpOfRowsFile, "dbName: %s, topic: %s, vgroupId: %d\n", dbName != NULL ? dbName : "invalid table",
                  tmq_get_topic_name(msg), vgroupId);
  }
  
  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);

    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    //int32_t*    length = taos_fetch_lengths(msg);
    //int32_t     precision = taos_result_precision(msg);
    const char* tbName = tmq_get_table_name(msg);
    //dumpToFileForCheck(pInfo->pConsumeRowsFile, row, fields, length, numOfFields, precision);

    taos_print_row(buf, row, fields, numOfFields);

	if (pInfo->fpOfRowsFile) {
	    fprintf(pInfo->fpOfRowsFile, "tbname:%s, rows[%d]:\n%s\n", (tbName != NULL ? tbName : "null table"), totalRows, buf);
    }

    totalRows++;
  }

  //addRowsToVgroupId(pInfo, vgroupId, totalRows);

  return totalRows;
}

static void* tmqConsume(void* arg) {
    tmqThreadInfo *pThreadInfo = (tmqThreadInfo*)arg;

	int64_t totalMsgs = 0;
	int64_t totalRows = 0;
	
	char tmpString1[128];
	infoPrint("%s consumer id %d start to loop pull msg\n", getCurrentTimeString(tmpString1), pThreadInfo->id);
	
	//pInfo->ts = toolsGetTimestampMs();

	int64_t  lastTotalMsgs = 0;
	int64_t  lastTotalRows = 0;
	uint64_t lastPrintTime = toolsGetTimestampMs();
	//uint64_t startTs = toolsGetTimestampMs();
	
	int32_t consumeDelay = g_tmqInfo.consumerInfo.pollDelay == -1 ? -1 : g_tmqInfo.consumerInfo.pollDelay;
	while (running) {
	  TAOS_RES* tmqMsg = tmq_consumer_poll(pThreadInfo->tmq, consumeDelay);
	  if (tmqMsg) {
        tmq_res_t msgType = tmq_get_res_type(tmqMsg);
        if (msgType == TMQ_RES_TABLE_META) {
		  //infoPrint("get TMQ_RES_TABLE_META mesg!!!\n");
		  break;
        } else if (msgType == TMQ_RES_DATA) {
          totalRows += data_msg_process(tmqMsg, pThreadInfo, totalMsgs);
        } else if (msgType == TMQ_RES_METADATA) {
          //infoPrint("get TMQ_RES_METADATA mesg !!!\n");
		  break;
        }
	
		taos_free_result(tmqMsg);
	
		totalMsgs++;
	
		int64_t currentPrintTime = toolsGetTimestampMs();
		if (currentPrintTime - lastPrintTime > 10 * 1000) {
		  infoPrint("consumer id %d has poll total msgs: %" PRId64 ", period rate: %.3f msgs/s, total rows: %" PRId64 ", period rate: %.3f rows/s\n",
			  pThreadInfo->id, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / (currentPrintTime - lastPrintTime), totalRows, (totalRows - lastTotalRows) * 1000.0 / (currentPrintTime - lastPrintTime));
		  lastPrintTime = currentPrintTime;
		  lastTotalMsgs = totalMsgs;
		  lastTotalRows = totalRows;
		}

		if ((g_tmqInfo.consumerInfo.expectRows > 0) && (totalRows > g_tmqInfo.consumerInfo.expectRows)) {
		    char tmpString3[128];
    	    infoPrint("%s consumer id %d consumed rows: %" PRId64 " over than expect rows: %d, exit consume\n", getCurrentTimeString(tmpString3), pThreadInfo->id, totalRows, g_tmqInfo.consumerInfo.expectRows);
	        break;
		}
	  } else {
		char tmpString3[128];
		infoPrint("%s consumer id %d no poll more msg when time over, break consume\n", getCurrentTimeString(tmpString3), pThreadInfo->id);
		break;
	  }
	}
	
	if (0 == running) {
	  infoPrint("consumer id %d receive stop signal and not continue consume\n",  pThreadInfo->id);
	}
	
	pThreadInfo->totalMsgs = totalMsgs;
	pThreadInfo->totalRows = totalRows;
	
	infoPrint("consumerId: %d, consume msgs: %" PRId64 ", consume rows: %" PRId64 "\n", pThreadInfo->id, totalMsgs, totalRows);
    infoPrintToFile(g_arguments->fpOfInsertResult,
            "consumerId: %d, consume msgs: %" PRId64 ", consume rows: %" PRId64 "\n", pThreadInfo->id, totalMsgs, totalRows);

    return NULL;
}

int subscribeTestProcess() {
    int ret = 0;
	SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo; 
    if (pConsumerInfo->topicCount > 0) {
        if (create_topic()) {
            return -1;
        }
    }

    tmq_list_t * topic_list = buildTopicList();

    pthread_t * pids = benchCalloc(pConsumerInfo->concurrent, sizeof(pthread_t), true);
    tmqThreadInfo *infos = benchCalloc(pConsumerInfo->concurrent, sizeof(tmqThreadInfo), true);

    for (int i = 0; i < pConsumerInfo->concurrent; ++i) {
		char tmpBuff[64] = {0};
		
        tmqThreadInfo * pThreadInfo = infos + i;
        pThreadInfo->totalMsgs = 0;
        pThreadInfo->totalRows = 0;	
        pThreadInfo->id = i;

        memset(tmpBuff, 0, sizeof(tmpBuff));
        snprintf(tmpBuff, 60, "%s_%d", pConsumerInfo->rowsFile, i);
		pThreadInfo->fpOfRowsFile = fopen(pConsumerInfo->rowsFile, "a");
		if (NULL == pThreadInfo->fpOfRowsFile) {
			errorPrint("failed to open %s file for save rows\n", pConsumerInfo->rowsFile);
			ret = -1;
		    goto tmq_over;
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
        pthread_create(pids + i, NULL, tmqConsume, pThreadInfo);
    }

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        pthread_join(pids[i], NULL);
    }

	int64_t totalMsgs = 0;
	int64_t totalRows = 0;

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
		tmqThreadInfo * pThreadInfo = infos + i;
		int32_t code;
		code = tmq_unsubscribe(pThreadInfo->tmq);
		if (code != 0) {
		  errorPrint("thread %d tmq_unsubscribe() fail, reason: %s\n", i, tmq_err2str(code));
		}
		
		code = tmq_consumer_close(pThreadInfo->tmq);
		if (code != 0) {
		  errorPrint("thread %d tmq_consumer_close() fail, reason: %s\n", i, tmq_err2str(code));
		}
		pThreadInfo->tmq = NULL;	

		if (pThreadInfo->fpOfRowsFile) {
			fclose(pThreadInfo->fpOfRowsFile);
            pThreadInfo->fpOfRowsFile = NULL;
		}
		
		totalMsgs += pThreadInfo->totalMsgs;
		totalRows += pThreadInfo->totalRows;
    }	

    infoPrint("Consumed total msgs: %" PRId64 ", total rows: %" PRId64 "\n", totalMsgs, totalRows);
    infoPrintToFile(g_arguments->fpOfInsertResult,"Consumed total msgs: %" PRId64 ", total rows: %" PRId64 "\n", totalMsgs, totalRows);		
			
tmq_over:
    free(pids);
    free(infos);
    tmq_list_destroy(topic_list);
    return ret;
}
