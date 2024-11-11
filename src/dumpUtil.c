/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


#include <taos.h>
#include "dump.h"
#include "dumpUtil.h"


//
//   encapsulate the api of calling engine
//
#define RETRY_TYPE_CONNECT 0
#define RETRY_TYPE_QUERY   1
#define RETRY_TYPE_FETCH   2

// return true to do retry , false no retry , code is error code 
bool canRetry(int32_t code, int8_t type) {
    // rpc error
    if (code >= TSDB_CODE_RPC_BEGIN && code <= TSDB_CODE_RPC_END) {
        return true;
    }

    return false;
}


//
//  ---------------  native  ------------------
//

// connect
TAOS *taosConnect(const char *dbName) {
    int32_t i = 0;
    while (1) {
        TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password, dbName, g_args.port);
        if (taos) {
            // successful
            if (i > 0) {
                okPrint("Retry %d to connect %s:%d successfully!\n", i, g_args.host, g_args.port);
            }
            return taos;
        }

        // fail
        errorPrint("Failed to connect to server %s, code: 0x%08x, reason: %s! \n", g_args.host, taos_errno(NULL),
                   taos_errstr(NULL));

        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to connect for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }
    return NULL;
}

// query
TAOS_RES *taosQuery(TAOS *taos, const char *sql, int32_t *code) {
    int32_t   i = 0;
    TAOS_RES *res = NULL;
    while (1) {
        res = taos_query(taos, sql);
        *code = taos_errno(res);
        if (*code == 0) {
            // successful
            if (i > 0) {
                okPrint("Retry %d to execute taosQuery %s successfully!\n", i, sql);
            }
            return res;
        }

        // fail
        errorPrint("Failed to execute taosQuery, code: 0x%08x, reason: %s, sql=%s \n", *code, taos_errstr(res), sql);

        // can retry
        if(!canRetry(*code, RETRY_TYPE_QUERY)) {
            infoPrint("%s", "error code not in retry range , give up retry.\n");
            return NULL;
        }

        // check retry count
        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to execute taosQuery for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }

    return NULL;
}


//
//  ---------------  websocket  ------------------
//

#ifdef WEBSOCKET
// ws connect
WS_TAOS *wsConnect() {
    int32_t i = 0;
    while (1) {
        WS_TAOS *ws_taos = ws_connect(g_args.dsn);
        if (ws_taos) {
            // successful
            if (i > 0) {
                okPrint("Retry %d to connect %s:%d successfully!\n", i, g_args.host, g_args.port);
            }
            return ws_taos;
        }

        // fail
        char maskedDsn[256] = "\0";
        memcpy(maskedDsn, g_args.dsn, 20);
        memcpy(maskedDsn + 20, "...", 3);
        memcpy(maskedDsn + 23, g_args.dsn + strlen(g_args.dsn) - 10, 10);
        errorPrint("Failed to ws_connect to server %s, code: 0x%08x, reason: %s!\n", maskedDsn, ws_errno(NULL),
                   ws_errstr(NULL));

        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to ws_connect for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }
    return NULL;
}

// ws query
WS_RES *wsQuery(WS_TAOS **taos_v, const char *sql, int32_t *code) {
    int32_t i = 0;
    WS_RES *ws_res = NULL;
    while (1) {
        ws_res = ws_query_timeout(*taos_v, sql, g_args.ws_timeout);
        *code = ws_errno(ws_res);
        if (*code == 0) {
            if (i > 0) {
                okPrint("Retry %d to execute taosQuery %s successfully!\n", i, sql);
            }
            // successful
            return ws_res;
        }

        // fail
        errorPrint("Failed to execute taosQuery, code: 0x%08x, reason: %s, sql=%s \n", *code, taos_errstr(ws_res), sql);

        // can retry
        if(!canRetry(*code, RETRY_TYPE_QUERY)) {
            infoPrint("%s", "error code not in retry range , give up retry.\n");
            return ws_res;
        }        

        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to execute taosQuery for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }

    // need reconnect 
    infoPrint("query switch new connect to try , sql=%s \n", sql);
    WS_TAOS * new_conn = wsConnect();
    if(new_conn == NULL) {
        // return old
        return ws_res;
    }

    // use new conn to query
    ws_res = ws_query_timeout(new_conn, sql, g_args.ws_timeout);
    *code = ws_errno(ws_res);
    if (*code == 0) {
        // set new connect to old
        ws_close(*taos_v);
        *taos_v = new_conn;
        okPrint("execute taosQuery with new connection successfully! sql=%s\n", sql);
        // successful
        return ws_res;
    }

    // fail
    errorPrint("execute taosQuery with new connection failed, code: 0x%08x, reason: %s \n", *code, taos_errstr(ws_res));
    ws_close(new_conn);
    return ws_res;
}

#endif