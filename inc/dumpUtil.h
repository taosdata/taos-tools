#ifndef INC_DUMPUTIL_H_
#define INC_DUMPUTIL_H_

#include <taos.h>
#include <taoserror.h>
#include <toolsdef.h>

//
// ------------  error code range defin ------------
//

// engine can retry code range
#define TSDB_CODE_RPC_BEGIN                     TSDB_CODE_RPC_NETWORK_UNAVAIL
#define TSDB_CODE_RPC_END                       TSDB_CODE_RPC_ASYNC_MODULE_QUIT

// websocket can retry code range
// range1 [0x0001~0x00FF]
#define WEBSOCKET_CODE_BEGIN1      0x0001
#define WEBSOCKET_CODE_END1        0x00FF
// range2 [0x0001~0x00FF]
#define WEBSOCKET_CODE_BEGIN2      0xE000
#define WEBSOCKET_CODE_END2        0xE0FF

//
// ---------------  native ------------------
//


// connect
TAOS *taosConnect(const char *dbName);
// query
TAOS_RES *taosQuery(TAOS *taos, const char *sql, int32_t *code);


//
// ---------------  websocket ------------------
//
#ifdef WEBSOCKET
// ws connect
WS_TAOS *wsConnect();
// ws query
WS_RES *wsQuery(WS_TAOS **ws_taos, const char *sql, int32_t *code);

#endif


#endif  // INC_DUMPUTIL_H_