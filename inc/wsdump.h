#ifndef INC_WSDUMP_H_
#define INC_WSDUMP_H_

//
// ---------------  websocket ------------------
//

#ifdef WEBSOCKET

#include <taoserror.h>
#include <taosws.h>
#include <toolsdef.h>

int     cleanIfQueryFailedWS(const char *funcname, int lineno, char *command, WS_RES *res);
int     getTableRecordInfoWS(char *dbName, char *table, TableRecordInfo *pTableRecordInfo);
int     getDbCountWS(WS_RES *ws_res);
int64_t getNtbCountOfStbWS(char *dbName, const char *stbName);
int     getTableDesFromStbWS(WS_TAOS *ws_taos, const char *dbName, const TableDes *stbTableDes, const char *table,
                                TableDes **ppTableDes);
int     getTableDesWS(WS_TAOS *ws_taos, const char *dbName, const char *table, TableDes *tableDes, const bool colOnly);
int64_t queryDbForDumpOutCountWS(char *command, WS_TAOS *ws_taos, const char *dbName, const char *tbName,
                                 const int precision);
TAOS_RES *queryDbForDumpOutOffsetWS(WS_TAOS *ws_taos, char *command);
int64_t   dumpTableDataAvroWS(char *dataFilename, int64_t index, const char *tbName, const bool belongStb,
                              const char *dbName, const int precision, int colCount, TableDes *tableDes,
                              int64_t start_time, int64_t end_time);
int64_t fillTbNameArrWS(WS_TAOS *ws_taos, char *command, char **tbNameArr, const char *stable, const int64_t preCount);
int     readNextTableDesWS(void *ws_res, TableDes *tbDes, int *idx, int *cnt);
void    dumpExtraInfoVarWS(void *taos, FILE *fp);
int     queryDbImplWS(WS_TAOS *ws_taos, char *command);
void    dumpNormalTablesOfStbWS(threadInfo *pThreadInfo, FILE *fp, char *dumpFilename);
int64_t dumpStbAndChildTbOfDbWS(WS_TAOS *ws_taos, SDbInfo *dbInfo, FILE *fpDbs);
int64_t dumpNTablesOfDbWS(WS_TAOS *ws_taos, SDbInfo *dbInfo);
int     fillDbInfoWS(void *taos);
bool    jointCloudDsn();
bool    splitCloudDsn();
int64_t dumpTableDataWS(const int64_t index, FILE *fp, const char *tbName, const char *dbName, const int precision,
                        TableDes *tableDes, const int64_t start_time, const int64_t end_time);

#endif

#endif  // INC_WSDUMP_H_