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
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "bench.h"

inline void tmfclose(FILE *fp) {
    if (NULL != fp) {
        fclose(fp);
    }
}

inline void tmfree(void *buf) {
    if (NULL != buf) {
        free(buf);
        buf = NULL;
    }
}

void ERROR_EXIT(const char *msg) {
    errorPrint("%s", msg);
    exit(EXIT_FAILURE);
}

#ifdef WINDOWS
#define _CRT_RAND_S
#include <windows.h>
#include <winsock2.h>

typedef unsigned __int32 uint32_t;

#pragma comment(lib, "ws2_32.lib")
// Some old MinGW/CYGWIN distributions don't define this:
#ifndef ENABLE_VIRTUAL_TERMINAL_PROCESSING
#define ENABLE_VIRTUAL_TERMINAL_PROCESSING 0x0004
#endif  // ENABLE_VIRTUAL_TERMINAL_PROCESSING

HANDLE g_stdoutHandle;
DWORD  g_consoleMode;

void setupForAnsiEscape(void) {
    DWORD mode = 0;
    g_stdoutHandle = GetStdHandle(STD_OUTPUT_HANDLE);

    if (g_stdoutHandle == INVALID_HANDLE_VALUE) {
        exit(GetLastError());
    }

    if (!GetConsoleMode(g_stdoutHandle, &mode)) {
        exit(GetLastError());
    }

    g_consoleMode = mode;

    // Enable ANSI escape codes
    mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;

    if (!SetConsoleMode(g_stdoutHandle, mode)) {
        exit(GetLastError());
    }
}

void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");

    // Reset console mode
    if (!SetConsoleMode(g_stdoutHandle, g_consoleMode)) {
        exit(GetLastError());
    }
}

int taosRandom() {
    int number;
    rand_s(&number);

    return number;
}
#else  // Not windows
void setupForAnsiEscape(void) {}

void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");
}

#include <time.h>

int taosRandom() { return rand(); }

#endif

int getAllChildNameOfSuperTable(TAOS *taos, char *dbName, char *stbName,
                                char ** childTblNameOfSuperTbl,
                                int64_t childTblCountOfSuperTbl) {
    char cmd[SQL_BUFF_LEN] = "\0";
    snprintf(cmd, SQL_BUFF_LEN, "select tbname from %s.`%s` limit %" PRId64 "",
             dbName, stbName, childTblCountOfSuperTbl);
    TAOS_RES *res = taos_query(taos, cmd);
    int32_t   code = taos_errno(res);
    int64_t   count = 0;
    if (code) {
        errorPrint("failed to get child table name: %s. reason: %s", cmd,
                   taos_errstr(res));
        taos_free_result(res);

        return -1;
    }
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == strlen((char *)(row[0]))) {
            errorPrint("No.%" PRId64 " table return empty name\n", count);
            return -1;
        }
        childTblNameOfSuperTbl[count] = calloc(1, TSDB_TABLE_NAME_LEN);
        snprintf(childTblNameOfSuperTbl[count], TSDB_TABLE_NAME_LEN, "`%s`",
                 (char *)row[0]);
        debugPrint("childTblNameOfSuperTbl[%" PRId64 "]: %s\n", count,
                   childTblNameOfSuperTbl[count]);
        count++;
    }
    taos_free_result(res);
    return 0;
}

int convertHostToServAddr(char *host, uint16_t port,
                          struct sockaddr_in *serv_addr) {
    if (!host) {
        host = "localhost";
    }
    debugPrint("convertHostToServAddr(host: %s, port: %d)\n", host, port);
    struct hostent *server = gethostbyname(host);
    if ((server == NULL) || (server->h_addr == NULL)) {
        errorPrint("%s", "no such host");
        return -1;
    }
    memset(serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr->sin_family = AF_INET;
    serv_addr->sin_port = htons(port);
#ifdef WINDOWS
    serv_addr->sin_addr.s_addr = inet_addr(host);
#else
    memcpy(&(serv_addr->sin_addr.s_addr), server->h_addr, server->h_length);
#endif
    return 0;
}

void prompt(bool nonStopMode) {
    if (!g_arguments->answer_yes) {
        if (nonStopMode) {
            printf(
                "\n\n         Current is the Non-Stop insertion mode. "
                "taosBenchmark will continuously insert data unless you press "
                "Ctrl-C to end it.\n\n         press enter key to continue and "
                "Ctrl-C to "
                "stop\n\n");
            (void)getchar();
        } else {
            printf(
                "\n\n         Press enter key to continue or Ctrl-C to "
                "stop\n\n");
            (void)getchar();
        }
    }
}

static void appendResultBufToFile(char *resultBuf, threadInfo *pThreadInfo) {
    pThreadInfo->fp = fopen(pThreadInfo->filePath, "at");
    if (pThreadInfo->fp == NULL) {
        errorPrint(
            "failed to open result file: %s, result will not save "
            "to file\n",
            pThreadInfo->filePath);
        return;
    }

    fprintf(pThreadInfo->fp, "%s", resultBuf);
    tmfclose(pThreadInfo->fp);
    pThreadInfo->fp = NULL;
}

void replaceChildTblName(char *inSql, char *outSql, int tblIndex) {
    char sourceString[32] = "xxxx";
    char subTblName[TSDB_TABLE_NAME_LEN];
    sprintf(subTblName, "%s.%s", g_arguments->db->dbName,
            g_queryInfo.superQueryInfo.childTblName[tblIndex]);

    // printf("inSql: %s\n", inSql);

    char *pos = strstr(inSql, sourceString);
    if (0 == pos) return;

    tstrncpy(outSql, inSql, pos - inSql + 1);
    // printf("1: %s\n", outSql);
    strncat(outSql, subTblName, BUFFER_SIZE - 1);
    // printf("2: %s\n", outSql);
    strncat(outSql, pos + strlen(sourceString), BUFFER_SIZE - 1);
    // printf("3: %s\n", outSql);
}

int64_t toolsGetTimestampMs() {
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    return (int64_t)systemTime.tv_sec * 1000L +
           (int64_t)systemTime.tv_usec / 1000;
}

int64_t toolsGetTimestampUs() {
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

int64_t toolsGetTimestampNs() {
    struct timespec systemTime = {0};
    clock_gettime(CLOCK_REALTIME, &systemTime);
    return (int64_t)systemTime.tv_sec * 1000000000L +
           (int64_t)systemTime.tv_nsec;
}

int64_t toolsGetTimestamp(int32_t precision) {
    if (precision == TSDB_TIME_PRECISION_MICRO) {
        return toolsGetTimestampUs();
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        return toolsGetTimestampNs();
    } else {
        return toolsGetTimestampMs();
    }
}

void taosMsleep(int32_t mseconds) { usleep(mseconds * 1000); }

int64_t taosGetSelfPthreadId() {
    static __thread int id = 0;
    if (id != 0) return id;
    id = syscall(SYS_gettid);
    return id;
}

int regexMatch(const char *s, const char *reg, int cflags) {
    regex_t regex;
    char    msgbuf[100] = {0};

    /* Compile regular expression */
    if (regcomp(&regex, reg, cflags) != 0)
        ERROR_EXIT("Failed to regex compile\n");

    /* Execute regular expression */
    int reti = regexec(&regex, s, 0, NULL, 0);
    if (!reti) {
        regfree(&regex);
        return 1;
    } else if (reti == REG_NOMATCH) {
        regfree(&regex);
        return 0;
    } else {
        regerror(reti, &regex, msgbuf, sizeof(msgbuf));
        regfree(&regex);
        printf("Regex match failed: %s\n", msgbuf);
        exit(EXIT_FAILURE);
    }
    return 0;
}

int queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet) {
    TAOS_RES *res = taos_query(taos, command);
    int32_t   code = taos_errno(res);

    if (code != 0) {
        if (!quiet) {
            errorPrint("Failed to execute <%s>, reason: %s\n", command,
                       taos_errstr(res));
        }
        taos_free_result(res);
        return -1;
    }

    if (INSERT_TYPE == type) {
        int affectedRows = taos_affected_rows(res);
        taos_free_result(res);
        return affectedRows;
    }

    taos_free_result(res);
    return 0;
}

void encode_base_64() {
    char        userpass_buf[INPUT_BUF_LEN];
    static char base64[] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
    snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s", g_arguments->user,
             g_arguments->password);

    int mod_table[] = {0, 2, 1};

    size_t userpass_buf_len = strlen(userpass_buf);
    size_t encoded_len = 4 * ((userpass_buf_len + 2) / 3);

    g_arguments->base64_buf = calloc(1, INPUT_BUF_LEN);

    for (int n = 0, m = 0; n < userpass_buf_len;) {
        uint32_t oct_a =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t oct_b =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t oct_c =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t triple = (oct_a << 0x10) + (oct_b << 0x08) + oct_c;

        g_arguments->base64_buf[m++] = base64[(triple >> 3 * 6) & 0x3f];
        g_arguments->base64_buf[m++] = base64[(triple >> 2 * 6) & 0x3f];
        g_arguments->base64_buf[m++] = base64[(triple >> 1 * 6) & 0x3f];
        g_arguments->base64_buf[m++] = base64[(triple >> 0 * 6) & 0x3f];
    }

    for (int l = 0; l < mod_table[userpass_buf_len % 3]; l++)
        g_arguments->base64_buf[encoded_len - 1 - l] = '=';
}

int postProceSql(char *sqlstr, threadInfo *pThreadInfo) {
    SDataBase *  database = &(g_arguments->db[pThreadInfo->db_index]);
    SSuperTable *stbInfo = &(database->superTbls[pThreadInfo->stb_index]);
    int32_t      code = -1;
    char *       req_fmt =
        "POST %s HTTP/1.1\r\nHost: %s:%d\r\nAccept: */*\r\nAuthorization: "
        "Basic %s\r\nContent-Length: %d\r\nContent-Type: "
        "application/x-www-form-urlencoded\r\n\r\n%s";
    char url[1024];
    if (stbInfo->iface == REST_IFACE) {
        sprintf(url, "/rest/sql/%s", database->dbName);
    } else if (stbInfo->iface == SML_REST_IFACE &&
               stbInfo->lineProtocol == TSDB_SML_LINE_PROTOCOL) {
        sprintf(url, "/influxdb/v1/write?db=%s&precision=%s", database->dbName,
                database->dbCfg.precision == TSDB_TIME_PRECISION_MILLI
                    ? "ms"
                    : database->dbCfg.precision == TSDB_TIME_PRECISION_NANO
                          ? "ns"
                          : "u");
    } else if (stbInfo->iface == SML_REST_IFACE &&
               stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL) {
        sprintf(url, "/opentsdb/v1/put/telnet/%s", database->dbName);
    } else if (stbInfo->iface == SML_REST_IFACE &&
               stbInfo->lineProtocol == TSDB_SML_JSON_PROTOCOL) {
        sprintf(url, "/opentsdb/v1/put/json/%s", database->dbName);
    }

    int      bytes, sent, received, req_str_len, resp_len;
    char *   request_buf;
    char *   response_buf;
    uint16_t rest_port = g_arguments->port + TSDB_PORT_HTTP;

    int req_buf_len = (int)strlen(sqlstr) + REQ_EXTRA_BUF_LEN;

    request_buf = calloc(1, req_buf_len);
    uint64_t response_length;
    if (g_arguments->test_mode == INSERT_TEST) {
        response_length = RESP_BUF_LEN;
    } else {
        response_length = g_queryInfo.response_buffer;
    }
    response_buf = calloc(1, response_length);

    int r;
    if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL &&
        stbInfo->tcpTransfer) {
        r = snprintf(request_buf, req_buf_len, "%s", sqlstr);
    } else {
        r = snprintf(request_buf, req_buf_len, req_fmt, url, g_arguments->host,
                     rest_port, g_arguments->base64_buf, strlen(sqlstr),
                     sqlstr);
    }
    if (r >= req_buf_len) {
        free(request_buf);
        ERROR_EXIT("too long request");
    }

    req_str_len = (int)strlen(request_buf);
    debugPrint("request buffer: %s\n", request_buf);
    sent = 0;
    do {
#ifdef WINDOWS
        bytes = send(pThreadInfo->sockfd, request_buf + sent,
                     req_str_len - sent, 0);
#else
        bytes =
            write(pThreadInfo->sockfd, request_buf + sent, req_str_len - sent);
#endif
        if (bytes < 0) {
            errorPrint("%s", "writing no message to socket\n");
            goto free_of_post;
        }
        if (bytes == 0) break;
        sent += bytes;
    } while (sent < req_str_len);

    if (stbInfo->lineProtocol == TSDB_SML_TELNET_PROTOCOL &&
        stbInfo->iface == SML_REST_IFACE && stbInfo->tcpTransfer) {
        code = 0;
        goto free_of_post;
    }

    resp_len = response_length - 1;
    received = 0;

    char resEncodingChunk[] = "Encoding: chunked";
    char succMessage[] = "succ";
    char resHttp[] = "HTTP/1.1 ";
    char resHttpOk[] = "HTTP/1.1 200 OK";
    char influxHttpOk[] = "HTTP/1.1 204";
    bool chunked = false;

    do {
#ifdef WINDOWS
        bytes = recv(pThreadInfo->sockfd, response_buf + received,
                     resp_len - received, 0);
#else
        bytes = read(pThreadInfo->sockfd, response_buf + received,
                     resp_len - received);
#endif

        debugPrint("response_buffer: %s\n", response_buf);
        if (NULL != strstr(response_buf, resEncodingChunk)) {
            chunked = true;
        }
        int64_t index = strlen(response_buf) - 1;
        while (response_buf[index] == '\n' || response_buf[index] == '\r') {
            index--;
        }
        debugPrint("index: %" PRId64 "\n", index);
        if (chunked && response_buf[index] == '0') {
            break;
        }
        if (!chunked && response_buf[index] == '}') {
            break;
        }

        if (bytes <= 0) {
            errorPrint("%s", "reading no response from socket\n");
            goto free_of_post;
        }

        received += bytes;

        if (g_arguments->test_mode == INSERT_TEST) {
            if (strlen(response_buf)) {
                if (((NULL != strstr(response_buf, resEncodingChunk)) &&
                     (NULL != strstr(response_buf, resHttp))) ||
                    ((NULL != strstr(response_buf, resHttpOk)) ||
                     (NULL != strstr(response_buf, influxHttpOk)))) {
                    break;
                }
            }
        }
    } while (received < resp_len);

    if (received == resp_len) {
        errorPrint("%s", "storing complete response from socket\n");
        goto free_of_post;
    }

    if (NULL == strstr(response_buf, resHttpOk) &&
        NULL == strstr(response_buf, influxHttpOk) &&
        NULL == strstr(response_buf, succMessage)) {
        errorPrint("Response:\n%s\n", response_buf);
        goto free_of_post;
    }

    if (strlen(pThreadInfo->filePath) > 0) {
        appendResultBufToFile(response_buf, pThreadInfo);
    }
    code = 0;
free_of_post:
    tmfree(request_buf);
    tmfree(response_buf);
    return code;
}

void fetchResult(TAOS_RES *res, threadInfo *pThreadInfo) {
    TAOS_ROW    row = NULL;
    int         num_rows = 0;
    int         num_fields = taos_field_count(res);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    char *databuf = (char *)calloc(1, FETCH_BUFFER_SIZE);

    int64_t totalLen = 0;

    // fetch the records row by row
    while ((row = taos_fetch_row(res))) {
        if (totalLen >= (FETCH_BUFFER_SIZE - HEAD_BUFF_LEN * 2)) {
            if (strlen(pThreadInfo->filePath) > 0) {
                appendResultBufToFile(databuf, pThreadInfo);
            }
            totalLen = 0;
            memset(databuf, 0, FETCH_BUFFER_SIZE);
        }
        num_rows++;
        char temp[HEAD_BUFF_LEN] = {0};
        int  len = taos_print_row(temp, row, fields, num_fields);
        len += sprintf(temp + len, "\n");
        debugPrint("query result:%s\n", temp);
        memcpy(databuf + totalLen, temp, len);
        totalLen += len;
    }

    if (strlen(pThreadInfo->filePath) > 0) {
        appendResultBufToFile(databuf, pThreadInfo);
    }
    free(databuf);
}

char *taos_convert_datatype_to_string(int type) {
    switch (type) {
        case TSDB_DATA_TYPE_BINARY:
            return "binary";
        case TSDB_DATA_TYPE_NCHAR:
            return "nchar";
        case TSDB_DATA_TYPE_TIMESTAMP:
            return "timestamp";
        case TSDB_DATA_TYPE_TINYINT:
            return "tinyint";
        case TSDB_DATA_TYPE_UTINYINT:
            return "unsigned tinyint";
        case TSDB_DATA_TYPE_SMALLINT:
            return "smallint";
        case TSDB_DATA_TYPE_USMALLINT:
            return "unsigned smallint";
        case TSDB_DATA_TYPE_INT:
            return "int";
        case TSDB_DATA_TYPE_UINT:
            return "unsigned int";
        case TSDB_DATA_TYPE_BIGINT:
            return "bigint";
        case TSDB_DATA_TYPE_UBIGINT:
            return "unsigned bigint";
        case TSDB_DATA_TYPE_BOOL:
            return "bool";
        case TSDB_DATA_TYPE_FLOAT:
            return "float";
        case TSDB_DATA_TYPE_DOUBLE:
            return "double";
        case TSDB_DATA_TYPE_JSON:
            return "json";
        default:
            break;
    }
    return "unknown type";
}

int taos_convert_string_to_datatype(char *type) {
    if (0 == strcasecmp(type, "binary")) {
        return TSDB_DATA_TYPE_BINARY;
    } else if (0 == strcasecmp(type, "nchar")) {
        return TSDB_DATA_TYPE_NCHAR;
    } else if (0 == strcasecmp(type, "timestamp")) {
        return TSDB_DATA_TYPE_TIMESTAMP;
    } else if (0 == strcasecmp(type, "bool")) {
        return TSDB_DATA_TYPE_BOOL;
    } else if (0 == strcasecmp(type, "tinyint")) {
        return TSDB_DATA_TYPE_TINYINT;
    } else if (0 == strcasecmp(type, "utinyint")) {
        return TSDB_DATA_TYPE_UTINYINT;
    } else if (0 == strcasecmp(type, "smallint")) {
        return TSDB_DATA_TYPE_SMALLINT;
    } else if (0 == strcasecmp(type, "usmallint")) {
        return TSDB_DATA_TYPE_USMALLINT;
    } else if (0 == strcasecmp(type, "int")) {
        return TSDB_DATA_TYPE_INT;
    } else if (0 == strcasecmp(type, "uint")) {
        return TSDB_DATA_TYPE_UINT;
    } else if (0 == strcasecmp(type, "bigint")) {
        return TSDB_DATA_TYPE_BIGINT;
    } else if (0 == strcasecmp(type, "ubigint")) {
        return TSDB_DATA_TYPE_UBIGINT;
    } else if (0 == strcasecmp(type, "float")) {
        return TSDB_DATA_TYPE_FLOAT;
    } else if (0 == strcasecmp(type, "double")) {
        return TSDB_DATA_TYPE_DOUBLE;
    } else if (0 == strcasecmp(type, "json")) {
        return TSDB_DATA_TYPE_JSON;
    } else {
        errorPrint("unknown data type: %s\n", type);
        exit(EXIT_FAILURE);
    }
}

int init_taos_list() {
    if (strlen(configDir)) {
        wordexp_t full_path;
        if (wordexp(configDir, &full_path, 0) != 0) {
            errorPrint("Invalid path %s\n", configDir);
            exit(EXIT_FAILURE);
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
    }
    int        size = g_arguments->connection_pool;
    TAOS_POOL *pool = g_arguments->pool;
    pool->taos_list = calloc(size, sizeof(TAOS *));
    g_memoryUsage += size * sizeof(TAOS *);
    pool->current = 0;
    pool->size = size;
    for (int i = 0; i < size; ++i) {
        pool->taos_list[i] =
            taos_connect(g_arguments->host, g_arguments->user,
                         g_arguments->password, NULL, g_arguments->port);
        if (pool->taos_list[i] == NULL) {
            errorPrint("Failed to connect to TDengine, reason:%s\n",
                       taos_errstr(NULL));
            return -1;
        }
    }
    return 0;
}

TAOS *select_one_from_pool(char *db_name) {
    TAOS_POOL *pool = g_arguments->pool;
    TAOS *     taos = pool->taos_list[pool->current];
    if (db_name != NULL) {
        int code = taos_select_db(taos, db_name);
        if (code) {
            errorPrint("failed to select %s, reason: %s\n", db_name,
                       taos_errstr(NULL));
            return NULL;
        }
    }
    pool->current++;
    if (pool->current >= pool->size) {
        pool->current = 0;
    }
    return taos;
}

void cleanup_taos_list() {
    for (int i = 0; i < g_arguments->pool->size; ++i) {
        taos_close(g_arguments->pool->taos_list[i]);
    }
    tmfree(g_arguments->pool->taos_list);
}
void delay_list_init(delayList *list) {
    list->size = 0;
    list->head = NULL;
    list->tail = NULL;
}

void delay_list_destroy(delayList *list) {
    while (list->size > 0) {
        delayNode *current = list->head;
        list->head = list->head->next;
        tmfree(current);
        list->size--;
    }
}

int compare(const void *a, const void *b) {
    return *(uint64_t *)a - *(uint64_t *)b;
}
