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

void tmfclose(FILE *fp) {
    if (NULL != fp) {
        fclose(fp);
    }
}

void tmfree(void *buf) {
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

char *formatTimestamp(char *buf, int64_t val, int precision) {
    time_t tt;
    if (precision == TSDB_TIME_PRECISION_MICRO) {
        tt = (time_t)(val / 1000000);
    }
    if (precision == TSDB_TIME_PRECISION_NANO) {
        tt = (time_t)(val / 1000000000);
    } else {
        tt = (time_t)(val / 1000);
    }

    /* comment out as it make testcases like select_with_tags.sim fail.
       but in windows, this may cause the call to localtime crash if tt < 0,
       need to find a better solution.
       if (tt < 0) {
       tt = 0;
       }
       */

#ifdef WINDOWS
    if (tt < 0) tt = 0;
#endif

    struct tm *ptm = localtime(&tt);
    size_t     pos = strftime(buf, 32, "%Y-%m-%d %H:%M:%S", ptm);

    if (precision == TSDB_TIME_PRECISION_MICRO) {
        sprintf(buf + pos, ".%06d", (int)(val % 1000000));
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        sprintf(buf + pos, ".%09d", (int)(val % 1000000000));
    } else {
        sprintf(buf + pos, ".%03d", (int)(val % 1000));
    }

    return buf;
}

int getChildNameOfSuperTableWithLimitAndOffset(TAOS *taos, char *dbName,
                                               char *   stbName,
                                               char **  childTblNameOfSuperTbl,
                                               int64_t *childTblCountOfSuperTbl,
                                               int64_t limit, uint64_t offset,
                                               bool escapChar) {
    char command[SQL_BUFF_LEN] = "\0";
    char limitBuf[100] = "\0";

    TAOS_RES *res;
    TAOS_ROW  row = NULL;
    int64_t   childTblCount = (limit < 0) ? DEFAULT_CHILDTABLES : limit;
    int64_t   count = 0;
    char *    childTblName = *childTblNameOfSuperTbl;

    if (childTblName == NULL) {
        childTblName = (char *)calloc(1, childTblCount * TSDB_TABLE_NAME_LEN);
    }
    char *pTblName = childTblName;

    snprintf(limitBuf, 100, " limit %" PRId64 " offset %" PRIu64 "", limit,
             offset);

    // get all child table name use cmd: select tbname from superTblName;
    snprintf(command, SQL_BUFF_LEN,
             escapChar ? "select tbname from %s.`%s` %s"
                       : "select tbname from %s.%s %s",
             dbName, stbName, limitBuf);

    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        taos_free_result(res);

        errorPrint("failed to run command %s, reason: %s\n", command,
                   taos_errstr(res));
        return -1;
    }

    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t *len = taos_fetch_lengths(res);

        if (0 == strlen((char *)row[0])) {
            errorPrint("No.%" PRId64 " table return empty name\n", count);
            return -1;
        }

        tstrncpy(pTblName, (char *)row[0], len[0] + 1);
        // printf("==== sub table name: %s\n", pTblName);
        count++;
        if (count >= childTblCount - 1) {
            char *tmp = realloc(
                childTblName,
                (size_t)(childTblCount * 1.5 * TSDB_TABLE_NAME_LEN + 1));
            if (tmp != NULL) {
                childTblName = tmp;
                childTblCount = (int)(childTblCount * 1.5);
                memset(childTblName + count * TSDB_TABLE_NAME_LEN, 0,
                       (size_t)((childTblCount - count) * TSDB_TABLE_NAME_LEN));
            } else {
                // exit, if allocate more memory failed
                tmfree(childTblName);
                taos_free_result(res);

                errorPrint(
                    "realloc fail for save child table name of "
                    "%s.%s\n",
                    dbName, stbName);
                return -1;
            }
        }
        pTblName = childTblName + count * TSDB_TABLE_NAME_LEN;
    }

    *childTblCountOfSuperTbl = count;
    *childTblNameOfSuperTbl = childTblName;

    taos_free_result(res);
    return 0;
}

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
    uint16_t        rest_port = port + TSDB_PORT_HTTP;
    struct hostent *server = gethostbyname(host);
    if ((server == NULL) || (server->h_addr == NULL)) {
        errorPrint("%s", "no such host");
        return -1;
    }
    memset(serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr->sin_family = AF_INET;
    serv_addr->sin_port = htons(rest_port);
#ifdef WINDOWS
    serv_addr->sin_addr.s_addr = inet_addr(host);
#else
    memcpy(&(serv_addr->sin_addr.s_addr), server->h_addr, server->h_length);
#endif
    return 0;
}

void prompt(SArguments *arguments) {
    if (!arguments->answer_yes) {
        printf(
            "\n\n         Press enter key to continue or Ctrl-C to stop\n\n");
        (void)getchar();
    }
}

void replaceChildTblName(char *inSql, char *outSql, int tblIndex) {
    char sourceString[32] = "xxxx";
    char subTblName[TSDB_TABLE_NAME_LEN];
    sprintf(subTblName, "%s.%s", g_queryInfo.dbName,
            g_queryInfo.superQueryInfo.childTblName[tblIndex]);

    // printf("inSql: %s\n", inSql);

    char *pos = strstr(inSql, sourceString);
    if (0 == pos) {
        return;
    }

    tstrncpy(outSql, inSql, pos - inSql + 1);
    // printf("1: %s\n", outSql);
    strncat(outSql, subTblName, BUFFER_SIZE - 1);
    // printf("2: %s\n", outSql);
    strncat(outSql, pos + strlen(sourceString), BUFFER_SIZE - 1);
    // printf("3: %s\n", outSql);
}

int64_t taosGetTimestampMs() {
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    return (int64_t)systemTime.tv_sec * 1000L +
           (int64_t)systemTime.tv_usec / 1000;
}

int64_t taosGetTimestampUs() {
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

int64_t taosGetTimestampNs() {
    struct timespec systemTime = {0};
    clock_gettime(CLOCK_REALTIME, &systemTime);
    return (int64_t)systemTime.tv_sec * 1000000000L +
           (int64_t)systemTime.tv_nsec;
}

int64_t taosGetTimestamp(int32_t precision) {
    if (precision == TSDB_TIME_PRECISION_MICRO) {
        return taosGetTimestampUs();
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        return taosGetTimestampNs();
    } else {
        return taosGetTimestampMs();
    }
}

void taosMsleep(int32_t mseconds) { usleep(mseconds * 1000); }

int64_t taosGetSelfPthreadId() {
    static __thread int id = 0;
    if (id != 0) return id;
    id = syscall(SYS_gettid);
    return id;
}

int isCommentLine(char *line) {
    if (line == NULL) return 1;

    return regexMatch(line, "^\\s*#.*", REG_EXTENDED);
}

int regexMatch(const char *s, const char *reg, int cflags) {
    regex_t regex;
    char    msgbuf[100] = {0};

    /* Compile regular expression */
    if (regcomp(&regex, reg, cflags) != 0) {
        ERROR_EXIT("Fail to compile regex\n");
    }

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

int postProceSql(char *host, uint16_t port, char *sqlstr,
                 threadInfo *pThreadInfo) {
    int32_t code = -1;
    char *  req_fmt =
        "POST %s HTTP/1.1\r\nHost: %s:%d\r\nAccept: */*\r\nAuthorization: "
        "Basic %s\r\nContent-Length: %d\r\nContent-Type: "
        "application/x-www-form-urlencoded\r\n\r\n%s";

    char *url = "/rest/sql";

    int      bytes, sent, received, req_str_len, resp_len;
    char *   request_buf;
    char *   response_buf;
    uint16_t rest_port = port + TSDB_PORT_HTTP;

    int req_buf_len = (int)strlen(sqlstr) + REQ_EXTRA_BUF_LEN;

    request_buf = calloc(1, req_buf_len);
    uint64_t response_length;
    if (g_args.test_mode == INSERT_TEST) {
        response_length = RESP_BUF_LEN;
    } else {
        response_length = g_queryInfo.response_buffer;
    }
    response_buf = calloc(1, response_length);
    char userpass_buf[INPUT_BUF_LEN];
    int  mod_table[] = {0, 2, 1};

    static char base64[] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

    if (g_args.test_mode == INSERT_TEST) {
        snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s", g_args.user,
                 g_args.password);
    } else {
        snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s", g_queryInfo.user,
                 g_queryInfo.password);
    }

    size_t userpass_buf_len = strlen(userpass_buf);
    size_t encoded_len = 4 * ((userpass_buf_len + 2) / 3);

    char base64_buf[INPUT_BUF_LEN];

    memset(base64_buf, 0, INPUT_BUF_LEN);

    for (int n = 0, m = 0; n < userpass_buf_len;) {
        uint32_t oct_a =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t oct_b =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t oct_c =
            n < userpass_buf_len ? (unsigned char)userpass_buf[n++] : 0;
        uint32_t triple = (oct_a << 0x10) + (oct_b << 0x08) + oct_c;

        base64_buf[m++] = base64[(triple >> 3 * 6) & 0x3f];
        base64_buf[m++] = base64[(triple >> 2 * 6) & 0x3f];
        base64_buf[m++] = base64[(triple >> 1 * 6) & 0x3f];
        base64_buf[m++] = base64[(triple >> 0 * 6) & 0x3f];
    }

    for (int l = 0; l < mod_table[userpass_buf_len % 3]; l++)
        base64_buf[encoded_len - 1 - l] = '=';

    char *auth = base64_buf;

    int r = snprintf(request_buf, req_buf_len, req_fmt, url, host, rest_port,
                     auth, strlen(sqlstr), sqlstr);
    if (r >= req_buf_len) {
        free(request_buf);
        ERROR_EXIT("too long request");
    }

    req_str_len = (int)strlen(request_buf);
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

    resp_len = response_length - 1;
    received = 0;

    char resEncodingChunk[] = "Encoding: chunked";
    char resHttp[] = "HTTP/1.1 ";
    char resHttpOk[] = "HTTP/1.1 200 OK";

    do {
#ifdef WINDOWS
        bytes = recv(pThreadInfo->sockfd, response_buf + received,
                     resp_len - received, 0);
#else
        bytes = read(pThreadInfo->sockfd, response_buf + received,
                     resp_len - received);
#endif
        debugPrint("receive %d bytes from server\n", bytes);
        if (bytes < 0) {
            errorPrint("%s", "reading no response from socket\n");
            goto free_of_post;
        }
        if (bytes == 0) {
            break;
        }
        received += bytes;

        if (strlen(response_buf)) {
            if (((NULL != strstr(response_buf, resEncodingChunk)) &&
                 (NULL != strstr(response_buf, resHttp))) ||
                ((NULL != strstr(response_buf, resHttpOk)) &&
                 (NULL != strstr(response_buf, "\"status\":")))) {
                break;
            }
        }
    } while (received < resp_len);

    if (received == resp_len) {
        errorPrint("%s", "storing complete response from socket\n");
        goto free_of_post;
    }

    if (NULL == strstr(response_buf, resHttpOk)) {
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
            if (strlen(pThreadInfo->filePath) > 0)
                appendResultBufToFile(databuf, pThreadInfo);
            totalLen = 0;
            memset(databuf, 0, FETCH_BUFFER_SIZE);
        }
        num_rows++;
        char temp[HEAD_BUFF_LEN] = {0};
        int  len = taos_print_row(temp, row, fields, num_fields);
        len += sprintf(temp + len, "\n");
        // printf("query result:%s\n", temp);
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
        return TSDB_DATA_TYPE_NULL;
    }
}

int init_taos_list(TAOS_POOL *pool, int size) {
    pool->taos_list = calloc(size, sizeof(TAOS *));
    pool->current = 0;
    pool->size = size;
    for (int i = 0; i < size; ++i) {
        pool->taos_list[i] = taos_connect(g_args.host, g_args.user,
                                          g_args.password, NULL, g_args.port);
        if (pool->taos_list[i] == NULL) {
            errorPrint("Failed to connect to TDengine, reason:%s\n",
                       taos_errstr(NULL));
            return -1;
        }
    }
    return 0;
}

TAOS *select_one_from_pool(TAOS_POOL *pool, char *db_name) {
    TAOS *taos = pool->taos_list[pool->current];
    if (db_name != NULL) {
        int code = taos_select_db(taos, db_name);
        if (code) {
            errorPrint("failed to select %s, reason: %s\n", db_name,
                       tstrerror(code));
            return NULL;
        }
    }
    pool->current++;
    if (pool->current >= pool->size) {
        pool->current = 0;
    }
    return taos;
}

void cleanup_taos_list(TAOS_POOL *pool) {
    for (int i = 0; i < pool->size; ++i) {
        taos_close(pool->taos_list[i]);
    }
    tmfree(pool->taos_list);
}