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

char *g_aggreFuncDemo[] = {"*",
                           "count(*)",
                           "avg(current)",
                           "sum(current)",
                           "max(current)",
                           "min(current)",
                           "first(current)",
                           "last(current)"};
char *g_aggreFunc[] = {"*",       "count(*)", "avg(C0)",   "sum(C0)",
                       "max(C0)", "min(C0)",  "first(C0)", "last(C0)"};


int count_datatype(char *dataType, uint32_t *number) {
    char *dup_str;
    *number = 0;
    if (strstr(dataType, ",") == NULL) {
        *number = 1;
        return 0;
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        while (token != NULL) {
            (*number)++;
            token = strsep(&running, ",");
        }
    }
    tmfree(dup_str);
    return 0;
}

int parse_tag_datatype(char *dataType, Column *tags) {
    char *dup_str;
    if (strstr(dataType, ",") == NULL) {
        if (0 == strcasecmp(dataType, "int")) {
            tags[0].type = TSDB_DATA_TYPE_INT;
            tags[0].length = sizeof(int32_t);
        } else if (0 == strcasecmp(dataType, "float")) {
            tags[0].type = TSDB_DATA_TYPE_FLOAT;
            tags[0].length = sizeof(float);
        } else if (0 == strcasecmp(dataType, "double")) {
            tags[0].type = TSDB_DATA_TYPE_DOUBLE;
            tags[0].length = sizeof(double);
        } else if (0 == strcasecmp(dataType, "tinyint")) {
            tags[0].type = TSDB_DATA_TYPE_TINYINT;
            tags[0].length = sizeof(int8_t);
        } else if (0 == strcasecmp(dataType, "bool")) {
            tags[0].type = TSDB_DATA_TYPE_BOOL;
            tags[0].length = sizeof(char);
        } else if (0 == strcasecmp(dataType, "smallint")) {
            tags[0].type = TSDB_DATA_TYPE_SMALLINT;
            tags[0].length = sizeof(int16_t);
        } else if (0 == strcasecmp(dataType, "bigint")) {
            tags[0].type = TSDB_DATA_TYPE_BIGINT;
            tags[0].length = sizeof(int64_t);
        } else if (0 == strcasecmp(dataType, "timestamp")) {
            tags[0].type = TSDB_DATA_TYPE_TIMESTAMP;
            tags[0].length = sizeof(int64_t);
        } else if (0 == strcasecmp(dataType, "utinyint")) {
            tags[0].type = TSDB_DATA_TYPE_UTINYINT;
            tags[0].length = sizeof(uint8_t);
        } else if (0 == strcasecmp(dataType, "usmallint")) {
            tags[0].type = TSDB_DATA_TYPE_USMALLINT;
            tags[0].length = sizeof(uint16_t);
        } else if (0 == strcasecmp(dataType, "uint")) {
            tags[0].type = TSDB_DATA_TYPE_UINT;
            tags[0].length = sizeof(uint32_t);
        } else if (0 == strcasecmp(dataType, "ubigint")) {
            tags[0].type = TSDB_DATA_TYPE_UBIGINT;
            tags[0].length = sizeof(uint64_t);
        } else if (0 == strcasecmp(dataType, "nchar")) {
            tags[0].type = TSDB_DATA_TYPE_NCHAR;
            tags[0].length = 0;
        } else if (0 == strcasecmp(dataType, "binary")) {
            tags[0].type = TSDB_DATA_TYPE_BINARY;
            tags[0].length = 0;
        } else if (0 == strcasecmp(dataType, "json")) {
            tags[0].type = TSDB_DATA_TYPE_JSON;
            tags[0].length = 0;
        } else if (1 == regexMatch(dataType, "^(BINARY)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            tags[0].type = TSDB_DATA_TYPE_BINARY;
            tags[0].length = atoi(length);
        } else if (1 == regexMatch(dataType, "^(NCHAR)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            tags[0].type = TSDB_DATA_TYPE_NCHAR;
            tags[0].length = atoi(length);
        } else if (1 == regexMatch(dataType, "^(json)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            tags[0].type = TSDB_DATA_TYPE_JSON;
            tags[0].length = atoi(length);
        } else {
            errorPrint(stderr, "Invalid data type: %s\n", dataType);
            return -1;
        }
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        int   index = 0;
        while (token != NULL) {
            if (0 == strcasecmp(token, "int")) {
                tags[index].type = TSDB_DATA_TYPE_INT;
                tags[index].length = sizeof(int32_t);
            } else if (0 == strcasecmp(token, "float")) {
                tags[index].type = TSDB_DATA_TYPE_FLOAT;
                tags[index].length = sizeof(float);
            } else if (0 == strcasecmp(token, "double")) {
                tags[index].type = TSDB_DATA_TYPE_DOUBLE;
                tags[index].length = sizeof(double);
            } else if (0 == strcasecmp(token, "tinyint")) {
                tags[index].type = TSDB_DATA_TYPE_TINYINT;
                tags[index].length = sizeof(int8_t);
            } else if (0 == strcasecmp(token, "bool")) {
                tags[index].type = TSDB_DATA_TYPE_BOOL;
                tags[index].length = sizeof(char);
            } else if (0 == strcasecmp(token, "smallint")) {
                tags[index].type = TSDB_DATA_TYPE_SMALLINT;
                tags[index].length = sizeof(int16_t);
            } else if (0 == strcasecmp(token, "bigint")) {
                tags[index].type = TSDB_DATA_TYPE_BIGINT;
                tags[index].length = sizeof(int64_t);
            } else if (0 == strcasecmp(token, "timestamp")) {
                tags[index].type = TSDB_DATA_TYPE_TIMESTAMP;
                tags[index].length = sizeof(int64_t);
            } else if (0 == strcasecmp(token, "utinyint")) {
                tags[index].type = TSDB_DATA_TYPE_UTINYINT;
                tags[index].length = sizeof(uint8_t);
            } else if (0 == strcasecmp(token, "usmallint")) {
                tags[index].type = TSDB_DATA_TYPE_USMALLINT;
                tags[index].length = sizeof(uint16_t);
            } else if (0 == strcasecmp(token, "uint")) {
                tags[index].type = TSDB_DATA_TYPE_UINT;
                tags[index].length = sizeof(uint32_t);
            } else if (0 == strcasecmp(token, "ubigint")) {
                tags[index].type = TSDB_DATA_TYPE_UBIGINT;
                tags[index].length = sizeof(uint64_t);
            } else if (0 == strcasecmp(token, "nchar")) {
                tags[index].type = TSDB_DATA_TYPE_NCHAR;
                tags[index].length = 0;
            } else if (0 == strcasecmp(token, "binary")) {
                tags[index].type = TSDB_DATA_TYPE_BINARY;
                tags[index].length = 0;
            } else if (1 == regexMatch(token, "^(BINARY)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                tags[index].type = TSDB_DATA_TYPE_BINARY;
                tags[index].length = atoi(length);
            } else if (1 == regexMatch(token, "^(NCHAR)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                tags[index].type = TSDB_DATA_TYPE_NCHAR;
                tags[index].length = atoi(length);
            } else if (1 == regexMatch(token, "^(JSON)(\\([1-9][0-9]*\\))?$",
                                       REG_ICASE | REG_EXTENDED)) {
                errorPrint(stderr, "%s",
                           "Json tag type cannot use with other type tags\n");
                tmfree(dup_str);
                return -1;
            } else {
                errorPrint(stderr, "Invalid data type <%s>\n", token);
                tmfree(dup_str);
                return -1;
            }
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
    return 0;
}

int parse_col_datatype(char *dataType, Column *columns) {
    char *dup_str;
    if (strstr(dataType, ",") == NULL) {
        if (0 == strcasecmp(dataType, "int")) {
            columns[0].type = TSDB_DATA_TYPE_INT;
            columns[0].length = sizeof(int32_t);
        } else if (0 == strcasecmp(dataType, "float")) {
            columns[0].type = TSDB_DATA_TYPE_FLOAT;
            columns[0].length = sizeof(float);
        } else if (0 == strcasecmp(dataType, "double")) {
            columns[0].type = TSDB_DATA_TYPE_DOUBLE;
            columns[0].length = sizeof(double);
        } else if (0 == strcasecmp(dataType, "tinyint")) {
            columns[0].type = TSDB_DATA_TYPE_TINYINT;
            columns[0].length = sizeof(int8_t);
        } else if (0 == strcasecmp(dataType, "bool")) {
            columns[0].type = TSDB_DATA_TYPE_BOOL;
            columns[0].length = sizeof(char);
        } else if (0 == strcasecmp(dataType, "smallint")) {
            columns[0].type = TSDB_DATA_TYPE_SMALLINT;
            columns[0].length = sizeof(int16_t);
        } else if (0 == strcasecmp(dataType, "bigint")) {
            columns[0].type = TSDB_DATA_TYPE_BIGINT;
            columns[0].length = sizeof(int64_t);
        } else if (0 == strcasecmp(dataType, "timestamp")) {
            columns[0].type = TSDB_DATA_TYPE_TIMESTAMP;
            columns[0].length = sizeof(int64_t);
        } else if (0 == strcasecmp(dataType, "utinyint")) {
            columns[0].type = TSDB_DATA_TYPE_UTINYINT;
            columns[0].length = sizeof(uint8_t);
        } else if (0 == strcasecmp(dataType, "usmallint")) {
            columns[0].type = TSDB_DATA_TYPE_USMALLINT;
            columns[0].length = sizeof(uint16_t);
        } else if (0 == strcasecmp(dataType, "uint")) {
            columns[0].type = TSDB_DATA_TYPE_UINT;
            columns[0].length = sizeof(uint32_t);
        } else if (0 == strcasecmp(dataType, "ubigint")) {
            columns[0].type = TSDB_DATA_TYPE_UBIGINT;
            columns[0].length = sizeof(uint64_t);
        } else if (0 == strcasecmp(dataType, "nchar")) {
            columns[0].type = TSDB_DATA_TYPE_NCHAR;
            columns[0].length = 0;
        } else if (0 == strcasecmp(dataType, "binary")) {
            columns[0].type = TSDB_DATA_TYPE_BINARY;
            columns[0].length = 0;
        } else if (1 == regexMatch(dataType, "^(BINARY)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            columns[0].type = TSDB_DATA_TYPE_BINARY;
            columns[0].length = atoi(length);
        } else if (1 == regexMatch(dataType, "^(NCHAR)(\\([1-9][0-9]*\\))$",
                                   REG_ICASE | REG_EXTENDED)) {
            char type[DATATYPE_BUFF_LEN];
            char length[BIGINT_BUFF_LEN];
            sscanf(dataType, "%[^(](%[^)]", type, length);
            columns[0].type = TSDB_DATA_TYPE_NCHAR;
            columns[0].length = atoi(length);
        } else {
            errorPrint(stderr, "Invalid data type: %s\n", dataType);
            return -1;
        }
    } else {
        dup_str = strdup(dataType);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        int   index = 0;
        while (token != NULL) {
            if (0 == strcasecmp(token, "int")) {
                columns[index].type = TSDB_DATA_TYPE_INT;
                columns[index].length = sizeof(int32_t);
            } else if (0 == strcasecmp(token, "float")) {
                columns[index].type = TSDB_DATA_TYPE_FLOAT;
                columns[index].length = sizeof(float);
            } else if (0 == strcasecmp(token, "double")) {
                columns[index].type = TSDB_DATA_TYPE_DOUBLE;
                columns[index].length = sizeof(double);
            } else if (0 == strcasecmp(token, "tinyint")) {
                columns[index].type = TSDB_DATA_TYPE_TINYINT;
                columns[index].length = sizeof(int8_t);
            } else if (0 == strcasecmp(token, "bool")) {
                columns[index].type = TSDB_DATA_TYPE_BOOL;
                columns[index].length = sizeof(char);
            } else if (0 == strcasecmp(token, "smallint")) {
                columns[index].type = TSDB_DATA_TYPE_SMALLINT;
                columns[index].length = sizeof(int16_t);
            } else if (0 == strcasecmp(token, "bigint")) {
                columns[index].type = TSDB_DATA_TYPE_BIGINT;
                columns[index].length = sizeof(int64_t);
            } else if (0 == strcasecmp(token, "timestamp")) {
                columns[index].type = TSDB_DATA_TYPE_TIMESTAMP;
                columns[index].length = sizeof(int64_t);
            } else if (0 == strcasecmp(token, "utinyint")) {
                columns[index].type = TSDB_DATA_TYPE_UTINYINT;
                columns[index].length = sizeof(uint8_t);
            } else if (0 == strcasecmp(token, "usmallint")) {
                columns[index].type = TSDB_DATA_TYPE_USMALLINT;
                columns[index].length = sizeof(uint16_t);
            } else if (0 == strcasecmp(token, "uint")) {
                columns[index].type = TSDB_DATA_TYPE_UINT;
                columns[index].length = sizeof(uint32_t);
            } else if (0 == strcasecmp(token, "ubigint")) {
                columns[index].type = TSDB_DATA_TYPE_UBIGINT;
                columns[index].length = sizeof(uint64_t);
            } else if (0 == strcasecmp(token, "nchar")) {
                columns[index].type = TSDB_DATA_TYPE_NCHAR;
                columns[index].length = 0;
            } else if (0 == strcasecmp(token, "binary")) {
                columns[index].type = TSDB_DATA_TYPE_BINARY;
                columns[index].length = 0;
            } else if (1 == regexMatch(token, "^(BINARY)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                columns[index].type = TSDB_DATA_TYPE_BINARY;
                columns[index].length = atoi(length);
            } else if (1 == regexMatch(token, "^(NCHAR)(\\([1-9][0-9]*\\))$",
                                       REG_ICASE | REG_EXTENDED)) {
                char type[DATATYPE_BUFF_LEN];
                char length[BIGINT_BUFF_LEN];
                sscanf(token, "%[^(](%[^)]", type, length);
                columns[index].type = TSDB_DATA_TYPE_NCHAR;
                columns[index].length = atoi(length);
            } else {
                errorPrint(stderr, "Invalid data type <%s>\n", token);
                tmfree(dup_str);
                return -1;
            }
            index++;
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
    return 0;
}

static SSuperTable *init_stable() {
    SDataBase *database = g_arguments->db;
    database->superTbls = calloc(1, sizeof(SSuperTable));
    if (database->superTbls == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    g_memoryUsage += sizeof(SSuperTable);
    SSuperTable *stbInfo = database->superTbls;
    stbInfo->iface = TAOSC_IFACE;
    stbInfo->stbName = "meters";
    stbInfo->childTblPrefix = DEFAULT_TB_PREFIX;
    stbInfo->escape_character = 0;
    stbInfo->use_metric = 1;
    stbInfo->columns = calloc(3, sizeof(Column));
    if (stbInfo->columns == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    g_memoryUsage += 3 * sizeof(Column);
    stbInfo->columns[0].type = TSDB_DATA_TYPE_FLOAT;
    stbInfo->columns[1].type = TSDB_DATA_TYPE_INT;
    stbInfo->columns[2].type = TSDB_DATA_TYPE_FLOAT;
    stbInfo->columns[0].length = sizeof(float);
    stbInfo->columns[1].length = sizeof(int32_t);
    stbInfo->columns[2].length = sizeof(float);
    stbInfo->tags = calloc(3, sizeof(Column));
    if (stbInfo->tags == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    g_memoryUsage += 3 * sizeof(Column);
    stbInfo->tags[0].type = TSDB_DATA_TYPE_INT;
    stbInfo->tags[1].type = TSDB_DATA_TYPE_BINARY;
    stbInfo->tags[0].length = sizeof(int32_t);
    stbInfo->tags[1].length = 16;
    stbInfo->columnCount = 3;
    stbInfo->tagCount = 2;
    stbInfo->insert_interval = 0;
    stbInfo->timestamp_step = 1;
    stbInfo->interlaceRows = 0;
    stbInfo->childTblCount = DEFAULT_CHILDTABLES;
    stbInfo->childTblLimit = 0;
    stbInfo->childTblOffset = 0;
    stbInfo->autoCreateTable = false;
    stbInfo->childTblExists = false;
    stbInfo->random_data_source = true;
    stbInfo->lineProtocol = TSDB_SML_LINE_PROTOCOL;
    stbInfo->tsPrecision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    stbInfo->startTimestamp = DEFAULT_START_TIME;
    stbInfo->insertRows = DEFAULT_INSERT_ROWS;
    stbInfo->disorderRange = DEFAULT_DISORDER_RANGE;
    stbInfo->disorderRatio = 0;
    stbInfo->file_factor = -1;
    stbInfo->delay = -1;
    return stbInfo;
}

static SDataBase *init_database() {
    g_arguments->db = calloc(1, sizeof(SDataBase));
    if (g_arguments->db == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    g_memoryUsage += sizeof(SDataBase);
    SDataBase *database = g_arguments->db;
    database->dbName = DEFAULT_DATABASE;
    database->drop = 1;
    database->superTblCount = 1;
    database->dbCfg.minRows = -1;
    database->dbCfg.maxRows = -1;
    database->dbCfg.comp = -1;
    database->dbCfg.walLevel = -1;
    database->dbCfg.cacheLast = -1;
    database->dbCfg.fsync = -1;
    database->dbCfg.replica = -1;
    database->dbCfg.update = -1;
    database->dbCfg.keep = -1;
    database->dbCfg.days = -1;
    database->dbCfg.cache = -1;
    database->dbCfg.blocks = -1;
    database->dbCfg.quorum = -1;
    database->dbCfg.buffer = -1;
    database->dbCfg.pages = -1;
    database->dbCfg.page_size = -1;
    database->dbCfg.retentions = NULL;
    database->dbCfg.single_stable = -1;
    database->dbCfg.vgroups = -1;
    database->dbCfg.strict = -1;
    database->dbCfg.precision = TSDB_TIME_PRECISION_MILLI;
    database->dbCfg.sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    return database;
}
void init_argument() {
    g_arguments = calloc(1, sizeof(SArguments));
    if (g_arguments == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    g_memoryUsage += sizeof(SArguments);
    if (taos_get_client_info()[0] == '3') {
        g_arguments->taosc_version = 3;
    } else {
        g_arguments->taosc_version = 2;
    }
    g_arguments->pool = calloc(1, sizeof(TAOS_POOL));
    if (g_arguments->pool == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    g_memoryUsage += sizeof(TAOS_POOL);
    g_arguments->test_mode = INSERT_TEST;
    g_arguments->demo_mode = 1;
    g_arguments->dbCount = 1;
    g_arguments->host = NULL;
    g_arguments->port = DEFAULT_PORT;
    g_arguments->telnet_tcp_port = TELNET_TCP_PORT;
    g_arguments->user = TSDB_DEFAULT_USER;
    g_arguments->password = TSDB_DEFAULT_PASS;
    g_arguments->answer_yes = 0;
    g_arguments->debug_print = 0;
    g_arguments->performance_print = 0;
    g_arguments->output_file = DEFAULT_OUTPUT;
    g_arguments->nthreads = DEFAULT_NTHREADS;
    g_arguments->table_threads = DEFAULT_NTHREADS;
    g_arguments->connection_pool = DEFAULT_NTHREADS;
    g_arguments->binwidth = DEFAULT_BINWIDTH;
    g_arguments->prepared_rand = DEFAULT_PREPARED_RAND;
    g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
    g_arguments->g_totalChildTables = DEFAULT_CHILDTABLES;
    g_arguments->g_actualChildTables = 0;
    g_arguments->g_autoCreatedChildTables = 0;
    g_arguments->g_existedChildTables = 0;
    g_arguments->chinese = 0;
    g_arguments->aggr_func = 0;
    g_arguments->db = init_database();
    g_arguments->db->superTbls = init_stable();
}

void modify_argument() {
    SSuperTable *superTable = g_arguments->db->superTbls;
    if (init_taos_list()) exit(EXIT_FAILURE);

    if (g_arguments->db->superTbls->iface == STMT_IFACE) {
        if (g_arguments->reqPerReq > INT16_MAX) {
            g_arguments->reqPerReq = INT16_MAX;
        }
        if (g_arguments->prepared_rand > g_arguments->reqPerReq) {
            g_arguments->prepared_rand = g_arguments->reqPerReq;
        }
    }

    for (int i = 0; i < superTable->columnCount; ++i) {
        if (superTable->columns[i].length == 0) {
            superTable->columns[i].length = g_arguments->binwidth;
        }
    }

    if (g_arguments->demo_mode) {
        tstrncpy(superTable->tags[0].name, "groupid", TSDB_COL_NAME_LEN + 1);
        superTable->tags[0].min = -1 * (RAND_MAX >> 1);
        superTable->tags[1].min = -1 * (RAND_MAX >> 1);
        superTable->tags[0].max = RAND_MAX >> 1;
        superTable->tags[1].max = RAND_MAX >> 1;
        tstrncpy(superTable->tags[1].name, "location", TSDB_COL_NAME_LEN + 1);
        tstrncpy(superTable->columns[0].name, "current", TSDB_COL_NAME_LEN + 1);
        tstrncpy(superTable->columns[1].name, "voltage", TSDB_COL_NAME_LEN + 1);
        tstrncpy(superTable->columns[2].name, "phase", TSDB_COL_NAME_LEN + 1);
        superTable->columns[0].min = -1 * (RAND_MAX >> 1);
        superTable->columns[0].max = RAND_MAX >> 1;
        superTable->columns[1].min = -1 * (RAND_MAX >> 1);
        superTable->columns[1].max = RAND_MAX >> 1;
        superTable->columns[2].min = -1 * (RAND_MAX >> 1);
        superTable->columns[2].max = RAND_MAX >> 1;
    } else {
        for (int i = 0; i < superTable->tagCount; ++i) {
            sprintf(superTable->tags[i].name, "t%d", i);
            superTable->tags[i].min = -1 * (RAND_MAX >> 1);
            superTable->tags[i].max = RAND_MAX >> 1;
        }
    }

    for (int i = 0; i < superTable->tagCount; ++i) {
        if (superTable->tags[i].length == 0) {
            superTable->tags[i].length = g_arguments->binwidth;
        }
    }

    if (g_arguments->intColumnCount > superTable->columnCount) {
        Column *tmp_col = (Column *)realloc(
            superTable->columns, g_arguments->intColumnCount * sizeof(Column));
        if (tmp_col != NULL) {
            superTable->columns = tmp_col;
            for (int i = superTable->columnCount;
                 i < g_arguments->intColumnCount; ++i) {
                memset(&superTable->columns[i], 0, sizeof(Column));
                superTable->columns[i].type = TSDB_DATA_TYPE_INT;
                superTable->columns[i].length = sizeof(int32_t);
            }
        }
        superTable->columnCount = g_arguments->intColumnCount;
    }
    if (!g_arguments->demo_mode) {
        for (int i = 0; i < superTable->columnCount; ++i) {
            sprintf(superTable->columns[i].name, "c%d", i);
            superTable->columns[i].min = -1 * (RAND_MAX >> 1);
            superTable->columns[i].max = RAND_MAX >> 1;
        }
    }
}

static void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
#ifdef LINUX
    prctl(PR_SET_NAME, "queryStableAggrFunc");
#endif
    char *command = calloc(1, BUFFER_SIZE);
    if (command == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    FILE *  fp = g_arguments->fpOfInsertResult;
    int64_t totalData = g_arguments->db->superTbls->insertRows *
                        g_arguments->db->superTbls->childTblCount;
    char **aggreFunc;
    int    n;

    if (g_arguments->demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]);
    } else {
        aggreFunc = g_aggreFunc;
        n = sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0]);
    }

    infoPrint(stdout, "total Data: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp, "Querying On %" PRId64 " records:\n", totalData);
    }
    for (int j = 0; j < n; j++) {
        char condition[COND_BUF_LEN] = "\0";
        char tempS[64] = "\0";

        int64_t m = 10 < g_arguments->db->superTbls->childTblCount
                        ? 10
                        : g_arguments->db->superTbls->childTblCount;

        for (int64_t i = 1; i <= m; i++) {
            if (i == 1) {
                if (g_arguments->demo_mode) {
                    sprintf(tempS, "groupid = %" PRId64 "", i);
                } else {
                    sprintf(tempS, "t0 = %" PRId64 "", i);
                }
            } else {
                if (g_arguments->demo_mode) {
                    sprintf(tempS, " or groupid = %" PRId64 " ", i);
                } else {
                    sprintf(tempS, " or t0 = %" PRId64 " ", i);
                }
            }
            strncat(condition, tempS, COND_BUF_LEN - 1);

            sprintf(command, "SELECT %s FROM meters WHERE %s", aggreFunc[j],
                    condition);
            if (fp) {
                fprintf(fp, "%s\n", command);
            }

            double t = (double)toolsGetTimestampUs();

            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint(stderr, "Failed to query:%s\n", taos_errstr(pSql));
                taos_free_result(pSql);
                free(command);
                return NULL;
            }
            int count = 0;
            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }
            t = toolsGetTimestampUs() - t;
            if (fp) {
                fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
                        totalData / (t / 1000), t);
            }
            infoPrint(stdout, "%s took %.6f second(s)\n\n", command,
                      t / 1000000);

            taos_free_result(pSql);
        }
    }
    free(command);
    return NULL;
}

static void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *      taos = pThreadInfo->taos;
#ifdef LINUX
    prctl(PR_SET_NAME, "queryNtableAggrFunc");
#endif
    char *  command = calloc(1, BUFFER_SIZE);
    if (command == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    FILE *  fp = g_arguments->fpOfInsertResult;
    int64_t totalData = g_arguments->db->superTbls->childTblCount *
                        g_arguments->db->superTbls->insertRows;
    char **aggreFunc;
    int    n;

    if (g_arguments->demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0]);
    } else {
        aggreFunc = g_aggreFunc;
        n = sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0]);
    }

    infoPrint(stdout, "totalData: %" PRId64 "\n", totalData);
    if (fp) {
        fprintf(fp,
                "| QFunctions |    QRecords    |   QSpeed(R/s)   |  "
                "QLatency(ms) |\n");
    }

    for (int j = 0; j < n; j++) {
        double   totalT = 0;
        uint64_t count = 0;
        for (int64_t i = 0; i < g_arguments->db->superTbls->childTblCount;
             i++) {
            if (g_arguments->db->superTbls->escape_character) {
                sprintf(command,
                        "SELECT %s FROM `%s%" PRId64 "` WHERE ts>= %" PRIu64,
                        aggreFunc[j],
                        g_arguments->db->superTbls->childTblPrefix, i,
                        DEFAULT_START_TIME);
            } else {
                sprintf(
                    command, "SELECT %s FROM %s%" PRId64 " WHERE ts>= %" PRIu64,
                    aggreFunc[j], g_arguments->db->superTbls->childTblPrefix, i,
                    DEFAULT_START_TIME);
            }

            double    t = (double)toolsGetTimestampUs();
            TAOS_RES *pSql = taos_query(taos, command);
            int32_t   code = taos_errno(pSql);

            if (code != 0) {
                errorPrint(stderr, "Failed to query <%s>, reason:%s\n", command,
                           taos_errstr(pSql));
                taos_free_result(pSql);
                free(command);
                return NULL;
            }

            while (taos_fetch_row(pSql) != NULL) {
                count++;
            }

            t = toolsGetTimestampUs() - t;
            totalT += t;

            taos_free_result(pSql);
        }
        if (fp) {
            fprintf(fp, "|%10s  |   %" PRId64 "   |  %12.2f   |   %10.2f  |\n",
                    aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j],
                    totalData,
                    (double)(g_arguments->db->superTbls->childTblCount *
                             g_arguments->db->superTbls->insertRows) /
                        totalT,
                    totalT / 1000000);
        }
        infoPrint(stdout, "<%s> took %.6f second(s)\n", command,
                  totalT / 1000000);
    }
    free(command);
    return NULL;
}

void queryAggrFunc() {
    pthread_t   read_id;
    threadInfo *pThreadInfo = calloc(1, sizeof(threadInfo));
    if (pThreadInfo == NULL) {
        errorPrint(stderr, "%s", "memory allocation failed\n");
        exit(EXIT_FAILURE);
    }
    pThreadInfo->taos = select_one_from_pool(g_arguments->db->dbName);
    if (g_arguments->db->superTbls->use_metric) {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    free(pThreadInfo);
}
