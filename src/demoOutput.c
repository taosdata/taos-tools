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

#include "demo.h"

#define SHOW_PARSE_RESULT_START_TO_FILE(fp)                               \
    do {                                                                  \
        if (g_args.metaFile)                                              \
            fprintf(fp,                                                   \
                    "\033[1m\033[40;32m================ %s parse result " \
                    "START ================\033[0m\n",                    \
                    g_args.metaFile);                                     \
    } while (0)

#define SHOW_PARSE_RESULT_END_TO_FILE(fp)                                 \
    do {                                                                  \
        if (g_args.metaFile)                                              \
            fprintf(fp,                                                   \
                    "\033[1m\033[40;32m================ %s parse result " \
                    "END================\033[0m\n",                       \
                    g_args.metaFile);                                     \
    } while (0)

int getDbFromServer(TAOS *taos, SDbInfo **dbInfos) {
    TAOS_RES *res;
    TAOS_ROW  row = NULL;
    int       count = 0;

    res = taos_query(taos, "show databases;");
    int32_t code = taos_errno(res);

    if (code != 0) {
        errorPrint("failed to run <show databases>, reason: %s\n",
                   taos_errstr(res));
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    while ((row = taos_fetch_row(res)) != NULL) {
        // sys database name : 'log'
        if (strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log",
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0) {
            continue;
        }

        dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
        if (dbInfos[count] == NULL) {
            errorPrint("failed to allocate memory for some dbInfo[%d]\n",
                       count);
            return -1;
        }

        tstrncpy(dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                 fields[TSDB_SHOW_DB_NAME_INDEX].bytes);
        formatTimestamp(dbInfos[count]->create_time,
                        *(int64_t *)row[TSDB_SHOW_DB_CREATED_TIME_INDEX],
                        TSDB_TIME_PRECISION_MILLI);
        dbInfos[count]->ntables = *((int64_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
        dbInfos[count]->vgroups = *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);
        dbInfos[count]->replica = *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
        dbInfos[count]->quorum = *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
        dbInfos[count]->days = *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);

        tstrncpy(dbInfos[count]->keeplist, (char *)row[TSDB_SHOW_DB_KEEP_INDEX],
                 fields[TSDB_SHOW_DB_KEEP_INDEX].bytes);
        dbInfos[count]->cache = *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
        dbInfos[count]->blocks = *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
        dbInfos[count]->minrows = *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
        dbInfos[count]->maxrows = *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
        dbInfos[count]->wallevel =
            *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
        dbInfos[count]->fsync = *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
        dbInfos[count]->comp =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
        dbInfos[count]->cachelast =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_CACHELAST_INDEX]));

        tstrncpy(dbInfos[count]->precision,
                 (char *)row[TSDB_SHOW_DB_PRECISION_INDEX],
                 fields[TSDB_SHOW_DB_PRECISION_INDEX].bytes);
        dbInfos[count]->update = *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
        tstrncpy(dbInfos[count]->status, (char *)row[TSDB_SHOW_DB_STATUS_INDEX],
                 fields[TSDB_SHOW_DB_STATUS_INDEX].bytes);

        count++;
        if (count > MAX_DATABASE_COUNT) {
            errorPrint("The database count overflow than %d\n",
                       MAX_DATABASE_COUNT);
            break;
        }
    }

    return count;
}

void xDumpFieldToFile(FILE *fp, const char *val, TAOS_FIELD *field,
                      int32_t length, int precision) {
    if (val == NULL) {
        fprintf(fp, "%s", TSDB_DATA_NULL_STR);
        return;
    }

    char buf[TSDB_MAX_BYTES_PER_ROW];
    switch (field->type) {
        case TSDB_DATA_TYPE_BOOL:
            fprintf(fp, "%d", ((((int32_t)(*((int8_t *)val))) == 1) ? 1 : 0));
            break;

        case TSDB_DATA_TYPE_TINYINT:
            fprintf(fp, "%d", *((int8_t *)val));
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            fprintf(fp, "%d", *((uint8_t *)val));
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            fprintf(fp, "%d", *((int16_t *)val));
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            fprintf(fp, "%d", *((uint16_t *)val));
            break;

        case TSDB_DATA_TYPE_INT:
            fprintf(fp, "%d", *((int32_t *)val));
            break;

        case TSDB_DATA_TYPE_UINT:
            fprintf(fp, "%d", *((uint32_t *)val));
            break;

        case TSDB_DATA_TYPE_BIGINT:
            fprintf(fp, "%" PRId64 "", *((int64_t *)val));
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            fprintf(fp, "%" PRId64 "", *((uint64_t *)val));
            break;

        case TSDB_DATA_TYPE_FLOAT:
            fprintf(fp, "%.5f", GET_FLOAT_VAL(val));
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            fprintf(fp, "%.9f", GET_DOUBLE_VAL(val));
            break;

        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
            memcpy(buf, val, length);
            buf[length] = 0;
            fprintf(fp, "\'%s\'", buf);
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(buf, *(int64_t *)val, precision);
            fprintf(fp, "'%s'", buf);
            break;

        default:
            break;
    }
}

int xDumpResultToFile(const char *fname, TAOS_RES *tres) {
    TAOS_ROW row = taos_fetch_row(tres);
    if (row == NULL) {
        return 0;
    }

    FILE *fp = fopen(fname, "at");
    if (fp == NULL) {
        errorPrint("failed to open file: %s\n", fname);
        return -1;
    }

    int         num_fields = taos_num_fields(tres);
    TAOS_FIELD *fields = taos_fetch_fields(tres);
    int         precision = taos_result_precision(tres);

    for (int col = 0; col < num_fields; col++) {
        if (col > 0) {
            fprintf(fp, ",");
        }
        fprintf(fp, "%s", fields[col].name);
    }
    fputc('\n', fp);

    int numOfRows = 0;
    do {
        int32_t *length = taos_fetch_lengths(tres);
        for (int i = 0; i < num_fields; i++) {
            if (i > 0) {
                fputc(',', fp);
            }
            xDumpFieldToFile(fp, (const char *)row[i], fields + i, length[i],
                             precision);
        }
        fputc('\n', fp);

        numOfRows++;
        row = taos_fetch_row(tres);
    } while (row != NULL);

    fclose(fp);

    return numOfRows;
}

#ifndef TAOSDEMO_COMMIT_SHA1
#define TAOSDEMO_COMMIT_SHA1 "unknown"
#endif

#ifndef TD_VERNUMBER
#define TD_VERNUMBER "unknown"
#endif

#ifndef TAOSDEMO_STATUS
#define TAOSDEMO_STATUS "unknown"
#endif

void printVersion() {
    char tdengine_ver[] = TD_VERNUMBER;
    char taosdemo_ver[] = TAOSDEMO_COMMIT_SHA1;
    char taosdemo_status[] = TAOSDEMO_STATUS;

    if (strlen(taosdemo_status) == 0) {
        printf("taosdemo version %s-%s\n", tdengine_ver, taosdemo_ver);
    } else {
        printf("taosdemo version %s-%s, status:%s\n", tdengine_ver,
               taosdemo_ver, taosdemo_status);
    }
    exit(EXIT_SUCCESS);
}

void printHelp() {
    char indent[10] = "  ";
    printf("%s\n\n", "Usage: taosBenchmark [OPTION...]");
    printf("%s%s%s%s\n", indent, "-f, --file=FILE", "\t\t",
           "The meta file to the execution procedure.");
    printf("%s%s%s%s\n", indent, "-u, --user=USER", "\t\t",
           "The user name to use when connecting to the server.");
#ifdef _TD_POWER_
    printf("%s%s%s%s\n", indent, "-p, --password", "\t\t",
           "The password to use when connecting to the server. By default is "
           "'powerdb'");
    printf("%s%s%s%s\n", indent, "-c, --config-dir=CONFIG_DIR", "\t",
           "Configuration directory. By default is '/etc/power/'.");
#elif (_TD_TQ_ == true)
    printf("%s%s%s%s\n", indent, "-p, --password", "\t\t",
           "The password to use when connecting to the server. By default is "
           "'tqueue'");
    printf("%s%s%s%s\n", indent, "-c, --config-dir=CONFIG_DIR", "\t",
           "Configuration directory. By default is '/etc/tq/'.");
#elif (_TD_PRO_ == true)
    printf("%s%s%s%s\n", indent, "-p, --password", "\t\t",
           "The password to use when connecting to the server. By default is "
           "'prodb'");
    printf("%s%s%s%s\n", indent, "-c, --config-dir=CONFIG_DIR", "\t",
           "Configuration directory. By default is '/etc/ProDB/'.");
#else
    printf("%s%s%s%s\n", indent, "-p, --password", "\t\t",
           "The password to use when connecting to the server.");
    printf("%s%s%s%s\n", indent, "-c, --config-dir=CONFIG_DIR", "\t",
           "Configuration directory.");
#endif
    printf("%s%s%s%s\n", indent, "-h, --host=HOST", "\t\t",
           "TDengine server FQDN to connect. The default host is localhost.");
    printf("%s%s%s%s\n", indent, "-P, --port=PORT", "\t\t",
           "The TCP/IP port number to use for the connection.");
    printf("%s%s%s%s\n", indent, "-I, --interface=INTERFACE", "\t",
           "The interface (taosc, rest, stmt, and sml(line protocol)) taosdemo "
           "uses. By default "
           "use 'taosc'.");
    printf("%s%s%s%s\n", indent, "-d, --database=DATABASE", "\t",
           "Destination database. By default is 'test'.");
    printf("%s%s%s%s\n", indent, "-a, --replica=REPLICA", "\t\t",
           "Set the replica parameters of the database, By default use 1, min: "
           "1, max: 3.");
    printf("%s%s%s%s\n", indent, "-m, --table-prefix=TABLEPREFIX", "\t",
           "Table prefix name. By default use 'd'.");
    printf("%s%s%s%s\n", indent, "-E, --escape-character", "\t",
           "Use escape character for Both Stable and normmal table name");
    printf("%s%s%s%s\n", indent, "-pressure", "\t",
           "Use taosdemo pressure mode");
    printf("%s%s%s%s\n", indent, "-C, --chinese", "\t",
           "Use chinese characters as the data source for binary/nchar data");
    printf("%s%s%s%s\n", indent, "-s, --sql-file=FILE", "\t\t",
           "The select sql file.");
    printf("%s%s%s%s\n", indent, "-N, --normal-table", "\t\t",
           "Use normal table flag.");
    printf("%s%s%s%s\n", indent, "-o, --output=FILE", "\t\t",
           "Direct output to the named file. By default use './output.txt'.");
    printf("%s%s%s%s\n", indent, "-q, --query-mode=MODE", "\t\t",
           "Query mode -- 0: SYNC, 1: ASYNC. By default use SYNC.");
    printf("%s%s%s%s\n", indent, "-b, --data-type=DATATYPE", "\t",
           "The col_type of columns, By default use: FLOAT,INT,FLOAT. NCHAR "
           "and BINARY can also use custom length. Eg: NCHAR(16),BINARY(8)");
    printf("%s%s%s%s%d\n", indent, "-w, --binwidth=WIDTH", "\t\t",
           "The width of col_type 'BINARY' or 'NCHAR'. By default use ",
           g_args.binwidth);
    printf("%s%s%s%s%d%s%d\n", indent, "-l, --columns=COLUMNS", "\t\t",
           "The number of columns per record. Demo mode by default is ",
           DEFAULT_DATATYPE_NUM, " (float, int, float). Max values is ",
           MAX_NUM_COLUMNS);
    printf("%s%s%s%s\n", indent, indent, indent,
           "\t\t\t\tAll of the new column(s) type is INT. If use -b to specify "
           "column type, -l will be ignored.");
    printf("%s%s%s%s%d.\n", indent, "-T, --threads=NUMBER", "\t\t",
           "The number of threads. By default use ", DEFAULT_NTHREADS);
    printf("%s%s%s%s\n", indent, "-i, --insert-interval=NUMBER", "\t",
           "The sleep time (ms) between insertion. By default is 0.");
    printf("%s%s%s%s%d.\n", indent, "-S, --time-step=TIME_STEP", "\t",
           "The timestamp step between insertion. By default is ",
           DEFAULT_TIMESTAMP_STEP);
    printf("%s%s%s%s%d.\n", indent, "-B, --interlace-rows=NUMBER", "\t",
           "The interlace rows of insertion. By default is ",
           DEFAULT_INTERLACE_ROWS);
    printf("%s%s%s%s\n", indent, "-r, --rec-per-req=NUMBER", "\t",
           "The number of records per request. By default is 30000.");
    printf("%s%s%s%s\n", indent, "-t, --tables=NUMBER", "\t\t",
           "The number of tables. By default is 10000.");
    printf("%s%s%s%s\n", indent, "-n, --records=NUMBER", "\t\t",
           "The number of records per table. By default is 10000.");
    printf("%s%s%s%s\n", indent, "-M, --random", "\t\t\t",
           "The value of records generated are totally random.");
    printf("%s\n", "\t\t\t\tBy default to simulate power equipment scenario.");
    printf("%s%s%s%s\n", indent, "-x, --aggr-func", "\t\t",
           "Test aggregation functions after insertion.");
    printf("%s%s%s%s\n", indent, "-y, --answer-yes", "\t\t",
           "Input yes for prompt.");
    printf("%s%s%s%s\n", indent, "-O, --disorder=NUMBER", "\t\t",
           "Insert order mode--0: In order, 1 ~ 50: disorder ratio. By default "
           "is in order.");
    printf("%s%s%s%s\n", indent, "-R, --disorder-range=NUMBER", "\t",
           "Out of order data's range. Unit is ms. By default is 1000.");
    printf("%s%s%s%s\n", indent, "-g, --debug", "\t\t\t", "Print debug info.");
    printf("%s%s%s%s\n", indent, "-?, --help\t", "\t\t", "Give this help list");
    printf("%s%s%s%s\n", indent, "    --usage\t", "\t\t",
           "Give a short usage message");
    printf("%s%s\n", indent, "-V, --version\t\t\tPrint program version.");
    /*    printf("%s%s%s%s\n", indent, "-D", indent,
          "Delete database if exists. 0: no, 1: yes, default is 1");
          */
    printf(
        "\nMandatory or optional arguments to long options are also mandatory or optional\n\
for any corresponding short options.\n\
\n\
Report bugs to <support@taosdata.com>.\n");
    exit(EXIT_SUCCESS);
}

void printfInsertMetaToFileStream(FILE *fp) {
    setupForAnsiEscape();
    SHOW_PARSE_RESULT_START_TO_FILE(fp);

    if (g_args.demo_mode && !g_args.pressure_mode) {
        fprintf(
            fp,
            "\ntaosBenchmark is simulating data generated by power equipment "
            "monitoring...\n\n");
    } else if (g_args.pressure_mode) {
        fprintf(fp,
                "\ntaosBenchmark is simulating data in pressure mode ... \n\n");
    } else {
        fprintf(
            fp,
            "\ntaosBenchmark is simulating random data as you request..\n\n");
    }

    fprintf(fp, "interface:                  \033[33m%s\033[0m\n",
            (g_args.iface == TAOSC_IFACE)
                ? "taosc"
                : (g_args.iface == REST_IFACE)
                      ? "rest"
                      : (g_args.iface == STMT_IFACE) ? "stmt" : "sml");

    fprintf(fp, "host:                       \033[33m%s:%u\033[0m\n",
            g_args.host, g_args.port);
    fprintf(fp, "user:                       \033[33m%s\033[0m\n", g_args.user);
    fprintf(fp, "password:                   \033[33m%s\033[0m\n",
            g_args.password);
    fprintf(fp, "configDir:                  \033[33m%s\033[0m\n", configDir);
    fprintf(fp, "resultFile:                 \033[33m%s\033[0m\n",
            g_args.output_file);
    fprintf(fp, "thread num of insert data:  \033[33m%d\033[0m\n",
            g_args.nthreads);
    fprintf(fp, "thread num of create table: \033[33m%d\033[0m\n",
            g_args.nthreads);
    fprintf(fp, "top insert interval:        \033[33m%" PRIu64 "\033[0m\n",
            g_args.insert_interval);
    fprintf(fp, "number of records per req:  \033[33m%u\033[0m\n",
            g_args.reqPerReq);
    fprintf(fp, "random prepare data:        \033[33m%" PRId64 "\033[0m\n",
            g_args.prepared_rand);
    fprintf(fp, "chinese:                    \033[33m%s\033[0m\n",
            g_args.chinese ? "yes" : "no");

    fprintf(fp, "database count:             \033[33m%d\033[0m\n",
            g_args.dbCount);

    for (int i = 0; i < g_args.dbCount; i++) {
        fprintf(fp, "database[\033[33m%d\033[0m]:\n", i);
        fprintf(fp, "  database[%d] name:      \033[33m%s\033[0m\n", i,
                db[i].dbName);
        if (0 == db[i].drop) {
            fprintf(fp, "  drop:                 \033[33m no\033[0m\n");
        } else {
            fprintf(fp, "  drop:                 \033[33m yes\033[0m\n");
        }

        if (db[i].dbCfg.blocks > 0) {
            fprintf(fp, "  blocks:                \033[33m%d\033[0m\n",
                    db[i].dbCfg.blocks);
        }
        if (db[i].dbCfg.cache > 0) {
            fprintf(fp, "  cache:                 \033[33m%d\033[0m\n",
                    db[i].dbCfg.cache);
        }
        if (db[i].dbCfg.days > 0) {
            fprintf(fp, "  days:                  \033[33m%d\033[0m\n",
                    db[i].dbCfg.days);
        }
        if (db[i].dbCfg.keep > 0) {
            fprintf(fp, "  keep:                  \033[33m%d\033[0m\n",
                    db[i].dbCfg.keep);
        }
        if (db[i].dbCfg.replica > 0) {
            fprintf(fp, "  replica:               \033[33m%d\033[0m\n",
                    db[i].dbCfg.replica);
        }
        if (db[i].dbCfg.update > 0) {
            fprintf(fp, "  update:                \033[33m%d\033[0m\n",
                    db[i].dbCfg.update);
        }
        if (db[i].dbCfg.minRows > 0) {
            fprintf(fp, "  minRows:               \033[33m%d\033[0m\n",
                    db[i].dbCfg.minRows);
        }
        if (db[i].dbCfg.maxRows > 0) {
            fprintf(fp, "  maxRows:               \033[33m%d\033[0m\n",
                    db[i].dbCfg.maxRows);
        }
        if (db[i].dbCfg.comp > 0) {
            fprintf(fp, "  comp:                  \033[33m%d\033[0m\n",
                    db[i].dbCfg.comp);
        }
        if (db[i].dbCfg.walLevel > 0) {
            fprintf(fp, "  walLevel:              \033[33m%d\033[0m\n",
                    db[i].dbCfg.walLevel);
        }
        if (db[i].dbCfg.fsync > 0) {
            fprintf(fp, "  fsync:                 \033[33m%d\033[0m\n",
                    db[i].dbCfg.fsync);
        }
        if (db[i].dbCfg.quorum > 0) {
            fprintf(fp, "  quorum:                \033[33m%d\033[0m\n",
                    db[i].dbCfg.quorum);
        }
        if (db[i].dbCfg.precision[0] != 0) {
            if ((0 == strncasecmp(db[i].dbCfg.precision, "ms", 2)) ||
                (0 == strncasecmp(db[i].dbCfg.precision, "us", 2)) ||
                (0 == strncasecmp(db[i].dbCfg.precision, "ns", 2))) {
                fprintf(fp, "  precision:             \033[33m%s\033[0m\n",
                        db[i].dbCfg.precision);
            } else {
                fprintf(
                    fp,
                    "\033[1m\033[40;31m  precision error:       %s\033[0m\n",
                    db[i].dbCfg.precision);
            }
        }

        if (g_args.use_metric) {
            fprintf(fp, "  super table count:     \033[33m%" PRIu64 "\033[0m\n",
                    db[i].superTblCount);
            for (uint64_t j = 0; j < db[i].superTblCount; j++) {
                fprintf(fp, "  super table[\033[33m%" PRIu64 "\033[0m]:\n", j);

                fprintf(fp, "      stbName:           \033[33m%s\033[0m\n",
                        db[i].superTbls[j].stbName);

                if (PRE_CREATE_SUBTBL == db[i].superTbls[j].autoCreateTable) {
                    fprintf(fp, "      autoCreateTable:   \033[33m%s\033[0m\n",
                            "no");
                } else if (AUTO_CREATE_SUBTBL ==
                           db[i].superTbls[j].autoCreateTable) {
                    fprintf(fp, "      autoCreateTable:   \033[33m%s\033[0m\n",
                            "yes");
                } else {
                    fprintf(fp, "      autoCreateTable:   \033[33m%s\033[0m\n",
                            "error");
                }

                if (TBL_NO_EXISTS == db[i].superTbls[j].childTblExists) {
                    fprintf(fp, "      childTblExists:    \033[33m%s\033[0m\n",
                            "no");
                } else if (TBL_ALREADY_EXISTS ==
                           db[i].superTbls[j].childTblExists) {
                    fprintf(fp, "      childTblExists:    \033[33m%s\033[0m\n",
                            "yes");
                } else {
                    fprintf(fp, "      childTblExists:    \033[33m%s\033[0m\n",
                            "error");
                }

                fprintf(fp,
                        "      childTblCount:     \033[33m%" PRId64 "\033[0m\n",
                        db[i].superTbls[j].childTblCount);
                fprintf(fp, "      childTblPrefix:    \033[33m%s\033[0m\n",
                        db[i].superTbls[j].childTblPrefix);
                fprintf(fp, "      dataSource:        \033[33m%s\033[0m\n",
                        db[i].superTbls[j].dataSource);
                fprintf(fp, "      iface:             \033[33m%s\033[0m\n",
                        (db[i].superTbls[j].iface == TAOSC_IFACE)
                            ? "taosc"
                            : (db[i].superTbls[j].iface == REST_IFACE)
                                  ? "rest"
                                  : (db[i].superTbls[j].iface == STMT_IFACE)
                                        ? "stmt"
                                        : "sml");
                if (db[i].superTbls[j].iface == SML_IFACE) {
                    fprintf(fp, "      lineProtocol:      \033[33m%s\033[0m\n",
                            (db[i].superTbls[j].lineProtocol ==
                             TSDB_SML_LINE_PROTOCOL)
                                ? "line"
                                : (db[i].superTbls[j].lineProtocol ==
                                   TSDB_SML_TELNET_PROTOCOL)
                                      ? "telnet"
                                      : "json");
                }
                if (db[i].superTbls[j].childTblOffset > 0) {
                    fprintf(fp,
                            "      childTblOffset:    \033[33m%" PRIu64
                            "\033[0m\n",
                            db[i].superTbls[j].childTblOffset);
                }
                fprintf(fp,
                        "      insertRows:        \033[33m%" PRId64 "\033[0m\n",
                        db[i].superTbls[j].insertRows);
                fprintf(fp, "      interlaceRows:     \033[33m%u\033[0m\n",
                        db[i].superTbls[j].interlaceRows);

                if (db[i].superTbls[j].interlaceRows > 0) {
                    fprintf(fp,
                            "      stable insert interval:   \033[33m%" PRIu64
                            "\033[0m\n",
                            db[i].superTbls[j].insertInterval);
                }

                fprintf(fp, "      disorderRange:     \033[33m%d\033[0m\n",
                        db[i].superTbls[j].disorderRange);
                fprintf(fp, "      disorderRatio:     \033[33m%d\033[0m\n",
                        db[i].superTbls[j].disorderRatio);
                fprintf(fp,
                        "      timeStampStep:     \033[33m%" PRId64 "\033[0m\n",
                        db[i].superTbls[j].timeStampStep);
                fprintf(fp, "      startTimestamp:    \033[33m%s\033[0m\n",
                        db[i].superTbls[j].startTimestamp);
                fprintf(fp, "      sampleFormat:      \033[33m%s\033[0m\n",
                        db[i].superTbls[j].sampleFormat);
                fprintf(fp, "      sampleFile:        \033[33m%s\033[0m\n",
                        db[i].superTbls[j].sampleFile);
                fprintf(fp, "      useSampleTs:       \033[33m%s\033[0m\n",
                        db[i].superTbls[j].useSampleTs
                            ? "yes (warning: disorderRange/disorderRatio is "
                              "disabled)"
                            : "no");
                fprintf(fp, "      tagsFile:          \033[33m%s\033[0m\n",
                        db[i].superTbls[j].tagsFile);
                fprintf(fp,
                        "      columnCount:       \033[33m%d\033[0m\n        ",
                        db[i].superTbls[j].columnCount);
                for (int k = 0; k < db[i].superTbls[j].columnCount; k++) {
                    if ((db[i].superTbls[j].col_type[k] ==
                         TSDB_DATA_TYPE_NCHAR) ||
                        (db[i].superTbls[j].col_type[k] ==
                         TSDB_DATA_TYPE_BINARY)) {
                        fprintf(fp, "column[%d]:\033[33m%s(%d)\033[0m ", k,
                                taos_convert_datatype_to_string(
                                    db[i].superTbls[j].col_type[k]),
                                db[i].superTbls[j].col_length[k]);
                    } else {
                        fprintf(fp, "column[%d]:\033[33m%s\033[0m ", k,
                                taos_convert_datatype_to_string(
                                    db[i].superTbls[j].col_type[k]));
                    }
                }
                fprintf(fp, "\n");

                fprintf(
                    fp,
                    "      tagCount:            \033[33m%d\033[0m\n        ",
                    db[i].superTbls[j].tagCount);
                for (int k = 0; k < db[i].superTbls[j].tagCount; k++) {
                    if ((db[i].superTbls[j].tag_type[k] ==
                         TSDB_DATA_TYPE_BINARY) ||
                        (db[i].superTbls[j].tag_type[k] ==
                         TSDB_DATA_TYPE_NCHAR)) {
                        fprintf(fp, "tag[%d]:\033[33m%s(%d)\033[0m ", k,
                                taos_convert_datatype_to_string(
                                    db[i].superTbls[j].tag_type[k]),
                                db[i].superTbls[j].tag_length[k]);
                    } else if (db[i].superTbls[j].tag_type[k] ==
                               TSDB_DATA_TYPE_JSON) {
                        fprintf(
                            fp,
                            "tag[%d]:\033[33mjson{key(%d):value(%d)}\033[0m ",
                            k, db[i].superTbls[j].tagCount,
                            db[i].superTbls[j].tag_length[k]);
                        break;
                    } else {
                        fprintf(fp, "tag[%d]:\033[33m%s\033[0m ", k,
                                taos_convert_datatype_to_string(
                                    db[i].superTbls[j].tag_type[k]));
                    }
                }
                fprintf(fp, "\n");
            }
        } else {
            fprintf(fp, "  childTblCount:     \033[33m%" PRId64 "\033[0m\n",
                    g_args.ntables);
            fprintf(fp, "  insertRows:        \033[33m%" PRId64 "\033[0m\n",
                    g_args.insertRows);
        }
        fprintf(fp, "\n");
    }

    SHOW_PARSE_RESULT_END_TO_FILE(fp);
}

void printfQueryMeta() {
    setupForAnsiEscape();
    SHOW_PARSE_RESULT_START_TO_FILE(stdout);

    printf("host:                    \033[33m%s:%u\033[0m\n", g_queryInfo.host,
           g_queryInfo.port);
    printf("user:                    \033[33m%s\033[0m\n", g_queryInfo.user);
    printf("database name:           \033[33m%s\033[0m\n", g_queryInfo.dbName);

    printf("\n");

    if ((SUBSCRIBE_TEST == g_args.test_mode) ||
        (QUERY_TEST == g_args.test_mode)) {
        printf("specified table query info:                   \n");
        printf("sqlCount:       \033[33m%d\033[0m\n",
               g_queryInfo.specifiedQueryInfo.sqlCount);
        if (g_queryInfo.specifiedQueryInfo.sqlCount > 0) {
            printf("specified tbl query times:\n");
            printf("                \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.queryTimes);
            printf("query interval: \033[33m%" PRIu64 " ms\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.queryInterval);
            printf("top query times:\033[33m%" PRIu64 "\033[0m\n",
                   g_args.query_times);
            printf("concurrent:     \033[33m%d\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.concurrent);
            printf(
                "mod:            \033[33m%s\033[0m\n",
                (g_queryInfo.specifiedQueryInfo.asyncMode) ? "async" : "sync");
            printf("interval:       \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.subscribeInterval);
            printf("restart:        \033[33m%d\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.subscribeRestart);
            printf("keepProgress:   \033[33m%d\033[0m\n",
                   g_queryInfo.specifiedQueryInfo.subscribeKeepProgress);

            for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
                printf("  sql[%d]: \033[33m%s\033[0m\n", i,
                       g_queryInfo.specifiedQueryInfo.sql[i]);
            }
            printf("\n");
        }

        printf("super table query info:\n");
        printf("sqlCount:       \033[33m%d\033[0m\n",
               g_queryInfo.superQueryInfo.sqlCount);

        if (g_queryInfo.superQueryInfo.sqlCount > 0) {
            printf("query interval: \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.queryInterval);
            printf("threadCnt:      \033[33m%d\033[0m\n",
                   g_queryInfo.superQueryInfo.threadCnt);
            printf("childTblCount:  \033[33m%" PRId64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.childTblCount);
            printf("stable name:    \033[33m%s\033[0m\n",
                   g_queryInfo.superQueryInfo.stbName);
            printf("stb query times:\033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.queryTimes);

            printf("mod:            \033[33m%s\033[0m\n",
                   (g_queryInfo.superQueryInfo.asyncMode) ? "async" : "sync");
            printf("interval:       \033[33m%" PRIu64 "\033[0m\n",
                   g_queryInfo.superQueryInfo.subscribeInterval);
            printf("restart:        \033[33m%d\033[0m\n",
                   g_queryInfo.superQueryInfo.subscribeRestart);
            printf("keepProgress:   \033[33m%d\033[0m\n",
                   g_queryInfo.superQueryInfo.subscribeKeepProgress);

            for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                printf("  sql[%d]: \033[33m%s\033[0m\n", i,
                       g_queryInfo.superQueryInfo.sql[i]);
            }
            printf("\n");
        }
    }

    SHOW_PARSE_RESULT_END_TO_FILE(stdout);
}

void printfDbInfoForQueryToFile(char *filename, SDbInfo *dbInfos, int index) {
    if (filename[0] == 0) return;

    FILE *fp = fopen(filename, "at");
    if (fp == NULL) {
        errorPrint("failed to open file: %s\n", filename);
        return;
    }

    fprintf(fp, "================ database[%d] ================\n", index);
    fprintf(fp, "name: %s\n", dbInfos->name);
    fprintf(fp, "created_time: %s\n", dbInfos->create_time);
    fprintf(fp, "ntables: %" PRId64 "\n", dbInfos->ntables);
    fprintf(fp, "vgroups: %d\n", dbInfos->vgroups);
    fprintf(fp, "replica: %d\n", dbInfos->replica);
    fprintf(fp, "quorum: %d\n", dbInfos->quorum);
    fprintf(fp, "days: %d\n", dbInfos->days);
    fprintf(fp, "keep0,keep1,keep(D): %s\n", dbInfos->keeplist);
    fprintf(fp, "cache(MB): %d\n", dbInfos->cache);
    fprintf(fp, "blocks: %d\n", dbInfos->blocks);
    fprintf(fp, "minrows: %d\n", dbInfos->minrows);
    fprintf(fp, "maxrows: %d\n", dbInfos->maxrows);
    fprintf(fp, "wallevel: %d\n", dbInfos->wallevel);
    fprintf(fp, "fsync: %d\n", dbInfos->fsync);
    fprintf(fp, "comp: %d\n", dbInfos->comp);
    fprintf(fp, "cachelast: %d\n", dbInfos->cachelast);
    fprintf(fp, "precision: %s\n", dbInfos->precision);
    fprintf(fp, "update: %d\n", dbInfos->update);
    fprintf(fp, "status: %s\n", dbInfos->status);
    fprintf(fp, "\n");

    fclose(fp);
}

void printfQuerySystemInfo(TAOS *taos) {
    char      filename[MAX_FILE_NAME_LEN] = {0};
    char      buffer[SQL_BUFF_LEN] = {0};
    TAOS_RES *res;

    time_t     t;
    struct tm *lt;
    time(&t);
    lt = localtime(&t);
    snprintf(filename, MAX_FILE_NAME_LEN, "querySystemInfo-%d-%d-%d %d:%d:%d",
             lt->tm_year + 1900, lt->tm_mon, lt->tm_mday, lt->tm_hour,
             lt->tm_min, lt->tm_sec);

    // show variables
    res = taos_query(taos, "show variables;");
    // fetchResult(res, filename);
    xDumpResultToFile(filename, res);

    // show dnodes
    res = taos_query(taos, "show dnodes;");
    xDumpResultToFile(filename, res);
    // fetchResult(res, filename);

    // show databases
    res = taos_query(taos, "show databases;");
    SDbInfo **dbInfos =
        (SDbInfo **)calloc(MAX_DATABASE_COUNT, sizeof(SDbInfo *));
    if (dbInfos == NULL) {
        errorPrint("%s", "failed to allocate memory\n");
        return;
    }
    int dbCount = getDbFromServer(taos, dbInfos);
    if (dbCount <= 0) {
        tmfree(dbInfos);
        return;
    }

    for (int i = 0; i < dbCount; i++) {
        // printf database info
        printfDbInfoForQueryToFile(filename, dbInfos[i], i);

        // show db.vgroups
        snprintf(buffer, SQL_BUFF_LEN, "show %s.vgroups;", dbInfos[i]->name);
        res = taos_query(taos, buffer);
        xDumpResultToFile(filename, res);

        // show db.stables
        snprintf(buffer, SQL_BUFF_LEN, "show %s.stables;", dbInfos[i]->name);
        res = taos_query(taos, buffer);
        xDumpResultToFile(filename, res);
        free(dbInfos[i]);
    }

    free(dbInfos);
    resetAfterAnsiEscape();
}

void printStatPerThread(threadInfo *pThreadInfo) {
    if (0 == pThreadInfo->totalDelay) pThreadInfo->totalDelay = 1;

    infoPrint("thread[%d] completed total inserted rows: %" PRIu64
              ", total affected rows: %" PRIu64 ". %.2f records/second\n",
              pThreadInfo->threadID, pThreadInfo->totalInsertRows,
              pThreadInfo->totalAffectedRows,
              (double)(pThreadInfo->totalAffectedRows /
                       ((double)pThreadInfo->totalDelay / 1000000.0)));
}

void appendResultBufToFile(char *resultBuf, threadInfo *pThreadInfo) {
    pThreadInfo->fp = fopen(pThreadInfo->filePath, "at");
    if (pThreadInfo->fp == NULL) {
        errorPrint(
            "%s() LN%d, failed to open result file: %s, result will not save "
            "to file\n",
            __func__, __LINE__, pThreadInfo->filePath);
        return;
    }

    fprintf(pThreadInfo->fp, "%s", resultBuf);
    tmfclose(pThreadInfo->fp);
    pThreadInfo->fp = NULL;
}