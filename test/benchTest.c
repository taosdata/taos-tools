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
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "CUnit/Basic.h"
#include "benchData.h"

SArguments test_g_args;
SDataBase* db;

int init_suite1(void) {
    db = calloc(1, sizeof(SDataBase));
    db->superTbls = calloc(1, sizeof(SSuperTable));
    return 0;
}

int clean_suite1(void) { return 0; }

void testINITARGUMENT(void) {
    init_argument(&test_g_args);
    CU_ASSERT_EQUAL(test_g_args.test_mode, INSERT_TEST);
    CU_ASSERT_TRUE(test_g_args.demo_mode);
    CU_ASSERT_EQUAL(test_g_args.dbCount, 1);
    CU_ASSERT_STRING_EQUAL(test_g_args.host, DEFAULT_HOST);
    CU_ASSERT_EQUAL(test_g_args.port, DEFAULT_PORT);
    CU_ASSERT_EQUAL(test_g_args.iface, TAOSC_IFACE);
    CU_ASSERT_STRING_EQUAL(test_g_args.output_file, DEFAULT_OUTPUT);
    CU_ASSERT_STRING_EQUAL(test_g_args.user, TSDB_DEFAULT_USER);
    CU_ASSERT_STRING_EQUAL(test_g_args.password, TSDB_DEFAULT_PASS);
    CU_ASSERT_EQUAL(test_g_args.replica, 1);
    CU_ASSERT_STRING_EQUAL(test_g_args.database, DEFAULT_DATABASE);
    CU_ASSERT_STRING_EQUAL(test_g_args.tb_prefix, DEFAULT_TB_PREFIX);
    CU_ASSERT_FALSE(test_g_args.escapeChar);
    CU_ASSERT_TRUE(test_g_args.use_metric);
    CU_ASSERT_FALSE(test_g_args.aggr_func);
    CU_ASSERT_FALSE(test_g_args.answer_yes);
    CU_ASSERT_FALSE(test_g_args.debug_print);
    CU_ASSERT_FALSE(test_g_args.performance_print);

    CU_ASSERT_EQUAL(test_g_args.columnCount, 3);
    CU_ASSERT_EQUAL(test_g_args.col_length[0], sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_length[1], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[2], sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_type[0], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.col_type[1], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[2], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 2);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], 16);
    CU_ASSERT_EQUAL(test_g_args.binwidth, DEFAULT_BINWIDTH);
    CU_ASSERT_EQUAL(test_g_args.nthreads, DEFAULT_NTHREADS);
    CU_ASSERT_EQUAL(test_g_args.nthreads_pool, DEFAULT_NTHREADS + 5);
    CU_ASSERT_EQUAL(test_g_args.insert_interval, 0);
    CU_ASSERT_EQUAL(test_g_args.timestamp_step, 1);
    CU_ASSERT_EQUAL(test_g_args.prepared_rand, DEFAULT_PREPARED_RAND);
    CU_ASSERT_EQUAL(test_g_args.interlaceRows, 0);
    CU_ASSERT_EQUAL(test_g_args.reqPerReq, DEFAULT_REQ_PER_REQ);
    CU_ASSERT_EQUAL(test_g_args.ntables, DEFAULT_CHILDTABLES);
    CU_ASSERT_EQUAL(test_g_args.insertRows, DEFAULT_INSERT_ROWS);
    CU_ASSERT_EQUAL(test_g_args.disorderRatio, 0);
    CU_ASSERT_EQUAL(test_g_args.disorderRange, DEFAULT_DISORDER_RANGE);
    CU_ASSERT_FALSE(test_g_args.chinese);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    tmfree(test_g_args.pool);
}
void testPARSECOLS(void) {
    init_argument(&test_g_args);
    char* all_type_cols[] = {
        "taosBenchmark", "-b",
        "bool,tinyint,utinyint,smallint,usmallint,int,uint,bigint,ubigint,"
        "timestamp,float,double,binary,nchar"};
    commandLineParseArgument(3, all_type_cols, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.columnCount, 14);
    CU_ASSERT_EQUAL(test_g_args.col_length[0], (int32_t)sizeof(int8_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[1], (int32_t)sizeof(int8_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[2], (int32_t)sizeof(uint8_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[3], (int32_t)sizeof(int16_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[4], (int32_t)sizeof(uint16_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[5], (int32_t)sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[6], (int32_t)sizeof(uint32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[7], (int32_t)sizeof(int64_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[8], (int32_t)sizeof(u_int64_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[9], (int32_t)sizeof(int64_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[10], (int32_t)sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_length[11], (int32_t)sizeof(double));
    CU_ASSERT_EQUAL(test_g_args.col_length[12], (int32_t)test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.col_length[13], (int32_t)test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.col_type[0], TSDB_DATA_TYPE_BOOL);
    CU_ASSERT_EQUAL(test_g_args.col_type[1], TSDB_DATA_TYPE_TINYINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[2], TSDB_DATA_TYPE_UTINYINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[3], TSDB_DATA_TYPE_SMALLINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[4], TSDB_DATA_TYPE_USMALLINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[5], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[6], TSDB_DATA_TYPE_UINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[7], TSDB_DATA_TYPE_BIGINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[8], TSDB_DATA_TYPE_UBIGINT);
    CU_ASSERT_EQUAL(test_g_args.col_type[9], TSDB_DATA_TYPE_TIMESTAMP);
    CU_ASSERT_EQUAL(test_g_args.col_type[10], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.col_type[11], TSDB_DATA_TYPE_DOUBLE);
    CU_ASSERT_EQUAL(test_g_args.col_type[12], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.col_type[13], TSDB_DATA_TYPE_NCHAR);
    char* nchar_binary_length_type_cols[] = {"taosBenchmark", "-b",
                                             "nchar(13),binary(21)"};
    commandLineParseArgument(3, nchar_binary_length_type_cols, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.columnCount, 2);
    CU_ASSERT_EQUAL(test_g_args.col_length[0], 13);
    CU_ASSERT_EQUAL(test_g_args.col_length[1], 21);
    CU_ASSERT_EQUAL(test_g_args.col_type[0], TSDB_DATA_TYPE_NCHAR);
    CU_ASSERT_EQUAL(test_g_args.col_type[1], TSDB_DATA_TYPE_BINARY);
    char* invalid_type[] = {"taosBenchmark", "-b", "json"};
    commandLineParseArgument(3, invalid_type, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.columnCount, 3);
    CU_ASSERT_EQUAL(test_g_args.col_length[0], sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_length[1], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[2], sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_type[0], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.col_type[1], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[2], TSDB_DATA_TYPE_FLOAT);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    tmfree(test_g_args.pool);
}
void testPARSETAGS(void) {
    init_argument(&test_g_args);
    char* all_type_tags[] = {
        "taosBenchmark", "-A",
        "bool,tinyint,utinyint,smallint,usmallint,int,uint,bigint,ubigint,"
        "timestamp,float,double,binary,nchar"};
    commandLineParseArgument(3, all_type_tags, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 14);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], (int32_t)sizeof(int8_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], (int32_t)sizeof(int8_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[2], (int32_t)sizeof(uint8_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[3], (int32_t)sizeof(int16_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[4], (int32_t)sizeof(uint16_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[5], (int32_t)sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[6], (int32_t)sizeof(uint32_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[7], (int32_t)sizeof(int64_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[8], (int32_t)sizeof(u_int64_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[9], (int32_t)sizeof(int64_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[10], (int32_t)sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.tag_length[11], (int32_t)sizeof(double));
    CU_ASSERT_EQUAL(test_g_args.tag_length[12], (int32_t)test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.tag_length[13], (int32_t)test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_BOOL);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_TINYINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[2], TSDB_DATA_TYPE_UTINYINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[3], TSDB_DATA_TYPE_SMALLINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[4], TSDB_DATA_TYPE_USMALLINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[5], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[6], TSDB_DATA_TYPE_UINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[7], TSDB_DATA_TYPE_BIGINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[8], TSDB_DATA_TYPE_UBIGINT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[9], TSDB_DATA_TYPE_TIMESTAMP);
    CU_ASSERT_EQUAL(test_g_args.tag_type[10], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[11], TSDB_DATA_TYPE_DOUBLE);
    CU_ASSERT_EQUAL(test_g_args.tag_type[12], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.tag_type[13], TSDB_DATA_TYPE_NCHAR);
    char* nchar_binary_length_type_tags[] = {"taosBenchmark", "-A",
                                             "nchar(13),binary(21)"};
    commandLineParseArgument(3, nchar_binary_length_type_tags, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 2);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], 13);
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], 21);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_NCHAR);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_BINARY);
    char* json_type_tags[] = {"taosBenchmark", "-A", "json"};
    commandLineParseArgument(3, json_type_tags, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 1);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_JSON);
    char* single_type[] = {
        "taosBenchmark", "-A", "bool",      "-A", "tinyint",   "-A",
        "utinyint",      "-A", "smallint",  "-A", "usmallint", "-A",
        "int",           "-A", "uint",      "-A", "bigint",    "-A",
        "ubigint",       "-A", "timestamp", "-A", "float",     "-A",
        "double",        "-A", "binary",    "-A", "binary(8)", "-A",
        "nchar",         "-A", "nchar(9)",  "-A", "json(8)"};
    commandLineParseArgument(35, single_type, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 1);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], 8);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_JSON);
    char* invalid_type[] = {"taosBenchmark", "-A", "int,x"};
    commandLineParseArgument(3, invalid_type, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 2);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], 16);
    char* invalid2_type[] = {"taosBenchmark", "-A", "int,json"};
    commandLineParseArgument(3, invalid2_type, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 2);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], 16);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    tmfree(test_g_args.pool);
}
void testCOMMANDLINEPARSE(void) {
    char* command[] = {"taosBenchmark",
                       "-f",
                       "new_json",
                       "-h",
                       "new_host",
                       "-P",
                       "123",
                       "-p",
                       "new_password",
                       "-u",
                       "new_user",
                       "-o",
                       "./new_file.txt",
                       "-T",
                       "13",
                       "-i",
                       "23",
                       "-S",
                       "17",
                       "-B",
                       "7",
                       "-r",
                       "31",
                       "-t",
                       "29",
                       "-n",
                       "37",
                       "-d",
                       "new_database",
                       "-l",
                       "41",
                       "-w",
                       "43",
                       "-m",
                       "new_perfix",
                       "-E",
                       "-C",
                       "-N",
                       "-M",
                       "-x",
                       "-y",
                       "-R",
                       "47",
                       "-O",
                       "53",
                       "-a",
                       "2",
                       "-F",
                       "61",
                       "-g",
                       "-G",
                       "-c",
                       "/var/taos"};
    commandLineParseArgument(53, command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.metaFile, "new_json");
    CU_ASSERT_STRING_EQUAL(test_g_args.host, "new_host");
    CU_ASSERT_EQUAL(test_g_args.port, 123);
    CU_ASSERT_STRING_EQUAL(test_g_args.password, "new_password");
    CU_ASSERT_STRING_EQUAL(test_g_args.user, "new_user");
    CU_ASSERT_STRING_EQUAL(test_g_args.output_file, "./new_file.txt");
    CU_ASSERT_EQUAL(test_g_args.nthreads, 13);
    CU_ASSERT_EQUAL(test_g_args.insert_interval, 23);
    CU_ASSERT_EQUAL(test_g_args.timestamp_step, 17);
    CU_ASSERT_EQUAL(test_g_args.interlaceRows, 7);
    CU_ASSERT_EQUAL(test_g_args.reqPerReq, 31);
    CU_ASSERT_EQUAL(test_g_args.ntables, 29);
    CU_ASSERT_EQUAL(test_g_args.insertRows, 37);
    CU_ASSERT_STRING_EQUAL(test_g_args.database, "new_database");
    CU_ASSERT_EQUAL(test_g_args.intColumnCount, 41);
    CU_ASSERT_EQUAL(test_g_args.binwidth, 43);
    CU_ASSERT_STRING_EQUAL(test_g_args.tb_prefix, "new_perfix");
    CU_ASSERT_TRUE(test_g_args.escapeChar);
    CU_ASSERT_TRUE(test_g_args.chinese);
    CU_ASSERT_FALSE(test_g_args.use_metric);
    CU_ASSERT_FALSE(test_g_args.demo_mode);
    CU_ASSERT_TRUE(test_g_args.aggr_func);
    CU_ASSERT_TRUE(test_g_args.answer_yes);
    CU_ASSERT_EQUAL(test_g_args.disorderRange, 47);
    CU_ASSERT_EQUAL(test_g_args.disorderRatio, 53);
    CU_ASSERT_EQUAL(test_g_args.replica, 2);
    CU_ASSERT_EQUAL(test_g_args.prepared_rand, 61);
    CU_ASSERT_TRUE(test_g_args.debug_print);
    CU_ASSERT_TRUE(test_g_args.performance_print);
    CU_ASSERT_STRING_EQUAL(configDir, "/var/taos");
    char* rest_command[] = {"taosBenchmark", "-I", "rest"};
    commandLineParseArgument(3, rest_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.iface, REST_IFACE);
    char* stmt_command[] = {"taosBenchmark", "-I", "stmt"};
    commandLineParseArgument(3, stmt_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.iface, STMT_IFACE);
    char* sml_command[] = {"taosBenchmark", "-I", "sml"};
    commandLineParseArgument(3, sml_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.iface, SML_IFACE);
    char* taosc_command[] = {"taosBenchmark", "-I", "taosc"};
    commandLineParseArgument(3, taosc_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.iface, TAOSC_IFACE);
}
void testINVALIDCOMMAND(void) {
    char* Invalidcommand[] = {
        "taosBenchmark", "-F", "-10",     "-P", "abc", "-I",
        "-10",           "-T", "-10",     "-i", "-10", "-S",
        "-10",           "-B", "-10",     "-r", "-10", "-t",
        "-10",           "-n", "-10",     "-l", "-10", "-w",
        "-10",           "-w", "9999999", "-R", "-10", "-O",
        "-10",           "-a", "-10"};
    commandLineParseArgument(33, Invalidcommand, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.prepared_rand, DEFAULT_PREPARED_RAND);
    CU_ASSERT_EQUAL(test_g_args.port, DEFAULT_PORT);
    CU_ASSERT_EQUAL(test_g_args.iface, TAOSC_IFACE);
    CU_ASSERT_EQUAL(test_g_args.nthreads, DEFAULT_NTHREADS);
    CU_ASSERT_EQUAL(test_g_args.insert_interval, 0);
    CU_ASSERT_EQUAL(test_g_args.timestamp_step, 1);
    CU_ASSERT_EQUAL(test_g_args.interlaceRows, 0);
    CU_ASSERT_EQUAL(test_g_args.ntables, DEFAULT_CHILDTABLES);
    CU_ASSERT_EQUAL(test_g_args.intColumnCount, 0);
    CU_ASSERT_EQUAL(test_g_args.binwidth, DEFAULT_BINWIDTH);
    CU_ASSERT_EQUAL(test_g_args.disorderRange, DEFAULT_DISORDER_RANGE);
    CU_ASSERT_EQUAL(test_g_args.disorderRatio, 0);
    CU_ASSERT_EQUAL(test_g_args.replica, 1);
}
void testSETPARAFROMARG(void) {
    init_argument(&test_g_args);
    char* command0[] = {"taosBenchmark", "-b", "nchar,int,float", "-x"};
    commandLineParseArgument(4, command0, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.aggr_func);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_FALSE(test_g_args.aggr_func);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    tmfree(test_g_args.pool);
    init_argument(&test_g_args);
    char* command1[] = {"taosBenchmark", "-b", "bool,int,float", "-x"};
    commandLineParseArgument(4, command1, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.aggr_func);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_FALSE(test_g_args.aggr_func);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    tmfree(test_g_args.pool);
    init_argument(&test_g_args);
    char* command[] = {"taosBenchmark",    "-l", "10", "-b",
                       "binary,int,float", "-x"};
    commandLineParseArgument(6, command, &test_g_args);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_TRUE(db->drop);
    CU_ASSERT_STRING_EQUAL(db->dbName, test_g_args.database);
    CU_ASSERT_EQUAL(db->dbCfg.replica, test_g_args.replica);
    CU_ASSERT_STRING_EQUAL(db->dbCfg.precision, "ms");
    CU_ASSERT_EQUAL(test_g_args.columnCount, 10);
    CU_ASSERT_EQUAL(test_g_args.col_type[0], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.col_type[1], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[2], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.col_type[3], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[4], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[5], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[6], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[7], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[8], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[9], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_length[0], test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.col_length[1], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[2], sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_length[3], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[4], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[5], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[6], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[7], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[8], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[9], sizeof(int32_t));
    CU_ASSERT_EQUAL(db->superTblCount, 1);
    CU_ASSERT_EQUAL(db->superTbls[0].childTblCount, test_g_args.ntables);
    CU_ASSERT_EQUAL(db->superTbls[0].childTblLimit, test_g_args.ntables);
    CU_ASSERT_EQUAL(db->superTbls[0].childTblOffset, 0);
    CU_ASSERT_EQUAL(db->superTbls[0].escapeChar, test_g_args.escapeChar);
    CU_ASSERT_EQUAL(db->superTbls[0].autoCreateTable, PRE_CREATE_SUBTBL);
    CU_ASSERT_EQUAL(db->superTbls[0].childTblExists, TBL_NO_EXISTS);
    CU_ASSERT_EQUAL(db->superTbls[0].disorderRatio, test_g_args.disorderRatio);
    CU_ASSERT_EQUAL(db->superTbls[0].disorderRange, test_g_args.disorderRange);
    CU_ASSERT_STRING_EQUAL(db->superTbls[0].childTblPrefix,
                           test_g_args.tb_prefix);
    CU_ASSERT_STRING_EQUAL(db->superTbls[0].dataSource, "rand");
    CU_ASSERT_EQUAL(db->superTbls[0].iface, test_g_args.iface);
    CU_ASSERT_EQUAL(db->superTbls[0].lineProtocol, TSDB_SML_LINE_PROTOCOL);
    CU_ASSERT_EQUAL(db->superTbls[0].tsPrecision,
                    TSDB_SML_TIMESTAMP_MILLI_SECONDS);
    CU_ASSERT_STRING_EQUAL(db->superTbls[0].startTimestamp,
                           "2017-07-14 10:40:00.000");
    CU_ASSERT_EQUAL(db->superTbls[0].timeStampStep, test_g_args.timestamp_step);
    CU_ASSERT_EQUAL(db->superTbls[0].insertRows, test_g_args.insertRows);
    CU_ASSERT_EQUAL(db->superTbls[0].interlaceRows, test_g_args.interlaceRows);
    CU_ASSERT_EQUAL(db->superTbls[0].columnCount, test_g_args.columnCount);
    CU_ASSERT_PTR_EQUAL(db->superTbls[0].col_type, test_g_args.col_type);
    CU_ASSERT_PTR_EQUAL(db->superTbls[0].col_length, test_g_args.col_length);
    CU_ASSERT_EQUAL(db->superTbls[0].tagCount, test_g_args.tagCount);
    CU_ASSERT_PTR_EQUAL(db->superTbls[0].tag_type, test_g_args.tag_type);
    CU_ASSERT_PTR_EQUAL(db->superTbls[0].tag_length, test_g_args.tag_length);
    CU_ASSERT_STRING_EQUAL(db->superTbls[0].stbName, "meters");
    CU_ASSERT_FALSE(test_g_args.aggr_func);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    tmfree(test_g_args.pool);
}
void testQUERYAGGRFUNC(void) {
    tmfree(db->superTbls);
    tmfree(db);
    db = calloc(1, sizeof(SDataBase));
    db->superTbls = calloc(1, sizeof(SSuperTable));
    test_g_args.metaFile = NULL;
    init_argument(&test_g_args);
    char* comand1[] = {
        "taosBenchmark", "-n", "1", "-t", "1", "-T", "1", "-x", "-y"};
    commandLineParseArgument(9, comand1, &test_g_args);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_EQUAL(test(&test_g_args, db), 0);
    postFreeResource(&test_g_args, db);
    db = calloc(1, sizeof(SDataBase));
    db->superTbls = calloc(1, sizeof(SSuperTable));
    test_g_args.metaFile = NULL;
    init_argument(&test_g_args);
    char* comand11[] = {
        "taosBenchmark", "-n", "1", "-t", "1", "-T", "1", "-x", "-y", "-M"};
    commandLineParseArgument(10, comand11, &test_g_args);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_EQUAL(test(&test_g_args, db), 0);
    postFreeResource(&test_g_args, db);
    db = calloc(1, sizeof(SDataBase));
    db->superTbls = calloc(1, sizeof(SSuperTable));
    test_g_args.metaFile = NULL;
    init_argument(&test_g_args);
    char* comand2[] = {"taosBenchmark",
                       "-n",
                       "1",
                       "-t",
                       "2",
                       "-T",
                       "1",
                       "-x",
                       "-y",
                       "-N",
                       "-M"};
    commandLineParseArgument(11, comand2, &test_g_args);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_EQUAL(test(&test_g_args, db), 0);
    postFreeResource(&test_g_args, db);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
    db = calloc(1, sizeof(SDataBase));
    db->superTbls = calloc(1, sizeof(SSuperTable));
    test_g_args.metaFile = NULL;
    init_argument(&test_g_args);
    char* comand22[] = {
        "taosBenchmark", "-n", "1", "-t", "2", "-T", "1", "-x", "-y", "-N"};
    commandLineParseArgument(10, comand22, &test_g_args);
    setParaFromArg(&test_g_args, db);
    CU_ASSERT_EQUAL(test(&test_g_args, db), 0);
    postFreeResource(&test_g_args, db);
    tmfree(test_g_args.col_type);
    tmfree(test_g_args.col_length);
    tmfree(test_g_args.tag_length);
    tmfree(test_g_args.tag_type);
}
/* The main() function for setting up and running the tests.
 * Returns a CUE_SUCCESS on successful running, another
 * CUnit error code on failure.
 */
int main() {
    CU_pSuite benchCommandSuite = NULL;

    /* initialize the CUnit test registry */
    if (CUE_SUCCESS != CU_initialize_registry()) return CU_get_error();

    /* add a suite to the registry */
    benchCommandSuite =
        CU_add_suite("benchCommandOpt.c", init_suite1, clean_suite1);
    if (NULL == benchCommandSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* add the tests to the suite */
    if ((NULL ==
         CU_add_test(benchCommandSuite, "init_argument()", testINITARGUMENT)) ||
        (NULL == CU_add_test(benchCommandSuite, "setParaFromArg()",
                             testSETPARAFROMARG)) ||
        (NULL == CU_add_test(benchCommandSuite, "commandLineParseArgument(-A)",
                             testPARSETAGS)) ||
        (NULL == CU_add_test(benchCommandSuite, "commandLineParseArgument(-b)",
                             testPARSECOLS)) ||
        (NULL == CU_add_test(benchCommandSuite, "commandLineParseArgument()",
                             testCOMMANDLINEPARSE)) ||
        (NULL == CU_add_test(benchCommandSuite,
                             "commandLineParseArgument(Invalid)",
                             testINVALIDCOMMAND)) ||
        (NULL == CU_add_test(benchCommandSuite, "queryAggrFunc()",
                             testQUERYAGGRFUNC))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
