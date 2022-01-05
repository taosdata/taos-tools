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

int init_suite1(void) { return 0; }

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
    CU_ASSERT_EQUAL(test_g_args.tagCount, 2);
    CU_ASSERT_EQUAL(test_g_args.columnCount, 3);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(test_g_args.col_type[0], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.col_type[1], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(test_g_args.col_type[2], TSDB_DATA_TYPE_FLOAT);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], 16);
    CU_ASSERT_EQUAL(test_g_args.col_length[0], sizeof(float));
    CU_ASSERT_EQUAL(test_g_args.col_length[1], sizeof(int32_t));
    CU_ASSERT_EQUAL(test_g_args.col_length[2], sizeof(float));
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
}
void testPARSEDATATYPE(void) {
    int32_t* data_length;
    int32_t  number = 0;
    char*    data_type;
    char*    dataType = "json";
    CU_ASSERT_EQUAL(count_datatype(dataType, &number), 0);
    CU_ASSERT_EQUAL(number, 1);
    data_length = calloc(number, sizeof(int32_t));
    data_type = calloc(number, sizeof(char));
    CU_ASSERT_EQUAL(parse_datatype(dataType, data_type, data_length, true), 0);
    CU_ASSERT_EQUAL(data_type[0], TSDB_DATA_TYPE_JSON);
    CU_ASSERT_EQUAL(data_length[0], DEFAULT_BINWIDTH);
    tmfree(data_length);
    tmfree(data_type);
    data_length = calloc(number, sizeof(int32_t));
    data_type = calloc(number, sizeof(char));
    CU_ASSERT_EQUAL(parse_datatype(dataType, data_type, data_length, false),
                    -1);
    tmfree(data_length);
    tmfree(data_type);
    dataType = "int,nchar(16),BINARY";
    CU_ASSERT_EQUAL(count_datatype(dataType, &number), 0);
    CU_ASSERT_EQUAL(number, 3);
    data_length = calloc(number, sizeof(int32_t));
    data_type = calloc(number, sizeof(char));
    CU_ASSERT_EQUAL(parse_datatype(dataType, data_type, data_length, true), 0);
    CU_ASSERT_EQUAL(data_type[0], TSDB_DATA_TYPE_INT);
    CU_ASSERT_EQUAL(data_length[0], sizeof(int32_t));
    CU_ASSERT_EQUAL(data_type[1], TSDB_DATA_TYPE_NCHAR);
    CU_ASSERT_EQUAL(data_length[1], 16);
    CU_ASSERT_EQUAL(data_type[2], TSDB_DATA_TYPE_BINARY);
    CU_ASSERT_EQUAL(data_length[2], DEFAULT_BINWIDTH);
    tmfree(data_length);
    tmfree(data_type);
    dataType = "int,nchar(16),BINARY,json";
    CU_ASSERT_EQUAL(count_datatype(dataType, &number), 0);
    CU_ASSERT_EQUAL(number, 4);
    data_length = calloc(number, sizeof(int32_t));
    data_type = calloc(number, sizeof(char));
    CU_ASSERT_EQUAL(parse_datatype(dataType, data_type, data_length, true), -1);
    tmfree(data_length);
    tmfree(data_type);
}
void testPARSECOLS(void) {
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
}
void testPARSETAGS(void) {
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
}
void testCOMMANDLINEPARSE(void) {
    char* file_command[] = {"taosBenchmark", "-f", "test.json"};
    commandLineParseArgument(3, file_command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.metaFile, "test.json");
    CU_ASSERT_STRING_EQUAL(test_g_args.host, "localhost");
    char* host_command[] = {"taosBenchmark", "-h", "new_host"};
    commandLineParseArgument(3, host_command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.host, "new_host");
    CU_ASSERT_EQUAL(test_g_args.port, 6030);
    char* port_command[] = {"taosBenchmark", "-P", "123"};
    commandLineParseArgument(3, port_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.port, 123);
    CU_ASSERT_EQUAL(test_g_args.iface, TAOSC_IFACE);
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
    CU_ASSERT_STRING_EQUAL(test_g_args.user, "root");
    char* user_command[] = {"taosBenchmark", "-u", "new_user"};
    commandLineParseArgument(3, user_command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.user, "new_user");
    CU_ASSERT_STRING_EQUAL(test_g_args.password, "taosdata");
    CU_ASSERT_STRING_EQUAL(test_g_args.output_file, "./output.txt");
    char* output_command[] = {"taosBenchmark", "-o", "./new_file.txt"};
    commandLineParseArgument(3, output_command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.output_file, "./new_file.txt");
    CU_ASSERT_EQUAL(test_g_args.nthreads, 8);
    char* thread_command[] = {"taosBenchmark", "-T", "13"};
    commandLineParseArgument(3, thread_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.nthreads, 13);
    CU_ASSERT_EQUAL(test_g_args.insert_interval, 0);
    char* interval_command[] = {"taosBenchmark", "-i", "23"};
    commandLineParseArgument(3, interval_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.insert_interval, 23);
    CU_ASSERT_EQUAL(test_g_args.timestamp_step, 1);
    char* timestep_command[] = {"taosBenchmark", "-S", "17"};
    commandLineParseArgument(3, timestep_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.timestamp_step, 17);
    CU_ASSERT_EQUAL(test_g_args.interlaceRows, 0);
    char* interlace_command[] = {"taosBenchmark", "-B", "7"};
    commandLineParseArgument(3, interlace_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.interlaceRows, 7);
    CU_ASSERT_EQUAL(test_g_args.reqPerReq, 30000);
    char* reqperreq_command[] = {"taosBenchmark", "-r", "31"};
    commandLineParseArgument(3, reqperreq_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.reqPerReq, 31);
    CU_ASSERT_EQUAL(test_g_args.ntables, 10000);
    char* tables_command[] = {"taosBenchmark", "-t", "29"};
    commandLineParseArgument(3, tables_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.ntables, 29);
    CU_ASSERT_EQUAL(test_g_args.insertRows, 10000);
    char* rows_command[] = {"taosBenchmark", "-n", "37"};
    commandLineParseArgument(3, rows_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.insertRows, 37);
    CU_ASSERT_STRING_EQUAL(test_g_args.database, "test");
    char* database_command[] = {"taosBenchmark", "-d", "new_database"};
    commandLineParseArgument(3, database_command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.database, "new_database");
    CU_ASSERT_EQUAL(test_g_args.intColumnCount, 0);
    char* intcolumn_command[] = {"taosBenchmark", "-l", "41"};
    commandLineParseArgument(3, intcolumn_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.intColumnCount, 41);
    CU_ASSERT_EQUAL(test_g_args.binwidth, 64);
    char* binwidth_command[] = {"taosBenchmark", "-w", "43"};
    commandLineParseArgument(3, binwidth_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.binwidth, 43);
    CU_ASSERT_STRING_EQUAL(test_g_args.tb_prefix, "d");
    char* prefix_command[] = {"taosBenchmark", "-m", "new_perfix"};
    commandLineParseArgument(3, prefix_command, &test_g_args);
    CU_ASSERT_STRING_EQUAL(test_g_args.tb_prefix, "new_perfix");
    CU_ASSERT_FALSE(test_g_args.escapeChar);
    char* escape_command[] = {"taosBenchmark", "-E"};
    commandLineParseArgument(2, escape_command, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.escapeChar);
    CU_ASSERT_FALSE(test_g_args.chinese);
    char* chinese_command[] = {"taosBenchmark", "-C"};
    commandLineParseArgument(2, chinese_command, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.chinese);
    CU_ASSERT_TRUE(test_g_args.use_metric);
    char* normal_command[] = {"taosBenchmark", "-N"};
    commandLineParseArgument(2, normal_command, &test_g_args);
    CU_ASSERT_FALSE(test_g_args.use_metric);
    char* random_command[] = {"taosBenchmark", "-M"};
    commandLineParseArgument(2, random_command, &test_g_args);
    CU_ASSERT_FALSE(test_g_args.demo_mode);
    CU_ASSERT_FALSE(test_g_args.aggr_func);
    char* aggr_command[] = {"taosBenchmark", "-x"};
    commandLineParseArgument(2, aggr_command, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.aggr_func);
    CU_ASSERT_FALSE(test_g_args.answer_yes);
    char* answer_command[] = {"taosBenchmark", "-y"};
    commandLineParseArgument(2, answer_command, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.answer_yes);
    CU_ASSERT_EQUAL(test_g_args.disorderRange, 1000);
    char* range_command[] = {"taosBenchmark", "-R", "47"};
    commandLineParseArgument(3, range_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.disorderRange, 47);
    CU_ASSERT_EQUAL(test_g_args.disorderRatio, 0);
    char* ratio_command[] = {"taosBenchmark", "-O", "53"};
    commandLineParseArgument(3, ratio_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.disorderRatio, 53);
    CU_ASSERT_EQUAL(test_g_args.replica, 1);
    char* replica_command[] = {"taosBenchmark", "-a", "2"};
    commandLineParseArgument(3, replica_command, &test_g_args);
    CU_ASSERT_EQUAL(test_g_args.replica, 2);
    CU_ASSERT_FALSE(test_g_args.debug_print);
    char* debug_command[] = {"taosBenchmark", "-g"};
    commandLineParseArgument(2, debug_command, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.debug_print);
    CU_ASSERT_FALSE(test_g_args.performance_print);
    char* performace_command[] = {"taosBenchmark", "-G"};
    commandLineParseArgument(2, performace_command, &test_g_args);
    CU_ASSERT_TRUE(test_g_args.performance_print);
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
        (NULL == CU_add_test(benchCommandSuite, "parse_datatype()",
                             testPARSEDATATYPE)) ||
        (NULL == CU_add_test(benchCommandSuite, "commandLineParseArgument(-A)",
                             testPARSETAGS)) ||
        (NULL == CU_add_test(benchCommandSuite, "commandLineParseArgument(-b)",
                             testPARSECOLS)) ||
        (NULL == CU_add_test(benchCommandSuite, "commandLineParseArgument()",
                             testCOMMANDLINEPARSE))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
