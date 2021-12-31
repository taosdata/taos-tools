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

int init_suite1(void) {
    init_g_args(&test_g_args);
    return 0;
}

int clean_suite1(void) { return 0; }

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

void testPARSEARGS(void) {
    char* all_type_tags[] = {
        "taosBenchmark", "-A",
        "bool,tinyint,utinyint,smallint,usmallint,int,uint,bigint,ubigint,"
        "timestamp,float,double,binary,nchar"};
    CU_ASSERT_EQUAL(parse_args(3, all_type_tags, &test_g_args), 0);
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
    CU_ASSERT_EQUAL(parse_args(3, nchar_binary_length_type_tags, &test_g_args),
                    0);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 2);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], 13);
    CU_ASSERT_EQUAL(test_g_args.tag_length[1], 21);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_NCHAR);
    CU_ASSERT_EQUAL(test_g_args.tag_type[1], TSDB_DATA_TYPE_BINARY);
    char* json_type_tags[] = {"taosBenchmark", "-A", "json"};
    CU_ASSERT_EQUAL(parse_args(3, json_type_tags, &test_g_args), 0);
    CU_ASSERT_EQUAL(test_g_args.tagCount, 1);
    CU_ASSERT_EQUAL(test_g_args.tag_length[0], test_g_args.binwidth);
    CU_ASSERT_EQUAL(test_g_args.tag_type[0], TSDB_DATA_TYPE_JSON);
    char* invalid_type_tags[] = {"taosBenchmark", "-A", "non-exists_type"};
    CU_ASSERT_EQUAL(parse_args(3, invalid_type_tags, &test_g_args), -1);
}
/* The main() function for setting up and running the tests.
 * Returns a CUE_SUCCESS on successful running, another
 * CUnit error code on failure.
 */
int main() {
    CU_pSuite demoCommandSuite = NULL;

    /* initialize the CUnit test registry */
    if (CUE_SUCCESS != CU_initialize_registry()) return CU_get_error();

    /* add a suite to the registry */
    demoCommandSuite =
        CU_add_suite("benchCommandOpt.c", init_suite1, clean_suite1);
    if (NULL == demoCommandSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* add the tests to the suite */
    if ((NULL == CU_add_test(demoCommandSuite, "parse_datatype()",
                             testPARSEDATATYPE)) ||
        (NULL ==
         CU_add_test(demoCommandSuite, "parse_args()", testPARSEARGS))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
