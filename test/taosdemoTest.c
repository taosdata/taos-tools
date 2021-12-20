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

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "CUnit/Basic.h"
#include "demo.h"
#include "demoData.h"

int init_suite1(void) { return 0; }

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
        CU_add_suite("demoCommandOpt.c", init_suite1, clean_suite1);
    if (NULL == demoCommandSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* add the tests to the suite */
    if ((NULL == CU_add_test(demoCommandSuite, "parse_datatype()",
                             testPARSEDATATYPE))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
