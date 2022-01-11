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

int init_suite1(void) { return 0; }

int clean_suite1(void) { return 0; }

void testInvalidOpt(void) {}
/* The main() function for setting up and running the tests.
 * Returns a CUE_SUCCESS on successful running, another
 * CUnit error code on failure.
 */
int main() {
    CU_pSuite benchCommandSuite = NULL;
    CU_pSuite benchDataSuite = NULL;

    /* initialize the CUnit test registry */
    if (CUE_SUCCESS != CU_initialize_registry()) return CU_get_error();

    /* add a suite to the registry */
    benchCommandSuite =
        CU_add_suite("benchCommandOpt.c", init_suite1, clean_suite1);
    benchDataSuite = CU_add_suite("benchData.c", init_suite1, clean_suite1);
    if (NULL == benchCommandSuite || NULL == benchDataSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* add the tests to the suite */
    if ((NULL ==
         CU_add_test(benchCommandSuite, "invalid options", testInvalidOpt))) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
