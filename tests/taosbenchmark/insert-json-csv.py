###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        now = time.time()
        self.ts = int(round(now * 1000))

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        return paths[0]

    def run(self):
        binPath = self.getPath("taosBenchmark")
        if (binPath == ""):
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql
        # test case for https://jira.taosdata.com:18080/browse/TD-4985
        os.system(
            "%s -f ./taosbenchmark/json/insert-json-csv.json -y " %
            binPath)
        tdSql.execute("use db")
        tdSql.query("select count(*) from (select distinct(tbname) from stb0)")
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            "select * from stb0 where  tbname like 'stb00_0'  limit 10")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
