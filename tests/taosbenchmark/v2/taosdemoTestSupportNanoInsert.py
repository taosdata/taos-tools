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

import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        elif "src" in selfPath:
            projPath = selfPath[: selfPath.find("src")]
        elif "/tools/" in selfPath:
            projPath = selfPath[: selfPath.find("/tools/")]
        elif "/tests/" in selfPath:
            projPath = selfPath[: selfPath.find("/tests/")]
        else:
            tdLog.info("cannot found %s in path: %s, use system's" % (tool, selfPath))
            projPath = "/usr/local/taos/bin/"

        paths = []
        for root, dummy, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            return ""
        return paths[0]

    def run(self):
        binPath = self.getPath("taosBenchmark")
        if binPath == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        # insert: create one  or multiple tables per sql and insert multiple rows per sql
        # insert data from a special timestamp
        # check stable stb0

        os.system(
            "%s -f ./taosbenchmark/json/taosdemoTestNanoDatabase.json -y " % binPath
        )
        tdSql.execute("use nsdb")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from tb0_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("describe stb0")
        tdSql.checkDataType(9, 1, "TIMESTAMP")
        tdSql.query("select last(ts) from stb0")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.990000000")

        # check stable stb1 which is insert with disord

        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from tb1_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 10000)
        # check c8 is an nano timestamp
        tdSql.query("describe stb1")
        tdSql.checkDataType(9, 1, "TIMESTAMP")
        # check insert timestamp_step is nano_second
        tdSql.query("select last(ts) from stb1")
        tdSql.checkData(0, 0, "2021-07-01 00:00:00.990000000")

        # insert data from now time

        # check stable stb0
        os.system(
            "%s -f ./taosbenchmark/json/taosdemoTestNanoDatabaseNow.json -y " % binPath
        )

        tdSql.execute("use nsdb2")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from tb0_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        # check c8 is an nano timestamp
        tdSql.query("describe stb0")
        tdSql.checkDataType(9, 1, "TIMESTAMP")

        # insert by csv files and timestamp is long int , strings  in ts and
        # cols

        os.system(
            "%s -f ./taosbenchmark/json/taosdemoTestNanoDatabasecsv.json  -y " % binPath
        )
        tdSql.execute("use nsdbcsv")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.query("describe stb0")
        tdSql.checkDataType(3, 1, "TIMESTAMP")

        tdSql.query(
            'select count(*) from nsdbcsv.stb0 where ts > "2021-07-01 00:00:00.490000000"'
        )
        tdSql.checkData(0, 0, 5000)

        tdSql.query("select count(*) from stb0 where ts < 1626918583000000000")
        tdSql.checkData(0, 0, 10000)

        os.system("rm -rf ./insert_res.txt")
        os.system(
            "rm -rf tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNano*.py.sql"
        )

        # taosdemo test insert with command and parameter , detals show
        # taosdemo --help
        os.system(
            "%s  -u root -ptaosdata -P 6030  -a 1  -m pre -n 10 -T 20 -t 60 -o res.txt -y "
            % binPath
        )
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 600)

        os.system("rm -rf ./res.txt")
        os.system("rm -rf ./*.py.sql")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
