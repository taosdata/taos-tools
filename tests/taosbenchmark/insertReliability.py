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
import json
import sys
import os
import time
import datetime
import platform
import subprocess

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from util.dnodes import tdDnodes


# reomve single and double quotation
def removeQuotation(origin):
    value = ""
    for c in origin:
        if c != '\'' and c != '"':
            value += c

    return value

class TDTestCase:
    def caseDescription(self):
        """
        [TD-11510] taosBenchmark test cases
        """

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
        else:
            projPath = selfPath[: selfPath.find("tests")]

        paths = []
        for root, dummy, files in os.walk(projPath):
            if (tool) in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if len(paths) == 0:
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]



    def runSeconds(self, command, seconds, timeout = 60):

        tdLog.info(f"run with {command} after seconds:{seconds} ...")
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(seconds)

        # check have dbRows
        for i in range(60):
            # maybe db can not create , so need try
            try:
                rows = self.getDbRows(4)
            except:
                time.sleep(1)
                continue

            # check break condition
            if rows > 0:
                tdLog.info(f" runSecond loop = {i} wait db have record ok, records={rows}, break wait ...")
                break

        index = 1
        tdLog.info(f"stop taosd index={index} ...")
        tdDnodes.stop(index)

        tdLog.info(f"wait taosbenchmark end ...")
        process.wait(timeout)

        tdLog.info(f"start taosd index={index} ...")
        tdDnodes.startWithoutSleep(index)
        tdLog.info(f"start taosd index={index} ok")

        # get output
        output = process.stdout.read().decode(encoding="gbk")
        error = process.stderr.read().decode(encoding="gbk")
        return output, error

    def getKeyValue(self, content, key, end):
        # find key
        s = content.find(key)
        if s == -1:
            return False,""
        
        # find end
        s += len(key)
        e = content.find(end, s)
        if e == -1:
            value = content[s : ]
        else:
            value = content[s : e]

        return True, value    

    def getDbRows(self, times):
        sql = f"select count(*) from {self.db}.meters"
        tdSql.waitedQuery(sql, 1, times)
        dbRows = tdSql.getData(0, 0)
        return dbRows

    
    def checkAfterRestart(self, command):  
        # taosc
        output, error = self.runSeconds(command, 10)
        
        #
        #  check succRows <= dbRows
        #

        # find value
        key = "seconds to insert rows: "
        result, value = self.getKeyValue(error, key, " ")
        if result == False:
            tdLog.exit(f"not found key:{key} value, content={error}")
        
        succRows = int(value)
        if succRows == 0:
            tdLog.exit(f"key:{key} value:{value} is zero, content={error}")
        
        # compare with database rows
        dbRows = self.getDbRows(100)
        if  dbRows < succRows:
            tdLog.exit(f"db rows less than insert success rows. {dbRows} < {succRows}")

        # succ
        tdLog.info(f"check write ok. succRows: {succRows} <= dbRows:{dbRows} command={command}")

        #
        # check no retry information
        #
        msgs = [
            "milliseconds then re-insert",
            "stmt2 start retry submit"
        ]

        for msg in msgs:
            pos = output.find(msg)
            if pos != -1:
                tdLog.exit(f"default not retry, buf found retry msg: {msg} pos={pos} content={output}")
    
    
    def writeSuccCheck(self):
        benchmark = self.getPath()
        self.db = "relia"

        #
        # rest
        #

        # batch
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000 -I rest -r 100 -y"
        self.checkAfterRestart(command)
        # interlace
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000 -I rest -r 100 -B 1 -y"
        self.checkAfterRestart(command)

        #
        # taosc
        #

        # batch
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000 -I taosc -r 100 -y"
        self.checkAfterRestart(command)
        # interlace
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000 -I taosc -r 100 -B 1 -y"
        self.checkAfterRestart(command)

        #
        # stmt2
        #

        # batch
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000 -I stmt2 -r 100 -y"
        self.checkAfterRestart(command)
        # interlace
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000 -I stmt2 -r 100 -B 1 -y"
        self.checkAfterRestart(command)

        #
        # websocket
        #

        # batch
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000  -r 100 --cloud_dsn=http://localhost:6041 -y"
        self.checkAfterRestart(command)
        # interlace
        command = f"{benchmark} -d {self.db} -t 100 -n 10000000  -r 100 -B 1 --cloud_dsn=http://localhost:6041 -y"
        self.checkAfterRestart(command)
        

    def run(self):
        # restart taosd test
        self.writeSuccCheck()
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
