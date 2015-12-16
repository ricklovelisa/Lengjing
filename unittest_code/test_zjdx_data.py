#!/usr/bin/python
#coding: utf-8
#
# visit_and_search_linux
# $Id: test_lengjing.py  2015-09-30 Qiu $
#
# history:
# 2015-10-09 Qiu created
#
# qiuqiu@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
#visit_and_search_linux.py is
#
# Copyright (c)  by ShangHai KunYan Data Service Co. Ltd..  All rights reserved.
#
# By obtaining, using, and/or copying this software and/or its
# associated documentation, you agree that you have read, understood,
# and will comply with the following terms and conditions:
#
# Permission to use, copy, modify, and distribute this software and
# its associated documentation for any purpose and without fee is
# hereby granted, provided that the above copyright notice appears in
# all copies, and that both that copyright notice and this permission
# notice appear in supporting documentation, and that the name of
# ShangHai KunYan Data Service Co. Ltd. or the author
# not be used in advertising or publicity
# pertaining to distribution of the software without specific, written
# prior permission.
#
# --------------------------------------------------------------------

"""
unit_testing.py

Test the code
"""

import unittest
import redis
import os
import re
import zjdx_data_linux

class TestLoadData(unittest.TestCase):
    """Test.

    Args:
        no
    """

    def test_load(self):
        """test load_data.py.

        Args:
            no
        """

        log_file = open("D:/WorkSapce/Python_workspace/lengjing_unittest/log")
        log_count_pre = - 1
        for log_count_pre, line in enumerate(log_file):
            pass
        log_count_pre += 1
        log_file.close()

        zjdx = zjdx_data_linux.ZjdxDataLinux()
        zjdx.main()

        all_file = os.listdir("D:/WorkSapce/Python_workspace/lengjing_unittest/")
        file_list = []
        for file_name in all_file:
            result = re.search(r'log', file_name)
            if result:
                file_list.append(file_name)

        log_file = open("D:/WorkSapce/Python_workspace/lengjing_unittest/log")
        log_count_after = - 1
        for log_count_after, line in enumerate(log_file):
            pass
        log_count_after += 1
        log_file.close()

        # check up
        active = redis.StrictRedis(host="192.168.0.2", port=6379, db=6, password='kunyandata')

        self.assertEqual(len(active.keys("*:*:2018-12-09 *")),
                         4, "total number check failed")
        self.assertEqual(len(active.keys("visit:*:2018-12-09 *")),
                         1, "visit incre data check failed")
        self.assertEqual(len(active.keys("search:*:2018-12-09 *")),
                         3, "search incre data check failed")
        self.assertEqual(active.zcard("visit:2018-12-09 00"),
                         1, "visit set num check failed")
        self.assertEqual(active.zcard("search:2018-12-09 01"),
                         3, "search set num check failed")
        self.assertEqual(active.get("stock:600796:jianpin"), "qjsh",
                         "jianpin check failed")
        self.assertEqual(int(active.get("visit:000001:2018-12-09 00")),
                         30, " visit value check failed")
        index_visit_set = active.zrange("visit:2018-12-09 00", 0, -1).index("000001")
        self.assertEqual(active.zrange("visit:2018-12-09 00", index_visit_set,
                                       index_visit_set, withscores=True)[0][1], 30.0,
                         "visit set value check failed")
        self.assertEqual(int(active.get("search:000007:2018-12-09 01")),
                         12, "search value check failed")
        self.assertEqual(int(active.get("search:000721:2018-12-09 01")),
                         12, "search value check failed")
        self.assertEqual(int(active.get("search:600796:2018-12-09 01")),
                         12, "search value check failed")
        search_set_keys = active.zrange("search:2018-12-09 01", 0, -1)
        index_search_set_1 = search_set_keys.index("000007")
        index_search_set_2 = search_set_keys.index("000721")
        index_search_set_3 = search_set_keys.index("600796")
        self.assertEqual(active.zrange("search:2018-12-09 01", index_search_set_1,
                                       index_search_set_1, withscores=True)[0][1],
                         12.0, "search set value check failed")
        self.assertEqual(active.zrange("search:2018-12-09 01", index_search_set_2,
                                       index_search_set_2, withscores=True)[0][1],
                         12.0, "search set value check failed")
        self.assertEqual(active.zrange("search:2018-12-09 01", index_search_set_3,
                                       index_search_set_3, withscores=True)[0][1],
                         12.0, "search set value check failed")
        self.assertEqual(len(file_list), 1, "log num check failed")
        self.assertEqual(log_count_after - log_count_pre, 4, "log lines count check failed")

if __name__ == '__main__':
    unittest.main()
