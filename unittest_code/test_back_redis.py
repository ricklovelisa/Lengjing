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
import MySQLdb

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

        # backup = back_redis.BackRedis()
        # backup.main()

        conn = MySQLdb.connect(host='127.0.0.1', user='root',
                               passwd='root', db='stock')
        cursor = conn.cursor()
        sql_last_back_hour = "select last_back_hour from last_back_hour "
        sql_visit_count = "select count(*) from stock_visit_per_hour"
        sql_search_count = "select count(*) from stock_search_per_hour"
        cursor.execute(sql_last_back_hour)
        result_last_back_hour = cursor.fetchone()[0]
        cursor.execute(sql_visit_count)
        result_visit_count = cursor.fetchone()[0]
        cursor.execute(sql_search_count)
        result_search_count = cursor.fetchone()[0]

        # check up
        self.assertEqual(result_last_back_hour, "2018-12-09 01", "last back hour check failed")
        self.assertEqual(result_visit_count, 1, "visit count check failed")
        self.assertEqual(result_search_count, 3, "visit count check failed")

if __name__ == '__main__':
    unittest.main()
