#!/usr/bin/python
#coding: utf-8
#
# followStockToRedisFromHbase
# $Id: followStockToRedisFromHbase.py  2015-10-02 Haitao $
#
# history:
# 2015-10-02 Haitao   created
# 2015-10-02 Haitao   modified
#
# wanghaitao@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# followStockToRedisFromHbase.py is
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


"""followStockToRedisFromHbase.py

Load data from Hbase to Redis.
"""


import re
import time
import string

import redis

from thrift.transport import TSocket
from hbase import Hbase
from hbase.ttypes import *


class FollowStockToRedisFromHbase(object):

    """Load data from Hbase to Redis.

    Load follow stock html data and log time to redis.

    Attributes:
        table: table of data in Hbase.
        rowkey: rowkey of data in Hbase.
        follow_stock_list: follow stocks list.
        log_hour: time of log in Hbase.
    """

    def __init__(self, table, rowkey):
        """inition for FollowStockToRedisFromHbase.

        Args:
            no
        """
        self.table = table
        self.rowkey = rowkey
        hbase_ip = '192.168.0.2'
        hbase_port = 9090
        self.transport = TSocket.TSocket(hbase_ip, hbase_port)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(protocol)
        self.transport.open()
        redis1 = redis.StrictRedis(host='192.168.0.2', port=6379,
                                   db=1, password='kunyandata')
        self.stock_codes = redis1.lrange('stock:list', 0, -1)
        redis2 = redis.StrictRedis(host='192.168.0.2', port=6379,
                                   db=4, password='kunyandata')
        self.pipe2 = redis2.pipeline()
        self.follow_stock_list = []
        self.log_hour = '2015-10-02 12'

    def _data_and_time_from_base(self):
        """Get html_data and log_hour from Hbase.

        Args:
            follow_stock_list: follow stocks list.
            log_hour: time of log in Hbase.
        """
        row_data = self.client.getRow(self.table, self.rowkey, None)
        for row in row_data:
            html_data = row.columns.get('content:').value
            self.follow_stock_list = re.findall(r'<td><a data-code="(.*?)" '
                                                r'class="stock_delete" '
                                                r'href="javascript:;" '
                                                r'data-statid="t_c_stocdel" '
                                                r'action-click="clickStat">',
                                                html_data)
            html_timestamp = str(row.columns.get('content:').timestamp)
            html_time_string = html_timestamp[:10]+'.0'
            html_time_float = string.atof(html_time_string)
            self.log_hour = time.strftime("%Y-%m-%d %H",
                                          time.localtime(html_time_float))
            break
        self.transport.close()

    def _data_and_time_to_redis(self):
        """Put follow stock_code and log_hour to redis.

        Args:
            no
        """
        for stock_code in self.follow_stock_list:
            if stock_code in self.stock_codes:
                self.pipe2.incr('follow:%s:%s' % (stock_code, self.log_hour))
                self.pipe2.expire('follow:%s:%s' % (stock_code, self.log_hour),
                                  24*60*60)
                self.pipe2.zincrby('follow:%s' % self.log_hour, stock_code, 1)
                self.pipe2.expire('follow:%s' % self.log_hour, 24*60*60)
        self.pipe2.execute()

    def main(self):
        """Main function.

        Args:
            no
        """
        self._data_and_time_from_base()
        self._data_and_time_to_redis()


if __name__ == '__main__':
    data_to_redis = FollowStockToRedisFromHbase(
        't.10jqka.com.cn', '4986a9216c4ebc4e181052af0d5d3f12')
    data_to_redis.main()
