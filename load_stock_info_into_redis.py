#!/usr/bin/python
# coding: utf-8
#
# load stock info into redis
# $Id: load_stock_nfo into redis.py  2015-12-14 Qiu $
#
# history:
# 2015-12-14 Qiu   created

# qiuqiu@kunyand-inc.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
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

import redis
import MySQLdb
from input_output import InputOutput

class LoadStockInfoIntoRedis(object):

    def __init__(self):

        self.redis = redis.StrictRedis(host='192.168.1.24', port=6379,
                                        db=4, password='kunyandata',charset='utf8')
        self.pipe = self.redis.pipeline()
        self.conn = MySQLdb.connect(host='127.0.0.1', user='root',
                                        passwd='root', db='stock',charset='utf8')
        self.cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        self.input_output = InputOutput(self.conn, self.cursor)

    def main(self):

        sql = 'select * from stock_info'
        stock_info = self.input_output.get_data(sql)
        for line in stock_info:
            code = line['v_code']
            self.pipe.set('stock:%s:jianpin' % code, '%s' % line['v_jian_pin'])
            self.pipe.set('stock:%s:name' % code, '%s' % line['v_name'])
            self.pipe.set('stock:%s:nameurl' % code, '%s' % line['v_name_url'])
            self.pipe.set('stock:%s:quanpin' % code, '%s' % line['v_quan_pin'])
        self.pipe.execute()

if __name__ == '__main__':
    load_stock_info_into_redis = LoadStockInfoIntoRedis()
    load_stock_info_into_redis.main()



