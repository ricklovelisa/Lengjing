#!/usr/bin/python
#coding: utf-8
#
# loadData
# $Id: load_data.py  2015-09-30 Haitao $
#
# history:
# 2015-09-30 Haitao   created
# 2015-09-30 Haitao   modified
# 2015-10-08 Haitao   modified
#
# wanghaitao@kunyan-inc.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# load_data.py is
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
load_data.py

Load data to redis and mysql
"""
import re
import time
import math
import redis
import string


class LoadData(object):

    """Load data to Redis.

    Load  stock data  time to redis.

    Attributes:
        input_file: input data..
    """

    def __init__(self, file_name):
        """inition for LoadData.

        Args:
            no
        """
        self.input_file = file_name
        self.name_urls = []
        self.jian_pins = []
        self.quan_pins = []
        self.stock_codes = []
        self._load_stock_codes()

    def _load_stock_codes(self):
        """Load stock_codes for Data.

        Args:
            no
        """
        server_ip = '192.168.1.24'
        server_port = 6379
        # redis1 = redis.StrictRedis(host=server_ip, port=server_port,
        #                            db=0, password='7ifW4i@M')
        # self.stock_codes = redis1.lrange('stock:list', 0, -1)
        # redis2 = redis.StrictRedis(host=server_ip, port=server_port,
        #                            db=0, password='7ifW4i@M')
        redis1 = redis.StrictRedis(host=server_ip, port=server_port,
                                   db=6, password='kunyandata')
        self.stock_codes = redis1.lrange('stock:list', 0, -1)
        redis2 = redis.StrictRedis(host=server_ip, port=server_port,
                                   db=6, password='kunyandata')
        self.pipe2 = redis2.pipeline()
        # self.pipe2 = redis2
        for stock_code in self.stock_codes:
            self.name_urls.append(redis1.get('stock:%s:nameurl'
                                             % stock_code))
            self.jian_pins.append(redis1.get('stock:%s:jianpin'
                                             % stock_code))
            self.quan_pins.append(redis1.get('stock:%s:quanpin'
                                             % stock_code))

    def main(self):
        """Main function.

        Args:
            no
        """
        open_file = open(self.input_file, 'r')
        # k = 1
        # for line in open_file:
        #     self._line_to_mysql_and_redis(line)
        #     self.pipe2.execute()
        #     k += 1
        #     print k
        count = -1
        for count, lines in enumerate(open_file):
            pass
        count += 1
        open_file.close()
        open_file = open(self.input_file, 'r')
        j = 1
        k = 1
        for line in open_file:
            self._line_to_mysql_and_redis(line)
            if k == count:
                self.pipe2.execute()
                print '%s load finished' % self.input_file
            elif k > (j * 500000):
                self.pipe2.execute()
                j += 1
                print j
            k += 1
        open_file.close()

    def _line_to_mysql_and_redis(self, line):
        """Load line data to redis.

        Args:
            no
        """
        line_split = line.split()
        if len(line_split) < 3:
            return
        if line_split[2] >= '0' and line_split[2] <= '5':
            self._visit(line_split)
        elif line_split[2] >= '6' and line_split[2] <= '9':
            self._search(line_split)

    def _visit(self, line_split):
        """Load visit data to redis.

        Args:
            no
        """
        stock_code = line_split[0]
        #visit_website = string.atoi(line_split[2])
        if stock_code in self.stock_codes:
            string_time = string.atol(line_split[1])/1000
            local_time = time.localtime(string_time)
            hour = time.strftime('%Y-%m-%d %H', local_time)
            self.pipe2.incr('visit:%s:%s' % (stock_code, hour))
            self.pipe2.expire('visit:%s:%s' % (stock_code, hour),
                              50*60*60)
            #self.pipe2.hincrby(stock_code, 'visit:%s' % hour, 1)
            self.pipe2.hincrby('visit:%s' % hour, stock_code, 1)
            self.pipe2.expire('visit:%s' % hour, 50*60*60)
            self.pipe2.incr('visit:count:%s' % hour)
            self.pipe2.expire('visit:count:%s' % hour, 50*60*60)

    def _search(self, line_split):
        """Load search data to redis.

        Args:
            no
        """
        #visit_website = string.atoi(line_split[2])
        keyword = line_split[0]
        length = len(keyword)
        if length < 4:
            return
        if keyword[0].isdigit():
            if length < 6:
                return
            for stock_code in self.stock_codes:
                if keyword in stock_code:
                    string_time = string.atol(line_split[1])/1000
                    local_time = time.localtime(string_time)
                    hour = time.strftime('%Y-%m-%d %H', local_time)
                    self.pipe2.incr('search:%s:%s' % (stock_code, hour))
                    self.pipe2.expire('search:%s:%s' % (stock_code, hour),
                                      50*60*60)
                    self.pipe2.hincrby('search:%s' % hour, stock_code, 1)
                    self.pipe2.expire('search:%s' % hour, 50*60*60)
                    self.pipe2.incr('search:count:%s' % hour)
                    self.pipe2.expire('search:count:%s' % hour, 50*60*60)
        elif keyword[0] == '%':
            result = re.search(r'%.*%.*%.*%.*%.*%.*%.*%', keyword)
            if not result:
                return
            keyword = keyword.upper()
            index = 0
            for name_url in self.name_urls:
                index += 1
                if keyword in name_url:
                    string_time = string.atol(line_split[1])/1000
                    local_time = time.localtime(string_time)
                    hour = time.strftime('%Y-%m-%d %H', local_time)
                    self.pipe2.incr('search:%s:%s' %
                                    (self.stock_codes[index-1], hour))
                    self.pipe2.expire('search:%s:%s' %
                                      (self.stock_codes[index-1],
                                       hour), 50*60*60)
                    self.pipe2.hincrby('search:%s' % hour,
                                       self.stock_codes[index-1], 1)
                    self.pipe2.expire('search:%s' % hour, 50*60*60)
                    self.pipe2.incr('search:count:%s' % hour)
                    self.pipe2.expire('search:count:%s' % hour, 50*60*60)
        elif keyword[0].isalpha():
            keyword = keyword.lower()
            index = 0
            for jian_pin in self.jian_pins:
                index += 1
                if keyword in jian_pin:
                    string_time = string.atol(line_split[1])/1000
                    local_time = time.localtime(string_time)
                    hour = time.strftime('%Y-%m-%d %H', local_time)
                    self.pipe2.incr('search:%s:%s' %
                                    (self.stock_codes[index-1], hour))
                    self.pipe2.expire('search:%s:%s' %
                                      (self.stock_codes[index-1], hour),
                                      50*60*60)
                    self.pipe2.hincrby('search:%s' % hour,
                                       self.stock_codes[index-1], 1)
                    self.pipe2.expire('search:%s' % hour, 50*60*60)
                    self.pipe2.incr('search:count:%s' % hour)
                    self.pipe2.expire('search:count:%s' % hour, 50*60*60)
            if index != 0:
                return
            for quan_pin in self.quan_pins:
                index += 1
                if keyword in quan_pin:
                    string_time = string.atol(line_split[1])/1000
                    local_time = time.localtime(string_time)
                    hour = time.strftime('%Y-%m-%d %H', local_time)
                    self.pipe2.incr('search:%s:%s' %
                                    (self.stock_codes[index-1], hour))
                    self.pipe2.expire('search:%s:%s' %
                                      (self.stock_codes[index-1], hour),
                                      50*60*60)
                    self.pipe2.hincrby('search:%s' % hour,
                                       self.stock_codes[index-1], 1)
                    self.pipe2.expire('search:%s' % hour, 50*60*60)
                    self.pipe2.incr('search:count:%s' % hour)
                    self.pipe2.expire('search:count:%s' % hour, 50*60*60)


if __name__ == '__main__':
    """Main function.

        Args:
            no
   """
    load = LoadData('D:/home/jsdx/unbacked_redis_files/jsdx_2015111301')
    load.main()
