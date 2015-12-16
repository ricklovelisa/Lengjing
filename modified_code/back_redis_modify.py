#!/usr/bin/python
#coding: utf-8
#
# back_redis_modify
# $Id: back_redis_modify.py  2015-10-15 Qiu $
#
# history:
# 2015-10-15 Qiu   created
#
# wangQiu@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# back_redis_modify.py is
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
back_redis_modify.py

Back up redis data to mysql
"""


import datetime
import redis
import MySQLdb


class BackRedis(object):

    """Back up redis data to mysql.

    Back up redis data to mysql.

    Attributes:
        work_dir: input data..
    """

    def __init__(self):
        """inition for LoadData.

        Args:
            no
        """
        self.work_dir = 'D:/home/zjdx/'
        # self.last_back_hour = '2015-09-26 01'
        redis_host = '120.55.189.211'
        redis_port = 6379
        redis_db = 0
        redis_password = 'kunyandata'
        self.redis1 = redis.StrictRedis(host=redis_host, port=redis_port,
                                        db=redis_db, password=redis_password)
        self.stock_codes = self.redis1.lrange('stock:list', 0, -1)
        try:
            self.conn = MySQLdb.connect(host='120.55.189.211', user='root',
                                        passwd='hadoop', db='stock')
        except Exception, e:
            print e
        self.cursor = self.conn.cursor()
        # backed_file = open(self.work_dir+'files')
        # self.back_time = []
        # for line in backed_file:
        #     if len(line) > 17:
        #         self.back_time.append(line[7:11] + '-' + line[11:13]
        #                               + '-' + line[13:15] + ' ' + line[15:17])
        # backed_file.close()
        self.cursor.execute("select * from unbacked_redis_data")
        unbacked_list = self.cursor.fetchall()
        self.back_time = []
        for line in unbacked_list:
            if len(line[0]) > 16:
                self.back_time.append(line[0][7:11] + '-' + line[0][11:13]
                                      + '-' + line[0][13:15] + ' ' + line[0][15:17])
        self.back_files = []
        for line in unbacked_list:
            self.back_files.append(line[0])

    def _run(self, hour):
        """run for back up data.

        Args:
            no
        """
        visit_hour = 'visit:%s' % hour
        visit_hour_data = self.redis1.hgetall(visit_hour)
        for stock_code in visit_hour_data:
            visit_stock_time = 'visit:%s:%s' % (stock_code, hour)
            visit_number = int(visit_hour_data[stock_code])
            self._visit_per_hour_to_mysql(visit_stock_time,
                                          visit_number, stock_code, hour)

        search_hour = 'search:%s' % hour
        search_hour_data = self.redis1.hgetall(search_hour)
        for stock_code in search_hour_data:
            search_stock_time = 'search:%s:%s' % (stock_code, hour)
            search_number = int(search_hour_data[stock_code])
            self._search_per_hour_to_mysql(search_stock_time,
                                           search_number, stock_code, hour)

        search_count = self.redis1.get('search:count:%s' % hour)
        visit_count = self.redis1.get('visit:count:%s' % hour)
        sql1 = "insert into search_count_per_hour(v_hour, " \
                    "i_frequency) VALUES ('%s', '%s')" \
                    % (hour, search_count)
        sql2 = "insert into visit_count_per_hour(v_hour, " \
                    "i_frequency) VALUES ('%s', '%s')" \
                    % (hour, visit_count)
        try:
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
        except Exception, e:
            print e

    def main(self):
        """Main function.

        Args:
            no
        """
        self._run_back_redis()
        self.cursor.close()
        self.conn.close()

    def _run_back_redis(self):
        """ back up data to mysql.

        Args:
            no
        """
        for back_hour in self.back_time:
            last_hour = datetime.datetime.strptime(back_hour, "%Y-%m-%d %H")
            now_hour = datetime.datetime.now()
            now_to_last = now_hour - last_hour
            hours_delta = int(now_to_last.days * 24
                              + now_to_last.seconds/3600)
            if hours_delta > 10:
                self._run(back_hour)
                self.conn.commit()

                sql = "insert into backed_hours(v_hour, " \
                           "v_backed_time) VALUES ('%s', '%s')" \
                           % (back_hour, now_hour)
                del_sql = "delete from unbacked_redis_data " \
                               "where unbacked_redis_file = '%s'" \
                               % self.back_files[self.back_time.index(back_hour)]
                try:
                    self.cursor.execute(sql)
                    self.cursor.execute(del_sql)
                    self.conn.commit()
                except Exception, e:
                    print e
                print "backuped: "+back_hour+'\n'

    def _visit_per_hour_to_mysql(
            self, stock_hour, visit_number, stock_code, hour):
        """ back up visit data to mysql.

        Args:
            no
        """
        sql1 = "insert into stock_visit_per_hour(v_stock_hour," \
                   "i_visit_number) values ('%s', '%d')"\
                   % (stock_hour, visit_number)
        sql2 = "insert into visit_stock(v_stock, v_hour, i_frequency) " \
                    "values ('%s', '%s', '%d')" \
                    % (stock_code, hour, visit_number)
        try:
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
        except Exception, e:
            print e

    def _search_per_hour_to_mysql(
            self, stock_hour, search_number, stock_code, hour):
        """ back up search data to mysql.

        Args:
            no
        """
        sql1 = "insert into stock_search_per_hour(v_stock_hour," \
                   "i_search_number) values ('%s', '%d')"\
                   % (stock_hour, search_number)
        sql2 = "insert into search_stock(v_stock, v_hour, i_frequency) " \
                    "values ('%s', '%s', '%d')" \
                    % (stock_code, hour, search_number)
        try:
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
        except Exception, e:
            print e

if __name__ == '__main__':
    back_up_redis = BackRedis()
    back_up_redis.main()
