#!/usr/bin/python
#coding: utf-8
#
# zjdx_data_linux
# $Id: zjdx_data_linux.py  2015-09-30 Haitao $
#
# history:
# 2015-09-30 Haitao   created
# 2015-09-30 Haitao   modified
# 2015-10-08 Haitao   modified
# 2015-10-19 Qiu      modified
#
# wanghaitao@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# zjdx_data_linux.py is
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
zjdx_data_linux.py

Load Zhejiang Telecom data.
"""

import os
import re
import time
import load_data
import MySQLdb
import shutil
import send_sms


class ZjdxDataLinux(object):

    """Load Zhejiang Telecom data.

    Load  Zhejiang Telecom to redis.

    Attributes:
        base_dir: input data path.
    """

    def __init__(self):
        """inition for ZjdxDataLinux.

        Args:
            no
        """
        self.base_dir = '/home/zjdx/'
        self.mysqlconn = MySQLdb.connect(host='120.55.189.211', user='root',
                                         passwd='hadoop', db='stock')
        self.cursor = self.mysqlconn.cursor()

    def main(self):
        """Main function.

        Args:
            no
        """
        all_file = os.listdir(self.base_dir)
        file_list = []
        for file_name in all_file:
            result = re.search(r'^kunyan_\d{10}$', file_name)
            if result:
                file_list.append(file_name)

        # old_file_list = []

        # new_file = open(self.base_dir+'files', 'a+')
        # new_file.close()
        #
        # file_old = open(self.base_dir+'files', 'r')
        # for lines in file_old:
        #     old_file_list.append(lines.strip('\n'))
        # file_old.close()

        self.cursor = self.mysqlconn.cursor()
        try:
            self.cursor.execute("select * from unbacked_redis_data")
        except Exception, e:
            print e
        result = self.cursor.fetchall()
        old_file_list = []
        for line in result:
            old_file_list.append(line[0])

        tag = 0
        for line in file_list:
            if line not in old_file_list:
                tag = 1
                log = open(self.base_dir+'log', 'a+')
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                begin_out = line + " begin_time: " + log_time + "\n"
                log.write(begin_out)
                print begin_out
                log.close()
                load = load_data.LoadData(self.base_dir+line)
                load.main()
                try:
                    self.cursor.execute("INSERT INTO unbacked_redis_data"
                                        "(unbacked_redis_file) VALUES ('%s')" % line)
                    self.mysqlconn.commit()
                except Exception, e:
                    print e
                isexists = os.path.exists(self.base_dir+"unbacked_redis_files/")
                if not isexists:
                    os.makedirs(self.base_dir+"unbacked_redis_files/")
                    print self.base_dir+"unbacked_redis_files" + u' 创建成功\n'
                shutil.move(self.base_dir+line, self.base_dir+"unbacked_redis_files")
                # file_new = open(self.base_dir+'files', 'a+')
                # file_new.write(line)
                # file_new.write('\n')
                # file_new.close()
                log = open(self.base_dir+'log', 'a+')
                log_time = time.strftime('%Y-%m-%d %H:%M:%S')
                end_out = line + " end_time: " + log_time + "\n"
                log.write(end_out)
                print end_out
                log.close()
        self.cursor.close()
        # self.mysqlconn.commit()
        self.mysqlconn.close()
        if tag == 0:
            if not os.path.exists(self.base_dir+'miss_data'):
                miss_data_file = open(self.base_dir+'miss_data', 'a+')
                miss_data_file.write('0')
                miss_data_file.close()
            miss_data_file_read = open(self.base_dir+'miss_data', 'r')
            miss_data = miss_data_file_read.read()
            miss_data_file_read.close()
            num_of_miss = int(miss_data) + 1
            if num_of_miss > 4:
                sms = send_sms.SendSms()
                send_mms = sms.main('18668169052,18118973826', '【数据平台】电信没有数据传入')
                if send_mms:
                    print "message sent success!"
                else:
                    print "message sent faild!"
            str_miss = str(num_of_miss)
            miss_data_file_write = open(self.base_dir+'miss_data', 'w')
            miss_data_file_write.write(str_miss)
            miss_data_file_write.close()
        else:
            if not os.path.exists(self.base_dir+'miss_data'):
                miss_data_file = open(self.base_dir+'miss_data', 'a+')
                miss_data_file.write('0')
                miss_data_file.close()
            miss_data_file = open(self.base_dir+'miss_data', 'w')
            miss_data_file.write('0')
            miss_data_file.close()

if __name__ == '__main__':
    zjdx = ZjdxDataLinux()
    zjdx.main()
