#!/usr/bin/python
# coding: utf-8
#
# multiple load data to redis
# $Id: Maper.py  2015-12-04 Qiu $
#
# history:
# 2015-12-04 Qiu   created

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

import sys
import string
import time
import re


# class FiletoRedisMapper(object):
#
#     def __init__(self):
#
#         self.input = sys.stdin
#         # self.input = open('D:/home/zjdx/unbacked_redis_files/kunyan_2015112414')
#
#     def _trans_time(self, TIME):
#
#         string_time = string.atol(TIME) / 1000
#         local_time = time.localtime(string_time)
#         hour = time.strftime('%Y-%m-%d %H', local_time)
#         return hour
#
#     def _visit(self, items):
#
#         keyword = items[0]
#         time_string = items[1]
#         grep = re.search(r'^(((002|000|300|600)[\d]{3})|60[\d]{4})$', keyword)
#         if grep and keyword != '000000':
#             hour = self._trans_time(time_string)
#             print "visit:%s:%s\t%s" % (keyword, hour, 1)
#             print "visit:count:%s\t%s" % (hour, 1)
#             print "hash:visit:%s\t%s:%s" % (hour, keyword, 1)
#             # f.write("visit:%s:%s\t%s\n" % (keyword, hour, 1))
#             # f.write("visit:count:%s\t%s\n" % (hour, 1))
#             # f.write("hash:visit:%s\t%s:%s\n" % (hour, keyword, 1))
#
#     def _search(self, items):
#
#         keyword = items[0]
#         time_string = items[1]
#         length = len(keyword)
#         if length < 4:
#             return
#         if keyword[0].isdigit():
#             if length < 6:
#                 return
#             grep = re.search(r'^(((002|000|300|600)[\d]{3})|60[\d]{4})$', keyword)
#             if grep and keyword != '000000':
#                 hour = self._trans_time(time_string)
#                 print "search:%s:%s\t%s" % (keyword, hour, 1)
#                 print "search:count:%s\t%s" % (hour, 1)
#                 print "hash:search:%s\t%s:%s" % (hour, keyword, 1)
#                 # f.write("search:%s:%s\t%s\n" % (keyword, hour, 1))
#                 # f.write("search:count:%s\t%s\n" % (hour, 1))
#                 # f.write("hash:search:%s\t%s:%s\n" % (hour, keyword, 1))
#         elif keyword[0] == '%':
#             result = re.search(r'%.*%.*%.*%.*%.*%.*%.*%', keyword)
#             if not result:
#                 return
#             keyword = keyword.upper()
#             hour = self._trans_time(time_string)
#             print "search:%s:%s\t%s" % (keyword, hour, 1)
#             print "search:count:%s\t%s" % (hour, 1)
#             print "hash:search:%s\t%s:%s" % (hour, keyword, 1)
#             # f.write("search:%s:%s\t%s\n" % (keyword, hour, 1))
#             # f.write("search:count:%s\t%s\n" % (hour, 1))
#             # f.write("hash:search:%s\t%s:%s\n" % (hour, keyword, 1))
#         elif keyword[0].isalpha():
#             keyword = keyword.lower()
#             hour = self._trans_time(time_string)
#             print "search:%s:%s\t%s" % (keyword, hour, 1)
#             print "search:count:%s\t%s" % (hour, 1)
#             print "hash:search:%s\t%s:%s" % (hour, keyword, 1)
#             # f.write("search:%s:%s\t%s\n" % (keyword, hour, 1))
#             # f.write("search:count:%s\t%s\n" % (hour, 1))
#             # f.write("hash:search:%s\t%s:%s\n" % (hour, keyword, 1))
#
#     def main(self):
#
#         # f = open('D:/home/test_data', 'a+')
#         for line in self.input:
#             line = line.strip()
#             items = line.split()
#             if '0' <= items[2] <= '5':
#                 self._visit(items)
#             elif '6' <= items[2] <= '9':
#                 self._search(items)
#
#
# if __name__ == '__main__':
#     doit = FiletoRedisMapper()
#     doit.main()

def trans_time(time_string):

    string_time = string.atol(time_string) / 1000
    local_time = time.localtime(string_time)
    hour = time.strftime('%Y-%m-%d %H', local_time)
    return hour


def visit(list):

    keyword = list[0]
    time_string = list[1]
    grep = re.search(r'^(((002|000|300|600)[\d]{3})|60[\d]{4})$', keyword)
    if grep and keyword != '000000':
        hour = trans_time(time_string)
        print "visit:%s:%s\t%s" % (keyword, hour, 1)
        print "visit:count:%s\t%s" % (hour, 1)
        print "hash:visit:%s\t%s:%s" % (hour, keyword, 1)


def search(list):

    keyword = list[0]
    time_string = list[1]
    length = len(keyword)
    if length < 4:
        return
    if keyword[0].isdigit():
        if length < 6:
            return
        grep = re.search(r'^(((002|000|300|600)[\d]{3})|60[\d]{4})$', keyword)
        if grep and keyword != '000000':
            hour = trans_time(time_string)
            print "search:%s:%s\t%s" % (keyword, hour, 1)
            print "search:count:%s\t%s" % (hour, 1)
            print "hash:search:%s\t%s:%s" % (hour, keyword, 1)
    elif keyword[0] == '%':
        result = re.search(r'%.*%.*%.*%.*%.*%.*%.*%', keyword)
        if not result:
            return
        keyword = keyword.upper()
        hour = trans_time(time_string)
        print "search:%s:%s\t%s" % (keyword, hour, 1)
        print "search:count:%s\t%s" % (hour, 1)
        print "hash:search:%s\t%s:%s" % (hour, keyword, 1)
    elif keyword[0].isalpha():
        keyword = keyword.lower()
        hour = trans_time(time_string)
        print "search:%s:%s\t%s" % (keyword, hour, 1)
        print "search:count:%s\t%s" % (hour, 1)
        print "hash:search:%s\t%s:%s" % (hour, keyword, 1)

for line in sys.stdin:
# for line in open('D:/home/zjdx/unbacked_redis_files/kunyan_2015112414'):
    items = line.split('\t')
    if '0' <= items[2].strip() <= '5':
        visit(items)
    elif '6' <= items[2].strip() <= '9':
        search(items)
