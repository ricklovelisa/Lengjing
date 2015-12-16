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

# from operator import itemgetter
import sys
import re

# class FiletoRedisReducer(object):
#
#     def __init__(self):
#
#         self.input = sys.stdin
#         # self.input = open("D:/home/test_data")
#
#     def main(self):
#
#         key2count = {}
#         for line in self.input:
#             line = line.strip()
#             key, value = line.split('\t', 1)
#             hash_grep = re.search('^hash:', key)
#             if hash_grep:
#                 print "%s\t%s" % (key, value)
#             else:
#                 try:
#                     count = int(value)
#                     key2count[key] = key2count.get(key, 0) + count
#                 except ValueError:
#                     pass
#         for key, count in key2count.items():
#             print '%s\t%s' % (key, count)
#
#
# if __name__ == '__main__':
#     doit = FiletoRedisReducer()
#     doit.main()

key2count = {}
for line in sys.stdin:
# for line in open('D:/home/test_data'):
    line = line.strip()
    key, value = line.split('\t', 1)
    hash_grep = re.search('^hash:', key)
    if hash_grep:
        print "%s\t%s" % (key, value)
    else:
        try:
            count = int(value)
            key2count[key] = key2count.get(key, 0) + count
        except ValueError:
            pass
for key, count in key2count.items():
    print '%s\t%s' % (key, count)


