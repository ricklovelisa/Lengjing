#!/usr/bin/python
# coding: utf-8
#
# input_output
# $Id: input_output.py  2015-12-14 Qiu $
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


class InputOutput(object):

    def __init__(self, conn, cursor):

        self.conn = conn
        self.cursor = cursor

    def get_data(self, sql):

        try:
            self.cursor.execute(sql)
        except Exception, e:
            print e
        result = self.cursor.fetchall()
        return result

    def update_data(self, sql):

        try:
            self.cursor.execute(sql)
        except Exception, e:
            print e
        self.conn.commit()

    def insert_data(self, sql, url):

        try:
            self.cursor.execute(sql)
        except Exception, e:
            print e
        self.conn.commit()
