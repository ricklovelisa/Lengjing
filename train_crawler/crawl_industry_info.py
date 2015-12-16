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

import MySQLdb
import urllib2
from bs4 import BeautifulSoup
from input_output import InputOutput

class CrawlIndustryInfo(object):

    def __init__(self):

        self.seed_url = 'http://q.10jqka.com.cn/stock/thshy/'
        self.conn = MySQLdb.connect(host='127.0.0.1', user='root',
                                    passwd='root', db='stock',
                                    charset='utf8')
        self.input_outpu = InputOutput(self.conn)

    def _get_html(self, url):

        req = urllib2.Request(url)
        con = urllib2.urlopen(req)
        doc = con.read()
        con.close()
        return doc

    def _get_industry_code_list(self, url, from_encoding='utf8'):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
        result_temp = soup.find_all('div', attrs={'class':'cate_items'})
        industry_urls, result = [], {}
        for line in result_temp:
            for item in line.find_all('a'):
                industry_urls.append(item['href'])
        for industry_url in industry_urls:
            doc = self._get_html(industry_url)
            soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
            result_temp = soup.find_all('div', attrs={'class':'stock_name'})
            name = result_temp[0].h2.string
            code = result_temp[0].input['value']
            result[code] = name
        return result

    def main(self):

        code_name = self._get_industry_code_list(self.seed_url, from_encoding='gbk')
        for k, v in code_name.items():
            sql = "insert into indus_info (indus_code, indus_name) value ('%s', '%s')" % (k, v)
            print k, v
            self.input_outpu.insert_data(sql)
        self.conn.close()

if __name__ == '__main__':
    crawl_indsutry_info = CrawlIndustryInfo()
    crawl_indsutry_info.main()






