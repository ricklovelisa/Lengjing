#!/usr/bin/python
# coding: utf-8
#
# crawler
# $Id: crawler.py  2015-12-14 Qiu $
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

import re
import urllib2
from bs4 import BeautifulSoup

url = r'http://index.10jqka.com.cn/list/field/0881155/'
url = 'http://q.10jqka.com.cn/stock/thshy/'
req = urllib2.Request(url)
con = urllib2.urlopen(req)
doc = con.read()
con.close()

# 解析网页内容
soup = BeautifulSoup(doc, "html.parser", from_encoding='gbk')
text_list = soup.find_all('div', attrs={'class':'list_item'})
url_list = []
for line in text_list:
    url_list.append(line.a['href'])


class TrainDataCrawler(object):

    def __init__(self, seed_url):

        self.seed_url = seed_url
        self.from_encoding = 'GBK'
        self.parser = 'html.parser'

    def _get_html(self, url):

        req = urllib2.Request(url)
        con = urllib2.urlopen(req)
        doc = con.read()
        con.close()
        return doc

    def _get_page_nums(self, soup):

        page_num = soup.find_all('div', attrs={'class':'list_pager'})
        num = 0
        for i in page_num[0].find_all('a'):
            n = i.string
            if n.isdigit() and n > num:
                num = n
        return num

    def _get_text_url_list(self, url, parser='html.parser', from_encoding='utf8'):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, parser, from_encoding)
        text_list = soup.find_all('div', attrs={'class':'list_item'})
        url_list = []
        for line in text_list:
            url_list.append(line.a['href'])
        return url_list

    def _get_industry_page_list(self, url, code, parser='html.parser', from_encoding='utf8'):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, parser, from_encoding)
        page_nums = self._get_page_nums(soup)
        url_list = []
        for i in range(page_nums):
            url_temp = url + i + '.shtml'
            url_list.append(url_temp)
        return url_list

    def _get_text_content(self, url, parser='html.parser', from_encoding='utf8'):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, parser, from_encoding)
        text = soup.find_all('div', attrs={'id':'J_article'})
        head_temp = text[0].find_all('div', attrs={'class':'art_head'})
        head = head_temp[0].h1.string
        content = []
        content_temp = text[0].find_all('div', attrs={'class':'art_cnt'})
        content_temp = content_temp[0].find_all('p')
        for line in content_temp:
            content.append(line.string)
        result = {'head':head, 'content':content}
        return result

    def _get_industry_code_list(self, parser='html.parser', from_encoding='utf8'):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
        result_temp = soup.find_all('div', attrs={'class':'cate_items'})
        industry_urls = []
        result = []
        for line in result_temp:
            for item in line.find_all('a'):
                industry_urls.append(item['href'])
        for industry_url in industry_urls:
            doc = self._get_html(industry_url)
            soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
            result_temp = soup.find_all('input', attrs={'id':'gn_code'})
            result.append(result_temp[0]['value'])
        return result

    def main(self):

        article = {}
        industry_code = self._get_industry_code_list(self.parser, self.from_encoding)
        for code in industry_code:
            code_url = r'http://index.10jqka.com.cn/list/field/' + code
            page_url_list = self._get_industry_page_list(code_url, code,
                                                         from_encoding=self.from_encodin)
            for
            for text_url in text_url_list:
                text = self._get_text_content(text_url,from_encoding=self.from_encoding)












