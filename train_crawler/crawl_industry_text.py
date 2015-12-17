#!/usr/bin/python
# coding: utf-8
#
# crawler_industry_text
# $Id: crawler_industry_text.py  2015-12-14 Qiu $
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
import MySQLdb
import urllib2
from bs4 import BeautifulSoup
from input_output import InputOutput

class CrawlText(object):

    def __init__(self):

        self.from_encoding = 'GBK'
        self.conn = MySQLdb.connect(host='127.0.0.1', user='root',
                                    passwd='root', db='stock',
                                    charset='utf8')
        self.cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        self.input_output = InputOutput(self.conn, self.cursor)

    def _get_html(self, url):

        req = urllib2.Request(url)
        con = urllib2.urlopen(req)
        doc = con.read()
        con.close()
        return doc

    def _get_page_nums(self, soup):

        page_num = soup.find_all('div', attrs={'class':'list_pager'})
        if page_num[0].find_all('a', attrs={'class':'end'}):
            max_page = page_num[0].find_all('a', attrs={'class':'end'})[0]['href']
            max = max_page.split('_')[1].split('.')[0]
        else:
            max = 1
        return int(max)

    def _get_indus_page_list(self, from_encoding):

        sql = "select * from indus_info"
        code_name = self.input_output.get_data(sql)
        indus_page = {}
        for line in code_name:
            temp_url = 'http://field.10jqka.com.cn/list/field/'\
                       + line['indus_code'] + '/'
            doc = self._get_html(temp_url)
            soup = BeautifulSoup(doc, 'html.parser',
                                 from_encoding=from_encoding)
            print line['indus_code'], line['indus_name']
            page_counts = self._get_page_nums(soup)
            page_url_list = []
            for page in range(1, page_counts+1):
                page_url = temp_url + 'index_' + unicode(page) + '.shtml'
                page_url_list.append(page_url)
            indus_page[line['indus_code']] = page_url_list
        return indus_page

    def _get_text_url_list(self, url, from_encoding='utf8'):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
        text_list = soup.find_all('div', attrs={'class':'list_item'})
        url_list = []
        for line in text_list:
            url_list.append(line.a['href'])
        return url_list

    def _get_text_content(self, url, from_encoding):

        doc = self._get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
        grep = re.search(r'http://news', url)
        if grep:
            title_temp = soup.find_all('div', attrs={'class':'art_head'})
            title = title_temp[0].h1.string
            content = ''
            content_temp = soup.find_all('div', attrs={'class':'art_main'})
            content_temp_list = content_temp[0].find_all('p')
            for line in content_temp_list:
                line_temp = str(line)
                s_u_b = re.compile(r'<[^>]+>', re.S)
                content_str = s_u_b.sub('', line_temp)
                content = content_str + '\n' + content
            result = {'title':title, 'content':content}
        else:
            title_temp = soup.find_all('div', attrs={'class':'atc-head'})
            title = title_temp[0].h1.string
            content = ''
            content_temp = soup.find_all('div', attrs={'class':'atc-content'})
            content_temp_list = content_temp[0].find_all('p')
            print url
            for line in content_temp_list:
                line_temp = str(line)
                s_u_b = re.compile(r'<[^>]+>', re.S)
                content_str = s_u_b.sub('', line_temp)
                content = content_str + '\n' + content
            result = {'title':title, 'content':content}
        return result

    def main(self):

        page_url_list = self._get_indus_page_list(self.from_encoding)
        text_with_label = {}
        text_url_list = {}
        for indus_code, page_urls in page_url_list.items():
            text_url_list[indus_code] = []
            for page_url in page_urls:
                text_url_list_temp = self._get_text_url_list(page_url,
                                                             self.from_encoding)
                for text_url in text_url_list_temp:
                    text_content = self._get_text_content(text_url, self.from_encoding)
                    sql = "INSERT INTO indus_text_with_label ('indus_code'," \
                          " 'title', 'content') VALUES ('%s', '%s', '%s')" % \
                          (indus_code, text_content['title'], text_content['content'])
                    self.input_output.insert_data(sql)
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    crawl_industry_text = CrawlText()
    crawl_industry_text.main()




