import urllib2
import MySQLdb
# from input_output import InputOutput
from bs4 import BeautifulSoup

from_encoding = 'GBK'
conn = MySQLdb.connect(host='127.0.0.1', user='root',
                       passwd='root', db='stock',
                       charset='utf8')
cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

def _get_html(url):

        req = urllib2.Request(url)
        con = urllib2.urlopen(req)
        doc = con.read()
        con.close()
        return doc

def _get_page_nums(soup):

        page_num = soup.find_all('div', attrs={'class':'list_pager'})
        if page_num[0].find_all('a', attrs={'class':'end'}):
            max_page = page_num[0].find_all('a', attrs={'class':'end'})[0]['href']
            max = max_page.split('_')[1].split('.')[0]
        else:
            max = 1
        return int(max)

def _get_indus_page_list(from_encoding):

        sql = "select * from indus_info"
        code_name = input_output.get_data(sql)
        indus_page = {}
        for line in code_name:
            temp_url = 'http://field.10jqka.com.cn/list/field/'\
                       + line['indus_code'] + '/'
            doc = _get_html(temp_url)
            soup = BeautifulSoup(doc, 'html.parser',
                                 from_encoding=from_encoding)
            print line['indus_code'], line['indus_name']
            page_counts = _get_page_nums(soup)
            page_url_list = []
            for page in range(1, page_counts+1):
                page_url = temp_url + 'index_' + unicode(page) + '.shtml'
                page_url_list.append(page_url)
            indus_page[line['indus_code']] = page_url_list
        return indus_page

def _get_text_url_list(url, from_encoding='utf8'):

        doc = _get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
        text_list = soup.find_all('div', attrs={'class':'list_item'})
        url_list = []
        for line in text_list:
            url_list.append(line.a['href'])
        return url_list

def _get_text_content(url, from_encoding):

        doc = _get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
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