import urllib2
from bs4 import BeautifulSoup

seed_url = 'http://q.10jqka.com.cn/stock/thshy/'
from_encoding = 'gbk'

def _get_html(url):

        req = urllib2.Request(url)
        con = urllib2.urlopen(req)
        doc = con.read()
        con.close()
        return doc

def _get_page_nums(soup):

        page_num = soup.find_all('div', attrs={'class':'list_pager'})
        num = 0
        for i in page_num[0].find_all('a'):
            n = i.string
            if n.isdigit() and n > num:
                num = n
        return num

def _get_text_url_list( url, from_encoding='utf8'):

        doc = _get_html(url)
        soup = BeautifulSoup(doc, parser=parser, from_encoding=from_encoding)
        text_list = soup.find_all('div', attrs={'class':'list_item'})
        url_list = []
        for line in text_list:
            url_list.append(line.a['href'])
        return url_list

def _get_industry_page_list(url, code, parser='html.parser', from_encoding='utf8'):

        doc = _get_html(url)
        soup = BeautifulSoup(doc, parser, from_encoding)
        page_nums = _get_page_nums(soup)
        url_list = []
        for i in range(page_nums):
            url_temp = url + i + '.shtml'
            url_list.append(url_temp)
        return url_list

def _get_text_content(url, parser='html.parser', from_encoding='utf8'):

        doc = _get_html(url)
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

def _get_industry_code_list(url, from_encoding='utf8'):

        doc = _get_html(url)
        soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
        result_temp = soup.find_all('div', attrs={'class':'cate_items'})
        industry_urls, result = [], {}
        for line in result_temp:
            for item in line.find_all('a'):
                industry_urls.append(item['href'])
        for industry_url in industry_urls:
            doc = _get_html(industry_url)
            soup = BeautifulSoup(doc, 'html.parser', from_encoding=from_encoding)
            result_temp = soup.find_all('div', attrs={'class':'stock_name'})
            name = result_temp[0].h2.string
            code = result_temp[0].input['value']
            result[code] = name
        return result