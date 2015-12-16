#coding: utf-8
import redis
import MySQLdb
server_ip = '192.168.0.2'
server_port = 6379
redis1 = redis.StrictRedis(host=server_ip, port=server_port,
                           db=0, password='kunyandata')
stock_codes = redis1.lrange('stock:list', 0, -1)
try:
    conn = MySQLdb.connect(host='120.55.189.211', user='root',
                                        passwd='hadoop', db='stock', charset='utf8')
except Exception, e:
    print e
cursor = conn.cursor()

for stock_code in stock_codes:
    name = redis1.get('stock:%s:name'% stock_code)
    name_url = redis1.get('stock:%s:nameurl'% stock_code)
    jian_pin = redis1.get('stock:%s:jianpin'% stock_code)
    quan_pin = redis1.get('stock:%s:quanpin'% stock_code)
    print name
    sql = "insert into stock_info(v_code, v_name, v_name_url, v_jian_pin, v_quan_pin) values ('%s', '%s', '%s', '%s', '%s')" % (stock_code, name, name_url, jian_pin, quan_pin)
    try:
        cursor.execute(sql)
    except Exception, e:
        print e

conn.commit()
cursor.close()
conn.close()
