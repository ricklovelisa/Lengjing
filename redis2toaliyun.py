import redis

r1 = redis.StrictRedis(host='192.168.0.2',port=6379,db=1,password='kunyandata')
p1 = r1.pipeline()
stock_codes = r1.lrange('stock:list', 0, -1)
r2 = redis.StrictRedis(host='120.55.189.211',port=6379,db=0,password='kunyandata')
p2 = r2.pipeline()
name_urls = []
jian_pins = []
quan_pins = []
for stock_code in stock_codes:
    r2.set('stock:%s:nameurl'% stock_code, r1.get('stock:%s:nameurl' % stock_code))
    r2.set('stock:%s:jianpin'% stock_code, r1.get('stock:%s:jianpin' % stock_code))
    r2.set('stock:%s:quanpin'% stock_code, r1.get('stock:%s:quanpin' % stock_code))
p1.execute()
p2.execute()