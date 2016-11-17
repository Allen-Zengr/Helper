# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
#      mail:gqzeng@iflytek.com          #
# --------------------------------------#
from mysqlbase import MySQLBase

import datetime,time,hashlib

def md5Encode(str):
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()
def getTimestampFromDatetime(d=None):
    #计算时间差
    if d is None:
        d = datetime.datetime.now()
    return time.mktime(d.timetuple())

start = getTimestampFromDatetime()

data = {
    "id": 11008,
    "statDate": "2016-11-14 16:53:23",
    "totalCount": 298723,
    "hour": 12,
    "appid": "yyapp"
}

msb = MySQLBase("localhost", 3306, "root", "rootroot")
msb.selectDB("musiclog")
'''
for i in range(1000, 2000):
    newData = {
        "id": i,
        "statDate": datetime.datetime.now(),
        "totalCount": i*20,
        "hour": i,
        "appid": "wwwmigu"
    }
    #msb.insert("houroverview", newData)


'''
#print msb.insert("houroverview", data)
#msb.commit()
#print msb.queryAll("select * from houroverview")
#print msb.getLastInsertId()
#print msb.queryOne("select * from houroverview WHERE id > 11003")
#print msb.rowcount()
'''pData = {"hour": 23,
         "appid": "wwwmigu"
         }
whereData={"id": 11004}
'''
#msb.update("houroverview", pData, whereData)
msb.delete("houroverview", {"id":1000})
#print msb.queryOne("select * from houroverview WHERE id = 11004")

msb.close()
end = getTimestampFromDatetime()
print('time: {0}s'.format(end-start))