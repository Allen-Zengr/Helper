# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
# int n nId、float f fId、bool b bIsOK  #
# --------------------------------------#

from mongodbase import MongodBase
import datetime,time,hashlib
from pymongo import IndexModel, ASCENDING, DESCENDING

def md5Encode(str):
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()
def getTimestampFromDatetime(d=None):
    #计算时间差
    if d is None:
        d = datetime.datetime.now()
    return time.mktime(d.timetuple())

mb = MongodBase("127.0.0.1", 27017, "mytest", "mydata")
start = getTimestampFromDatetime()


def deletecoll():
    mb.delete_coll()
posts=[]
def insertTest():
    #测试插入数据
    for i in range(1000):
        data = {
            "name": md5Encode(str(i))
        }
        posts.append(data)
    mb.insert_many(posts)
def printCount():
   print mb.find_count({"name":100})

def deletewhere():
    print mb.delete_where({"name":100})
#printCount()

def findmany():
    item = mb.find_many({"name": {"$lt": 100}})
    for i in item:
        print i
#deletewhere()
#findmany()

def updateone():
    mb.update_many({"name":"OK"}, { "$set": {"name" : "1"} }, True, True)
#updateone()

#insertTest()
index1 = IndexModel([("name", DESCENDING), ("_id", ASCENDING)], name="names")
#print mb.create_Index([index1])
#print mb.drop_Index("names")
end = getTimestampFromDatetime()
print('time: {0}s'.format(end-start))