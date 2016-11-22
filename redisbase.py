# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
#      mail:gqzeng@iflytek.com          #
# --------------------------------------#

from redis import Redis, ConnectionPool


'''
    需要依赖redis模块，现模块信息重写
'''

class RedisBase():
    def __init__(self, host='localhost', port=6379, db=0, ispool=False):
        self.host = host
        self.port = port
        self.db = db
        self.ispool = ispool

    def __retRedis(self):
        if self.ispool is True:
            return Redis(connection_pool=ConnectionPool(host=self.host, port=self.port, db=self.db))
        else:
            return Redis(self.host, self.port, self.db)

    '''
        单个redis操作
    '''
    def getRedisOne(self, key):
        r = self.__retRedis()
        return r.get(key)

    def setRdisOne(self, key, value):
        r = self.__retRedis()
        return r.set(key, value)

    def retInfo(self):
        r = self.__retRedis()
        return r.info()

    def retdbSize(self):
        r = self.__retRedis()
        return r.dbsize()

    def delete(self, key):
        r = self.__retRedis()
        return r.delete(key)

    def getAllkeys(self):
        r = self.__retRedis()
        return r.keys()

    def addone(self, key):
        r = self.__retRedis()
        return r.incr(key)
    '''
     hash操作
    '''
    def sethash(self, name, key, value):
        r = self.__retRedis()
        return r.hset(name, key, value)

    def gethash(self, key):
        r = self.__retRedis()
        return r.hgetall(key)

    def hashaddone(self, name, key, mount=1):
        r = self.__retRedis()
        return r.hincrby(name, key, mount)
    '''
    成员命令操作
    '''
    def sadd(self, name, key):
        r = self.__retRedis()
        return r.sadd(name, key)
    def smember(self, name):
        r = self.__retRedis()
        return r.smembers(name)

    def sinter(self, keys, *args):
        '''
        取交集
        :param keys: 键值
        :param args:
        :return:
        '''
        r = self.__retRedis()
        return r.sinter(keys, *args)

    def sunion(self, keys, *args):
        '''
        取并集
        :param keys:
        :param args:
        :return:
        '''
        r = self.__retRedis()
        return r.sunion(keys, *args)

    '''
        __Pipelines操作
    '''

    def __Pipelines(self):
        return self.__retRedis().pipeline()

    def setpipmany(self, name, key, *args):
        p = self.__Pipelines()
        for v in args:
            p.hset(name, key, v)
        return p.execute()

    def getpipmany(self, name, *args):
        p = self.__Pipelines()
        for v in args:
            p.hget(name, v)
        return p.execute()
