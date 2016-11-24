# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
#      mail:gqzeng@iflytek.com          #
# --------------------------------------#
from mysqlbase import MySQLBase
from mongodbase import MongodBase
import datetime, time


class MysqlConvertMongo():
    def __getTimestampFromDatetime(self, d=None):
        #计算时间差
        if d is None:
            d = datetime.datetime.now()
        return time.mktime(d.timetuple())

    def __init__(self, mysqlhost='localhost',
                 mysqlport=3306,
                 mysqluser='root',
                 mysqlpasswd='rootroot',
                 mysqldb=None,
                 mysqltable=None,
                 mongohost='localhost',
                 mongoport=27017,
                 mongodb=None,
                 mongocoll=None):
        '''
        初始化函数
        :param mysqlhost:mysql的服务地址，默认为'localhost'
        :param mysqlport:mysql的服务端口,默认为3306
        :param mysqluser:mysql登录用户名
        :param mysqlpasswd:mysql登录密码
        :param mysqldb:选择数据库名，可以为空，需要后面指定
        :param mysqltable:选择的表名，可以为空，需要后面指定
        :param mongohost:mongodb的服务地址,默认为'localhost'
        :param mongoport:mongodb的服务端口,默认为27017
        :param mongodb:mongodb的数据库名，可以为空，需要后面指定
        :param mongocoll:mongodb的集合名，可以为空，需要后面指定
        :return:
        '''
        self.mysb = MySQLBase(mysqlhost, mysqlport, mysqluser, mysqlpasswd, mysqldb)
        self.mb = MongodBase(mongohost, mongoport, mongodb, mongocoll)
        self.mysqltable = mysqltable
        self.start = self.__getTimestampFromDatetime()

    def mysqlConvertMongo(self, sql=""):
        '''
        mysql转换mongo的方法
        1、sql默认参数为空时执行mysql全表扫描，若不为空时可按条件查询后插入到指定mongo集合中
        2、由于mysql查询返回的是字典类型，可直接调用mongo的批量插入方法执行插入
        :param sql:条件sql
        :return:
        '''
        if sql == "" and self.mysqltable is not None:
            self.mb.insert_many(self.mysb.queryAll("SELECT * FROM  %s;" % self.mysqltable))
        else:
            self.mb.insert_many(self.mysb.queryAll(sql))
        end = self.__getTimestampFromDatetime()
        print('Insert suc. Time: {0}s'.format(end-self.start))


#mcm = MysqlConvertMongo('localhost', 3306, 'root', 'rootroot', 'musiclog','houroverview1','localhost', 27017, 'mysql', 'test')
#mcm.mb.delete_coll()
#mcm.mysqlConvertMongo()
#从mysql读取数据后插入到mongodb 15000条耗时1秒