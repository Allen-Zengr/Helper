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


class MongoConvertMysql():
    '''
    mongodb向mysql转换数据
    '''

    def __getTimestampFromDatetime(self, d=None):
        '''
        该方法为计算总耗时的方法
        :param d:
        :return:
        '''
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

    def __delmysqlInsert(self, val):
        '''
        处理从mongo读取数据后value的构建，例如：('1000','1000'),('1001','1001')
        :param val:传入原始字典
        :return:返回构造后的字符串
        '''

        tmpStr = ""
        for v in val:
           tmpStr += self.mysb.dealInsertStr(v)
        return tmpStr[:-1]#去除末尾的逗号

    def mongoConvertMysql(self):
        '''
        转换方法
        1、find_manyforMysql_desc()获取mongo的数据，参数为空时，返回当前集合\
            按降序排列的剔除了'_id'字段的字典数据
        2、获取到插入的key键值
        3、构建value的字符串
        4、执行数据插入
        :return:
        '''
        val = self.mb.find_manyforMysql_desc()
        key = ','.join(val[0].keys())
        ret_val = self.__delmysqlInsert(val)
        ret_sql = "INSERT INTO " + self.mysqltable + " (" + key + ") VALUES  " + ret_val + " "
        self.mysb.execsql(ret_sql)
        self.mysb.commit()
        end = self.__getTimestampFromDatetime()
        print('Insert suc. Time: {0}s'.format(end-self.start))

#从mongo转换mysql；15000条耗时2s
#mcm = MongoConvertMysql('localhost', 3306, 'root', 'rootroot', 'musiclog','houroverview1','localhost', 27017, 'mysql', 'test')
#mcm.mongoConvertMysql()