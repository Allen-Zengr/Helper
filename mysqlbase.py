# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
#      mail:gqzeng@iflytek.com          #
# --------------------------------------#
import MySQLdb
import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='mysqlbase.log',
                filemode='w')

'''
     依赖MySQLdb,logging模块
'''
class MySQLBase():
    def __init__(self, host, port, user, password, charset="utf8"):
        '''
        初始化
        :param host:mysql服务的ip地址
        :param port:mysql服务的端口
        :param user:连接用户名
        :param password:连接密码
        :param charset:字符集默认utf-8
        :return:初始化连接
        '''
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__charset = charset
        try:
            self.__con = MySQLdb.connect(host=self.__host, user=self.__user, passwd=self.__password)
            self.__con.set_character_set(self.__charset)
            self.__cur = self.__con.cursor()
        except MySQLdb.Error as e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def selectDB(self, db):
        '''
        选择数据库
        :param db:数据库名
        :return:无返回
        '''
        try:
            self.__con.select_db(db)
        except MySQLdb.Error as e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def __execSQL(self, sql):
        '''
        执行sql语句的方法
        :param sql: sql语句
        :return: 返回执行结果
        '''
        try:
            ret_val = self.__cur.execute(sql)
            return ret_val
        except MySQLdb.Error as e:
            print "Mysql Error %s: %s" % (e, sql)
            logging.error("Mysql Error %s: %s" % (e, sql))

    def queryOne(self, sql):
        '''
        查询返回一个值
        :param sql: sql语句
        :return: 返回执行结果
        '''
        self.__execSQL(sql)
        ret_val = self.__cur.fetchone()
        return ret_val

    def queryAll(self, sql):
        '''
        查询数据返回全部数据
        :param sql:sql语句
        :return:返回字典，如：[{'totalCount': '298723', 'id': '11002'}]
        '''
        try:
            self.__execSQL(sql)
            result = self.__cur.fetchall()
            desc = self.__cur.description
            ret_val = []
            for inv in result:
                tmp = {}
                for i in range(0, len(inv)):
                    tmp[desc[i][0]] = str(inv[i])
                ret_val.append(tmp)
            return ret_val
        except MySQLdb.Error as e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def insert(self, p_table_name, p_data):
        '''
        插入
        :param p_table_name:表名
        :param p_data: 数据需是字典类型，如：{"id": 11008,"totalCount": 298723}
        :return: 返回执行结果
        '''
        try:
            for key in p_data:
                p_data[key] = "'"+str(p_data[key])+"'"
            key = ','.join(p_data.keys())
            value = ','.join(p_data.values())
            ret_sql = "INSERT INTO " + p_table_name + " (" + key + ") VALUES (" + value + ")"
            self.__execSQL(ret_sql)
            self.commit()
            return True
        except MySQLdb.Error as e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def update(self, tableName, p_Data, where_Data):
        '''
        更新修改
        :param tableName:表名
        :param p_Data:修改的数据字典 {"hour": 23,"appid": "wwwmigu"}
        :param where_Data:where语句字典 {"id": 11004}
        :return:返回执行结果
        '''
        try:
            setData = []
            keys = p_Data.keys()
            for i in keys:
                item = "%s=%s" % (i, "'"+str(p_Data[i])+"'")
                setData.append(item)
            items = ','.join(setData)
            whereData = []
            keys = where_Data.keys()
            for i in keys:
                item = "%s=%s" % (i, "'"+str(where_Data[i])+"'")
                whereData.append(item)
            whereItems = " AND ".join(whereData)
            sql = "UPDATE "+tableName+" SET "+items+" WHERE "+whereItems
            self.__execSQL("set names 'utf8'")
            self.__execSQL(sql)
            self.commit()
            return True
        except MySQLdb.Error as e:
            print('MySql Error: %s %s' % (e.args[0], e.args[1]))
            logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            return False

    def delete(self, p_table, p_where):
        '''
        删除
        :param p_table:表名
        :param p_where:where语句必须为字典类型：{"id": 11004}
        :return:返回执行结果
        '''
        try:
            whereData = []
            keys = p_where.keys()
            for i in keys:
                item = "%s=%s" % (i, "'"+str(p_where[i])+"'")
                whereData.append(item)
            whereItem = " AND ".join(whereData)
            sql = "DELETE FROM "+p_table+" WHERE "+whereItem
            self.__execSQL(sql)
            self.commit()
            return True
        except MySQLdb.Error as e:
            print('MySql Error: %s %s'% (e.args[0], e.args[1]))
            logging.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            return False

    def execsql(self, sql):
        '''
        执行sql，可以建表等等其他操作
        :param sql:sql语句
        :return:返回执行结果
        '''
        return self.__execSQL(sql)

    def rowcount(self):
        '''
        返回当前游标执行语句的行数
        :return:返回int
        '''
        return self.__cur.rowcount

    def getLastInsertId(self):
        '''
        返回当前执行语句的最后一个id
        :return:返回int
        '''
        return self.__cur.lastrowid

    def commit(self):
        '''
        执行提交
        '''
        self.__con.commit()

    def close(self):
        '''
        关闭游标和连接
        '''
        self.__cur.close()
        self.__con.close()








