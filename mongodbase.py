# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
#      mail:gqzeng@iflytek.com          #
# --------------------------------------#
'''
    需要依赖pymongo, logging 模块
'''

import pymongo
import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='Mongodbase.log',
                filemode='w')
class MongodBase():
    def __init__(self, host, port, databases, collection):
        '''
        初始化函数
        :param host:mongodb的服务地址
        :param port:mongodb的服务端口
        :param databases:库名
        :param collection:集合名
        :return:初始化结果
        '''
        self.__host = host
        self.__port = port
        self.databases = databases
        self.collection = collection
        logging.info("connect:"+str(self.__host)+":"+str(self.__port)+"<"+self.databases+"-->"+self.collection+">")

    def __getMongodb(self):
        '''
        连接并获取库
        :return:返回库的对象
        '''
        con = pymongo.MongoClient(host=self.__host, port=self.__port)
        db = con[self.databases]
        return db

    def __getColl(self):
        '''
        获取当前传入的结合名
        :return:返回结合对象
        '''
        db = self.__getMongodb()
        coll = db[self.collection]
        return coll

    def is_dict(self, mydict):
        '''
        判断是否为字典类型
        :param mydict: 插入的字典
        :return: 返回判断结果
        '''
        if type(mydict)is dict:
            return True
        else:
            return False
    def is_List(self, mylist):
        '''
        判断是否为列表类型
        :param mylist: 插入的列表
        :return: 返回判断结果
        '''
        if type(mylist)is list:
            return True
        else:
            return False

    def insert_one(self, args):
        '''
        插入一条数据
        :param args:插入数据
        :return: 返回插入结果
        '''
        db = self.__getColl()
        if self.is_dict(args):
            db.insert_one(args)
            logging.info("insert_one:"+str(args))
            return True
        else:
            return False
    def insert_many(self, args):
        '''
        批量插入数据
        :param args:插入数据为列表,例如：args=[{"name":1},{"name":2}]
        :return: 返回插入结果
        '''
        db = self.__getColl()
        if self.is_List(args) is True:
            db.insert_many(args)
            logging.info("insert_many:"+str(args))
            return True
        else:
            return False

    def update_one(self, args, update):
        '''
        修改一条数据
        :param args: 传参{"name":"OK"}如where 条件必须为字典类型
        :param update: 传参{ "$set": {"name" : "1"} }set需要修改的内容
        :return: 返回执行结果
        '''
        db = self.__getColl()
        if self.is_dict(args) and self.is_dict(update):
            db.update_one(args, update)
            logging.info("update_one:"+str(args))
            return True
        else:
            return False

    def update_many(self, args, update, upsert=True, multi=True):
        '''
        :param args: 传参{"name":"OK"}如where 条件必须为字典类型
        :param update: 传参{ "$set": {"name" : "1"} }set需要修改的内容
        :param upsert:默认为True全部更新，若全为False则只更新一条
        :param multi:默认为True全部更新，若全为True则全部更新
        :return:返回执行结果
        '''
        db = self.__getColl()
        if self.is_dict(args) and self.is_dict(update):
            db.update_many(args, update, upsert, multi)
            logging.info("update_one:"+str(args))
            return True
        else:
            return False

    def find_many(self, args):
        '''
        查询批量数据不进行排序
        :param args:按条件查询数据不进行排序默认为降序，请求方式为 {"name": {"$lt": 100}}
        :return:items
        '''
        db = self.__getColl()
        if self.is_dict(args):
            items = db.find(args)
            logging.info("find:"+str(args))
            return items
        else:
            return None

    def find_many_desc(self, args, sort, desc=1):
        '''
        查询数据并排序。排序规则：1为降序，其他为升序，默认为降序
        请求方式为 {"name": {"$lt": 100}}, "_id", 1
        可根据条件进行查询，但条件需为字典dict类型，
        可根据某个字段进行排序，直接输入字段名即可
        返回items
        '''
        db = self.__getColl()
        if self.is_dict(args):
            if desc == 1:
                itme = db.find(args).sort([(sort, pymongo.DESCENDING)])
                logging.info("find:"+str(args)+"and"+str(sort)+"DESCENDING")
            else:
                itme = db.find(args).sort([(sort, pymongo.ASCENDING)])
                logging.info("find:"+str(args)+"and"+str(sort)+"ASCENDING")
            return itme
        else:
            return None

    def find_count(self, args):
        '''
        按条件查询返回结果条数
        :param args:查询条件
        :return:返回条数 int
        '''
        #查询数据返回数据条数
        db = self.__getColl()
        if self.is_dict(args):
            num = db.find(args).count()
            logging.info("find:"+str(args)+":"+str(num))
            return num
        else:
            return None

    def find_all_count(self):
        '''
        全量返回集合的所有条数
        :return: 返回条数 int
        '''
        db = self.__getColl()
        num = db.count()
        logging.info("findall:"+str(num))
        return num

    def delete_where(self, args):
        '''
        按条件删除内容
        :param args:传入删除条件
        :return:返回删除结果
        '''
        db = self.__getColl()
        if self.is_dict(args):
            db.remove(args)
            logging.info("Remove:"+str(args))
            return True
        else:
            return None

    def delete_coll(self):
        '''
        删除整个集合
        :return:返回执行结果
        '''
        db = self.__getColl()
        db.remove()
        logging.info("Remove:"+str(db))
        return True

    def create_IndexOnlyOne(self, args, uniq=True):
        '''
        :param args: 创建索引名 str类型
        :param uniq: 唯一性默认为True
        :return:返回执行结果
        '''
        db = self.__getColl()
        rt = db.ensure_index(args, unique=uniq)
        logging.info("Create Index OnlyOne:"+args)
        return rt

    def create_Index(self, args):
        '''
        from pymongo import IndexModel, ASCENDING, DESCENDING
        index1 = IndexModel([("hello", DESCENDING),
                              ("world", ASCENDING)], name="hello_world")
        index2 = IndexModel([("goodbye", DESCENDING)])
        db.test.create_indexes([index1, index2])
        :param args:index的对象，需要是传入的列表。
        :return:结果
        '''
        db = self.__getColl()
        if self.is_List(args):
            db.create_indexes(args)
            logging.info("Create Index:Suc")
            return True
        else:
            return False

    def drop_Index(self, args):
        '''
        删除索引
        :param args:删除索引名
        :return:返回结果
        '''
        db = self.__getColl()
        if type(args) == str:
            for index in db.index_information().keys():
                if index == args:
                    db.drop_index(args)
                    logging.info("Drop Index:"+args+" Suc")
                    return True
            logging.info("Drop Index:"+args+" Fail,is Not exits.")
            return False
        else:
            return False
