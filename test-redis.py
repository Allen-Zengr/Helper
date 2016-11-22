# coding:utf-8
# --------------------------------------#
#     人的活动如果没有理想的鼓舞        #
#      就会变得空虚而渺小               #
#      Author：曾广巧                   #
#      mail:gqzeng@iflytek.com          #
# --------------------------------------#

from redisbase import RedisBase

rb = RedisBase('localhost', 6379, 0, True)

'''
单个数据操作及提取信息
#print rb.getRedisOne("email")
#print rb.setRdisOne("sex", "1")
#print rb.retInfo()
#print rb.retdbSize()
#print rb.addone("sex")
'''

'''
    :hash表操作
print rb.getAllkeys()
print rb.sethash("user:name", "phone", "13100010001")
print rb.gethash("user:name")
{'phone': '13100010001', 'visit': '1', 'email': 'ilove@sina.com'}
print rb.hashaddone("user:name", "visit", 1)
print rb.gethash("user:name")
{'phone': '13100010001', 'visit': '2', 'email': 'ilove@sina.com'}
'''

'''

    member操作

rb.sadd('circle:game:lol','user:debugo')
rb.sadd('circle:game:lol','user:leo')
rb.sadd('circle:game:lol','user:Guo')
rb.sadd('circle:soccer:InterMilan','user:Guo')
rb.sadd('circle:soccer:InterMilan','user:Levis')
rb.sadd('circle:soccer:InterMilan','user:leo')

print rb.smember("circle:game:lol")
print rb.sinter("circle:game:lol","circle:soccer:InterMilan")
print rb.sunion("circle:game:lol","circle:soccer:InterMilan")
'''

'''
Pipelines 操作
keys = ['01102014', '01112014', '01122014', '01132014', '01142014', '01152014', '01162014']
#print rb.setpipmany("some:key", "key", keys)
print rb.getpipmany("some:key", "key")
'''