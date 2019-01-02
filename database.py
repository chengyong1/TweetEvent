from pymongo import *
import json
import sys
import time
import datetime

'''
                      :;J7, :,                        ::;7:
                      ,ivYi, ,                       ;LLLFS:
                      :iv7Yi                       :7ri;j5PL
                     ,:ivYLvr                    ,ivrrirrY2X,
                     :;r@Wwz.7r:                :ivu@kexianli.
                    :iL7::,:::iiirii:ii;::::,,irvF7rvvLujL7ur
                   ri::,:,::i:iiiiiii:i:irrv177JX7rYXqZEkvv17
                ;i:, , ::::iirrririi:i:::iiir2XXvii;L8OGJr71i
              :,, ,,:   ,::ir@mingyi.irii:i:::j1jri7ZBOS7ivv,
                 ,::,    ::rv77iiiriii:iii:i::,rvLq@huhao.Li
             ,,      ,, ,:ir7ir::,:::i;ir:::i:i::rSGGYri712:
           :::  ,v7r:: ::rrv77:, ,, ,:i7rrii:::::, ir7ri7Lri
          ,     2OBBOi,iiir;r::        ,irriiii::,, ,iv7Luur:
        ,,     i78MBBi,:,:::,:,  :7FSL: ,iriii:::i::,,:rLqXv::
        :      iuMMP: :,:::,:ii;2GY7OBB0viiii:i:iii:i:::iJqL;::
       ,     ::::i   ,,,,, ::LuBBu BBBBBErii:i:i:i:i:i:i:r77ii
      ,       :       , ,,:::rruBZ1MBBqi, :,,,:::,::::::iiriri:
     ,               ,,,,::::i:  @arqiao.       ,:,, ,:::ii;i7:
    :,       rjujLYLi   ,,:::::,:::::::::,,   ,:i,:,,,,,::i:iii
    ::      BBBBBBBBB0,    ,,::: , ,:::::: ,      ,,,, ,,:::::::
    i,  ,  ,8BMMBBBBBBi     ,,:,,     ,,, , ,   , , , :,::ii::i::
    :      iZMOMOMBBM2::::::::::,,,,     ,,,,,,:,,,::::i:irr:i:::,
    i   ,,:;u0MBMOG1L:::i::::::  ,,,::,   ,,, ::::::i:i:iirii:i:i:
    :    ,iuUuuXUkFu7i:iii:i:::, :,:,: ::::::::i:i:::::iirr7iiri::
    :     :rk@Yizero.i:::::, ,:ii:::::::i:::::i::,::::iirrriiiri::,
     :      5BMBBBBBBSr:,::rv2kuii:::iii::,:i:,, , ,,:,:i@petermu.,
          , :r50EZ8MBBBBGOBBBZP7::::i::,:::::,: :,:,::i;rrririiii::
              :jujYY7LS0ujJL7r::,::i::,::::::::::::::iirirrrrrrr:ii:
           ,:  :@kevensun.:,:,,,::::i:i:::::,,::::::iir;ii;7v77;ii;i,
           ,,,     ,,:,::::::i:iiiii:i::::,, ::::iiiir@xingjief.r;7:i,
        , , ,,,:,,::::::::iiiiiiiiii:,:,:::::::::iiir;ri7vL77rrirri::
         :,, , ::::::::i:::i:::i:i::,,,,,:,::i:i:::iir;@Secbone.ii:::
'''
class Mymongo():

    def __init__(self, url, database=False, collection=False):
        self.cleanWords = ['comment', 'studio', 'job', 'hire', 'hiring', 'humidity', 'rain', 'wind', 'temperature', 'CO₂', "hum", "internship"]
        self.url = url
        self.client = MongoClient(self.url) # 创建连接
        self.dbList = self.client.database_names()
        if(database==False and collection==False):
            for i, data in enumerate(self.dbList):
                print(i + 1, data, end=' ' * (22 - len(data)))
                if ((i + 1) % 5 == 0):
                    print(end='\n')
            n = input("\n#####请选择你想要操作的数据库编号(数字即可)#####: ")
            self.db = self.client[self.dbList[int(n) - 1]]
            print("进入数据库--->", self.db.name)
            self.collectionList = self.db.collection_names()
            for i, data in enumerate(self.collectionList):
                print(i + 1, data)
            n = input("\n#####请选择你想要操作的集合编号(数字即可)#####: ")
            self.collection = self.db[self.collectionList[int(n) - 1]]
            print("进入集合--->", self.collection.name)
        else:
            self.db = self.client[database]
            self.collection = self.db[collection]

    '''删除当前集合'''
    def drop(self):
        self.collection.drop()

    '''查看当前数据库信息'''
    def showdb(self):
        print("当前数据库包含的集合有:", self.db.collection_names())

    '''查看当前集合的基本信息'''
    def info(self):
        if(self.count() == 0):
            print("集合为空!")
            return None
        print("所属数据库:", self.db.name)
        print("集合里面数据量:", self.count())
        print("集合里面数据的属性:", self.attribute())
        print("举例一条数据:", self.find_one())

    '''查看集合里面数据量'''
    def count(self):
        return self.collection.count()

    '''查看集合中一条数据'''
    def find_one(self):
        if(self.collection.count() == 0):
            print("集合中没有数据")
            return None
        return self.collection.find_one()

    '''插入数据'''
    def insert_one(self, item):
        self.collection.insert_one(item)

    '''插入多条数据'''
    def insert_many(self, data):
        self.collection.insert_many(data)

    '''将数据转入到其他账户上, 默认为本地'''
    def transfer (self, database, collection, cursor, url='localhost:27017'):
        client = MongoClient(url)
        db = client[database]
        collection = db[collection]
        Num = self.collection.count()
        count = 0
        begin_time = time.clock()
        for item in cursor:
            collection.insert_one(item)
            count += 1
            sys.stdout.write('\r')
            sys.stdout.write("%s%% |%s" % (int(count * 100 / Num), int(count * 100 / Num) * '#'))
            sys.stdout.flush()
        end_time = time.clock()
        print("\n写入成功！写入时间%.2fs"%(end_time - begin_time))

    '''查看数据有哪些属性'''
    def attribute(self):
        attr = []
        for item in self.collection.find_one().keys():
            attr.append(item)
        return attr

    '''查找符合条件的数据'''
    def find(self, factor={}, keep=False):
        reserve = {}
        attr = self.attribute()
        for i, item in enumerate(attr):
            if(i<=10):
                print(i + 1, item, end=' ' * (22 - len(item)))
                if ((i + 1) % 5 == 0):
                    print(end='\n')
            if(i>10):
                print(i + 1, item, end=' ' * (21 - len(item)))
                if ((i + 1) % 5 == 0):
                    print(end='\n')
        condition = input("\n请输入您的处理式子:").split(',')
        for item in condition:
            if (item.find('have') != -1):
                factor[item[:-4]] = {'$exists': True}
                reserve[item[:-4]] = 1
            if (item.find('=') != -1 and item.find('!=') == -1):
                temp = item.split('=')
                factor[temp[0]] = temp[1]
                reserve[temp[0]] = 1
            if (item.find('!=') != -1):
                temp = item.split('!=')
                if (temp[1] == 'null'):
                    factor[temp[0]] = {"$nin": [None], '$exists': True}
                    reserve[temp[0]] = 1
                else:
                    factor[temp[0]] = {"$ne": temp[1]}
                    reserve[temp[0]] = 1
            if (item.find('>') != -1):
                temp = item.split('>')
                factor[temp[0]] = {"$gt": float(temp[1])}
                reserve[temp[0]] = 1
            if (item.find('<') != -1):
                temp = item.split('<')
                factor[temp[0]] = {"$lt": float(temp[1])}
                reserve[temp[0]] = 1
        if(keep==True):
            return self.collection.find(factor)
        if(keep==False):
            return self.collection.find(factor, reserve)

    def writeToJson(self, fileName, cursor):
        begin_time = time.clock()
        with open(fileName, 'w+') as f:
            Num = cursor.count()
            count = 0
            for item in cursor:
                flag = 1
                for word in self.cleanWords:
                    if(item['text'].lower().find(word) != -1):
                        flag = 0
                        break
                if(flag == 0):
                    continue
                count += 1
                item['_id'] = count
                json_str = json.dumps(item)
                f.write(json_str)
                f.write('\n')
                sys.stdout.write('\r')
                sys.stdout.write("%s%% |%s" % (int(count * 100 / Num), int(count * 100 / Num) * '#'))
                sys.stdout.flush()
        end_time = time.clock()
        print("\n写入成功！写入时间%.2fs" % (end_time - begin_time))

    '''读取数据库内容，返回为游标cursor'''
    def read(self):
        cursor = self.collection.find()
        return cursor

    '''打印cursor内信息'''
    def showcursor(self, cursor):
        for item in cursor:
            print(item)

    '''处理逻辑关系较为复杂的数据删选查找'''
    def find_advance(self, Filter, reserve):
        return self.collection.find(Filter, reserve).batch_size(100)



if __name__ == '__main__':
    Lab = Mymongo('mongodb://readAnyDatabase:Fzdwxxcl.121@121.49.99.14:30011', database='tweet_stream',
                  collection='UK')
    # Local = Mymongo('localhost:27017', database='tweet', collection='UK2')


    cursor = Lab.find_advance({'timestamp_ms': {'$gt': '1539705600000'}, 'place.country': 'United Kingdom','coordinates': {"$nin": [None]}},
                              {'timestamp_ms': 1, 'text': 1, 'coordinates': 1})

    Lab.writeToJson(fileName='uk', cursor=cursor)















