# coding=utf-8
from datetime import datetime
import pymongo

__author__ = 'Jayvee'


def dump_train_user(csv_path, db_address):
    csvfile = open(csv_path)
    # for line in csvfile:
    head = csvfile.readline()
    head = head.replace('\n', '')
    title = []
    for x in head.split(','):
        title.append(x)
    print title
    conn = pymongo.Connection(db_address, 27017)
    db = conn.TianchiData
    train_userdb = db.train_user
    # line = csvfile.readline()
    count = 0
    for line in csvfile:
        line = line.replace('\n', '')
        data = {}
        temp = line.split(',')
        for i in range(len(title)):
            if title[i] != 'time':
                data[title[i]] = temp[i]
            else:
                data[title[i]] = datetime.strptime(str(temp[i]), '%Y-%m-%d %H')
        train_userdb.insert(data)
        count += 1
        # line = csvfile.readline()
    conn.disconnect()
    print '处理完毕'


def dump_train_item(csv_path, db_address):
    csvfile = open(csv_path)
    # for line in csvfile:
    head = csvfile.readline()
    head = head.replace('\n', '')
    title = []
    for x in head.split(','):
        title.append(x)
    print title
    conn = pymongo.Connection(db_address, 27017)
    db = conn.TianchiData
    train_item_db = db.train_item
    # line = csvfile.readline()
    count = 0
    for line in csvfile:
        line = line.replace('\n', '')
        data = {}
        temp = line.split(',')
        for i in range(len(title)):
            # if title[i] != 'time':
            data[title[i]] = temp[i]
            # else:
            # data[title[i]] = datetime.strptime(str(temp[i]), '%Y-%m-%d %H')
        train_item_db.insert(data)
        count += 1
        # line = csvfile.readline()
    conn.disconnect()
    print '处理完毕'


# def dump



# if __name__ == '__main__':
# dump_train_item(r'D:\CS\DataMining\tianchi\data\tianchi_mobile_recommend_train_item.csv','localhost')
