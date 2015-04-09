# coding=utf-8
import json
import logging
import math
from data_preprocess.MongoDB_Utils import MongodbUtils

__author__ = 'ITTC-Jayvee'

# logger初始化
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler
fh = logging.FileHandler('../logs/data_process_find_user_likehood.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s- '
                              '%(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add handlers to logger
logger.addHandler(fh)


def load_user_data():
    """
    从数据库中读取用户数据到内存中
    （不建议常用，耗时约45分钟）
    :return:dict
    """
    mongodb_utils = MongodbUtils('10.108.192.165', 27017)
    db = mongodb_utils.get_db()
    userdb = db.train_user
    user_item_data = {}
    for user_id in userdb.distinct("user_id"):
        user_item = {}
        user_item_info = {}
        behavior_count = 0
        for behavior in userdb.find({"user_id": user_id}):
            behavior_count += 1
            item_id = behavior['item_id']
            if user_item.has_key(item_id):
                user_item[item_id] += 1
            else:
                user_item[item_id] = 1
        user_item_info['behavior_count'] = behavior_count
        user_item_info['items'] = user_item
        user_item_data[user_id] = user_item_info
        logger.info(user_id + ' done!')
    return user_item_data


def cal_user_likehood(user_id1, user_id2, data=None):
    """
    计算给定id的两个用户之间的相似度，默认使用数据库数据，给定data后使用内存数据
    :param user_id1:
    :param user_id2:
    :param data: 由 get_user_item_data()所返回的json对象
    :return:
    """
    if user_id1 == user_id2:
        return 1.0
    if data is None:
        mongodb_utils = MongodbUtils('10.108.192.165', 27017)
        db = mongodb_utils.get_db()
        userdb = db.train_user
        items1 = userdb.find({"user_id": user_id1}).distinct("item_id")
        items2 = userdb.find({"user_id": user_id2}).distinct("item_id")
        maxcount1 = userdb.find({"user_id": user_id1}).count()
        maxcount2 = userdb.find({"user_id": user_id2}).count()
        # common_items = []
        vec1 = []
        vec2 = []
        fenzi = 0.0
        fenmu1 = 0.0
        fenmu2 = 0.0
        cos = 0
        for i in items1:
            if i in items2:
                x1 = userdb.find({"user_id": user_id1, "item_id": i}).count() / math.log(float(maxcount1))
                vec1.append(x1)
                x2 = userdb.find({"user_id": user_id2, "item_id": i}).count() / math.log(float(maxcount2))
                vec2.append(x2)
                fenzi += x1 * x2
                fenmu1 += x1 * x1
                fenmu2 += x2 * x2
        if fenzi is not 0.0:
            # cos = fenzi / math.sqrt(fenmu1 * fenmu2)
            cos = math.log(fenzi) - 0.5 * (math.log(fenmu1) + math.log(fenmu2))
        # logger.info('calculate user likehood between ' + user_id1 + ' and ' + user_id2 + ' is done!')
        return cos
    else:
        vec1 = []
        vec2 = []
        cos = 0.0
        user1 = data[user_id1]
        user2 = data[user_id2]
        items1 = user1['items'].keys()
        items2 = user2['items'].keys()
        itemlist1 = user1['items']
        itemlist2 = user2['items']
        maxcount1 = math.log(float(user1['behavior_count']))
        maxcount2 = math.log(float(user2['behavior_count']))
        # 分子分母的初始化，此处加入一个修正项(maxcount1,maxcount2)
        # 以防止相似度过于接近1
        fenzi = maxcount1 * maxcount2
        temp = fenzi
        fenmu1 = maxcount1 * maxcount1
        fenmu2 = maxcount2 * maxcount2
        # list1 = items1.values()
        # list2 = items2.values()
        common_items1 = {}
        common_items2 = {}
        if len(items1) <= len(items2):  # 外循环次数尽量最小
            for i in items1:
                if i in items2:
                    common_items1[i] = itemlist1[i]
                    common_items2[i] = itemlist2[i]
                    x1 = itemlist1[i] / maxcount1
                    x2 = itemlist2[i] / maxcount2
                    # x1 = itemlist1[i]
                    # x2 = itemlist2[i]
                    vec1.append(x1)
                    vec2.append(x2)
                    fenzi += x1 * x2
                    fenmu1 += x1 * x1
                    fenmu2 += x2 * x2
        else:
            for i in items2:
                if i in items1:
                    common_items1[i] = itemlist1[i]
                    common_items2[i] = itemlist2[i]
                    x1 = itemlist1[i] / maxcount1
                    x2 = itemlist2[i] / maxcount2
                    # x1 = itemlist1[i]
                    # x2 = itemlist2[i]
                    vec1.append(x1)
                    vec2.append(x2)
                    fenzi += x1 * x2
                    fenmu1 += x1 * x1
                    fenmu2 += x2 * x2
        if fenzi != temp:
            cos = fenzi / math.sqrt(fenmu1 * fenmu2)
            t1 = math.log(fenzi)
            t2 = math.log(fenmu1)
            t3 = math.log(fenmu2)
            cos1 = math.log(fenzi) - 0.5 * (math.log(fenmu1) + math.log(fenmu2))
            if cos == 1.0:
                print common_items1
                print common_items2
                # logger.info('calculate user likehood between ' + user_id1 + ' and ' + user_id2 + ' is done!')
        return cos


        # common_items.append(i)

        # return common_items


def get_user_item_data(path):
    """
    读取文件中保存的json数据，返回user-item的dict
    :param path:
    :return:
    """
    temp = open(path, "r")
    return json.load(temp)


def cal_likehood_matrix(data_json):
    """
    计算所有用户之间的相似度
    :param data_json:
    :return: 相似度矩阵
    """
    ids = data_json.keys()
    id_count = len(ids)
    print '初始化矩阵……'
    logger.info('初始化矩阵……')
    # likehood_matrix = [[0 for x in range(id_count)] for y in range(id_count)]
    likehood_matrix = []
    print '开始计算相似度……'
    logger.info('开始计算相似度……')
    for i in range(id_count):
        id1 = ids[i]
        col = []
        print '开始计算第' + str(i) + '个id'
        logger.debug('开始计算第' + str(i) + '个id')
        for j in range(id_count):
            if j < i:
                col.append(likehood_matrix[j][i])
            else:
                id2 = ids[j]
                col.append(cal_user_likehood(id1, id2, data_json))
                # likehood_matrix[i][j] = cal_user_likehood(id1, id2, data_json)
        likehood_matrix.append(col)
        # matfile.write(likehood_matrix[i])
        logger.debug(str(i) + ' rows done')
    print 'done'
    return likehood_matrix


def save_matrix(matfile_name, matrix=None):
    if not matrix:
        matrix = [[]]
    matfile = open(matfile_name, 'w')
    row_count = 0
    for row in matrix:
        line = ''
        col_count = 0
        for col in row:
            if col_count != len(row) - 1:
                line = line + str(col) + ','
            else:
                line = line + str(col) + '\n'
        matfile.write(line)
        logger.info('Row:' + row_count + ' written')
        row_count += 1


if __name__ == "__main__":
    # s = cal_user_likehood("106218649", "106218649")
    # print s

    # 将数据库写入本地json文件
    # json_str = json.dumps(load_user_data())
    # logger.info('load data done!')
    # jsonfile = open('user_item_data.json', "w")
    # jsonfile.write(json_str)
    # logger.info('write data done!')

    # 测试计算相似度
    # user_item_data = get_user_item_data('user_item_data.json')
    # print type(user_item_data)
    # s = cal_user_likehood("95554894", "99712177", user_item_data)
    # print s

    mat = cal_likehood_matrix(get_user_item_data('user_item_data.json'))
    save_matrix('matfile', mat)