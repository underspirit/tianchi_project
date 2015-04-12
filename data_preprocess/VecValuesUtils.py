# coding=utf-8
import json
from datetime import datetime
import math
from data_preprocess.MongoDB_Utils import MongodbUtils
from log.get_logger import logger


__author__ = 'Jayvee'

db_address = json.loads(open('../conf/DB_Address.conf', 'r').read())['MongoDB_Address']
MAX_BOUGHT_BEHAVIOR_COUNT = 120236


# 说明：从正样本中选取user_id与item_id，在建立向量的过程中只根据12-18日之前的数据，
# 即在数据库查询语句中添加时间戳《12-18-00的条件

def cal_item_popularity(item_id):
    """
    计算商品热门度，由于被除数都一样所以不再除以被购买商品总数，改为count的sigmoid形式
    :param item_id:
    :return:float类型的商品热度
    """
    # logger.info('cal_item_popularity: item_id = ' + item_id)
    mongodb = MongodbUtils(db_address, 27017)
    train_user = mongodb.get_db().train_user
    # print len(train_user.find({'behavior_type': '4'}).distinct('item_id'))
    # startTime = datetime.strptime(str('2014-12-18 00'), '%Y-%m-%d %H')
    stopTime = datetime.strptime(str('2014-12-18 00'), '%Y-%m-%d %H')
    bought_count = train_user.find({'item_id': item_id, 'behavior_type': '4', "time": {"$lt": stopTime}}).count()
    popularity = 1 / (1 + math.e ** (-bought_count)) - 0.5
    return popularity


def cal_user_desire(user_id):
    """
    计算用户购买欲
    :param user_id:
    :return:float类型的用户购买欲
    """
    # logger.info('cal_user_desire: user_id = ' + user_id)
    mongodb = MongodbUtils(db_address, 27017)
    train_user = mongodb.get_db().train_user
    stopTime = datetime.strptime(str('2014-12-18 00'), '%Y-%m-%d %H')
    max_count = train_user.find({"user_id": user_id, "time": {"$lt": stopTime}}).count()
    bought_count = train_user.find({"user_id": user_id, 'behavior_type': '4', "time": {"$lt": stopTime}}).count()
    if max_count == 0:
        return 0
    return float(bought_count) / float(max_count)


def cal_useritem_behavior_rate(user_id, item_id):
    """
    计算指定用户对指定商品的操作数占该用户总操作数的比重
    :param user_id:
    :param item_id:
    :return:
    """
    # logger.info('cal_useritem_behavior_rate: user_id = ' + user_id + '\titem_id = ' + item_id)
    mongodb = MongodbUtils(db_address, 27017)
    train_user = mongodb.get_db().train_user
    stopTime = datetime.strptime(str('2014-12-18 00'), '%Y-%m-%d %H')
    max_count = train_user.find({"user_id": user_id, "time": {"$lt": stopTime}}).count()
    item_behavior_count = train_user.find({"user_id": user_id, "item_id": item_id, "time": {"$lt": stopTime}}).count()
    if max_count == 0:
        return 0
    return float(item_behavior_count) / float(max_count)


def cal_positive_userset_vecvalues(fin_path='../data/positive_userset_2015-04-12-14-32-11.csv',
                                   fout_path='../data/popularity_desire_behaviorRate_data.csv'):
    """
    计算剩下的3个维度的值（商品热门度、用户购买欲、操作比重），并保存在csv文件中
    格式：[user_id]_[item_id],popularity,desire,behavior_rate
    :param fin_path: 正样本训练集的csv数据文件
    :param fout_path: 结果输出路径
    :return:
    """
    fin = open(fin_path, 'r')
    fout = open(fout_path, 'w')
    logger.info('cal_positive_userset_vecvalues start')
    for line in fin:
        line = line.replace('\n', '')
        ids = line.split(',')
        user_id = ids[0]
        desire = cal_user_desire(user_id)
        for index in range(1, len(ids)):
            item_id = ids[index]
            fout.write(user_id + '_' + item_id + ',')
            popularity = cal_item_popularity(item_id)
            behavior_rate = cal_useritem_behavior_rate(user_id, item_id)
            fout.write(str(popularity) + ',' + str(desire) + ',' + str(behavior_rate) + '\n')
    logger.info('cal_positive_userset_vecvalues done,output path=' + fout_path)


if __name__ == '__main__':
    # print cal_item_popularity('71486077')
    print cal_item_popularity('324474695')
    print cal_user_desire('38056569')
    print cal_useritem_behavior_rate('38056569', '324474695')
    cal_positive_userset_vecvalues()
    # for i in xrange(10):
    # popularity = 1 / (1 + math.e ** (-(i/100.0)))-0.5
    # print popularity