# -*- coding: utf-8 -*-

"""生成训练集和测试集"""

import json
import os
import sys
import MySQLdb
from datetime import datetime
# from data_preprocess.MongoDB_Utils import MongodbUtils
#from log.get_logger import logger

# project path
project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
data_path = '%s/data' % (project_path)

# project import
sys.path.append(project_path)
from log.get_logger import logger, Timer

__author__ = 'Jayvee, jiaying.lu'


def generate_positive_userset(foutpath='../data/positive_userset.json'):
    # 移动位置，因为服务器上有一些包依赖不完整
    from data_preprocess.MongoDB_Utils import MongodbUtils

    db_address = json.loads(open('../conf/DB_Address.conf', 'r').read())['MongoDB_Address']
    # end 移动位置

    logger.info('start generate_positive_userset')
    mongodb = MongodbUtils(db_address, 27017)
    train_user = mongodb.get_db().train_user
    # user = train_user.find_one()
    startTime = datetime.strptime(str('2014-12-18 00'), '%Y-%m-%d %H')
    stopTime = datetime.strptime(str('2014-12-19 00'), '%Y-%m-%d %H')
    user_ids = train_user.find({"behavior_type": "4", "time": {"$gt": startTime, "$lt": stopTime}}).distinct("user_id")
    # print startTime

    fout = open(foutpath, 'w')
    for userid in user_ids:
        # datastr = userid
        fout.write(userid)
        # data = {"user_id": userid}
        bought_item_ids = train_user.find(
            {'user_id': userid, "behavior_type": "4", "time": {"$gt": startTime, "$lt": stopTime}},
            {'item_id': 1, '_id': 0}).distinct("item_id")
        # bought_items = []
        for itemid in bought_item_ids:
            fout.write(',' + itemid)
            # bought_items.append(itemid)
        # data['bought_items'] = bought_items
        # jsonstr = json.dumps(data)
        fout.write('\n')
        # fout.write(jsonstr + '\n')
    logger.info('generate_positive_userset done,output path = ' + foutpath)
    # item_ids = train_item.distinct("item_id")
    # print len(user_ids)
    # for item_id in item_ids:
    # print item_id


@Timer
def generate_train_set(connect,
                       positive_set_timerange,
                       negative_set_timerange,
                       f_train_set='%s/train_set.csv' % (data_path)):
    """
    构建训练集

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        positive_set_timerange: tuple, 正样本的时间筛选条件, (start, end) e.g. ('2014-12-17', '2014-12-18')
        negative_set_timerange: tuple, 负样本的时间筛选条件, (start, end) e.g. ('2014-11-17', '2014-12-17')
        f_train_set: string, 训练集结果文件
                     tag=1,正样本；tag=-1，负样本
                 ------ content ------
                | user_id,item_id,tag |
                 ---------------------
    Returns:
        None
    """
    import arrow
    from random import randint

    cursor = connect.cursor()
    # 正样本的时间过滤条件
    # positive_timestamp_start = arrow.get('2014-12-17').timestamp
    # positive_timestamp_end = arrow.get('2014-12-18').timestamp
    time2timestamp = lambda elem: arrow.get(elem).timestamp
    (positive_timestamp_start, positive_timestamp_end) = map(time2timestamp, positive_set_timerange)
    (negative_timestamp_start, negative_timestamp_end) = map(time2timestamp, negative_set_timerange)

    with open(f_train_set, 'w') as fout:
        fout.write('user_id,item_id,tag\n')
        set_counter = 0
        # 正样本
        tag = 1
        sql = 'select distinct user_id, item_id from train_user where behavior_type=4 and time>%s and time <=%s;' % (
        positive_timestamp_start, positive_timestamp_end)
        logger.debug('positive sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        positive_set = set()  # 保存正样本，以防止负样本与正样本相同
        logger.debug('start store positive set')
        for [user_id, item_id] in result:
            set_counter += 1
            positive_set.add((user_id, item_id))
            fout.write('%s,%s,%s\n' % (user_id, item_id, tag))
            if set_counter % 300 == 0:
                logger.debug('[train set] positive No.%s' % (set_counter))
        logger.info('[train set] positive set DONE, num of set = %s' % (set_counter))

        # 负样本
        tag = -1
        log_counter = 0
        # order by rand() 效率太低，使用两步法代替
        """
        # sql = 'select distinct user_id, item_id from train_user where behavior_type!=4 and time>%s and time <=%s order by rand() limit %s;' % (negative_timestamp_start, negative_timestamp_end, set_counter+1000)
        sql = 'select distinct user_id, item_id from train_user where behavior_type!=4 and time>%s and time <=%s limit %s;' % (negative_timestamp_start, negative_timestamp_end, set_counter*2)
        logger.debug('negtive sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        result = list(result)  # result type: tuple -> list
        logger.debug('start store negtive set')
        shuffle(result)
        for [user_id, item_id] in result:
            if (user_id, item_id) not in positive_set:
                log_counter += 1
                fout.write('%s,%s,%s\n' % (user_id, item_id, tag))
                if log_counter == set_counter:
                    break
                if log_counter % 1000 == 0:
                    logger.debug('[train set] negtive No.%s' % (log_counter))
        """

        # Step 1: 获得PK的最小值和PK的最大值
        sql_PK_min = 'select record_id from train_user where time>%s and time <=%s order by time limit 1;' % (
        negative_timestamp_start, negative_timestamp_end)
        cursor.execute(sql_PK_min)
        result = cursor.fetchall()
        PK_min = int(result[0][0])
        logger.debug('min Primary Key = %s' % (PK_min))
        sql_PK_max = 'select record_id from train_user where time>%s and time <=%s order by time DESC limit 1;' % (
        negative_timestamp_start, negative_timestamp_end)
        cursor.execute(sql_PK_max)
        result = cursor.fetchall()
        PK_max = int(result[0][0])
        logger.debug('max Primary Key = %s' % (PK_max))
        # Step 2: 生成随机数(min,max)，直至取出与正样本相同数目的负样本
        logger.debug('start store negtive set')
        while log_counter < set_counter:
            sql = 'select user_id, item_id from train_user where record_id=%s' % (randint(PK_min, PK_max))
            cursor.execute(sql)
            result = cursor.fetchall()
            for [user_id, item_id] in result:
                if (user_id, item_id) not in positive_set:
                    log_counter += 1
                    fout.write('%s,%s,%s\n' % (user_id, item_id, tag))
                    if log_counter % 300 == 0:
                        logger.debug('[train set] negtive No.%s' % (log_counter))
        logger.info('[train set] negtive set DONE, num of set = %s' % (log_counter))

    cursor.close()


@Timer
def generate_test_set(connect,
                      timerange,
                      f_train_set='%s/test_set.csv' % (data_path)):
    """
    构建测试集

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        timerange: tuple, 测试集的时间筛选条件, (start, end)
        f_train_set: string, 测试集结果文件
                 ---- content ----
                | user_id,item_id |
                 -----------------
    Returns:
        None
    """
    import arrow

    cursor = connect.cursor()
    (timerange_start, timerange_end) = map(lambda elem: arrow.get(elem).timestamp, timerange)

    with open(f_train_set, 'w') as fout:
        fout.write('user_id,item_id\n')
        sql = 'select distinct user_id, item_id from train_user where behavior_type=4 and time>%s and time<=%s;' % (
        timerange_start, timerange_end)
        logger.debug('sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        logger.debug('start generate test set')
        for [user_id, item_id] in result:
            fout.write('%s,%s\n' % (user_id, item_id,))
        logger.debug('success generate test set')

    cursor.close()


@Timer
def generate_predict_set(connect,
                         timerange,
                         f_train_set='%s/test_set.csv' % (data_path)):
    """
    构建预测集

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        timerange: tuple, 测试集的时间筛选条件, (start, end)
        f_train_set: string, 测试集结果文件
                 ---- content ----
                | user_id,item_id |
                 -----------------
    Returns:
        None
    """
    import arrow

    cursor = connect.cursor()
    (timerange_start, timerange_end) = map(lambda elem: arrow.get(elem).timestamp, timerange)

    with open(f_train_set, 'w') as fout:
        fout.write('user_id,item_id,tag\n')
        sql = 'select distinct user_id, item_id from train_user where time>%s and time<=%s;' % (
                                                                                               timerange_start, timerange_end)
        logger.debug('sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        logger.debug('start generate test set')
        for [user_id, item_id] in result:
            fout.write('%s,%s,%s\n' % (user_id, item_id, -1))
        logger.debug('success generate test set')

    cursor.close()


if __name__ == '__main__':
    #generate_positive_userset('../data/positive_userset_' + str(datetime.now().strftime('%Y-%m-%d-%H-%M-%S')) + '.json')

    # 生成训练集
    connect = MySQLdb.connect(host='127.0.0.1',
                              user='tianchi_data',
                              passwd='tianchi_data',
                              db='tianchi')
    f_train_set = '%s/train_set_1218.csv' % (data_path)
    f_test_set = '%s/test_set_1218.csv' % (data_path)
    generate_train_set(connect, ('2014-12-17', '2014-12-18'), ('2014-11-18', '2014-12-17'), f_train_set)
    generate_test_set(connect, ('2014-12-18', '2014-12-19'), f_test_set)
    connect.close()
