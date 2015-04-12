# -*- coding: utf-8 -*-

"""生成训练集"""

import json
import os
import sys
import MySQLdb
from datetime import datetime
from data_preprocess.MongoDB_Utils import MongodbUtils
from log.get_logger import logger

# project path
project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
data_path = '%s/data' % (project_path)

# project import
sys.path.append(project_path)
from log.get_logger import logger, Timer

__author__ = 'ITTC-Jayvee, jiaying.lu'


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
            fout.write(','+itemid)
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
                       f_train_set='%s/train_set.csv' % (data_path)):
    """
    构建训练集

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        f_train_set: string, 训练集结果文件
                     tag=1,正样本；tag=-1，负样本
                 ------ content ------
                | user_id,item_id,tag |
                 ---------------------
    Returns:
        None
    """
    import arrow
    cursor = connect.cursor()

    with open(f_train_set, 'w') as fout:
        fout.write('user_id,item_id,tag\n')
        set_counter = 0
        # 正样本
        tag = 1
        sql = 'select distinct user_id, item_id from train_user where behavior_type=4 and time>%s and time <=%s;' % (arrow.get('2014-12-16').timestamp, arrow.get('2014-12-17').timestamp)
        logger.debug('positive sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        positive_set = set()  # 保存正样本，以防止负样本与正样本相同
        logger.debug('start store positive set')
        for [user_id, item_id] in result:
            set_counter += 1
            positive_set.add((user_id, item_id))
            fout.write('%s,%s,%s\n' % (user_id, item_id, tag))
            if set_counter % 1000 == 0:
                logger.debug('[train set] positive No.%s' % (set_counter))

        # 负样本
        tag = -1
        log_counter = 0
        sql = 'select distinct user_id, item_id, time from train_user where behavior_type!=4 order by rand() limit %s;' % (set_counter+1000)
        logger.debug('negtive sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        train_timestamp = arrow.get('2014-12-17').timestamp 
        logger.debug('start store negtive set')
        for [user_id, item_id, time] in result:
            if time < train_timestamp and (user_id, item_id) not in positive_set:
                log_counter += 1
                fout.write('%s,%s,%s\n' % (user_id, item_id, tag))
                if log_counter == set_counter:
                    break
                if log_counter % 1000 == 0:
                    logger.debug('[train set] negtive No.%s' % (log_counter))

    cursor.close()


if __name__ == '__main__':
    #generate_positive_userset('../data/positive_userset_' + str(datetime.now().strftime('%Y-%m-%d-%H-%M-%S')) + '.json')

    # 生成训练集
    connect = MySQLdb.connect(host='127.0.0.1',
                              user='tianchi_data',
                              passwd='tianchi_data',
                              db='tianchi')

    generate_train_set(connect)
    connect.close()
