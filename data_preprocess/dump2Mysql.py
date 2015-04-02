# -*- coding: utf-8 -*-

"""把数据集写入数据库"""

import MySQLdb
import arrow
import logging

# logger初始化
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler
fh = logging.FileHandler('data_process.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s- %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add handlers to logger
logger.addHandler(fh)


def insert2table(connect, fin='tianchi_mobile_recommend_train_user.csv'):
    """
    把文件的内容插入到数据库中

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        fin: string, 用户对商品的操作记录文件
    Returns:
        None
    """
    cursor = connect.cursor()
    counter = 0
    with open(fin, 'rb') as f:
        f.readline()    # 忽略首行
        for line in f:
            cols = line.strip().split(',')
            sql = "INSERT INTO train_user SET user_id=%s, item_id=%s, behavior_type=%s, user_geohash='%s', item_category=%s, time=%s;" % (cols[0], cols[1], cols[2], cols[3], cols[4], arrow.get(cols[5], 'YYYY-MM-DD HH').timestamp)
            cursor.execute(sql)
            counter += 1
            if counter % 5000 == 0:
                connect.commit()
                logger.debug('Insert counter:%s' % (counter))
    connect.commit()
    logger.info('Done, and insert counter:%s' % (counter))
    cursor.close()


def output(connect, fout="first_try.csv", top_N=5):
    """把用户最后浏览过的N条记录作为推荐结果输出

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        fout: string, 结果的输出文件
        top_N: int, 对每个用户的推荐数
    Returns:
        None
    """
    cursor = connect.cursor()
    sql_uids = 'select distinct user_id from train_user;'
    cursor.execute(sql_uids)
    result_uids = cursor.fetchall()

    count = 0
    with open(fout, 'w') as f:
        for [uid] in result_uids:
            sql = 'select distinct item_id from train_user where user_id=%s and behavior_type=1 order by time DESC limit %s;' % (uid, top_N)
            cursor.execute(sql)
            result_item_ids = cursor.fetchall()
            count += 1
            if count % 500 == 0:
                logger.debug('output %s users' % (count))
            for [item_id] in result_item_ids:
                f.write('%s,%s\n' % (uid, item_id))


if __name__ == '__main__':
    connect = MySQLdb.connect(host='127.0.0.1',
                              user='tianchi_data',
                              passwd='tianchi_data',
                              db='tianchi')

#    insert2table(connect)
#    output(connect)
    connect.close()
