# -*- coding: utf-8 -*-

"""基于用户评分的推荐"""

import MySQLdb
import arrow
import logging
import os
import sys
import json
from math import exp

# project path
project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
data_path = '%s/data' % (project_path)

# project import
sys.path.append(project_path)
from log.get_logger import logger


def add_time_affect(connect,
                    time_atten=3600*48,
                    fout_name='%s/UserCF_result.json' % (data_path),
                    fin_name='%s/split_json_item_rates.json' % (data_path)):
    """
    根据已有的推荐结果，加入时间衰减的因子

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        time_atten: int, 时间戳的衰减因子, exp(-1/a * delta_t)
        fin_name: string, 未加入时间考量的推荐评分文件
        fout_name: string, 结果的输出文件
    Returns:
        None
    """
    cursor = connect.cursor()
    predict_timestamp = arrow.get('2014-12-19').timestamp
    counter = 0

    with open(fin_name, 'r') as fin, open(fout_name, 'w') as fout:
        for line in fin:
            record = json.loads(line.strip())
            user_id = record['user_id']
            for item_id in record['items'].iterkeys():
                sql = 'select time from train_user where user_id=%s and item_id=%s;' % (user_id, item_id)
                cursor.execute(sql)
                result = cursor.fetchall()
                time_weight = 0    # 记录时间因子
                for [timestamp] in result:
                    time_weight += exp((timestamp-predict_timestamp)/time_atten)
                record['items'][item_id] *= time_weight
            counter += 1
            logger.debug('user_id=%s done, %s/10000' % (user_id, counter))
            fout.write('%s\n' % (json.dumps(record)))

    cursor.close()    


def output_userCF(topN = 3,
                  fout_name='%s/UserCF_recommend_3.csv' % (data_path),
                  fin_name='%s/UserCF_result.json' % (data_path)):
    """
    根据评分，得出推荐结果

    Args:
        topN: int, 推荐给每个用户topN个商品
        fin_name: string, 用户评分结果文件
        fout_name: string, 结果的输出文件
    Returns:
        None
    """
    with open(fin_name, 'r') as fin, open(fout_name, 'w') as fout:
        fout.write('user_id,item_id\n')
        for line in fin:
            record = json.loads(line.strip())
            user_id = record['user_id']
            recommend_result = sorted(record['items'].iteritems(), key=lambda e: e[1], reverse=True)[:topN]
            for (item_id, rate) in recommend_result:
                fout.write('%s,%s\n' % (user_id, item_id))


if __name__ == '__main__':
    connect = MySQLdb.connect(host='127.0.0.1',
                              user='tianchi_data',
                              passwd='tianchi_data',
                              db='tianchi')

    #add_time_affect(connect)
    connect.close()
    output_userCF()
    output_userCF(topN=1, fout_name='%s/UserCF_recommend_1.csv' % (data_path))
