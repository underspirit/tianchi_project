# -*- coding: utf-8 -*-

"""生成测试集"""

import os
import sys
import MySQLdb
import arrow

# project path
project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
data_path = '%s/data' % (project_path)

# project import
sys.path.append(project_path)
from log.get_logger import logger


def generate_test_set(connect,
                       f_train_set='%s/test_set.csv' % (data_path)):
    """
    构建测试集

    Args:
        connect: Mysqldb.connect(), 数据库连接句柄
        f_train_set: string, 测试集结果文件
                 ---- content ----
                | user_id,item_id |
                 -----------------
    Returns:
        None
    """
    cursor = connect.cursor()

    with open(f_train_set, 'w') as fout:
        fout.write('user_id,item_id\n')
        sql = 'select distinct user_id, item_id from train_user where behavior_type=4 and time>%s;' % (arrow.get('2014-12-17').timestamp)
        #logger.debug('sql: %s' % (sql))
        cursor.execute(sql)
        result = cursor.fetchall()
        logger.debug('start generate test set')
        for [user_id, item_id] in result:
            fout.write('%s,%s\n' % (user_id, item_id,))
        logger.debug('success generate test set')
    
    cursor.close()


if __name__ == '__main__':
    connect = MySQLdb.connect(host='127.0.0.1',
                              user='tianchi_data',
                              passwd='tianchi_data',
                              db='tianchi')

    generate_test_set(connect)
    connect.close()
