# -*- coding: utf-8 -*-

"""对推荐结果和商品子集取交集"""

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


def intersect(f_result='%s/UserCF_recommend_3.csv' % (data_path),
              f_item_set='%s/tianchi_mobile_recommend_train_item.csv' % (data_path)):
    """
    对结果和给出的item_set取交集，剔除结果中不属于

    Args:
        f_result: string, 原始的结果文件
                 -------------- content -------------
                | item_id,item_geohash,item_category |
                 ------------------------------------
        f_item_set: string, 阿里提供的物品集文件
                 ---- content ----
                | user_id,item_id |
                 -----------------
    Returns:
        None
    """
    item_id_set = set()
    with open(f_item_set) as fin:
        fin.readline()    # 忽略首行
        for line in fin:
            cols = line.strip().split(',')
            item_id_set.add(cols[0])
        
    fout_name = f_result.replace('.csv', '_intersect.csv')
    with open(f_result) as fin, open(fout_name, 'w') as fout:
        fout.write(fin.readline())    # 首行特殊处理
        for line in fin:
            cols = line.strip().split(',')
            if cols[1] in item_id_set:
                fout.write(line)


if __name__ == '__main__':
    #intersect()
    intersect('%s/UserCF_recommend_1.csv'%(data_path))
