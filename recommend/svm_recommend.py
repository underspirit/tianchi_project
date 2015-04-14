# -*- coding: utf-8 -*-

"""使用svm推荐"""

import os
import sys
from sklearn import svm, grid_search
import scipy

# project path
project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
data_path = '%s/data' % (project_path)

# project import
sys.path.append(project_path)
from log.get_logger import logger, Timer
from user_modeling.classifier_selection import generate_X_y_arrays
from recommend.intersection import intersect


# Randomized Parameter Optimization
randomized_parameter = {'kernel':['rbf'], 'C': scipy.stats.expon(scale=100), 'gamma': scipy.stats.expon(scale=.1)}
clf = grid_search.RandomizedSearchCV(svm.SVC(), randomized_parameter)


@Timer
def train_svm():
    """
    训练分类器

    Args:
        None
    Returns:
        None
    """
    (X, y) = generate_X_y_arrays('%s/train_combined_vec_data.csv' % (data_path))
    clf.fit(X, y)
    logger.info('Classifier fit Done. Best params are %s with a best score of %0.2f' % (clf.best_params_, clf.best_score_))


@Timer
def generate_predict_result(f_predict='%s/predict_set/predict_result.csv' % (data_path),
                            f_vec_set='%s/predict_set/predict_combined_vec_data.csv' % (data_path),
                            f_uid_iid_set='%s/predict_set/predict_set.csv' % (data_path)):
    """
    生成预测结果

    Args:
        f_predict: string, 存放预测结果
        f_vec_set: string, 存放待预测向量的文件名
        f_uid_iid_set: string, 存放与向量对应的user_id, item_id
    Returns:

    """
    predict_X, predict_y = generate_X_y_arrays(f_vec_set)
    logger.debug('predict start.')
    predict_y = clf.predict(predict_X)
    logger.debug('predict done, predict result size=%s' % (len(predict_y)))

    with open(f_uid_iid_set, 'r') as fin, open(f_predict, 'w') as fout:
        counter = 0
        fin.readline()    # 忽略首行
        fout.write('user_id,item_id,tag')
        
        logger.debug('start store predict result')
        for line in fin:
            line_result = line.strip() + ',%s\n' % (predict_y[counter])
            fout.write(line_result)
            counter += 1

    if counter != len(predict_y):
        logger.error('predict result size:%s, but uid_iid_set size:%s' % (len(predict_y), counter))
    else:
        logger.info('predict success, generate predict result in %s' % (f_predict))


@Timer
def generate_recommend_result(f_predict_set, f_recommend_set):
    """
    根据预测结果生成推荐结果

    Args:
        f_predict_set: string, 存放预测结果
        f_recommend_set: string, 存放推荐结果
    Returns:
        None
    """
    with open(f_predict_set, 'r') as fin, open(f_recommend_set, 'w') as fout:
        fin.readline()    # 忽略首行
        fout.write('user_id,item_id\n')

        counter = 0
        for line in fin:
            cols = line.strip().split(',')
            if cols[-1] == '1':
                counter += 1
                fout.write('%s,%s\n' % (cols[0], cols[1]))

    logger.info('generate recommend result Done. Total: %s' % (counter))


if __name__ == '__main__':
    f_predict_set = '%s/predict_set/predict_result.csv' % (data_path)
    f_recommend_set = '%s/predict_set/recommend_result.csv' % (data_path)
    train_svm()
    generate_predict_result(f_predict_set)
    generate_recommend_result(f_predict_set, f_recommend_set)
    intersect(f_recommend_set)
