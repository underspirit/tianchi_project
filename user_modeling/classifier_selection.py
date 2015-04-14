# -*- coding: utf-8 -*-

"""分类器选型"""

import os
import sys

# project path
project_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
data_path = '%s/data' % (project_path)

# project import
sys.path.append(project_path)
from log.get_logger import logger, Timer


@Timer
def generate_X_y_arrays(f_train_set='%s/train_set.csv' % (data_path)):
    """
    生成分类器的训练集X 和标签集y

    Args:
        f_train_set: 训练集的csv文件
    Returns:
        X: training samples, size=[n_samples, n_features]
        y: class labels, size=[n_samples, 1]
    """
    from sklearn import preprocessing
    import numpy as np
    X = []
    y = []

    with open(f_train_set, 'r') as fin:
        fin.readline()  # 忽略首行
        for line in fin:
            cols = line.strip().split(',')
            X.append([float(i) for i in cols[1:]])
            y.append(int(cols[0]))  # tag在第一列，0 或 -1

    logger.debug('classifier input X_size=[%s, %s] y_size=[%s, 1]' % (len(X), len(X[0]), len(y)))
    X = preprocessing.scale(np.array(X))
    y = preprocessing.scale(np.array(y))
    return X, y


@Timer
def train_classifier(clf, X, y):
    """
    训练分类器

    Args:
        X: training samples, size=[n_samples, n_features]
        y: class labels, size=[n_samples, 1]
    Returns:
        None
    """
    from sklearn import grid_search, cross_validation

    """grid search 的结果
    clf.fit(X, y)
    #logger.info('Classifier fit Done. Best params are %s with a best score of %0.2f' % (clf.best_params_, clf.best_score_))
    #logger.info('And scores ars %s' % (clf.grid_scores_))
    """

    # 简单的交叉验证
    clf.fit(X, y)
    scores = cross_validation.cross_val_score(clf, X, y, cv=5)
    logger.info('Classifier fit Done. And simple cross-validated scores ars %s' % (scores))

    # 十折法
    kf = cross_validation.KFold(len(X), n_folds=10)
    for train_index, test_index in kf:
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        clf.fit(X_train, y_train)
        score = clf.score(X_test, y_test)
        logger.info('10 folds cross-validated scores are %s.' % (score))


@Timer
def classifier_comparison(X, y):
    """
    分类器比较

    Args:
        X: training samples, size=[n_samples, n_features]
        y: class labels, size=[n_samples, 1]
    Returns:
        None
    """
    from sklearn import grid_search
    from sklearn.svm import SVC
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
    from sklearn.naive_bayes import GaussianNB
    from sklearn.lda import LDA
    from sklearn.qda import QDA
    import scipy

    # Exhaustive Grid Search
    exhaustive_parameters = {'kernel':['rbf'], 'C':[1, 10, 100, 1000], 'gamma':[1e-3, 1e-4]}
    clf_SVC_exhaustive = grid_search.GridSearchCV(SVC(), exhaustive_parameters)
    # Randomized Parameter Optimization
    randomized_parameter = {'kernel':['rbf'], 'C': scipy.stats.expon(scale=100), 'gamma': scipy.stats.expon(scale=.1)}
    clf_SVC_randomized = grid_search.RandomizedSearchCV(SVC(), randomized_parameter)

    names = ["Linear SVM", "RBF SVM",
             "RBF SVM with Grid Search", "RBF SVM with Random Grid Search", 
             "Decision Tree", "Random Forest", 
             "AdaBoost", "Naive Bayes", "LDA", "QDA"]
    classifiers = [
        SVC(kernel="linear", C=0.025),
        SVC(gamma=2, C=1),
        clf_SVC_exhaustive,
        clf_SVC_randomized,
        DecisionTreeClassifier(max_depth=5),
        RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1),
        AdaBoostClassifier(),
        GaussianNB(),
        LDA(),
        QDA()]

    for name, clf in zip(names, classifiers):
        logger.info('Use %s:' % (name))
        train_classifier(clf, X, y)


if __name__ == '__main__':
    (X, y) = generate_X_y_arrays('%s/train_combined_vec_data.csv' % (data_path))
    classifier_comparison(X, y)
