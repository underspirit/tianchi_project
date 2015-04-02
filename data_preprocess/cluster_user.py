# coding=utf-8
from data_preprocess.find_user_likehood import cal_user_likehood
from data_preprocess.mongodbutils import MongodbUtils

__author__ = 'ITTC-Jayvee'

mongodb_utils = MongodbUtils("10.210.75.15", 27017)
db = mongodb_utils.get_db()
userdb = db.train_user
distinct = userdb.distinct("user_id")
ids = []

for x in distinct:
    ids.append(x)
# print ids
le = len(ids)
mat = [[0] * le for i in range(le)]
print '开始计算likehood'
matfile = open('likehood.txt', 'w+')
line = ''
for x in ids:
    line = line + x + '\t'

matfile.writelines(line)
line = ''
for i in range(le):
    for j in range(i, le):
        print i, '---', j
        mat[i][j] = cal_user_likehood(ids[i], ids[j])
        mat[j][i] = mat[i][j]
    for x in range(le):
        line = line + str(mat[i][x]) + '\t'
    matfile.writelines(line)
print len(ids)
# print mat
# matfile.write(mat)
# cal_user_likehood()