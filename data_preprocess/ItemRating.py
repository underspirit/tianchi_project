# coding=utf-8
import json
import logging
from data_preprocess.mongodbutils import MongodbUtils

__author__ = 'ITTC-Jayvee'

# logger初始化
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler
fh = logging.FileHandler('./logs/ItemRating.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s- '
                              '%(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add handlers to logger
logger.addHandler(fh)

# jsonobj的读取
jsonstr = open("../data/user_item_data.json", "r").read()
dataobj = json.loads(jsonstr)


def rate_items():
    A1 = 1.1
    A2 = 2
    A3 = 3
    mongodb_utils = MongodbUtils('10.108.192.165', 27017)
    db = mongodb_utils.get_db()
    train_user = db.train_user
    root = {}
    logger.info('start!')
    usercount = 0.0
    ids = train_user.distinct("user_id")
    maxusercount = len(ids)
    for user_id in ids:
        user_rates = {}
        itemcount = 0
        usercount += 1
        if usercount.__mod__(0.01 * maxusercount) == 0:
            logger.debug(str(usercount / (0.01 * maxusercount)) + r'% done!')
        for item_id in train_user.find({"user_id": user_id}).distinct("item_id"):
            itemcount += 1
            count1 = 0
            count2 = 0
            count3 = 0
            for behavior in train_user.find({"user_id": user_id, "item_id": item_id}):
                behavior_type = behavior['behavior_type']
                if behavior_type is '1':
                    count1 += 1
                elif behavior_type is '2' or '3':
                    count2 += 1
                elif behavior_type is '4':
                    count3 += 1
            rate = A1 ** count1 + A2 ** count2 + A3 ** count3
            user_rates[item_id] = rate
        user_rates['item_count'] = itemcount
        root[user_id] = user_rates
    return root


if __name__ == "__main__":
    root = rate_items()
    json.dump(root, open('../data/user_item_data.json', 'w'))