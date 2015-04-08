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
    A3 = 5
    mongodb_utils = MongodbUtils('10.108.192.165', 27017)
    db = mongodb_utils.get_db()
    train_user = db.train_user
    root = []
    logger.info('loading...')
    usercount = 0
    ids = train_user.distinct("user_id")
    maxusercount = len(ids)
    logger.info('start!')
    split_json = open('../data/split_json_item_rates.json', 'w')
    for user_id in ids:
        user_rates_info = {'user_id': user_id}
        user_rates = {}
        itemcount = 0
        usercount += 1
        print usercount
        if usercount.__mod__(0.01 * maxusercount) == 0.0:
            logger.debug(str(usercount / (0.01 * maxusercount)) + r'% done!')
        for item_id in train_user.find({"user_id": user_id}).distinct("item_id"):
            itemcount += 1
            count1 = 0
            count2 = 0
            count3 = 0
            for behavior in train_user.find({"user_id": user_id, "item_id": item_id}):
                behavior_type = behavior['behavior_type']
                if behavior_type == '1':
                    count1 += 1
                elif behavior_type == '2' or behavior_type == '3':
                    count2 += 1
                elif behavior_type == '4':
                    count3 += 1
            rate = A1 ** count1 + A2 ** count2 + A3 ** count3
            user_rates[item_id] = rate
        user_rates_info['item_count'] = itemcount
        user_rates_info['items'] = user_rates
        tempstr = json.dumps(user_rates_info)
        split_json.write(tempstr+'\n')
        root.append(user_rates_info)
    return root


if __name__ == "__main__":
    root = rate_items()
    json.dump(root, open('../data/user_item_rates_new.json', 'w'))