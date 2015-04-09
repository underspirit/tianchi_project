# coding=utf-8
import json
import logging
from data_preprocess.MongoDB_Utils import MongodbUtils

__author__ = 'ITTC-Jayvee'

# logger初始化
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler
fh = logging.FileHandler('../logs/ItemRating.log')
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

# db_address的读取
db_address = json.loads(open('../conf/DB_Address.conf', 'r').read())['MongoDB_Address']


def rate_items(output_path=None, write_while_calculate=False):
    """
    遍历所有用户，计算每个用户对自己所浏览过的商品的全局评分（未考虑时间、商品种类等因素，只计入行为数）。
    :param output_path: 如果需要在计算过程中写入已计算的数据，则填入输出文件路径
    :param write_while_calculate: 需要在过程中写入数据则为True
    :return:包含所有用户的所有评分的dict
    """
    A1 = 1.1
    A2 = 2
    A3 = 5
    mongodb_utils = MongodbUtils(db_address, 27017)
    db = mongodb_utils.get_db()
    train_user = db.train_user
    json_root = []
    logger.info('loading...')
    usercount = 0
    ids = train_user.distinct("user_id")
    maxusercount = len(ids)
    logger.info('start!')
    # split_json = open('../data/split_json_item_rates.json', 'w')
    split_json = None
    if output_path is not None and write_while_calculate:
        split_json = open(output_path, 'w')
    for user_id in ids:
        user_rates_info = {'user_id': user_id}
        user_rates = {}
        itemcount = 0
        usercount += 1
        # print usercount
        # if usercount.__mod__(0.01 * maxusercount) == 0.0:
        # logger.debug(str(usercount / (0.01 * maxusercount)) + r'% done!')
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
        # 是否在计算过程中写入文件
        if output_path is not None and write_while_calculate:
            tempstr = json.dumps(user_rates_info)
            split_json.write(tempstr + '\n')
            logger.debug(str(usercount) + 'th user ' + user_id + ' written.')
        json_root.append(user_rates_info)
    logger.info('done!')
    return json_root


if __name__ == "__main__":
    root = rate_items('../data/split_json_item_rates_test.json', True)
    # json.dump(root, open('../data/user_item_rates_new.json', 'w'))