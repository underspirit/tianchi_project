import json
from datetime import datetime
from data_preprocess.MongoDB_Utils import MongodbUtils
from log.get_logger import logger

__author__ = 'ITTC-Jayvee'

db_address = json.loads(open('../conf/DB_Address.conf', 'r').read())['MongoDB_Address']


def generate_positive_userset(foutpath='../data/positive_userset.json'):
    logger.info('start generate_positive_userset')
    mongodb = MongodbUtils(db_address, 27017)
    train_user = mongodb.get_db().train_user
    # user = train_user.find_one()
    startTime = datetime.strptime(str('2014-12-18 00'), '%Y-%m-%d %H')
    stopTime = datetime.strptime(str('2014-12-19 00'), '%Y-%m-%d %H')
    user_ids = train_user.find({"behavior_type": "4", "time": {"$gt": startTime, "$lt": stopTime}}).distinct("user_id")
    # print startTime

    fout = open(foutpath, 'w')
    for userid in user_ids:
        data = {"user_id": userid}
        bought_item_ids = train_user.find(
            {'user_id': userid, "behavior_type": "4", "time": {"$gt": startTime, "$lt": stopTime}},
            {'item_id': 1, '_id': 0})
        bought_items = []
        for itemid in bought_item_ids:
            bought_items.append(itemid)
        data['bought_items'] = bought_items
        jsonstr = json.dumps(data)
        fout.write(jsonstr + '\n')
    logger.info('generate_positive_userset done,output path = ' + foutpath)
    # item_ids = train_item.distinct("item_id")
    # print len(user_ids)
    # for item_id in item_ids:
    # print item_id


if __name__ == '__main__':
    generate_positive_userset('../data/positive_userset_' + str(datetime.now().strftime('%Y-%m-%d-%H-%M-%S')) + '.json')
