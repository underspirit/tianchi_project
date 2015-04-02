import math
import pymongo
from data_preprocess.mongodbutils import MongodbUtils

__author__ = 'ITTC-Jayvee'


def cal_user_likehood(user_id1, user_id2):
    """

    :rtype : double
    """
    if user_id1 == user_id2:
        return 1.0
    mongodb_utils = MongodbUtils('10.210.75.15', 27017)
    db = mongodb_utils.get_db()
    userdb = db.train_user
    items1 = userdb.find({"user_id": user_id1}).distinct("item_id")
    items2 = userdb.find({"user_id": user_id2}).distinct("item_id")
    common_items = []
    vec1 = []
    vec2 = []
    fenzi = 0.0
    fenmu1 = 0.0
    fenmu2 = 0.0
    cos = 0
    for i in items1:
        if i in items2:
            x1 = userdb.find({"user_id": user_id1, "item_id": i}).count()
            vec1.append(x1)
            x2 = userdb.find({"user_id": user_id2, "item_id": i}).count()
            vec2.append(x2)
            fenzi += x1 * x2
            fenmu1 += x1 * x1
            fenmu2 += x2 * x2
    if fenzi is not 0.0:
        cos = fenzi / math.sqrt(fenmu1 * fenmu2)
    return cos

    # common_items.append(i)

    # return common_items


if __name__ == "__main__":
    s = cal_user_likehood("106218649", "106218649")
    print s