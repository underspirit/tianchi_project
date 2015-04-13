import pymongo

__author__ = 'ITTC-Jayvee'


class MongodbUtils:
    def __init__(self, db_address, db_port=270107):
        # conn = pymongo.Connection(db_address, db_port)
        conn = pymongo.MongoClient(host=db_address,port=db_port)
        self.__db = conn.TianchiData

    def get_db(self):
        return self.__db


