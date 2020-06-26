from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
import ssl
from config import *


client = MongoClient(mdb_url, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)


def db_to_use(mkt):
  return db_name_in if mkt == IN_MKT else db_name_us


def get_collections(mkt):
  db = client[db_to_use(mkt)]
  return db.list_collection_names()


def add_index_on_date(mkt, coll_name):
  db = client[db_to_use(mkt)]
  coll = db[coll_name]

  if has_index(mkt, coll_name, 'date_1'):
    return

  return coll.create_index([ ('date', ASCENDING) ], unique=True)


def has_index(mkt, coll_name, index_name):
  return index_name in get_indexes(mkt, coll_name)


def get_indexes(mkt, coll_name):
  db = client[db_to_use(mkt)]
  coll = db[coll_name]
  return list(coll.index_information())


def get_latest_record(mkt, coll_name):
  db = client[db_to_use(mkt)]
  coll = db[coll_name]
  result = coll.find().sort("date", DESCENDING).limit(1)
  records = list(result)
  return records[0] if len(records) > 0 else None


def write_one_record(mkt, coll_name, doc):
  db = client[db_to_use(mkt)]
  coll = db[coll_name]
  return coll.insert_one(doc)


def write_many_records(mkt, coll_name, docs):
  db = client[db_to_use(mkt)]
  coll = db[coll_name]
  return coll.insert_many(docs)


def delete_one_record(mkt, coll_name, qry):
  db = client[db_to_use(mkt)]
  coll = db[coll_name]
  return coll.delete_one(qry)


def main():
  # result = get_latest_record('us', 'BA')
  # print(result)
  # result = get_collections_list('us')
  # result = get_indexes('us', 'AAPL')
  # print(result)
  # add_index_on_date('us', 'AAPL')
  pass


if __name__ == '__main__':
  main()
