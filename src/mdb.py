from pymongo import MongoClient, ASCENDING, DESCENDING
import ssl
from config import mdb_url
from constants import *


client = MongoClient(mdb_url, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)


def db_to_use(mkt):
  return DB_NAME_IN if mkt == IN_MKT else DB_NAME_US


def get_collections(db_name):
  db = client[db_name]
  return db.list_collection_names()


def add_index_on_date(mkt, db_name, coll_name):
  db = client[db_name]
  coll = db[coll_name]

  if has_index(mkt, coll_name, 'date_1'):
    return

  return coll.create_index([ ('date', ASCENDING) ], unique=True)


def has_index(db_name, coll_name, index_name):
  return index_name in get_indexes(db_name, coll_name)


def get_indexes(db_name, coll_name):
  db = client[db_name]
  coll = db[coll_name]
  return list(coll.index_information())


def get_latest_record(db_name, coll_name):
  db = client[db_name]
  coll = db[coll_name]
  result = coll.find().sort("date", DESCENDING).limit(1)
  records = list(result)
  return records[0] if len(records) > 0 else None


def write_one_record(db_name, coll_name, doc):
  db = client[db_name]
  coll = db[coll_name]
  return coll.insert_one(doc)


def write_many_records(db_name, coll_name, docs):
  db = client[db_name]
  coll = db[coll_name]
  return coll.insert_many(docs)


def delete_one_record(db_name, coll_name, qry):
  db = client[db_name]
  coll = db[coll_name]
  return coll.delete_one(qry)


def find_one_record(db_name, coll_name, filter=None, projection=None, sort=None):
  db = client[db_name]
  coll = db[coll_name]
  return coll.find_one(filter=filter, projection=projection, sort=sort)


def find_records(db_name, coll_name, filter=None, projection=None, sort=None):
  db = client[db_name]
  coll = db[coll_name]
  return coll.find(filter=filter, projection=projection, sort=sort)


def main():
  pass


if __name__ == '__main__':
  main()
