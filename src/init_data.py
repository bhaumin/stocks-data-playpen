from os.path import join
from config import base_path
from constants import *
from pprint import pprint
import utils
import mdb


def main():
  # migrate_csv_data_to_db()
  # init_markets()
  # init_stock_tickers()
  pass


def migrate_csv_data_to_db():
  for mkt in utils.list_dirs(join(base_path, 'data/csv')):
    dir_path = join(base_path, f'data/csv/{mkt}')
    files = utils.list_files(dir_path)

    for filename in files:
      ticker = utils.remove_filename_ext(filename)
      # for index tickers, change ^ to _
      collection = utils.convert_ticker_to_coll(ticker)
      print(f'ticker={ticker}, collection={collection}')

      with open(join(dir_path, filename)) as csv_file:
        documents = utils.parse_ohlcv_csv(csv_file)
        resp = mdb.write_many_records(mdb.db_to_use(mkt), collection, documents)
        added_index = mdb.add_index_on_date(mkt, mdb.db_to_use(mkt), collection)
        print(f'Inserted: {len(resp.inserted_ids)} records')
        print(f'Index created: {added_index}')


def init_markets():
  records = []
  for mkt in get_markets_to_init():
    records.append({
      "name": mkt
    })

  print(records)
  resp = mdb.write_many_records(DB_NAME_META, 'markets', records)
  print(f'Inserted: {len(resp.inserted_ids)} records')


def init_stock_tickers():
  for mkt in get_markets_to_init():
    records = []
    for ticker in get_stock_tickers_to_init(mkt):
      records.append({
        "ticker": ticker,
        "market": mkt
      })

    print(records)
    resp = mdb.write_many_records(DB_NAME_META, 'stocks', records)
    print(f'Inserted: {len(resp.inserted_ids)} records')


def get_markets_to_init():
  with open(join(base_path, 'data/markets.txt')) as f:
    return [mkt.strip() for mkt in f.read().split('\n') if mkt.strip() != '']


def get_stock_tickers_to_init(mkt):
  with open(join(base_path, f'data/tickers/{mkt}.txt')) as f:
    return [sym.strip() for sym in f.read().split('\n') if sym.strip() != '']


if __name__ == '__main__':
  main()
