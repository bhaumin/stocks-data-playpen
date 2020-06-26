from os.path import join
from mdb import write_many_records, db_to_use, add_index_on_date
from config import *
import utils
from pprint import pprint


def migrate_csv_data_to_db():
  for mkt in utils.get_markets():
    dir_path = '../data/csv/' + mkt
    files = utils.list_files(dir_path)

    error_count = 0
    for filename in files:
      ticker_symbol = utils.get_filename_without_ext(filename)
      # for index tickers, change ^ to _
      collection = ticker_symbol.replace('^', '_')
      print(f'ticker={ticker_symbol}, collection={collection}')

      with open(join(dir_path, filename)) as csv_file:
        documents = utils.parse_ohlcv_csv(csv_file)
        resp = write_many_records(db_to_use(mkt), collection, documents)
        added_index = add_index_on_date(db_to_use(mkt), collection)
        print(f'Inserted: {len(resp.inserted_ids)} records')
        print(f'Index created: {added_index}')


def init_markets():
  records = []
  for mkt in utils.get_markets():
    records.append({
      "name": mkt
    })

  print(records)
  resp = write_many_records(db_name_meta, 'markets', records)
  print(f'Inserted: {len(resp.inserted_ids)} records')


def init_stock_symbols():
  for mkt in utils.get_markets():
    records = []
    for symbol in utils.get_stock_symbols(mkt):
      records.append({
        "name": symbol,
        "market": mkt
      })

    print(records)
    resp = write_many_records(db_name_meta, 'stocks', records)
    print(f'Inserted: {len(resp.inserted_ids)} records')


def main():
  # migrate_csv_data_to_db()
  # init_markets()
  # init_stock_symbols()
  pass


if __name__ == '__main__':
  main()
