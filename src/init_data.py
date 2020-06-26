from os.path import join
import mdb
import utils
from pprint import pprint


def migrate_csv_data_to_db():
  for mkt in get_markets():
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
        resp = mdb.write_many_records(mkt, collection, documents)
        added_index = mdb.add_index_on_date(mkt, collection)
        print(f'Inserted: {len(resp.inserted_ids)} records')
        print(f'Index created: {added_index}')


def init_markets():
  pass


def init_stock_symbols():
  pass


def main():
  # migrate_csv_data_to_db()
  pass


if __name__ == '__main__':
  main()
