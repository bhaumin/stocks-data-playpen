from os import listdir
from os.path import isfile, join
from csv import DictReader
from datetime import datetime


def list_files(dir_path):
  return [f for f in listdir(dir_path) if isfile(join(dir_path, f))]


def get_markets():
  with open('../data/markets.txt') as f:
    return [mkt.strip() for mkt in f.read().split('\n') if mkt.strip() != '']


def get_stock_symbols(mkt):
  with open(f'../data/symbols/{mkt}.txt') as f:
    return [sym.strip() for sym in f.read().split('\n') if sym.strip() != '']


def get_filename_without_ext(filename):
  return '.'.join(filename.split('.')[0:-1])


def parse_ohlcv_csv(csv_data):
  csv_reader = DictReader(csv_data)

  error_count = 0
  records = []
  for row in csv_reader:
    if not is_data_valid(row):
      # print(f'Error=>{row}')
      error_count += 1
      continue

    records.append({
      'date': datetime.fromisoformat(row['Date']),
      'open': float(row['Open']),
      'high': float(row['High']),
      'low': float(row['Low']),
      'close': float(row['Close']),
      'adj_close': float(row['Adj Close']),
      'volume': int(row['Volume'])
    })

  print(f'Error counts => {error_count}')
  return records


def is_data_valid(rec):
  fields = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
  for field in fields:
    if rec[field] == None or rec[field] == 'null' or rec[field] == '':
      return False

  return True


def main():
  print(get_markets())
  print(get_stock_symbols('us'))
  print(get_stock_symbols('in'))


if __name__ == "__main__":
  main()
