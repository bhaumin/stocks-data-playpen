from os import listdir
from os.path import isfile, isdir, join
from csv import DictReader
from datetime import datetime
from constants import *
import time
import logging


def list_dirs(dir_path):
  return [d for d in listdir(dir_path) if isdir(join(dir_path, d))]


def list_files(dir_path):
  return [f for f in listdir(dir_path) if isfile(join(dir_path, f))]


def remove_filename_ext(filename):
  return '.'.join(filename.split('.')[0:-1])


def parse_date_time(date_str, time_str='00:00:00'):
  return datetime.strptime(f'{date_str} {time_str}', DATETIME_FORMAT)


def convert_date_to_ts(dt):
  return int(time.mktime(dt.timetuple()))


def convert_ticker_to_coll(ticker):
  return ticker.replace(INDEX_PREFIX_EXT, INDEX_PREFIX_INT)


def parse_ohlcv_csv(csv_data, last_saved_date):
  csv_reader = DictReader(csv_data)

  error_count = 0
  records = []
  for row in csv_reader:
    if not is_data_valid(row):
      error_count += 1
      continue

    new_date = datetime.fromisoformat(row['Date'])

    if last_saved_date and new_date <= last_saved_date:
      continue

    records.append({
      'date': new_date,
      'open': float(row['Open']),
      'high': float(row['High']),
      'low': float(row['Low']),
      'close': float(row['Close']),
      'adj_close': float(row['Adj Close']),
      'volume': int(row['Volume'])
    })

  logging.info(f'Error counts => {error_count}')
  return records


def is_data_valid(rec):
  fields = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
  for field in fields:
    if rec[field] == None or rec[field] == 'null' or rec[field] == '':
      return False

  return True


def main():
  pass


if __name__ == "__main__":
  main()
