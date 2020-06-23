from datetime import datetime, date, timedelta, timezone
import time
import requests
import csv
import utils
import logging


def get_stock_data(stock_symbol, mkt, period_start, period_end):
  URL_PREFIX = 'https://query1.finance.yahoo.com/v7/finance/download'
  CSV_URL = f'{URL_PREFIX}/{stock_symbol}?period1={period_start}&period2={period_end}&interval=1d&events=history'
  csv_file_path = get_csv_file_path(stock_symbol, mkt)

  with requests.Session() as s:
    r = s.get(CSV_URL, allow_redirects=True)

    if r.status_code != 200:
      logging.warning(f'ERROR: status_code={r.status_code}, reason={r.reason}')
      return

    decoded_content = r.content.decode('utf-8')

  try:
    f = open(csv_file_path)

    header_rest = decoded_content.split('\n', 1)

    if len(header_rest) < 2:
      return

    new_data = header_rest[1]
    append_stock_data(csv_file_path, new_data)
    f.close()
  except FileNotFoundError:
    all_data = decoded_content
    write_stock_data(csv_file_path, all_data)


def write_stock_data(file_path, data):
  with open(file_path, 'w') as f:
    f.write(data)


def append_stock_data(file_path, data):
  with open(file_path, 'a+') as f:
    f.seek(0)

    # If file is not empty then append '\n'
    content = f.read(100)
    if len(content) > 0:
      f.write('\n')

    # Append text at the end of file
    f.write(data)


def get_last_date(file_path):
  try:
    with open(file_path) as f:
      all_lines = f.readlines()

      if len(all_lines) < 2:
        return None

    last_line = all_lines[-1]
    last_date_str = last_line.split(',', 1)[0]
    time_str = get_default_start_time_utc_str()
    return datetime.strptime(f'{last_date_str} {time_str}', get_datetime_parse_format())
  except EnvironmentError:
    return None


def convert_date_to_ts(d):
  return time.mktime(d.timetuple())


def get_csv_file_path(stock_symbol, mkt='us'):
  return f'../data/csv/{mkt}/{stock_symbol}.csv'


def get_datetime_parse_format():
  return '%Y-%m-%d %H:%M:%S'


def parse_date_time(date_str, time_str='00:00:00'):
  return datetime.strptime(f'{date_str} {time_str}', get_datetime_parse_format())


def get_default_start_date_str():
  return '1927-12-30'


def get_default_start_time_utc_str():
  return '13:30:00'


def main():
  logging.basicConfig(filename='../logs/activity.log', format='%(asctime)s %(message)s', level=logging.DEBUG)
  is_custom_period = False
  custom_start_date_str = '1985-01-29'
  td = timedelta(days=1)
  today = datetime.utcnow()
  yesterday = today - td

  default_start_time_str = get_default_start_time_utc_str()
  custom_start_date = parse_date_time(custom_start_date_str, default_start_time_str)

  for mkt in utils.get_markets():
    for stock_symbol in utils.get_stock_symbols(mkt):
      default_start_date = parse_date_time(get_default_start_date_str(), get_default_start_time_utc_str())
      stock_csv_path = get_csv_file_path(stock_symbol, mkt)
      stock_last_date = get_last_date(stock_csv_path)

      if stock_last_date:
        default_start_date = (stock_last_date + td)

      start_date = custom_start_date if is_custom_period else default_start_date
      date_to_use = yesterday if stock_symbol[0] == '^' or mkt == 'us' else today
      end_date = datetime(date_to_use.year, date_to_use.month, date_to_use.day, 21, 0, 0)

      if end_date < start_date:
        continue

      start_ts, end_ts = convert_date_to_ts(start_date), convert_date_to_ts(end_date)
      start_ts, end_ts = int(start_ts), int(end_ts)

      logging.info('Fetching => mkt={mkt}, stock_symbol={stock_symbol}, start_date={start_date} ({start_ts}), end_date={end_date} ({end_ts})')

      get_stock_data(stock_symbol, mkt, start_ts, end_ts)

      # make program wait for 0.5 sec to throttle the api calls
      time.sleep(0.5)


if __name__ == "__main__":
  main()
