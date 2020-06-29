from datetime import datetime, timedelta
from config import yfinance_base_url, base_path
from os.path import join
from constants import *
import utils
import mdb
import sys
import time
import requests
import logging


def main():
  init_logging()

  for mkt in get_markets_to_process():
    for stock_ticker in get_stocks_to_process(mkt):
      last_saved_date = get_last_date(mkt, stock_ticker)
      start_date = calc_start_date(mkt, stock_ticker, last_saved_date)
      end_date = calc_end_date(mkt, stock_ticker)

      if end_date < start_date:
        continue

      start_ts, end_ts = utils.convert_date_to_ts(start_date), \
        utils.convert_date_to_ts(end_date)

      log_message = f'Fetching => mkt={mkt}, stock_ticker={stock_ticker},'
      log_message += f' start_date={start_date} ({start_ts}),'
      log_message += f' end_date={end_date} ({end_ts})'

      logging.info(log_message)

      new_data = fetch(mkt, stock_ticker, start_ts, end_ts)

      if new_data:
        save(mkt, stock_ticker, new_data, last_saved_date)

      # make program wait for 0.5 sec to throttle the api calls
      time.sleep(0.5)


def init_logging():
  file_handler = logging.FileHandler(filename=join(base_path, 'logs/activity.log'))
  stdout_handler = logging.StreamHandler(sys.stdout)
  handlers = [file_handler, stdout_handler]
  logging.basicConfig(
    handlers=handlers,
    format='%(asctime)s %(message)s',
    level=logging.INFO)


def fetch(mkt, stock_ticker, period_start, period_end):
  url_endpoint = f'{yfinance_base_url}/{stock_ticker}'
  payload = {
    'period1': period_start,
    'period2': period_end,
    'interval': '1d',
    'events': 'history'
  }

  with requests.Session() as s:
    r = s.get(url_endpoint, params=payload, allow_redirects=True)

    if r.status_code != 200:
      logging.warning(f'ERROR: status_code={r.status_code}, reason={r.reason}, url={r.url}')
      return

    decoded_content = r.content.decode('utf-8')

  return decoded_content.split('\n')


def save(mkt, stock_ticker, data, last_saved_date):
  coll_name = utils.convert_ticker_to_coll(stock_ticker)
  docs_to_save = utils.parse_ohlcv_csv(data, last_saved_date)

  if len(docs_to_save) > 0:
    resp = mdb.write_many_records(mdb.db_to_use(mkt), coll_name, docs_to_save)
    logging.info(f'Inserted: {len(resp.inserted_ids)} records')


def get_last_date(mkt, stock_ticker):
  coll_name = utils.convert_ticker_to_coll(stock_ticker)
  last_record = mdb.get_latest_record(mdb.db_to_use(mkt), coll_name)
  return last_record['date'] if last_record else None


def calc_start_date(mkt, stock_ticker, last_saved_date):
  td = timedelta(days=1)

  return (last_saved_date + td) \
    if last_saved_date \
    else utils.parse_date_time(DEFAULT_START_DATE, DEFAULT_START_TIME)


def calc_end_date(mkt, stock_ticker):
  today = datetime.utcnow()
  td = timedelta(days=1)

  # TODO Remove this special logic and just use today
  end_date_to_use = (today - td) \
    if (mkt == IN_MKT and stock_ticker[0] == INDEX_PREFIX_EXT) \
    else today

  return datetime(
    end_date_to_use.year,
    end_date_to_use.month,
    end_date_to_use.day,
    21, 0, 0)


def get_markets_to_process():
  if len(sys.argv) > 1:
    return sys.argv[1:2]

  results = mdb.find_records(DB_NAME_META, 'markets')
  return [r['name'] for r in results]


def get_stocks_to_process(mkt):
  if len(sys.argv) > 2:
    return sys.argv[2:]

  filter = { 'market': mkt }
  projection = { '_id': 0, 'ticker': 1 }
  sort = [( 'ticker', 1 )]

  results = mdb.find_records(DB_NAME_META, 'stocks',
    filter=filter,
    projection=projection,
    sort=sort)

  return [r['ticker'] for r in results]


if __name__ == "__main__":
  main()
