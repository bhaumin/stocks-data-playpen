Stocks Data Playpen
===================

Playpen to build data models on historical OHLCV stock data to gain valuable insights and predict future price movements


## Instructions
Create and init the following local files to boot


### src/config.py
```
yfinance_base_url = {yahoo_finance_url}
mdb_url = {mongodb_srv_url}
mdb_user = {mongodb_username}
mdb_pass = {mongodb_password}
base_path = {absolute_path_of_this_project}
```

Initialize with the stock markets whose stocks you are interested in following. This is just to setup the DB and populate it with some initial data. Going forward insert the market or stock directly in the DB.


### data/markets.txt (sample)
```
us
in
```

For each of the markets listed in the markets.txt file create corresponding ticker files for each market in the following format.


### data/tickers/us.txt
```
^GSPC
^IXIC
AAPL
TSLA
```


### data/tickers/in.txt
```
^NSEI
LT.NS
INFY.NS
RELIANCE.NS
TCS.NS
```


### Initialize DB

Modify the main() method of the following script to execute the 2 methods listed below.

* src/init_data.py
  * init_markets()
  * init_stock_tickers()


## Run
* Fetch data for all your markets and stocks
  * `python3 src/fetch_new_data.py`
* Fetch data for all stocks of only a given market
  * `python3 src/fetch_new_data.py us`
  * `python3 src/fetch_new_data.py in`
* Fetch data for select stocks only
  * `python3 src/fetch_new_data.py us AAPL`
  * `python3 src/fetch_new_data.py us AAPL TSLA`
  * `python3 src/fetch_new_data.py in INFY.NS TCS.NS`


## Credits

* Stocks data from Yahoo Finance
* Python - Pandas, Matplotlib, requests
* Jupyter notebook
