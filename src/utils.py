from os import listdir
from os.path import isfile, join


def list_files(dir_path):
  return [f for f in listdir(dir_path) if isfile(join(dir_path, f))]


def get_markets():
  with open('../data/markets.txt') as f:
    return [mkt.strip() for mkt in f.read().split('\n') if mkt.strip() != '']


def get_stock_symbols(mkt):
  with open(f'../data/symbols/{mkt}.txt') as f:
    return [sym.strip() for sym in f.read().split('\n') if sym.strip() != '']


def main():
  print(get_markets())
  print(get_stock_symbols('us'))
  print(get_stock_symbols('in'))


if __name__ == "__main__":
  main()
