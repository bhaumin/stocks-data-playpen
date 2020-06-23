from os import listdir
from os.path import isfile, join


def list_files(dir_path):
  return [f for f in listdir(dir_path) if isfile(join(dir_path, f))]


def get_markets():
  with open('../data/markets.txt') as f:
    return f.read().split('\n')


def get_stock_symbols(mkt):
  with open(f'../data/symbols/{mkt}.txt') as f:
    return f.read().split('\n')


def main():
  pass


if __name__ == "__main__":
  main()
