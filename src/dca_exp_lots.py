from pprint import pprint
import sys

'''
  This python script calculates "k" lots for a
  given input number "n", with each lot size
  increasing exponentially as per provided exponent "exp"

  For e.g. for k = 3, exp = 2 and n = 21
  It should return [3, 6, 12]

  As you can see, each number above is exponentially
  increasing by a factor of exp, and they all add up
  to n, which is 21 in this case

  This utility can be useful for dollar cost averaging
  for stock investment/trading, to bring the avg cost of
  the stocks bought exponentially lower, as opposed to buying
  the same quantity of the shares at a regular interval which
  works in a linear fashion in bringing the avg cost down.
'''
def main():
  DEFAULT_LOTS_COUNT = 2
  DEFAULT_EXP = 2

  lots_count = int(sys.argv[1]) \
    if len(sys.argv) > 1 \
    else DEFAULT_LOTS_COUNT

  exp = int(sys.argv[2]) \
    if len(sys.argv) > 2 \
    else DEFAULT_EXP

  if len(sys.argv) > 3:
    num = int(sys.argv[3])
    ans = get_lots_for_n(num, lots_count, exp)
    print(f'\nInput = {num}:\n')
    pprint(ans, indent=2, width=50)
    print('\n')
    return

  # Default series
  ans = get_lots_for_series(
    nums=[21, 50, 70, 90, 100, 144, 500, 700, 900, 987, 1000, 1600, 2500, 2700],
    lots_count=lots_count,
    exp=exp)

  print('\nDefault Series:\n')
  pprint(ans, indent=2)
  print('\n')


def get_lots_for_series(
  nums=None,
  series_count=20,
  lots_count=2,
  exp=2):

  if nums is None:
    divisors = [7, 9]
    nums = get_series(series_count, divisors)

  out = {}
  for num in nums:
    out[num] = get_lots_for_n(num, lots_count, exp)

  return out


def get_lots_for_n(n, k, exp):
  denom = int(((exp ** k) - 1) / (exp - 1))
  unit = n / denom
  exact = unit == int(unit)
  unit = round(unit)
  lots = []

  index = 0
  total = 0
  part = unit

  while index < k-1:
    total += part
    lots.append(part)
    part = part * exp
    index += 1

  lots.append(n - total)

  return {
    'lots': lots,
    'exact': exact,
    'total': sum(lots)}


if __name__ == '__main__':
  main()
