import argparse
import requests
import json
from tqdm import tqdm
from itertools import chain
parser = argparse.ArgumentParser()

SIZE = 250  # 250 is maximum for EconBiz API
SEARCH = "https://api.econbiz.de/v1/search"

parser.add_argument('--query', '-q', default="COVID-19", help="Query string")
# parser.add_argument('--start', default="2020-01-01", help="Start date YY-MM-DD")
# parser.add_argument('--end', default="2020-08-31", help="End date YY-MM-DD")
parser.add_argument('--output', '-o', default=None, help="Output path")

args = parser.parse_args()

query = args.query

# First query to get number of records
ret = requests.get(SEARCH, params={'q': query, 'size': 0})
if not ret.ok:
    print(f"Error occurred while connecting to: '{SEARCH}'")
data = ret.json()
n = int(data['hits']['total'])
print(f"Found {n} records for query '{query}'.")

def scroll(pos):
    ret = requests.get(SEARCH, params={'q': query, 'size': SIZE, 'from': pos})
    assert ret.ok
    data = ret.json()
    return data['hits']['hits']

# Second query to get actual data
list_of_record_lists = []
for cursor in tqdm(range(0, n, SIZE), desc="Fetching record data"):
    list_of_record_lists.append(scroll(cursor))
records = list(chain.from_iterable(list_of_record_lists))

if args.output:
    print(f"Saving records to '{args.output}'.")
    with open(args.output, 'w') as outfile:
        json.dump(records, outfile)
else:
    print(json.dumps(records, indent=4))
