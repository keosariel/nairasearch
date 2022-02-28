from testi.index import Index
import csv
from utils import get_index_data
import sys

index = Index("myindex")

def _add():
    with open('100-objects-v1.csv') as cf:
        reader = csv.DictReader(cf)
        index.add_doc(reader)

def add(limit=3000):
    data = get_index_data("nairaland", limit)
    index.add_doc(data)

if len(sys.argv) < 3:
    print("Usage: %s [FUNCTION] [PARAM]" % sys.argv[0])
    sys.exit(1)

func = sys.argv[1].strip()
if func == "-a":
    add(limit=int(sys.argv[2]))
elif func == "-s":
    index.search(sys.argv[2], pagesize=int(sys.argv[3]))
