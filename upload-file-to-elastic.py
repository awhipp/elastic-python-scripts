from elasticsearch import Elasticsearch
import json
import numbers
            
from multiprocessing.pool import ThreadPool as Pool
from random import randint
from time import sleep

# CONFIG
ELASTIC_HOST = '127.0.0.1'
ELASTIC_PORT = 9200
ELASTIC_IDX = 'INDEX_NAME'
ELASTIC_DOC_TYPE = 'DOC_TYPE'
CPU_THREADS = 4
FILE_NAME = "FILE.json"

# CONNECTS TO ELASTIC SEARCH
es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

# DELETES THE INDEX IF IT ALREADY EXISTS
es.indices.delete(index=ELASTIC_IDX, ignore=[400, 404])

###
# THREADED INSERT
###

# Process Line
def process_line(line):
    jsonObj = json.loads(line)
    try:
        es.index(index=ELASTIC_IDX, doc_type=ELASTIC_DOC_TYPE, body=jsonObj)
    except:
        print("Failed: {}".format(line.strip()))


# Get next line
def get_next_line():
    with open(FILE_NAME, 'r') as f:
        cnt = 0
        for line in f:
            yield line
            cnt += 1
            if cnt % 1000 == 0:
                print(cnt)

# Thread management
f = get_next_line()
t = Pool(processes=CPU_THREADS)
for i in f:
    t.map(process_line, (i,))
t.join()
t.close()
