from elasticsearch import Elasticsearch
import json
import numbers

ELASTIC_HOST = '127.0.0.1'
ELASTIC_PORT = 9200
ELASTIC_IDX = 'INDEX_NAME'
ELASTIC_DOC_TYPE = 'DOC_TYPE'

FILE_NAME = "file.json"

# CONNECTS TO ELASTIC SEARCH
es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

# DELETES THE INDEX IF IT ALREADY EXISTS
es.indices.delete(index=ELASTIC_IDX, ignore=[400, 404])

with open(FILE_NAME) as fp:
    line = fp.readline()
    cnt = 1
    while line:
        line = fp.readline()
        jsonObj = json.loads(line)
        try:
            es.index(index=ELASTIC_IDX, doc_type=ELASTIC_DOC_TYPE, id=cnt, body=jsonObj)
        except:
            print("Failed on {}: {}".format(cnt, line.strip()))
        finally:
            cnt += 1
        
print("Uploaded {}".format(cnt))
