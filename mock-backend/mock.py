import csv
import sys
import requests
import json
f = open('sample.csv','rb')
reader = csv.reader(f)
i = 0;
headers = []
cur = reader.next()
while len(cur) <= 1:
	cur = reader.next()
headers = cur
vals = []
i = 0
for row in reader:
	i+=1
	vals.append(row)
	if (i<10):
		print row

print len(vals)
json_req1 = {
	"body":{
        "entityName": "testCount",
        "keys": headers,
        "values": vals
        }
	}
json_req2 = {
	"body":{
        "entityName": "testJsonBulkeSeq",
        "keys": headers,
        "values": vals[20001:40000]
        }
	}

json_req3 = {
	"body":{
        "entityName": "testJsonBulkeSeq",
        "keys": headers,
        "values": vals[40001:60000]
        }
	}
json_req4 = {
	"body":{
        "entityName": "testJsonBulkeSeq",
        "keys": headers,
        "values": vals[60001:80000]
        }
	}

#print json.dumps(json_req)
r = requests.post('http://localhost:8080/json',json = json_req1)		
	

f.close()