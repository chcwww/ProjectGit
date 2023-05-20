import json_line

with open('test.jsonl', 'r+', encoding = 'utf8') as f:
    for item in jsonlines.Reader(f):
        print(item)





















