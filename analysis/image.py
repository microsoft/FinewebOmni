# Copyright (c) Microsoft Corporation.
lines_base = open('/data/mengcliu/DATASET/cc/base_processing-output-CC-MAIN-2024-30-00000.jsonl').read().splitlines()
# >>> len(lines_base)
# 9502
lines_omni = open('/data/mengcliu/DATASET/cc/omni_processing-output-CC-MAIN-2024-30-00000.jsonl').read().splitlines()
# >>> len(lines_omni)
# 8593
import json
js_omni = [json.loads(line) for line in lines_omni]
# >>> import 
#   File "<stdin>", line 1
#     import 
#            ^
# SyntaxError: invalid syntax
import re
def extract_urls(text):
    # Regular expression to match URLs inside [text](url)
    pattern = r'\[.*?\]\((.*?)\)'
    urls = re.findall(pattern, text)
    return urls

# urls = extract_urls(js_omni[0])

urls_all = []

for j in js_omni:
    urls_all.extend(extract_urls(j['text']))

images_jpg = [url for url in urls_all if url.endswith('.png')] # 

print(len(images_jpg))

print(images_jpg[:100])  