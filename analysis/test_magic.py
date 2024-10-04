# Copyright (c) Microsoft Corporation.
from magic_html import GeneralExtractor
import json
from bs4 import BeautifulSoup

# 初始化提取器
extractor = GeneralExtractor()

url = "http://example.com/"
# html = """

# <!doctype html>
# <html>
# <head>
#     <title>Example Domain</title>

#     <meta charset="utf-8" />
#     <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
#     <meta name="viewport" content="width=device-width, initial-scale=1" />  
# </head>

# <body>
# <div>
#     <h1>Example Domain</h1>
#     <p>This domain is for use in illustrative examples in documents. You may use this
#     domain in literature without prior coordination or asking for permission.</p>
#     <p><a href="https://www.iana.org/domains/example">More information...</a></p>
# </div>
# </body>
# </html>
# """

from bs4 import NavigableString, CData, Tag

# html = open('data/obelics_omni/analysis/test.html').read()

# test case 1:
html = json.load(open('data/obelics_omni/analysis/test.json'))['text']
soup = BeautifulSoup(html, "lxml")

# test case 2
# 文章类型HTML提取数据
# html = open('data/obelics_omni/analysis/test.html').read()
# data = extractor.extract(html, base_url=url)
# soup = BeautifulSoup(data["html"], "lxml")
# 使用get_text()方法抽取所有文本内容，参数"\n"作为不同标签间的分隔符，strip=True去除多余空白
# text_content = soup.get_text("\n", strip=True)


text_types = (NavigableString, CData)

with open('magic.txt', 'w') as file:
    
    for descendant in soup.descendants:
        
        # print(dir(descendant)); exit(0)
        
        # print(type(descendant)); exit(0)
        
        # print(descendant.name); exit(0)
        
        # print(type(descendant))
        
        if type(descendant) in text_types:
            text = descendant.strip()
            if len(text) > 0:
                print(text)
        elif isinstance(descendant, Tag):
            print(descendant.name)
        # elif type()
            
        
        
        # file.write("*"*30+"\n")
        # file.write(str(descendant)+"\n\n")
    
    # file.write(json.dumps(data, indent=2))

# print(data)