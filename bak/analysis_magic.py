# Copyright (c) Microsoft Corporation.

import json
from bs4 import NavigableString, CData, Tag
from bs4 import BeautifulSoup
import os

from datatrove.data import Document, Media

import orjson

text_types = (NavigableString, CData)
image_types = set(['img']) # 'figure', 'picture', 'canvas', 'svg', 
audio_types = set(['audio'])
video_types = set(['video'])

media_types = set(['a']) # set(['object', 'embed', 'a', 'iframe'])

lines = open('DATASET/cc/magic_processing-output-CC-MAIN-2024-30-00000.jsonl').read().splitlines()
print(f"n: {len(lines)}")

# n_image = 0
# n_audio = 0
# n_video = 0
# n_media = 0

# @dataclass
# class Media:
#     """Media metadata

#     For future uses, currently not used.
#     """

#     type: int
#     url: str
#     alt: str | None = None
#     local_path: str | None = None

# document

    # text: str
    # id: str
    # media: list[Media] = field(default_factory=list)
    # metadata: dict[str, str | int | float | bool] = field(default_factory=dict)


    # elif name == 'iframe':
    #     return tag.attrs.get('src', '')
    # elif name == 'embed':
    #     return tag.attrs.get('src', '')
    # elif name == 'object':
    #     return tag.attrs.get('data', '')
    # else:
    #     return ""
    
medias = {}
media_suffix = {}

images = []
audios = []
videos = []
contents = []

# file_write = open('magic.jsonl', 'w')

documents = []

for line_id, line in enumerate(lines):
    
    if line_id % 1000 == 0:
        print(f"processed: {line_id}")
    
    try:
        j = json.loads(line)
        # debug
        # j = json.load(open('data/obelics_omni/analysis/test.json'))
    except:
        j = {}
    
    texts = []
    
    
    if j:
        
        
            documents.append(document)    
            orjson.dumps(document, option=orjson.OPT_APPEND_NEWLINE)
            # print(document); print(len(document.media)); exit(0)
        
# print(f"n_image: {n_image}")
# print(f"n_audio: {n_audio}")
# print(f"n_video: {n_video}")
# print(f"n_media: {n_media}")

# print(medias)
# print(sorted([(k, v) for k, v in media_suffix.items() if k in image_suffix], key=lambda x:len(x[0])))


# for content in contents:
    



# with open('contents.txt', 'w') as file:
#     file.write('\n'.join(contents))
    
# with open('image.txt', 'w') as file:
#     file.write('\n'.join(images))

# with open('audio.txt', 'w') as file:
#     file.write('\n'.join(audios))
    
# with open('video.txt', 'w') as file:
#     file.write('\n'.join(videos))

print(len(documents))

video_documents = []
audio_documents = []
image_documents = []
text_documents = []

for document in documents:
    
    is_video = any([media.type == 1 for media in document.media])
    if is_video:
        video_documents.append(document)
        
    is_audio = any([media.type == 2 for media in document.media])
    if is_audio:
        audio_documents.append(document)
    
    is_image = any([media.type == 0 for media in document.media])
    if is_image:
        image_documents.append(document)
    
    is_text = (len(document.media) == 0)
    if is_text:
        text_documents.append(document)


print(len(text_documents))
print(len(image_documents))
print(len(video_documents))
print(len(audio_documents))

text_documents = text_documents[:100]
image_documents = image_documents[:100]
    

with open('text_documents.jsonl', 'wb') as file:
    for document in text_documents:
        file.write(orjson.dumps(document, option=orjson.OPT_APPEND_NEWLINE))
            
with open('image_documents.jsonl', 'wb') as file:
    for document in image_documents:
        file.write(orjson.dumps(document, option=orjson.OPT_APPEND_NEWLINE))
        
with open('video_documents.jsonl', 'wb') as file:
    for document in video_documents:
        file.write(orjson.dumps(document, option=orjson.OPT_APPEND_NEWLINE))
        
with open('audio_documents.jsonl', 'wb') as file:
    for document in audio_documents:
        file.write(orjson.dumps(document, option=orjson.OPT_APPEND_NEWLINE))


    
                