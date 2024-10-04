# Copyright (c) Microsoft Corporation.
import json

file = '/data/mengcliu/DATASET/cc/minhash-CC-MAIN-2024-30-deduped_output-00000.jsonl'

# lines = open('DATASET/cc/cluster-magic_processing-output-CC-MAIN-2024-30-00000.jsonl').read().splitlines()
lines = open(file).read().splitlines()

print(len(lines))

# stat
n_video_doc = 0
n_audio_doc = 0
n_image_doc = 0
n_text_doc = 0
for line in lines:
    try:
        j = json.loads(line)
    except:
        j = {}
    if any([media["type"] == 0 for media in j.get('media', [])]):
        n_image_doc += 1
    if any([media["type"] == 1 for media in j.get('media', [])]):
        n_video_doc += 1
    if any([media["type"] == 2 for media in j.get('media', [])]):
        n_audio_doc += 1
    if all([media["type"] == -1 for media in j.get('media', [])]):
        n_text_doc += 1

print(n_text_doc)
print(n_image_doc)
print(n_video_doc)
print(n_audio_doc)

exit(0)

selected = []

for line in lines:
    if len(selected) >= 100:
        break
    j = json.loads(line)
    if any([media["type"] == 0 for media in j.get('media', [])]):
        selected.append(j)

with open('analysis_magic_8_cluster_image.txt', 'w') as file:
    for document in selected:
        file.write('*'*30+"\n")
        file.write(json.dumps(document, indent=2)+"\n")