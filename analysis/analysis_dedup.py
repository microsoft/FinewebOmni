# Copyright (c) Microsoft Corporation.
import json
from json import JSONDecodeError
from typing import Callable, Literal
import fire
from datatrove.pipeline.readers import JsonlReader

from loguru import logger

            
def main(folder='DATASET/cc/minhash/CC-MAIN-2024-30/deduped_output/',
         filename='00000.jsonl.gz'):
    
    n_image = 0
    n_video = 0
    n_audio = 0
    
    reader = JsonlReader(data_folder=folder)
    
    for doc_id, document in enumerate(reader.read_file(filename)):
        if doc_id % 1000 == 0:
            print(f"processed: {doc_id} with {n_image}/{n_video}/{n_audio}")
        for media in document.media:
            if media['type'] == 0:
                n_image += 1
            elif media['type'] == 1:
                n_video += 1
            elif media['type'] == 2:
                n_audio += 1
    
    print(f"n_image {n_image}")
    print(f"n_video {n_video}")
    print(f"n_audio {n_audio}")
    
    # n_image 524031
    # n_video 1273                                              
    # n_audio 1448 
        

if __name__ == "__main__":
    fire.Fire(main)