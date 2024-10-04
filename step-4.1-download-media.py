# Portions Copyright (c) Microsoft Corporation.
"""
This file contains the code used to process and create the
FineWeb dataset (https://huggingface.co/datasets/HuggingFaceFW/fineweb)
"""

import fire
import time
import sys
sys.path.append('./')
import os
import subprocess
import multiprocessing
from functools import partial
import logging
import gzip
import json

import sys
sys.path.append('./')
from util.dist import init_dist
from data.obelics_omni.executor.azure import LocalPipelineExecutor
from data.obelics_omni.downloader.media import MediaDownloder
# from data.obelics.idle import avoid_idle

# from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.pipeline.dedup import MinhashDedupCluster, MinhashDedupFilter, MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.formatters import PIIFormatter
from datatrove.pipeline.tokens import TokensCounter


TASK = "magic" # "base" # omni

MAIN_OUTPUT_PATH = "DATASET/cc"# "s3://some_s3_bucket"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}/{TASK}_processing"

def get_ex_writer(snapshot, prefix, debug=False):
    if debug:
        return JsonlWriter(f"{FILTERING_OUTPUT_PATH}/{prefix}/{snapshot}")
    else:
        return None


def ensure_json(json_file, max_trials=10):
    
    for i in range(max_trials):
        try:
            j = json.load(open(json_file))
            return
        except Exception as e:
            print(f"{i} {e}")

def main(snapshot,
         job_id,
        #  size_per_job=64, # n_node per job
        #  size_per_rank=100,
         debug=False,
         download_root=MAIN_OUTPUT_PATH,
         workers=-1,
         media_type=0,
         number_sample_per_shard=5000, # image: 5000, audio: 100
         processes_count=16,
         ):
    
    job_id = int(job_id)

    # if not debug:
    #     stop_event, my_thread = avoid_idle(1)
    
    # this rank files:    
    TOTAL_TASKS = 700
    if debug:
        # world_size, rank, local_size, local_rank = init_dist()
        tasks = 14
        local_tasks = 1 # 7 # 1, image
        local_rank_offset = 0 # 7 # 1, image
    else:
        tasks = TOTAL_TASKS
        local_tasks = 1
        local_rank_offset = job_id
    

    MEDIA_BASE_PATH = f"{MAIN_OUTPUT_PATH}/media"
    S3_MINHASH_BASE_PATH = f"{MAIN_OUTPUT_PATH}/minhash"

    MEDIA_LOGS_FOLDER = f"{MAIN_OUTPUT_PATH}/logs/media/{snapshot}"

    # this is the original data that we want to deduplicate
    INPUT_READER = JsonlReader(
        f"{S3_MINHASH_BASE_PATH}/{snapshot}/deduped_output"
    )  # this is the output from dedup
    
    # stage 1 computes minhash signatures for each task (each task gets a set of files)
    stage1 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            MediaDownloder(
                output_folder=f"{MEDIA_BASE_PATH}/{snapshot}/media_type_{media_type}",
                media_type=media_type,
                config={
                    'number_sample_per_shard': number_sample_per_shard,
                    'processes_count': processes_count
                }
            ),
        ],
        tasks=tasks,
        local_tasks=local_tasks,
        local_rank_offset=local_rank_offset,
        workers=workers if workers > -1 else multiprocessing.cpu_count(),
        logging_dir=f"{MEDIA_LOGS_FOLDER}/media_type_{media_type}"
    )

    stage1.run()

    # if not debug:
    #     stop_event.set()
    #     my_thread.join()

if __name__ == "__main__":
    fire.Fire(main)