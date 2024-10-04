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


def download_nltk_resource(local_rank=0):
    # resource
    if local_rank == 0:
        import nltk         
        nltk.download('punkt')
        nltk.download('punkt_tab')
    else:
        time.sleep(60)

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
         task='',
         ):
    
    job_id = int(job_id)
        
    download_nltk_resource()
    
    # this rank files:    
    TOTAL_TASKS = 700
    if debug:
        world_size, rank, local_size, local_rank = init_dist()
        tasks = 14
        local_tasks = tasks // world_size
        local_rank_offset = rank * local_tasks
    else:
        tasks = TOTAL_TASKS
        local_tasks = 1
        local_rank_offset = job_id
    
    # minhash config
    # you can also change ngrams or the number of buckets and their size here
    minhash_config = MinhashConfig(
        use_64bit_hashes=True,  # better precision -> fewer false positives (collisions)
        num_buckets=14,
        hashes_per_bucket=8,
        n_grams=5,
    )

    S3_MINHASH_BASE_PATH = f"{MAIN_OUTPUT_PATH}/minhash"

    S3_LOGS_FOLDER = f"{MAIN_OUTPUT_PATH}/logs/minhash/{snapshot}"

    # this is the original data that we want to deduplicate
    INPUT_READER = JsonlReader(
        f"{FILTERING_OUTPUT_PATH}/output/{snapshot}"
    )  # this is the output from the first part

    # ensure json
    

    assert task != ''
    
    # stage 1 computes minhash signatures for each task (each task gets a set of files)
    stage1 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            MinhashDedupSignature(
                output_folder=f"{S3_MINHASH_BASE_PATH}/{snapshot}/signatures", config=minhash_config
            ),
        ],
        tasks=tasks,
        local_tasks=local_tasks,
        local_rank_offset=local_rank_offset,
        workers=workers if workers > -1 else multiprocessing.cpu_count(),
        logging_dir=f"{S3_LOGS_FOLDER}/signatures"
    )
    
    stage2 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupBuckets(
                input_folder=f"{S3_MINHASH_BASE_PATH}/{snapshot}/signatures",
                output_folder=f"{S3_MINHASH_BASE_PATH}/{snapshot}/buckets",
                config=MinhashConfig(use_64bit_hashes=True),
            ),
        ],
        tasks=tasks,  # the code supports parallelizing each bucket. here we run 50
        local_tasks=local_tasks,
        local_rank_offset=local_rank_offset,
        workers=workers if workers > -1 else multiprocessing.cpu_count(),
        logging_dir=f"{S3_LOGS_FOLDER}/buckets",
        depends=stage1,
    )
    
    stage3 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupCluster(
                input_folder=f"{S3_MINHASH_BASE_PATH}/{snapshot}/buckets",
                output_folder=f"{S3_MINHASH_BASE_PATH}/{snapshot}/remove_ids",
                config=minhash_config,
            ),
        ],
        tasks=1,  # this step runs on a single task
        local_rank_offset=0,
        local_tasks=1,
        logging_dir=f"{S3_LOGS_FOLDER}/clustering",
        depends=stage2,
    )

    stage4 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            TokensCounter(),  # you can remove this one, it's just a nice way to know how many tokens we have
            # before and after dedup
            MinhashDedupFilter(input_folder=f"{S3_MINHASH_BASE_PATH}/{snapshot}/remove_ids"),
            # run the PII removal
            PIIFormatter(),
            JsonlWriter(f"{S3_MINHASH_BASE_PATH}/{snapshot}/deduped_output"),
        ],
        tasks=tasks,
        local_tasks=local_tasks,
        local_rank_offset=local_rank_offset,
        logging_dir=f"{S3_LOGS_FOLDER}/filtering",
        depends=stage3,
    )

    # launch dedup pipelines
    if task == 'signatures':
        stage1.run()
    elif task == 'buckets':
        stage2.run()
    elif task == 'cluster': 
        stage3.run()
    elif task == 'filter':
        stage4.run()
    else:
        raise ValueError


if __name__ == "__main__":
    fire.Fire(main)