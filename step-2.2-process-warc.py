# Portions Copyright (c) Microsoft Corporation.
"""
This file contains the code used to process and create the
FineWeb dataset (https://huggingface.co/datasets/HuggingFaceFW/fineweb)
"""

# from datatrove.executor.slurm import SlurmPipelineExecutor
# from datatrove.executor.local import LocalPipelineExecutor

# from datatrove.pipeline.dedup import MinhashDedupCluster, MinhashDedupFilter, MinhashDedupSignature
# from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
# from datatrove.pipeline.formatters import PIIFormatter
from datatrove.pipeline.readers import JsonlReader, WarcReader
# from datatrove.pipeline.filters import URLFilter, LanguageFilter
# from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter
# from datatrove.utils.hashing import HashConfig

import fire
import time

import sys
sys.path.append('./')
from util.dist import init_dist
from data.obelics_omni.snapshots import get_warcs
import os
import subprocess
import multiprocessing
from functools import partial
# from data.obelics_omni.process.omni_trafilatura import OmniTrafilatura, MagicTrafilatura

from data.obelics_omni.filters.media import MediaFilter
from data.obelics_omni.filters.mediac4_filter import MediaC4QualityFilter
from data.obelics_omni.extractors.magic import MagicExtractor
from data.obelics_omni.executor.azure import LocalPipelineExecutor

import gzip

from datatrove.utils.logging import logger

"""
    we first ran the following pipeline for each dump
"""
# DUMP_TO_PROCESS = "CC-MAIN-2024-30"  # example

import logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
# # Disable the logging from Azure Python SDK
# logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
# # logger.setLevel(logging.WARNING)

TASK = "magic" # "base" # omni

MAIN_OUTPUT_PATH = "DATASET/cc"# "s3://some_s3_bucket"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}/{TASK}_processing"


# def build_logger():
#     # Define a filter that excludes WARNING messages
#     def exclude_warnings(record):
#         return record["level"].name != "WARNING"

#     # Add a sink (e.g., stdout) with the filter applied
#     logger.add(lambda msg: print(msg), filter=exclude_warnings)
    

def download_warc(warc, download_root=MAIN_OUTPUT_PATH, force_download=False):
    url = f"https://data.commoncrawl.org/{warc}"
    output_file_whole = os.path.join(download_root, warc)
    
    if not force_download:
        if os.path.exists(output_file_whole):
            logger.info(f'{output_file_whole} exist, skip')
            return output_file_whole, "" # downloaded, skip
    
    output_folder = os.path.dirname(output_file_whole)
    output_file = os.path.basename(output_file_whole)
    
    logger.info(f'download to: {output_folder}')
    print(f'download to: {output_folder}')
    
    os.makedirs(output_folder, exist_ok=True)
    
    try:
        command = ["wget", url, '-O', output_file]
        # Run the command with a timeout of 10 seconds
        logger.info(f'downloading: {output_file_whole}')
        print(f'downloading: {output_file_whole}')
        completed_process = subprocess.run(command, 
                                           check=True, 
                                           timeout=60*60,
                                           cwd=output_folder,
                                           stdout=subprocess.DEVNULL,
                                           stderr=subprocess.STDOUT)
        return output_file_whole, ""
    except Exception as e:
        logging.info(f"download_warc {e}")
        return output_file_whole, str(e)

# class OmniTrafilatura:
#     def __init__(
#         self,
#         favour_precision: bool = True,
#         include_images: bool = False,
#         timeout: float = 0.1,
#         deduplicate: bool = True,
#         **kwargs,
#     ):
#         super().__init__(favour_precision=favour_precision,
#                          include_images=False,
#                          timeout=timeout,
#                          deduplicate=deduplicate,
#                          **kwargs
#                          )
        # self.favour_precision = favour_precision
        # self.include_images = include_images
        # self.deduplicate = deduplicate
        # self.kwargs = kwargs
        # if self.include_images:
        #     raise NotImplementedError

def get_ex_writer(snapshot, prefix, debug=False):
    if debug:
        return JsonlWriter(f"{FILTERING_OUTPUT_PATH}/{prefix}/{snapshot}")
    else:
        return None


def construct_dummy_files(warc_files, download_root):
    for warc in warc_files:
        output_file_whole = os.path.join(download_root, warc)
        if os.path.exists(output_file_whole):
            continue
        
        output_folder = os.path.dirname(output_file_whole)
        os.makedirs(output_folder, exist_ok=True) 
        
        with gzip.open(output_file_whole, 'wb') as f:
            pass  # No content, just creating an empty file

def download_nltk_resource(local_rank=0):
    # resource
    if local_rank == 0:
        import nltk         
        nltk.download('punkt')
        nltk.download('punkt_tab')
    else:
        time.sleep(60)
    
    

def main(snapshot,
         job_id,
        #  size_per_job=64, # n_node per job
         size_per_rank=100,
         debug=False,
         download_root=MAIN_OUTPUT_PATH,
         workers=-1
         ):
    
    job_id = int(job_id)
    
    # build_logger()
    
    # in run-1, we use nxG8, in run-2, we use nxG1 
    # # get rank
    # world_size, rank, local_size, local_rank = init_dist()
    # if not debug:
    #     assert size_per_job == world_size
        
    download_nltk_resource()
    
    warc_files = get_warcs(snapshot)
    construct_dummy_files(warc_files, download_root)
    print(f"In total: {len(warc_files)} warcs")
    
    # this rank files:
    tasks = len(warc_files)
    local_tasks = size_per_rank
    # local_rank_offset = job_id * (size_per_job * size_per_rank) + rank * size_per_rank # for run-1
    local_rank_offset = job_id * size_per_rank
    if local_rank_offset + local_tasks > tasks:
        local_tasks = tasks - local_rank_offset
    if local_tasks <= 0:
        sys.exit(0)
    
    if debug:
        tasks = len(warc_files)
        local_tasks = 1
        local_rank_offset = 20

    
    main_processing_executor = LocalPipelineExecutor(
        # job_name=f"cc_{DUMP_TO_PROCESS}",
        pipeline=[
            WarcReader(
                # f"s3://commoncrawl/crawl-data/{DUMP_TO_PROCESS}/segments/",
                # "DATASET/cc/crawl-data/CC-MAIN-2024-30/segments/1720763514387.30/warc",
                os.path.join(download_root, "crawl-data", snapshot, "segments"),
                glob_pattern="*/warc/*",  # we want the warc files
                compression="gzip",
                default_metadata={"dump": snapshot},
            ),
            URLFilter(exclusion_writer=get_ex_writer(snapshot, "1_url", debug=debug)), # 
            # trafilatura, # return a string
            MagicExtractor(timeout=1.0), # change to cluster setting
            MediaFilter(),
            LanguageFilter(exclusion_writer=get_ex_writer(snapshot, "2_non_english", debug=debug)), 
            # exclusion_writer=JsonlWriter(
                #     f"{FILTERING_OUTPUT_PATH}/2_non_english/",
                #     output_filename="${language}/" + snapshot + "/${rank}.jsonl.gz",
                #     # folder structure: language/dump/file
                # )
            GopherRepetitionFilter(
                exclusion_writer=get_ex_writer(snapshot, "removed/3_gopher_rep", debug=debug)
            ),
            GopherQualityFilter(
                exclusion_writer=get_ex_writer(snapshot, "removed/4_gopher_qual", debug=debug)
            ),
            MediaC4QualityFilter(
                filter_no_terminal_punct=False,
                exclusion_writer=get_ex_writer(snapshot, "removed/5_c4", debug=debug),
            ),
            # C4QualityFilter(
            #     filter_no_terminal_punct=False,
            #     exclusion_writer=get_ex_writer(snapshot, "removed/5_c4", debug=debug),
            # ),
            FineWebQualityFilter(
                exclusion_writer=get_ex_writer(snapshot, "removed/6_fineweb_qual", debug=debug)
            ),
            JsonlWriter(f"{FILTERING_OUTPUT_PATH}/output/{snapshot}"),
        ],
        # 
        workers=workers if workers > -1 else multiprocessing.cpu_count(),
        # time="10:00:00",
        logging_dir=f"{MAIN_OUTPUT_PATH}/logs/{TASK}_processing/{snapshot}",
        # slurm_logs_folder=f"logs/base_processing/{DUMP_TO_PROCESS}/slurm_logs",  # must be local
        # randomize_start_duration=0, # 180,  # don't hit the bucket all at once with the list requests
        # mem_per_cpu_gb=2,
        # partition="hopper-cpu",
        # local_rank_offset=0,
        # start_method="spawn",
        tasks=tasks,
        local_tasks=local_tasks,
        local_rank_offset=local_rank_offset
    )
    
    # download necessary file
    to_download_warcfiles = []
    for rank in range(local_rank_offset, local_rank_offset+local_tasks):
        if not main_processing_executor.is_rank_completed(rank):
            to_download_warcfiles.append(warc_files[rank])
    print(f"to download: {len(to_download_warcfiles)}")
    
    with multiprocessing.Pool(processes=8) as pool:
        pool.map(partial(download_warc, 
                         download_root=download_root,
                         force_download=True), 
                 to_download_warcfiles)
    
    # run
    main_processing_executor.run()


if __name__ == "__main__":
    fire.Fire(main)