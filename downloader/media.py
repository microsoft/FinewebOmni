# Copyright (c) Microsoft Corporation.
import json
from loguru import logger
from urllib.parse import urljoin
import shutil

from img2dataset import download

from datatrove.data import DocumentsPipeline
from datatrove.io import DataFolderLike, get_datafolder
from datatrove.pipeline.base import PipelineStep
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.binaryio import read_tuples_from_file, seek_to_start
from datatrove.utils.text import TextNormConfig, sha1_hash32, sha1_hash64, simplify_text
from datatrove.utils.typeshelper import StatHints

from .util import download_urls_in_parallel 

from pydub import AudioSegment
from io import BytesIO
from functools import partial
import os
import pandas as pd
from urllib.parse import urlsplit, urljoin
import tempfile
from moviepy.editor import VideoFileClip

############ AUDIO #############

def convert_to_audio(data, url, format='mp3'):
    """
    Convert an audio file to WAV format using pydub.
    :param data: bytes
    :param url: source info, a dict
    """
    # _, format = os.path.splitext(url['url'])
    href = url['url']
    parsed_url = urlsplit(href)
    href_path = parsed_url.path
    _, suffix = os.path.splitext(href_path)
    suffix = suffix.lower()
    audio = AudioSegment.from_file(BytesIO(data), suffix.lstrip('.'))  # Automatically detect format
    buffer = BytesIO()
    # audio.export(buffer, format="wav") # Export as WAV
    audio.export(buffer, format=format) # Export as WAV
    return buffer.read() # return actual bytes

############ VIDEO #############

def convert_to_video(data, url, 
                     format='mp4',
                     fps=30):
    """
    Convert an audio file to WAV format using pydub.
    :param data: bytes
    :param url: source info, a dict
    """
    # _, format = os.path.splitext(url['url'])
    with tempfile.TemporaryDirectory() as temp_dir:
        
        href = url['url']
        parsed_url = urlsplit(href)
        href_path = parsed_url.path
        _, suffix = os.path.splitext(href_path)
        suffix = suffix.lower()
        
        with open(os.path.join(temp_dir, f"input.{suffix}"), 'wb') as file:
            file.write(data)
        
        video = VideoFileClip(os.path.join(temp_dir, f"input.{suffix}"))
        video.write_videofile(os.path.join(temp_dir, f"output.{format}"),
                              fps=fps, 
                              logger=None)
        # buffer = BytesIO()
        # audio.export(buffer, format="wav") # Export as WAV
        # audio.export(buffer, format="mp3") # Export as WAV
        with open(os.path.join(temp_dir, f"output.{format}"), 'rb') as file:
            return file.read() # return actual bytes

#### download ####

def download_to_parquet(batch, process_func, processes_count,
                        filename):

    results = download_urls_in_parallel(
        batch,
        process_func=process_func,
        max_workers=processes_count)
    # print(f"download_to_parquet: {len(results)}")                
    df = pd.DataFrame(results)
    # Save the DataFrame as a Parquet file
    df.to_parquet(filename, engine="pyarrow")


class MediaDownloder(PipelineStep):

    """
    Output: 
    - url jsonl: root/{rank}/url.jsonl
    - media: root/{rank}/media.parquet
    """
    
    type = "🫂 - DOWNLOADER"
    name = "🎯 Media downloader"
    # _requires_dependencies = ["nltk"]

    def __init__(
        self, 
        output_folder: DataFolderLike, 
        media_type: int = 0,
        config: dict = {}
    ):
        super().__init__()
        self.output_folder = get_datafolder(output_folder)
        self.media_type = media_type
        self.config = config

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        
        url_file = f"{rank:05d}/url.jsonl"
        media_folder = f"{rank:05d}/media"
        
        # shutil.rmtree(self.output_folder.resolve_paths(media_folder))
        
        with self.track_time():
            
            logger.info("Prepare media to download...")
            to_download = []
            
            for doc_idx, doc in enumerate(data):
                
                doc_url = doc.metadata['url']
                
                for media_idx, media in enumerate(doc.media):
                    
                    if media['type'] == self.media_type:
                        try:
                            to_download.append(json.dumps({
                                "url": urljoin(doc_url, media['url']),
                                "caption": f"{doc_idx}_{media_idx}"
                            }))
                        except:
                            logger.info(f"Error in preparing url: {doc_url} + {media['url']}")
            
            
            with self.output_folder.open(url_file, mode="w") as fo:
                fo.write('\n'.join(to_download))
                

            logger.info("Downloading media...")
            
            if self.media_type == 0: # image
                download(
                    processes_count=self.config.get('processes_count', 16),
                    thread_count=self.config.get('thread_count', 64),
                    
                    url_list=self.output_folder.resolve_paths(url_file),
                    
                    image_size=self.config.get('image_size', 1344),
                    resize_only_if_bigger=True,
                    resize_mode='keep_ratio_largest',
                    
                    # resize_mode='no', # option 2
                    
                    output_folder=self.output_folder.resolve_paths(media_folder),
                    output_format="parquet",
                    
                    input_format="jsonl",
                    url_col="url",
                    caption_col="caption",
                    # save_additional_columns=['caption'],
                    
                    # compute_hash=True,
                    
                    min_image_size=self.config.get("min_image_size", 150),
                    # max_image_area=20000,
                    max_aspect_ratio=self.config.get("max_aspect_ratio", 10.0),
                    
                    # enable_wandb=True,
                    # number_sample_per_shard=1000,
                    distributor="multiprocessing",
                    number_sample_per_shard=self.config.get("number_sample_per_shard", 5000), # 500M per chunk
                    oom_shard_count=self.config.get("oom_shard_count", 5), # save as 00000.parquet
                    # retries=MAX_RETRIES
                )
            elif self.media_type in [1, 2]: # audio, video
                
                process_func = {
                    1: convert_to_video,
                    2: convert_to_audio
                }[self.media_type]
                
                number_sample_per_shard = self.config.get("number_sample_per_shard", 1000)
                oom_shard_count=self.config.get("oom_shard_count", 5) # save as 00000.parquet
                processes_count = self.config.get('processes_count', 16)
                batch_id = 0
                output_folder=self.output_folder.resolve_paths(media_folder)
                os.makedirs(output_folder, exist_ok=True)
                
                with open(self.output_folder.resolve_paths(url_file)) as fo:
                    batch = []
                    for line in fo:
                        filename = os.path.join(output_folder, f"{str(batch_id).zfill(oom_shard_count)}.parquet")
                        if len(batch) < number_sample_per_shard:
                            batch.append(json.loads(line)) # ; print(batch); exit(0)
                        # check if full
                        if len(batch) == number_sample_per_shard:
                            download_to_parquet(batch, process_func, processes_count, filename)
                            batch = []
                            batch_id += 1
                            logger.info(f"downloaded batch {batch_id}")
                    # residual
                    download_to_parquet(batch, process_func, processes_count, filename)
                        
                
                