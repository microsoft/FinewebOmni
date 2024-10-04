# Copyright (c) Microsoft Corporation.


# https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-30/segment.paths.gz

# gzip -d

# wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2018-17/segments/1524125937193.1/warc/CC-MAIN-20180420081400-20180420101400-00000.warc.gz

# 1G per warc

# wget "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-26/warc.paths.gz" -O data/obelics_ml/cc/CC-MAIN-2024-26_warc.paths.gz

import fire
import subprocess
import sys
sys.path.append('./')
from util.dist import init_dist
from data.obelics_ml.snapshots import SNAPSHOTS, get_warcs
import os
from data.obelics_ml.obelics.processors.html_extractor import HtmlExtractor, EncodingExtractor
import io
from warcio.archiveiterator import WARCIterator
from datasets import Dataset
import timeit
from fastwarc.stream_io import GZipStream
from fastwarc.warc import ArchiveIterator, WarcRecordType
from warcio.archiveiterator import WARCIterator
import multiprocessing
from bs4.dammit import EncodingDetector

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
# Disable the logging from Azure Python SDK
logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
# logger.setLevel(logging.WARNING)



    
def warc2htmls(warc_file,
                 num_proc):
                #  convert_ds=True, 
                #  record_type=WarcRecordType.response):
    extractor = HtmlExtractor()
    # must sequentially read
    try:
        stream = GZipStream(open(warc_file, 'rb'))
        # records = [record for record in ArchiveIterator(stream, 
        #                                                 record_types=record_type)]
        # records = [record for record in WARCIterator(stream)]
        pages = []
        encodings = []
        
        for record in WARCIterator(stream):
            try:
                if record.rec_type == "response":
                    page = record.content_stream().read()
                    encoding = record.rec_headers["WARC-Identified-Content-Charset"]
                    # if not encoding:
                    #     for enc in EncodingDetector(page, is_html=True).encodings:
                    #         encoding = enc
                    #         break
                else:
                    page = None
                    encoding = None
            except:
                page = None
                encoding = None

            if page: # and encoding:
                pages.append(page)
                encodings.append(encoding)
                # if len(pages) % 1000 == 0:
                #     logger.info(len(pages))
            
            # if len(pages) >= 100:
            #     break
        # exit(0)
        
        record_dataset = Dataset.from_dict({'page': pages, 'encoding': encodings})
        
        # get encodings
        encoding_extractor = EncodingExtractor()
        record_dataset = record_dataset.map(encoding_extractor, num_proc=num_proc)
        record_dataset = record_dataset.filter(lambda example: example['encoding'] is not None)
        
        # html
        html_dataset = record_dataset.map(extractor, num_proc=num_proc)
        
        num_successes = len([1 for el in html_dataset["html_error"] if not el])
        logger.info(
            f"Success rate for the html extraction: {num_successes} /"
            f" {len(html_dataset)} ({num_successes / len(html_dataset) * 100}%)"
        )
        # exit(0)
        # print(html_dataset[0]); exit(0)
        
        return html_dataset, ""
        
    # except:
        
                        
                        
        # records = [record for record in records if record.rec_type == "response"]
        # record_types="response"
        # print(len(records)); exit(0) # 20991
        
        # if convert_ds:
        #     records = Dataset.from_dict({'record': records})
        
        # return [], ""
    
    except Exception as e:
        logger.info(f"warc2records {e}")
        return [], str(e)

        
def extract_html(records, num_proc, convert_ds=True):
    
    records = records[:100]
    
    try:
        with multiprocessing.Pool(processes=num_proc) as pool:
            html_dataset = pool.map(HtmlExtractor.get_html_from_record, records)
            if convert_ds:
                print(html_dataset)
                html_dataset = Dataset.from_dict({
                    'html': [_[0] for _ in html_dataset],
                    'html_error': [_[1] for _ in html_dataset]
                    })
        # html_dataset = records.map(html_extractor, num_proc=num_proc)
        return html_dataset, ""
    except Exception as e:
        logger.info(f"extract_html {e}")
        return [], str(e)


def main(debug=False,
         root='DATASET/cc',
         n_snapshots=1,
         n_warc=4,
         src='warc_paths',
         dst='crawl-data',
         num_proc=24):
    
    world_size, rank, local_size, local_rank = init_dist()
    
    warcs = get_warcs(root, src, n_snapshots, n_warc)

    # warcs_dataset = [{
    #     "warc": xxx,
    #     "warc_error": False
    # } for warc in warcs]
        
    for w_i, warc in enumerate(warcs[:n_warc]):
        
        if w_i % world_size != rank:
            continue
        
        # download
        logger.info("Starting download_warc")
        warc_file, warc_download_error = download_warc(warc, root)
        if warc_download_error:
            continue
        logger.info("Finished download_warc")
        
        # html
        logger.info("Starting warc2htmls")
        html_dataset, html_error = warc2htmls(warc_file, num_proc=num_proc)
        if html_error:
            continue
        logger.info("Finished warc2htmls")
        
        exit(0)
        
        
            
        # dataset_dict= {
        #     "warc": [b""], # open(warc_file, 'rb').read()
        #     "warc_error": [""]
        # }
        
        

        # warc_dataset = Dataset.from_dict(dataset_dict)

        # start_time = timeit.default_timer()
        # warc_dataset[0]['warc'] = open(warc_file, 'rb').read()
        # evalTime = timeit.default_timer() - start_time
        # print(evalTime)

        # if ("html" not in warc_dataset.column_names) and ("html_error" not in warc_dataset.column_names):
        #     warc_dataset = warc_dataset.add_column("html", [""] * len(warc_dataset))
        #     warc_dataset = warc_dataset.add_column("html_error", [""] * len(warc_dataset))

        # exit(0)

        # html_dataset = warc_dataset.map(html_extractor, num_proc=24)
        
        # print(len(html_dataset)); exit(0)

        # 62974 record -> parsed 41903, 40min
        
        exit(0)
        
        # print(len(htmls)); exit(0)
        
        

if __name__ == "__main__":
    fire.Fire(main)

