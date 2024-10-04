# Portions Copyright (c) Microsoft Corporation.

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
from data.obelics_ml.snapshots import SNAPSHOTS
import os


def main(debug=False,
         root='DATASET/cc',
         dst='warc_paths'):
    
    # world_size, rank, local_size, local_rank = init_dist()
    
    for snapshot in SNAPSHOTS:
        url = f"https://data.commoncrawl.org/crawl-data/{snapshot}/warc.paths.gz"
        output_folder = os.path.join(root, dst, snapshot)
        # output_file = os.path.join(output_folder, 'warc.paths.gz')
        os.makedirs(output_folder, exist_ok = True)
        
        # Command to execute
        
        try:
            command = ["wget", url, '-O', 'warc.paths.gz']
            # Run the command with a timeout of 10 seconds
            completed_process = subprocess.run(command, 
                                               check=True, 
                                               timeout=10,
                                               cwd=output_folder)
        except Exception as e:
            print(e)
            
        try:
            command = ["gzip", '-d', 'warc.paths.gz']
            # Run the command with a timeout of 10 seconds
            completed_process = subprocess.run(command, 
                                               check=True, 
                                               timeout=10,
                                               cwd=output_folder)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    fire.Fire(main)

