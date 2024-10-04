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
from data.obelics_ml.snapshots import SNAPSHOTS
import os


def main(debug=False,
         root='DATASET/cc',
         n_snapshots=-1,
         n_warc=-1,
         src='warc_paths',
         dst='crawl-data'):
    
    world_size, rank, local_size, local_rank = init_dist()
    
    if n_snapshots == -1:
        n_snapshots = len(SNAPSHOTS)
    
    for snapshot in SNAPSHOTS[:n_snapshots]:
        
        warcs = open(os.path.join(root, src, snapshot, 'warc.paths')).read().splitlines()
        
        if n_warc == -1:
            n_warc = len(warcs)
        
        for w_i, warc in enumerate(warcs[:n_warc]):
            
            if w_i % world_size != rank:
                continue
            
            url = f"https://data.commoncrawl.org/{warc}"            
            output_file_whole = os.path.join(root, warc)
            
            if os.path.exists(output_file_whole.strip('.gz')):
                continue
            
            output_folder = os.path.dirname(output_file_whole)
            output_file = os.path.basename(output_file_whole)
            
            # print(output_folder)
            # print(output_file)
            # exit(0)
            
            os.makedirs(output_folder, exist_ok=True)
        
            try:
                command = ["wget", url, '-O', output_file]
                # Run the command with a timeout of 10 seconds
                completed_process = subprocess.run(command, 
                                                check=True, 
                                                timeout=60*60,
                                                cwd=output_folder)
            except Exception as e:
                print(e)
            
            try:
                command = ["gzip", '-d', output_file]
                # Run the command with a timeout of 10 seconds
                completed_process = subprocess.run(command, 
                                                check=True, 
                                                timeout=60*60,
                                                cwd=output_folder)
            except Exception as e:
                print(e)

if __name__ == "__main__":
    fire.Fire(main)

