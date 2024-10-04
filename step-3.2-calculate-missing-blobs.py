# Portions Copyright (c) Microsoft Corporation.
import subprocess
import fire
import json
import os

def main(snapshot='CC-MAIN-2024-30',
         task='signatures',
         stat_root='DATASET/cc/stat',
         n=700): 
    
    j = []
    n_valid = 0
    non_valid = []
    
    lines = open(os.path.join(stat_root, f"{snapshot}-{task}.json")).read().splitlines()
    lines = [line for line in lines if not line.startswith('WARNING')]
            
    j = json.loads('\n'.join(lines))
        
    j = {_['name']: _['properties']['contentLength'] for _ in j}
    
    for i in range(n):
        filename = f"cc/logs/minhash/{snapshot}/{task}/completions/{str(i).zfill(5)}"
        if filename not in j:
            non_valid.append(i)
        else:
            n_valid += 1
    
    print(n_valid)
    print(non_valid)
            

if __name__ == "__main__":
    fire.Fire(main)

