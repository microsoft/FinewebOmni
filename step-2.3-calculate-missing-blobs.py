# Portions Copyright (c) Microsoft Corporation.
import subprocess
import fire
import json
import os

def main(snapshot='CC-MAIN-2024-30',
         n=90,
         chunk_size=1000,
         stat_root='DATASET/cc/stat',
         file_format='cc/magic_processing/output/{snapshot}/{index}.jsonl.gz',
         min_size=1000000,
         job_size = 80): # 10 per rank * 8 per job
    
    j = []
    
    for i in range(n):
        try:
            lines = open(os.path.join(stat_root, f"{snapshot}-{str(i).zfill(2)}.json")).read().splitlines()
            lines = [line for line in lines if not line.startswith('WARNING')]
            
            j.extend(json.loads('\n'.join(lines)))
        except Exception as e:
            print(f"{i} {e}")
    
    j = {_['name']: _['properties']['contentLength'] for _ in j}
    
    
    n_valid = 0
    non_valid = []
    
    for i in range(chunk_size * n):
        
        file_name = file_format.format(index=str(i).zfill(5),
                                       snapshot=snapshot)
        
        is_valid = False
        if file_name in j:
            if j[file_name] > min_size:
                is_valid = True
            else:
                print(f"{file_name} -> {j[file_name]}")
        
        if is_valid:
            n_valid += 1
        else:
            non_valid.append(i)
                
    
    print(n_valid)
    
    non_valid_jobs = [_ // job_size for _ in non_valid]
    non_valid_jobs = sorted(list(set(non_valid_jobs)))
    
    with open(os.path.join(stat_root, f"non-valid-jobs-{snapshot}.txt"), 'w') as file:
        file.write(str(non_valid_jobs))
        
    print(len(non_valid_jobs))
    print(non_valid_jobs)
            

if __name__ == "__main__":
    fire.Fire(main)

