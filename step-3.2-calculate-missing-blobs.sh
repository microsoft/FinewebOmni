# Portions Copyright (c) Microsoft Corporation.
snapshot=$1
task=$2


az storage blob list --account-name macamltwu3 --container-name mengcliu-data --prefix cc/logs/minhash/$snapshot/$task/completions/ --output json --auth-mode login > DATASET/cc/stat/$snapshot-$task.json 2>&1


