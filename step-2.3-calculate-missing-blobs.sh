# Portions Copyright (c) Microsoft Corporation.
snapshot=$1

for i in $(seq -w 0 90)
do
   echo "Iteration: $i"
   az storage blob list --account-name macamltwu3 --container-name mengcliu-data --prefix cc/magic_processing/output/$snapshot/$i --output json --auth-mode login > DATASET/cc/stat/$snapshot-$i.json 2>&1
done

