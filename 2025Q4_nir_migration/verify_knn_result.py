import json
import os 

dir_path = '/Users/yzhang/development/yzhang-adhoc-analysis/2025Q4_nir_migration/neuralir-knn-try1-yzhang'
file_paths = os.listdir(dir_path)

all_results = []
for fname in file_paths:
    full_path = os.path.join(dir_path, fname)
    with open(full_path, 'r') as f:
        data = json.load(f)
        all_results.extend(data)

print(len(all_results))
row = all_results[0]
row["evalDate"]
row["query"]
type(row["neighbor_listing_ids"])
len(row["neighbor_listing_ids"])

all_queries = sorted([row["query"] for row in all_results])



