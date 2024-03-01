from datasets import load_dataset
from gcsfs import GCSFileSystem

fs = GCSFileSystem()

train_dataset_path = "gs://training-dev-search-data-jtzn/semantic_relevance/dblincoe_LLM_finetune_data/train"
test_dataset_path = "gs://training-dev-search-data-jtzn/semantic_relevance/dblincoe_LLM_finetune_data/test"

train_dataset = load_dataset(
    "json",
    data_files=[
        "/Users/yzhang/development/yzhang-adhoc-analysis/dblincoe-shared/llm-finetuning/relevance_data/train_dataset.jsonl"
    ],
)
train_dataset.save_to_disk(train_dataset_path, fs=fs)

dev_data = load_dataset(
    "json",
    data_files=[
        "/Users/yzhang/development/yzhang-adhoc-analysis/dblincoe-shared/llm-finetuning/relevance_data/test_dataset.jsonl"
    ],
)
dev_data.save_to_disk(test_dataset_path, fs=fs)
