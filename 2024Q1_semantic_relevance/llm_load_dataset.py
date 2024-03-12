from datasets import load_dataset
from gcsfs import GCSFileSystem
from typing import Dict
from transformers import AutoTokenizer

MISTRAL_PROMPT = "[INST] Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.\n\n### Instruction:\n{instruction}\n\n### Input:\n{input}\n\n### Options:\n{options}\n\n [/INST] ### Response:\n"
MISTRAL_PROMPT_NO_OPTIONS = "[INST] Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.\n\n### Instruction:\n{instruction}\n\n### Input:\n{input}\n\n [/INST] ### Response:\n"
MISTRAL_RESPONSE_SPLIT = "[/INST] ### Response:"


class MistralPrompter:
    def generate_prompt(self, **row: Dict[str, str]) -> str:
        template = MISTRAL_PROMPT if row.get("options") else MISTRAL_PROMPT_NO_OPTIONS

        res = template.format(**row)

        if row.get("output"):
            res = f"{res}{row['output']}"

        return res

    def get_response(self, output: str) -> str:
        return output.split(MISTRAL_RESPONSE_SPLIT)[1].strip()


def get_data_preprocess_fn(base_model, max_length, train_on_inputs, add_eos_token):
    prompter = MistralPrompter()
    tokenizer = AutoTokenizer.from_pretrained(base_model)

    def tokenize(prompt, add_eos_token):
        result = tokenizer(
            prompt,
            truncation=True,
            max_length=max_length,
            padding=False,
            return_tensors=None,
        )
        if (
            result["input_ids"][-1] != tokenizer.eos_token_id
            and len(result["input_ids"]) < max_length
            and add_eos_token
        ):
            result["input_ids"].append(tokenizer.eos_token_id)
            result["attention_mask"].append(1)
        result["labels"] = result["input_ids"].copy()
        return result

    def generate_and_tokenize_prompt(row):
        full_prompt = prompter.generate_prompt(**row)
        tokenized_full_prompt = tokenize(full_prompt, True)
        if not train_on_inputs:
            if "output" in row:
                del row["output"]
            user_prompt = prompter.generate_prompt(**row)
            tokenized_user_prompt = tokenize(user_prompt, add_eos_token)
            user_prompt_len = len(tokenized_user_prompt["input_ids"])
            if add_eos_token:
                user_prompt_len -= 1
            tokenized_full_prompt["labels"] = [
                -100
            ] * user_prompt_len + tokenized_full_prompt["labels"][user_prompt_len:]
        return tokenized_full_prompt

    return generate_and_tokenize_prompt


base_model: str = "mistralai/Mistral-7B-Instruct-v0.2"
max_length: int = 2048
train_on_inputs: bool = False  # if False, masks out inputs in loss
add_eos_token: bool = False
preprocess_func = get_data_preprocess_fn(
    base_model, max_length, train_on_inputs, add_eos_token
)

fs = GCSFileSystem()

train_dataset_path = "gs://training-dev-search-data-jtzn/semantic_relevance/dblincoe_LLM_finetune_data/train"
test_dataset_path = "gs://training-dev-search-data-jtzn/semantic_relevance/dblincoe_LLM_finetune_data/test"

train_dataset = load_dataset(
    "json",
    data_files=[
        "/Users/yzhang/development/yzhang-adhoc-analysis/dblincoe-shared/llm-finetuning/relevance_data/train_dataset.jsonl"
    ],
)
test_data_format = train_dataset["train"].shuffle().map(preprocess_func)
print(type(test_data_format))

# train_dataset.save_to_disk(train_dataset_path, fs=fs)

# dev_data = load_dataset(
#     "json",
#     data_files=[
#         "/Users/yzhang/development/yzhang-adhoc-analysis/dblincoe-shared/llm-finetuning/relevance_data/test_dataset.jsonl"
#     ],
# )
# dev_data.save_to_disk(test_dataset_path, fs=fs)
