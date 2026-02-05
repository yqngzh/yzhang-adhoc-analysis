from neural_ir.config import NeuralIR
from etsyml.datasets.training_data.retrieval.nir_30d_loose import nir_30d_loose
from etsyml.runtime import paths

model_config = NeuralIR()
input = nir_30d_loose

positive_configs = [config for config in input.configs if (config.data_type == "positive")]
positive_paths = paths.expand(positive_configs)

negative_configs = [config for config in input.configs if (config.data_type == "negative")]
negative_paths = paths.expand(negative_configs)

pos_short_path_seq = []
for pos_path in positive_paths:
    path_split = pos_path.split("/")
    pos_short_path = path_split[-4] + path_split[-3] + path_split[-2]
    if not pos_short_path_seq:
        pos_short_path_seq.append(pos_short_path)
    elif pos_short_path != pos_short_path_seq[-1]:
        pos_short_path_seq.append(pos_short_path)

neg_short_path_seq = []
for neg_path in negative_paths:
    path_split = neg_path.split("/")
    neg_short_path = path_split[-4] + path_split[-3] + path_split[-2]
    if not neg_short_path_seq:
        neg_short_path_seq.append(neg_short_path)
    elif neg_short_path != neg_short_path_seq[-1]:
        neg_short_path_seq.append(neg_short_path)
# ['active_listingsv2_DATE=2026-02-02']
