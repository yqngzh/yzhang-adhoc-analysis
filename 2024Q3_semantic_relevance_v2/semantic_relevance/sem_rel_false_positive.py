import tensorflow as tf
import tensorflow_text as text
import tensorflow_ranking as tfr
import pandas as pd

teacher_model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l24-h1024-a16-v1/export/saved_model"
integration_model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l2-h128-a2-v1/export/saved_model"

teacher_model = tf.saved_model.load(teacher_model_path)
integration_model = tf.saved_model.load(integration_model_path)


df = pd.read_csv("/home/yzhang/development/0_yzhang_local/semantic_relevance/bert_ce_gold_query_set.csv")
query = list(df["query"].values)
title = list(df["listing_title"].values)

teacher_outputs = teacher_model.signatures["serving_default"](
    queries=query,
    titles=title,
)

integration_outputs = integration_model.signatures["serving_default"](
    queries=query,
    titles=title,
)

df["teacher_label"] = tf.argmax(teacher_outputs["softmax"], axis=1).numpy()
df["teacher_scores"] = teacher_outputs["softmax"][:, 3].numpy()
df["integration_label"] = tf.argmax(integration_outputs["softmax"], axis=1).numpy()
df["integration_scores"] = integration_outputs["softmax"][:, 3].numpy()

df.to_csv("/home/yzhang/development/0_yzhang_local/semantic_relevance/bert_ce_gold_query_set.csv", index=False)

df.teacher_label.value_counts()
df.teacher_scores[df.teacher_label != 3].describe()

df.integration_label.value_counts()
df.integration_scores[df.integration_label != 3].describe()
