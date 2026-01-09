import tensorflow as tf

# Example setup
batch_size = 3
embedding_vocab_size = 100
embedding_dim = 8

# Step 1: Create ragged tensors for features
# feature_a: Each sample has variable number of tokens
feature_a = tf.ragged.constant([
    [5, 12, 7],      # sample 0: 3 tokens
    [23, 45],        # sample 1: 2 tokens  
    [8, 15, 22, 31]  # sample 2: 4 tokens
])

feature_b = tf.ragged.constant([
    [67, 89],        # sample 0: 2 tokens
    [34],            # sample 1: 1 token
    [92, 11]         # sample 2: 2 tokens
])

print("feature_a shape:", feature_a.shape)  # (3, None) - 3 samples, variable length
print("feature_b shape:", feature_b.shape)  # (3, None)

# Step 2: Concatenate along axis=1
token_tensors = {"feature_a": feature_a, "feature_b": feature_b}
full_token_id_tensor = tf.concat(list(token_tensors.values()), axis=1)

print("\nfull_token_id_tensor shape:", full_token_id_tensor.shape)  # (3, None)
print("full_token_id_tensor values:")
print(full_token_id_tensor)
# <tf.RaggedTensor [
#   [5, 12, 7, 67, 89],        # sample 0: 3 + 2 = 5 tokens total
#   [23, 45, 34],               # sample 1: 2 + 1 = 3 tokens total
#   [8, 15, 22, 31, 92, 11]     # sample 2: 4 + 2 = 6 tokens total
# ]>

# Step 3: Apply embedding layer
embedding_layer = tf.keras.layers.Embedding(
    embedding_vocab_size, embedding_dim,
    embeddings_initializer="uniform"
)
embeddings = embedding_layer(full_token_id_tensor)

print("\nembeddings shape:", embeddings.shape)  # (3, None, 8)
print("embeddings.ragged_rank:", embeddings.ragged_rank)  # 1
# Structure: each token ID is now replaced with an 8-dim vector
# Sample 0: [[emb_5], [emb_12], [emb_7], [emb_67], [emb_89]]  -> shape (5, 8)
# Sample 1: [[emb_23], [emb_45], [emb_34]]                     -> shape (3, 8)
# Sample 2: [[emb_8], [emb_15], [emb_22], [emb_31], [emb_92], [emb_11]] -> shape (6, 8)

# Step 4: Average over the ragged dimension
average = tf.reduce_mean(embeddings, axis=embeddings.ragged_rank)

print("\naverage shape:", average.shape) # (3, 8)