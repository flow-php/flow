`collect()` merges every batch into one, so the next step sees the whole frame at once. Memory grows
with the data - keep it for small frames, or bound it with
[batchSize()](/batching/constant_size/#example).
