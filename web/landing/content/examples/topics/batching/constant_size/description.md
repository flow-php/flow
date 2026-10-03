`batchSize(2)` splits the frame into batches of two rows; `to_output()` prints each batch on its own.
Larger batches mean fewer steps and more memory. `batchSize(-1)` or
[collect()](/batching/collect/#example) puts the whole frame in one batch.
