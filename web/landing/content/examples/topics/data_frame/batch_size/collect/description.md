Load all rows into memory and process them at once. This is useful for debugging and working with small datasets.

**Warning:** Avoid using collect with large datasets as it loads everything into memory. For controlled memory consumption, use [batchSize](/data_frame/batch_size/#example) instead.
