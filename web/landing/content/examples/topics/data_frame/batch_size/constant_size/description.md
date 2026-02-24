Batch size defines the size of data frame. In other words, it defines how many rows are processed at once.
This is useful when you have a large dataset, and you want to process it in smaller chunks.
Larger batch size can speed up the processing, but it also requires more memory.
There is no universal rule for the optimal batch size, it depends on the dataset and types of applied transformations.

The Default batch size is `1` this means that each extractor will yield one row at a time.

To process all rows at once, you can use [collect](/data_frame/collect/#example) or set batchSize to `-1`.