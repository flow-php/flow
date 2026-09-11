Control how many rows are processed at once. Larger batch sizes can improve processing speed but require more memory. There is no universal optimal batch size-it depends on your dataset and transformations.

To process all rows at once, use [collect](/data_frame/collect/#example).
