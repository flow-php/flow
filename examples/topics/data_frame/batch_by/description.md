Batch by groups rows based on a column value, ensuring that related records stay together in the same batch.
This is particularly useful when processing hierarchical data (like orders with line items) where splitting
related records across batches would cause referential integrity issues during operations like DELETE+INSERT patterns.

When `minSize` is specified, batches are accumulated until they reach the minimum size, then yielded when a new
group is encountered. This improves processing efficiency while maintaining data integrity.

When `minSize` is not specified, each unique group value gets its own batch.

Key features:
- Groups are NEVER split across batches
- Batches may exceed minSize to preserve logical grouping
- Multiple small groups can be combined into one batch (when minSize is set)
