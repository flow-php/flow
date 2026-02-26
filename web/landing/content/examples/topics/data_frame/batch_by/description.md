Group rows based on column values, ensuring related records stay together in the same batch. This is useful when processing hierarchical data (like orders with line items) where splitting related records would cause integrity issues.

**Important:** Data must be sorted by the grouping column before using batch_by.
