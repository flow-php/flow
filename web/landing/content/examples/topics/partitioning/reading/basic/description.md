A glob over Hive-style directories (`color=*/sku=*/*.csv`) reads every partition and adds `color`
and `sku` as columns, taken from the path.
