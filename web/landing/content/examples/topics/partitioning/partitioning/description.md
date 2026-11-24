`partitionBy(...)` on a file loader writes one directory per value, in the Hive
`column=value` layout. The partition columns move into the path:

```
output/color=blue/sku=PRODUCT01/products.csv
output/color=blue/sku=PRODUCT02/products.csv
output/color=green/sku=PRODUCT01/products.csv
output/color=green/sku=PRODUCT02/products.csv
output/color=green/sku=PRODUCT03/products.csv
output/color=red/sku=PRODUCT01/products.csv
output/color=red/sku=PRODUCT02/products.csv
output/color=red/sku=PRODUCT03/products.csv
```

If a partition file already exists, the save mode decides what happens.
