`{column}` placeholders in the destination path name files and directories after the partition
values, instead of Hive `column=value` directories. A partition column without a placeholder still
gets a `column=value` directory.

```
output/blue/PRODUCT02.csv
output/green/PRODUCT01.csv
output/red/PRODUCT01.csv
output/red/PRODUCT02.csv
```

Reading with the same pattern restores `color` and `sku` from the path, and `filter()` on them
prunes files. The layout is not self-describing: a plain glob such as `output/**/*.csv` reads the
data without the partition columns.
