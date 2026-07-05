Partition into a flat directory structure using `{column}` placeholders in the destination path instead of Hive-style `column=value` directories. Every partition consumed by a placeholder becomes part of the file or directory name; remaining partitions still create `column=value` directories.

```bash
output
├── blue
│   └── PRODUCT02.csv
├── green
│   └── PRODUCT01.csv
└── red
    ├── PRODUCT01.csv
    └── PRODUCT02.csv
```

Reading with the same placeholder pattern recreates the partitions from the path, including support for partition pruning with `filterPartitions()`. Keep in mind that this layout is not self-describing - a plain glob like `output/**/*.csv` will read the data but won't recognize any partitions.
