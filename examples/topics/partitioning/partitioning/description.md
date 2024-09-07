Partitioning is a technique to divide a large dataset into smaller, more manageable parts.
When you partition a dataset and write it to any file-based destination Flow fill follow Hive partitioning convention.
The partitioning is done by creating a directory structure where each directory represents a partition.
The directory name is in the format of `column=value`.

```bash
output
├── color=blue
│   ├── sku=PRODUCT01
│   │   └── products.csv
│   └── sku=PRODUCT02
│       └── products.csv
├── color=green
│   ├── sku=PRODUCT01
│   │   └── products.csv
│   ├── sku=PRODUCT02
│   │   └── products.csv
│   └── sku=PRODUCT03
│       └── products.csv
└── color=red
    ├── sku=PRODUCT01
    │   └── products.csv
    ├── sku=PRODUCT02
    │   └── products.csv
    └── sku=PRODUCT03
        └── products.csv
```
