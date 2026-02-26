Divide large datasets into smaller, organized parts based on column values. Flow uses Hive partitioning convention, creating a directory structure where each folder represents a partition value.

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
