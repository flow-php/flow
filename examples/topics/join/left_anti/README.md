# Flow PHP - Left Anti Join

Left anti join purpose is to keep anything on the left side that is not present on the right side. 
Typical use case is to synchronize data with external systems, for example sync only new products.


**Left** - `external_products`

| id | sku       |
| -- | --------- | 
| 1  | PRODUCT01 |
| 2  | PRODUCT02 |
| 3  | PRODUCT03 |


**Right** - `internal_products`

| id | sku       |
| -- | --------- |
| 2  | PRODUCT02 |
| 3  | PRODUCT03 |


```SQL
external_products AS ep JOIN left_anti internal_products AS ip ON ep.id = ip.id
```

**Result**

| id | sku       |
| -- | --------- |
| 1  | PRODUCT01 |

Examples: 

- [DataFrame::join - left_anti](left_anti_join.php) - right side is small is constant size and it fits memory
- [DataFrame::joinEach - left_anti](left_anti_join_each.php) - right side is growing and keeping it in memory might become a bottleneck.
