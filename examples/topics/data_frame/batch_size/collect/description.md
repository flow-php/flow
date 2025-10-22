Collect is used to make sure that all rows are processed at once. This means that all rows are loaded into memory and processed at once.
It's useful mostly for debugging and while working with relatively small datasets.
In order to control memory consumption please use [batchSize](/data_frame/batch_size/#example).
```php