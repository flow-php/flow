The main difference between [join](/join/join/#example) and `joinEach` is that `joinEach` is designed to handle large data frames that do not fit into memory.  
Instead of loading entire `data_frame` into memory, joinEach expects an implementation of [DataFrameFactory](https://github.com/flow-php/flow/blob/1.x/src/core/etl/src/Flow/ETL/DataFrameFactory.php) 
which will be used to load only specific rows from a source based on passed Rows.

`joinEach` in some cases might become more optimal choice, especially when right size is much bigger then a left side.   
In that case it's better to reduce the ride side by fetching from the storage only what is relevant for the left side.

To maximize performance, you should adjust `DataFrame::batchSize(int $size)`, the default value is 1 which might result
in a large number of calls to `DataFrameFactory::from` method.

The rule of thumb is to set the batch size to the number of rows that DataFrameFactory can safely and quickly load into memory.