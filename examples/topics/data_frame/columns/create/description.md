To create new columns (row entries) we always use `DataFrame::withEntry(string $entryName, ScalarFunction|WindowFunction $ref)` method.  
We can create new entry by providing a unique `$entryName`, if the entry already exists it will be replaced.

As a second argument we can provide a static value or a function that will be evaluated for each row. 

* `DataFrame::withEntry('number', lit(5))` - creates a new column with a constant value of 5
* `DataFrame::withEntry('is_odd', ref('another_column')->isOdd())` - creates a new column that checks if the value of `another_column` in is odd
