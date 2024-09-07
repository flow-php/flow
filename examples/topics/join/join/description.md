Join allows you to combine two data frames into one, similarly to how SQL JOIN works.   
The first data source is the main one (left), and the second one is joined (right) to it. The join is done based on the specified columns.  

The following types of joins are supported:

* `inner` - only rows with matching keys in both data sources are included in the result
* `left` - all rows from the left data source are included, and matching rows from the right data source are added
* `right` - all rows from the right data source are included, and matching rows from the left data source are added
* `left_anti` - only rows from the left data source that do not have a match in the right data source are included

If joined (right) data frame is too large to fit into memory, consider using [joinEach](/join/join_each/#example) instead.