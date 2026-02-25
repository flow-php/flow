Type casting converts values from one type to another. The `cast()` method transforms the input value into the target  
type, throwing `InvalidArgumentException` if conversion is impossible. Unlike assertions which validate existing types,  
casting actively transforms values (e.g., string `"123"` becomes integer `123`, or `"yes"` becomes boolean `true`).  

Casting is ideal for normalizing data from external sources like CSV files, API responses, or user input.
