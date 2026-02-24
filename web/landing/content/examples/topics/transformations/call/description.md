The easiest way to `call` any callable. Whenever you need to apply a function or method to a column or a value,
and there is no existing scalar function available, you can use the `call` scalar function. 

There are two ways to use it: 

- `ref('column')->call()`
- `\Flow\ETL\DSL\call()`

The main difference is that the first one will always pass the column value as argument.   
When passing additional arguments, it might be necessary to use `refAlias` to provide a argument name for column value.