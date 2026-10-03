`call()` applies any PHP callable to a column when no built-in function fits. The callable's result
type is declared (`type_list(type_integer())`), so the schema knows it before a row is read.
