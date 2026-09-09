`ignore_error_handler()` keeps the row that threw, without the column that failed. The batches
then no longer share a schema, which is why the output arrives as separate tables.
