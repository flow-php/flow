`to_pgsql_transaction()` commits several loaders as one unit - every batch in its own transaction,
rolled back together if any loader fails. All wrapped loaders must share the wrapper's `Client`.
