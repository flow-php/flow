# Upsert (insert or update) data into a database

There are two ways to update data in the database: 

- `to_dbal_table_insert(...)`
- `to_dbal_table_update(...)`

In order to insert or update data in the database you need to 
use InsertOptions which are platform-specific.

- `MySQLInsertOptions` - `mysql_insert_options(...)`
- `PostgreSQLInsertOptions` - `postgresql_insert_options(...)`
- `SQLiteInsertOptions` - `sqlite_insert_options(...)`


