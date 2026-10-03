`to_dbal_table_delete()` deletes the table rows that match a frame row on all of its columns
(`WHERE (order_id) IN (...)` here). The frame holds every order id, so nothing is left.
