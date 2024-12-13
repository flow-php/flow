Read data from Database through Doctrine DBAL

The example below shows how to read from a single table using limit/offset extractor.  
To read data from more advanced queries, you can use one of the following extractors:

```php
// read from a single table using limit/offset pagination
from_dbal_limit_offset(
    $connection,
    'orders',
    new OrderBy('created_at', Order::DESC),
)
// read from a query using limit/offset pagination        
from_dbal_limit_offset_qb(
    $connection,
    $connection->createQueryBuilder()->select('*')->from('orders')
);
// read from a single query
from_dbal_query(
    $connection,
    'SELECT * FROM orders',
);
// read from multiple queries each time using next parameters from the provided set
from_dbal_queries(
    $connection,
    'SELECT * FROM orders LIMIT :limit OFFSET :offset',
    new \Flow\ETL\Adapter\Doctrine\ParametersSet(
        ['limit' => 2, 'offset' => 0],
        ['limit' => 2, 'offset' => 2],
    )
);
```

Additionally, each of them allows setting dataset schema through  `$extractor->withSchema(Schema $schema)` method.