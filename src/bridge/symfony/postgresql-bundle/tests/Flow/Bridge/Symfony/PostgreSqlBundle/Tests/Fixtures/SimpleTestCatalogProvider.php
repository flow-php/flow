<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures;

use function Flow\PostgreSql\DSL\{schema, schema_column_serial, schema_column_varchar, schema_primary_key, schema_table};

use Flow\PostgreSql\Schema\{Catalog, CatalogProvider};

final class SimpleTestCatalogProvider implements CatalogProvider
{
    public function get() : Catalog
    {
        return new Catalog([
            schema('public', [
                schema_table('test_users', [
                    schema_column_serial('id'),
                    schema_column_varchar('name', 255, false),
                ], schema_primary_key(['id'])),
            ]),
        ]);
    }
}
