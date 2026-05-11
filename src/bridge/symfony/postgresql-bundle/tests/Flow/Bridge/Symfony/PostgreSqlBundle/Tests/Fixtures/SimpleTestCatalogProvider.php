<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures;

use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;

use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_serial;
use function Flow\PostgreSql\DSL\schema_column_varchar;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;

final class SimpleTestCatalogProvider implements CatalogProvider
{
    public function get(): Catalog
    {
        return new Catalog([
            schema('public', [
                schema_table(
                    'test_users',
                    [
                        schema_column_serial('id'),
                        schema_column_varchar('name', 255, false),
                    ],
                    schema_primary_key(['id']),
                ),
            ]),
        ]);
    }
}
