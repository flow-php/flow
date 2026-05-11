<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures;

use Flow\Bridge\Symfony\PostgreSqlBundle\Attribute\AsCatalogProvider;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;

use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_table;

#[AsCatalogProvider]
final class AttributeTestCatalogProvider implements CatalogProvider
{
    public function get(): Catalog
    {
        return new Catalog([
            schema('public', [
                schema_table('attribute_test', [
                    schema_column_integer('id', nullable: false),
                ]),
            ]),
        ]);
    }
}
