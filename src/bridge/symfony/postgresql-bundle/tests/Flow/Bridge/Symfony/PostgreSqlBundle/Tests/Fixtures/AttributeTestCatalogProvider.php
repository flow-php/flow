<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures;

use function Flow\PostgreSql\DSL\{schema, schema_column_integer, schema_table};

use Flow\Bridge\Symfony\PostgreSqlBundle\Attribute\AsCatalogProvider;
use Flow\PostgreSql\Schema\{Catalog, CatalogProvider};

#[AsCatalogProvider]
final class AttributeTestCatalogProvider implements CatalogProvider
{
    public function get() : Catalog
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
