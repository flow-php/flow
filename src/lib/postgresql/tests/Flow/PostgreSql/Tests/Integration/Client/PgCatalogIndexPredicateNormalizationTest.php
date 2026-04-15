<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{
    client_catalog_provider,
    col,
    column,
    column_type_integer,
    column_type_varchar,
    create,
    eq,
    literal,
    schema_index
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class PgCatalogIndexPredicateNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_index_predicate_norm_test';

    protected function setUp() : void
    {
        parent::setUp();

        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_partial_index_predicate_round_trip_with_implicit_cast() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->table('orders', self::SCHEMA)
                ->column(column('id', column_type_integer())->notNull())
                ->column(column('status', column_type_varchar(16))->notNull())
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->index('idx_orders_active')
                ->on('orders', self::SCHEMA)
                ->columns('id')
                ->where(eq(col('status'), literal('active')))
                ->toSql()
        );

        $expected = schema_index(
            name: 'idx_orders_active',
            columns: ['id'],
            predicate: "status = 'active'",
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('orders');

        self::assertCount(1, $table->indexes);

        $dbIndex = $table->indexes[0];

        self::assertSame('idx_orders_active', $dbIndex->name);
        self::assertTrue(
            $expected->isEqualStructure($dbIndex),
            \sprintf(
                'Expected index predicates to be structurally equal.%sExpected: %s%sActual:   %s',
                \PHP_EOL,
                $expected->predicate ?? '<null>',
                \PHP_EOL,
                $dbIndex->predicate ?? '<null>'
            )
        );
    }
}
