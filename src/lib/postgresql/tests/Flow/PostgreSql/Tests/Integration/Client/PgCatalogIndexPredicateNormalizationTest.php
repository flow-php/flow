<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_index;
use function sprintf;

use const PHP_EOL;

final class PgCatalogIndexPredicateNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_index_predicate_norm_test';

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_partial_index_predicate_round_trip_with_implicit_cast(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('orders', self::SCHEMA)
                    ->column(column('id', column_type_integer())->notNull())
                    ->column(column('status', column_type_varchar(16))->notNull())
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->index('idx_orders_active')
                    ->on('orders', self::SCHEMA)
                    ->columns('id')
                    ->where(eq(col('status'), literal('active')))
                    ->toSql(),
            );

        $expected = schema_index(name: 'idx_orders_active', columns: ['id'], predicate: "status = 'active'");

        $table = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('orders');

        static::assertCount(1, $table->indexes);

        $dbIndex = $table->indexes[0];

        static::assertSame('idx_orders_active', $dbIndex->name);
        static::assertTrue(
            $expected->isEqualStructure($dbIndex),
            sprintf(
                'Expected index predicates to be structurally equal.%sExpected: %s%sActual:   %s',
                PHP_EOL,
                $expected->predicate ?? '<null>',
                PHP_EOL,
                $dbIndex->predicate ?? '<null>',
            ),
        );
    }
}
