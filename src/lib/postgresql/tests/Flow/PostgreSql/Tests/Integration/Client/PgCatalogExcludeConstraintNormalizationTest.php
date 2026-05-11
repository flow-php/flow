<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_exclude;

final class PgCatalogExcludeConstraintNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_exclude_norm_test';

    protected function setUp(): void
    {
        parent::setUp();

        if (!\extension_loaded('pg_query')) {
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

    public function test_exclude_constraint_round_trip_with_implicit_cast_in_predicate(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('bookings', self::SCHEMA)
                    ->column(column('room_id', column_type_integer())->notNull())
                    ->column(column('status', column_type_varchar(16))->notNull())
                    ->constraint(
                        ExcludeConstraint::create('btree')
                            ->name('exc_room_active')
                            ->element(col('room_id'), '=')
                            ->where(eq(col('status'), literal('active'))),
                    )
                    ->toSql(),
            );

        $expected = schema_exclude("USING btree (room_id WITH =) WHERE (status = 'active')", 'exc_room_active');

        $table = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('bookings');

        static::assertCount(1, $table->excludeConstraints);

        $dbConstraint = $table->excludeConstraints[0];

        static::assertSame('exc_room_active', $dbConstraint->name);
        static::assertTrue(
            $expected->isEqualStructure($dbConstraint),
            \sprintf(
                'Expected EXCLUDE constraint definitions to be structurally equal.%sExpected definition: %s%sActual definition:   %s',
                \PHP_EOL,
                $expected->definition,
                \PHP_EOL,
                $dbConstraint->definition,
            ),
        );
    }
}
