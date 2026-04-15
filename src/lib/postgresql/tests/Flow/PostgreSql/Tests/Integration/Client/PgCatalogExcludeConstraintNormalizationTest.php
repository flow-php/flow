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
    schema_exclude
};
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class PgCatalogExcludeConstraintNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_exclude_norm_test';

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

    public function test_exclude_constraint_round_trip_with_implicit_cast_in_predicate() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->table('bookings', self::SCHEMA)
                ->column(column('room_id', column_type_integer())->notNull())
                ->column(column('status', column_type_varchar(16))->notNull())
                ->constraint(
                    ExcludeConstraint::create('btree')
                        ->name('exc_room_active')
                        ->element(col('room_id'), '=')
                        ->where(eq(col('status'), literal('active')))
                )
                ->toSql()
        );

        $expected = schema_exclude(
            "USING btree (room_id WITH =) WHERE (status = 'active')",
            'exc_room_active'
        );

        $table = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('bookings');

        self::assertCount(1, $table->excludeConstraints);

        $dbConstraint = $table->excludeConstraints[0];

        self::assertSame('exc_room_active', $dbConstraint->name);
        self::assertTrue(
            $expected->isEqualStructure($dbConstraint),
            \sprintf(
                'Expected EXCLUDE constraint definitions to be structurally equal.%sExpected definition: %s%sActual definition:   %s',
                \PHP_EOL,
                $expected->definition,
                \PHP_EOL,
                $dbConstraint->definition
            )
        );
    }
}
