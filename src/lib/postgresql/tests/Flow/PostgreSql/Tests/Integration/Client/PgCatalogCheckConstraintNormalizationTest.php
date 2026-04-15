<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{
    check_constraint,
    client_catalog_provider,
    col,
    column,
    column_type_varchar,
    create,
    eq,
    literal,
    schema_check
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class PgCatalogCheckConstraintNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_check_norm_test';

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

    public function test_check_constraint_round_trip_with_implicit_cast_on_text_equality() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->table('orders', self::SCHEMA)
                ->column(column('status', column_type_varchar(32))->notNull())
                ->constraint(check_constraint(eq(col('status'), literal('active')))->name('chk_status_active'))
                ->toSql()
        );

        $expected = schema_check("status = 'active'", 'chk_status_active');

        $table = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('orders');

        self::assertCount(1, $table->checkConstraints);

        $dbConstraint = $table->checkConstraints[0];

        self::assertSame('chk_status_active', $dbConstraint->name);
        self::assertSame($expected->expression, $dbConstraint->expression);
        self::assertTrue($expected->isEqualStructure($dbConstraint));
    }
}
