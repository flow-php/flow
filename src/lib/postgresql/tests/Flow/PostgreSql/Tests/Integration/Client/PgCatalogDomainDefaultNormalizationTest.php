<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{
    client_catalog_provider,
    column_type_varchar,
    create,
    literal
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class PgCatalogDomainDefaultNormalizationTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_domain_default_norm_test';

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

    public function test_domain_default_round_trip_with_implicit_cast_on_varchar_literal() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->domain(self::SCHEMA . '.status_type')
                ->as(column_type_varchar(20))
                ->default(literal('pending'))
                ->toSql()
        );

        $schema = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA);

        self::assertCount(1, $schema->domains);
        self::assertSame('status_type', $schema->domains[0]->name);
        self::assertSame("'pending'", $schema->domains[0]->default);
    }
}
