<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Schema\Diff\ColumnDiff;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_double_precision;
use function Flow\PostgreSql\DSL\column_type_numeric;
use function Flow\PostgreSql\DSL\create;
use function sprintf;

final class ColumnDefaultStaleCastDriftConvergenceTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_stale_cast_drift_test';

    private const SOURCE_TABLE = 'amounts';

    private const TARGET_TABLE = 'amounts_target';

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());

        // Stale state: create the column as double precision with a default, then change only its type
        // to numeric(10,3) WITHOUT re-issuing SET DEFAULT. pg_attrdef keeps '0'::double precision.
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::SOURCE_TABLE, self::SCHEMA)
                    ->column(column('amount', column_type_double_precision())->default('0'))
                    ->toSql(),
            );
        $this
            ->pgsqlContext()
            ->client()
            ->execute(sprintf(
                'ALTER TABLE %s.%s ALTER COLUMN amount TYPE numeric(10, 3)',
                self::SCHEMA,
                self::SOURCE_TABLE,
            ));

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TARGET_TABLE, self::SCHEMA)
                    ->column(column('amount', column_type_numeric(10, 3))->default('0'))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_stale_cast_drift_is_detected_and_converges_without_type_change(): void
    {
        static::assertSame("'0'::double precision", $this->pgsqlContext()->columnDefaultExpression(
            self::SCHEMA,
            self::SOURCE_TABLE,
            'amount',
        ));

        $source = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table(self::SOURCE_TABLE)
            ->column('amount');
        $target = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table(self::TARGET_TABLE)
            ->column('amount');

        $sqls = (new ColumnDiff(self::SCHEMA . '.' . self::SOURCE_TABLE, $source, $target))->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            sprintf('ALTER TABLE %s.%s ALTER COLUMN amount SET DEFAULT \'0\'', self::SCHEMA, self::SOURCE_TABLE),
            $sqls[0]->toSql(),
        );

        foreach ($sqls as $sql) {
            $this->pgsqlContext()->client()->execute($sql->toSql());
        }

        static::assertSame("'0'::numeric", $this->pgsqlContext()->columnDefaultExpression(
            self::SCHEMA,
            self::SOURCE_TABLE,
            'amount',
        ));

        $sourceAfter = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table(self::SOURCE_TABLE)
            ->column('amount');

        static::assertSame(
            [],
            (new ColumnDiff(self::SCHEMA . '.' . self::SOURCE_TABLE, $sourceAfter, $target))->generate(),
        );
    }
}
