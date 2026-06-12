<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Diff\ColumnDiff;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_timestamptz;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function sprintf;

final class ColumnDefaultNoFalsePositiveDriftTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_no_false_positive_drift_test';

    private const TABLE = 'defaults';

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
        $this->pgsqlContext()->client()->execute(create()->sequence('seq', self::SCHEMA)->toSql());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE, self::SCHEMA)
                    ->column(column('n', column_type_integer())->default(5))
                    ->column(column('s', column_type_varchar(255))->default('pending'))
                    ->column(column('t', column_type_text())->default('plain'))
                    ->column(column('ts', column_type_timestamptz())->default(func('now')))
                    ->column(column('seq_col', column_type_integer())->default(func('nextval', [literal(self::SCHEMA
                    . '.seq')])))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_declared_defaults_do_not_drift_against_introspected_implicit_casts(): void
    {
        $table = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table(self::TABLE);

        $declared = [
            'n' => Column::create('n', column_type_integer(), true, 5),
            's' => Column::create('s', column_type_varchar(255), true, 'pending'),
            't' => Column::create('t', column_type_text(), true, 'plain'),
            'ts' => Column::create('ts', column_type_timestamptz(), true, func('now')),
        ];

        foreach ($declared as $name => $declaredColumn) {
            static::assertSame(
                [],
                (new ColumnDiff(self::SCHEMA . '.' . self::TABLE, $table->column($name), $declaredColumn))->generate(),
                sprintf('Column "%s" reported spurious drift', $name),
            );
        }
    }

    public function test_all_defaults_including_sequence_round_trip_without_drift(): void
    {
        $first = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table(self::TABLE);
        $second = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table(self::TABLE);

        foreach (['n', 's', 't', 'ts', 'seq_col'] as $name) {
            static::assertSame(
                [],
                (new ColumnDiff(
                    self::SCHEMA . '.' . self::TABLE,
                    $first->column($name),
                    $second->column($name),
                ))->generate(),
                sprintf('Column "%s" reported spurious drift on round-trip', $name),
            );
        }
    }
}
