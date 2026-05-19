<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

use function array_column;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\DSL\df;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function range;
use function sprintf;

final class PostgreSqlLimitOffsetExtractorIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_limit_offset_test';

    protected function setUp(): void
    {
        parent::setUp();

        $this->client->execute(
            create()
                ->table($this->tableName)
                ->column(column('id', column_type_integer())->primaryKey())
                ->column(column('name', column_type_text())),
        );

        $this->insertTestData(25);
    }

    public function test_extracts_all_rows_with_pagination(): void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withPageSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(25, $rows[24]['id']);
        static::assertSame(range(1, 25), array_column($rows, 'id'));
    }

    public function test_extracts_limited_rows_with_maximum(): void
    {
        $rows = df()
            ->read(
                from_pgsql_limit_offset(
                    $this->client,
                    select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
                )
                    ->withPageSize(5)
                    ->withMaximum(12),
            )
            ->fetch()
            ->toArray();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(12, $rows[11]['id']);
        static::assertSame(range(1, 12), array_column($rows, 'id'));
    }

    public function test_extracts_with_multiple_positional_parameters(): void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id >= $1 AND id <= $2 ORDER BY id',
                [5, 15],
            )->withPageSize(3))
            ->fetch()
            ->toArray();

        static::assertCount(11, $rows);
        static::assertSame(5, $rows[0]['id']);
        static::assertSame(15, $rows[10]['id']);
        static::assertSame(range(5, 15), array_column($rows, 'id'));
    }

    public function test_extracts_with_raw_sql(): void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' ORDER BY id',
            )->withPageSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(25, $rows[24]['id']);
        static::assertSame(range(1, 25), array_column($rows, 'id'));
    }

    public function test_extracts_with_single_positional_parameter(): void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id > $1 ORDER BY id',
                [10],
            )->withPageSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(15, $rows);
        static::assertSame(11, $rows[0]['id']);
        static::assertSame(25, $rows[14]['id']);
        static::assertSame(range(11, 25), array_column($rows, 'id'));
    }

    public function test_extracts_with_three_positional_parameters(): void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM '
                . $this->tableName
                . ' WHERE id >= $1 AND id <= $2 AND name LIKE $3 ORDER BY id',
                [5, 15, 'User_%'],
            )->withPageSize(3))
            ->fetch()
            ->toArray();

        static::assertCount(11, $rows);
        static::assertSame(5, $rows[0]['id']);
        static::assertSame(15, $rows[10]['id']);
        static::assertSame(range(5, 15), array_column($rows, 'id'));
    }

    public function test_returns_empty_for_empty_table(): void
    {
        $this->client->execute(delete()->from($this->tableName));

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withPageSize(10))
            ->fetch()
            ->toArray();

        static::assertSame([], $rows);
    }

    private function insertTestData(int $count): void
    {
        $insert = insert()->into($this->tableName)->columns('id', 'name');

        for ($i = 1; $i <= $count; $i++) {
            $insert = $insert->values(literal($i), literal(sprintf('User_%02d', $i)));
        }

        $this->client->execute($insert);
    }
}
