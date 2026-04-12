<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\DSL\df;
use function Flow\PostgreSql\DSL\{asc, col, column, column_type_integer, column_type_text, create, delete, insert, literal, select, star, table};
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

final class PostgreSqlLimitOffsetExtractorIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_limit_offset_test';

    protected function setUp() : void
    {
        parent::setUp();

        $this->client->execute(
            create()->table($this->tableName)
                ->column(column('id', column_type_integer())->primaryKey())
                ->column(column('name', column_type_text()))
        );

        $this->insertTestData(25);
    }

    public function test_extracts_all_rows_with_pagination() : void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withPageSize(5))
            ->fetch()
            ->toArray();

        self::assertCount(25, $rows);
        self::assertSame(1, $rows[0]['id']);
        self::assertSame(25, $rows[24]['id']);
        self::assertSame(\range(1, 25), \array_column($rows, 'id'));
    }

    public function test_extracts_limited_rows_with_maximum() : void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withPageSize(5)->withMaximum(12))
            ->fetch()
            ->toArray();

        self::assertCount(12, $rows);
        self::assertSame(1, $rows[0]['id']);
        self::assertSame(12, $rows[11]['id']);
        self::assertSame(\range(1, 12), \array_column($rows, 'id'));
    }

    public function test_extracts_with_multiple_positional_parameters() : void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id >= $1 AND id <= $2 ORDER BY id',
                [5, 15],
            )->withPageSize(3))
            ->fetch()
            ->toArray();

        self::assertCount(11, $rows);
        self::assertSame(5, $rows[0]['id']);
        self::assertSame(15, $rows[10]['id']);
        self::assertSame(\range(5, 15), \array_column($rows, 'id'));
    }

    public function test_extracts_with_raw_sql() : void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' ORDER BY id',
            )->withPageSize(5))
            ->fetch()
            ->toArray();

        self::assertCount(25, $rows);
        self::assertSame(1, $rows[0]['id']);
        self::assertSame(25, $rows[24]['id']);
        self::assertSame(\range(1, 25), \array_column($rows, 'id'));
    }

    public function test_extracts_with_single_positional_parameter() : void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id > $1 ORDER BY id',
                [10],
            )->withPageSize(5))
            ->fetch()
            ->toArray();

        self::assertCount(15, $rows);
        self::assertSame(11, $rows[0]['id']);
        self::assertSame(25, $rows[14]['id']);
        self::assertSame(\range(11, 25), \array_column($rows, 'id'));
    }

    public function test_extracts_with_three_positional_parameters() : void
    {
        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id >= $1 AND id <= $2 AND name LIKE $3 ORDER BY id',
                [5, 15, 'User_%'],
            )->withPageSize(3))
            ->fetch()
            ->toArray();

        self::assertCount(11, $rows);
        self::assertSame(5, $rows[0]['id']);
        self::assertSame(15, $rows[10]['id']);
        self::assertSame(\range(5, 15), \array_column($rows, 'id'));
    }

    public function test_returns_empty_for_empty_table() : void
    {
        $this->client->execute(delete()->from($this->tableName));

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withPageSize(10))
            ->fetch()
            ->toArray();

        self::assertSame([], $rows);
    }

    private function insertTestData(int $count) : void
    {
        $insert = insert()->into($this->tableName)->columns('id', 'name');

        for ($i = 1; $i <= $count; $i++) {
            $insert = $insert->values(literal($i), literal(\sprintf('User_%02d', $i)));
        }

        $this->client->execute($insert);
    }
}
