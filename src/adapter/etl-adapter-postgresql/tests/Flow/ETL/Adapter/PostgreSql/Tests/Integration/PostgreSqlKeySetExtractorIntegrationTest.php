<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_asc, pgsql_pagination_key_desc, pgsql_pagination_key_set};
use function Flow\ETL\DSL\df;
use function Flow\PostgreSql\DSL\{col, column, create, data_type_integer, data_type_text, delete, drop, insert, literal, select, star, table};
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

final class PostgreSqlKeySetExtractorIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_keyset_test';

    protected function setUp() : void
    {
        parent::setUp();

        $this->client->execute(
            drop()->table($this->tableName)->ifExists()->cascade()
        );

        $this->client->execute(
            create()->table($this->tableName)
                ->column(column('id', data_type_integer())->primaryKey())
                ->column(column('name', data_type_text()))
        );

        $this->client->execute(
            insert()->into($this->tableName)->columns('id', 'name')
                ->values(literal(1), literal('Alice'))
                ->values(literal(2), literal('Bob'))
                ->values(literal(3), literal('Charlie'))
                ->values(literal(4), literal('David'))
                ->values(literal(5), literal('Eve'))
        );
    }

    protected function tearDown() : void
    {
        if (isset($this->client)) {
            $this->client->execute(
                drop()->table($this->tableName)->ifExists()->cascade()
            );
        }

        parent::tearDown();
    }

    public function test_extracts_all_rows_with_keyset_pagination() : void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                pageSize: 2
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
                ['id' => 4, 'name' => 'David'],
                ['id' => 5, 'name' => 'Eve'],
            ],
            $rows
        );
    }

    public function test_extracts_limited_rows_with_maximum() : void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                pageSize: 2,
                maximum: 3
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
            ],
            $rows
        );
    }

    public function test_extracts_with_descending_order() : void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_desc('id')),
                pageSize: 2
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 5, 'name' => 'Eve'],
                ['id' => 4, 'name' => 'David'],
                ['id' => 3, 'name' => 'Charlie'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 1, 'name' => 'Alice'],
            ],
            $rows
        );
    }

    public function test_extracts_with_raw_sql() : void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName,
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                pageSize: 10
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
                ['id' => 3, 'name' => 'Charlie'],
                ['id' => 4, 'name' => 'David'],
                ['id' => 5, 'name' => 'Eve'],
            ],
            $rows
        );
    }

    public function test_returns_empty_for_empty_table() : void
    {
        $this->client->execute(delete()->from($this->tableName));

        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(star())->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                pageSize: 10
            ))
            ->fetch()
            ->toArray();

        self::assertSame([], $rows);
    }
}
