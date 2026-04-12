<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_limit_offset, pgsql_delete_options, pgsql_insert_options, pgsql_update_options, to_pgsql_table};
use function Flow\ETL\DSL\{df, from_array};
use function Flow\PostgreSql\DSL\{asc, col, column, column_type_integer, column_type_text, create, select, star, table};
use Flow\ETL\Adapter\PostgreSql\Operation;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

final class PostgreSqlLoaderIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_loader_test';

    protected function setUp() : void
    {
        parent::setUp();

        $this->client->execute(
            create()->table($this->tableName)
                ->column(column('id', column_type_integer())->primaryKey())
                ->column(column('name', column_type_text()))
                ->column(column('email', column_type_text())->unique())
        );
    }

    public function test_deletes_rows_by_primary_key() : void
    {
        $this->insertTestData();

        df()
            ->read(from_array([
                ['id' => 1],
                ['id' => 3],
            ]))
            ->write(
                to_pgsql_table($this->client, $this->tableName)
                    ->withOperation(Operation::DELETE)
                    ->withDeleteOptions(pgsql_delete_options(['id']))
            )
            ->run();

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
            ],
            $rows
        );
    }

    public function test_inserts_multiple_rows() : void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
            ]))
            ->write(to_pgsql_table($this->client, $this->tableName))
            ->run();

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
            ],
            $rows
        );
    }

    public function test_inserts_with_skip_conflicts() : void
    {
        $this->insertTestData();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice Updated', 'email' => 'alice@example.com'],
                ['id' => 4, 'name' => 'David', 'email' => 'david@example.com'],
            ]))
            ->write(
                to_pgsql_table($this->client, $this->tableName)
                    ->withInsertOptions(pgsql_insert_options(skipConflicts: true))
            )
            ->run();

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
                ['id' => 4, 'name' => 'David', 'email' => 'david@example.com'],
            ],
            $rows
        );
    }

    public function test_updates_existing_rows() : void
    {
        $this->insertTestData();

        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice Updated', 'email' => 'alice_updated@example.com'],
                ['id' => 2, 'name' => 'Bob Updated', 'email' => 'bob_updated@example.com'],
            ]))
            ->write(
                to_pgsql_table($this->client, $this->tableName)
                    ->withOperation(Operation::UPDATE)
                    ->withUpdateOptions(pgsql_update_options(['id']))
            )
            ->run();

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice Updated', 'email' => 'alice_updated@example.com'],
                ['id' => 2, 'name' => 'Bob Updated', 'email' => 'bob_updated@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
            ],
            $rows
        );
    }

    public function test_upserts_on_conflict_columns() : void
    {
        $this->insertTestData();

        df()
            ->read(from_array([
                ['id' => 10, 'name' => 'Alice Updated', 'email' => 'alice@example.com'],
                ['id' => 4, 'name' => 'David', 'email' => 'david@example.com'],
            ]))
            ->write(
                to_pgsql_table($this->client, $this->tableName)
                    ->withInsertOptions(pgsql_insert_options(
                        conflictColumns: ['email'],
                        updateColumns: ['name']
                    ))
            )
            ->run();

        $rows = df()
            ->read(from_pgsql_limit_offset(
                $this->client,
                select(star())->from(table($this->tableName))->orderBy(asc(col('id'))),
            ))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['id' => 1, 'name' => 'Alice Updated', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
                ['id' => 4, 'name' => 'David', 'email' => 'david@example.com'],
            ],
            $rows
        );
    }

    private function insertTestData() : void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
                ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
                ['id' => 3, 'name' => 'Charlie', 'email' => 'charlie@example.com'],
            ]))
            ->write(to_pgsql_table($this->client, $this->tableName))
            ->run();
    }
}
