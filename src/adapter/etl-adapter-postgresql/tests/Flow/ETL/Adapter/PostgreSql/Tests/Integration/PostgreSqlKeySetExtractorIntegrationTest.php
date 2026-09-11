<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;

use function array_column;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_key_set;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_desc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
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

final class PostgreSqlKeySetExtractorIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_keyset_test';

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

    public function test_extracts_all_rows_with_keyset_pagination(): void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize(5))
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
                from_pgsql_key_set(
                    $this->client,
                    select(col('id'), col('name'))->from(table($this->tableName)),
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                )
                    ->withBatchSize(5)
                    ->withMaximum(12),
            )
            ->fetch()
            ->toArray();

        static::assertCount(12, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(12, $rows[11]['id']);
        static::assertSame(range(1, 12), array_column($rows, 'id'));
    }

    public function test_extracts_with_descending_order(): void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_desc('id')),
            )->withBatchSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(25, $rows[0]['id']);
        static::assertSame(1, $rows[24]['id']);
        static::assertSame(range(25, 1), array_column($rows, 'id'));
    }

    public function test_extracts_with_multiple_positional_parameters(): void
    {
        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id >= $1 AND id <= $2',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                [5, 15],
            )->withBatchSize(3))
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
            ->read(from_pgsql_key_set(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName,
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize(5))
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
            ->read(from_pgsql_key_set(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id > $1',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                [10],
            )->withBatchSize(5))
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
            ->read(from_pgsql_key_set(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id >= $1 AND id <= $2 AND name LIKE $3',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                [5, 15, 'User_%'],
            )->withBatchSize(3))
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
            ->read(from_pgsql_key_set(
                $this->client,
                select(star())->from(table($this->tableName)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize(10))
            ->fetch()
            ->toArray();

        static::assertSame([], $rows);
    }

    public function test_schema_and_extract_agree(): void
    {
        $extractor = from_pgsql_key_set(
            $this->client,
            sprintf('SELECT id, name FROM %s', $this->tableName),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        );
        $batch = df()->read($extractor)->fetch();

        static::assertSame($extractor->schema()->references()->names(), $batch->first()->names());
        static::assertTrue($batch->schema()->isSame($extractor->schema()));
    }

    public function test_schema_comes_from_result_metadata(): void
    {
        $schema = from_pgsql_key_set(
            $this->client,
            sprintf('SELECT id, name FROM %s', $this->tableName),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->schema();

        static::assertSame(['id', 'name'], $schema->references()->names());
        static::assertTrue($schema->findDefinition('id')?->isNullable());
        static::assertTrue($schema->findDefinition('name')?->isNullable());
    }

    public function test_a_parameterised_query_describes(): void
    {
        $extractor = from_pgsql_key_set(
            $this->client,
            sprintf('SELECT id, name FROM %s WHERE id > $1', $this->tableName),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            [20],
        );

        static::assertSame(['id', 'name'], $extractor->schema()->references()->names());
        static::assertCount(5, df()->read($extractor)->fetch()->toArray());
    }

    public function test_a_trailing_semicolon_query_still_describes(): void
    {
        static::assertSame(
            ['id'],
            from_pgsql_key_set(
                $this->client,
                sprintf('SELECT id FROM %s;', $this->tableName),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_reads_inside_from_all(): void
    {
        // ChainExtractor matches every child batch to its own schema(), so an extractor that derives
        // schema() but leaves extract() untyped throws SchemaMismatchException here.
        static::assertCount(
            26,
            df()
                ->read(from_all(
                    from_pgsql_key_set(
                        $this->client,
                        sprintf('SELECT id, name FROM %s', $this->tableName),
                        pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                    ),
                    from_array([['id' => 99, 'name' => 'from array']]),
                ))
                ->fetch()
                ->toArray(),
        );
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
