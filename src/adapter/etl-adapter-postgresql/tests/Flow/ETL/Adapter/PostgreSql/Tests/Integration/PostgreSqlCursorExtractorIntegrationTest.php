<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\PostgreSql\Client\Exception\QueryException;

use function array_column;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\PostgreSql\DSL\analyze;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\binary_expr;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_array;
use function Flow\PostgreSql\DSL\column_type_custom;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\gt;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\table_func;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function range;
use function sprintf;

final class PostgreSqlCursorExtractorIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_cursor_test';

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

    public function test_extracts_all_rows_with_cursor(): void
    {
        $rows = df()
            ->read(from_pgsql_cursor(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withBatchSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(25, $rows[24]['id']);
        static::assertSame(range(1, 25), array_column($rows, 'id'));
    }

    public function test_extracts_all_rows_without_order_by(): void
    {
        $rows = df()
            ->read(from_pgsql_cursor(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName)),
            )->withBatchSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
    }

    public function test_extracts_limited_rows_with_maximum(): void
    {
        $rows = df()
            ->read(
                from_pgsql_cursor(
                    $this->client,
                    select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
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

    public function test_extracts_with_custom_cursor_name(): void
    {
        $rows = df()
            ->read(
                from_pgsql_cursor(
                    $this->client,
                    select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
                )
                    ->withBatchSize(5)
                    ->withCursorName('my_custom_cursor'),
            )
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(25, $rows[24]['id']);
        static::assertSame(range(1, 25), array_column($rows, 'id'));
    }

    public function test_extracts_with_custom_batch_size(): void
    {
        $rows = df()
            ->read(from_pgsql_cursor(
                $this->client,
                select(col('id'), col('name'))->from(table($this->tableName))->orderBy(asc(col('id'))),
            )->withBatchSize(3))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(25, $rows[24]['id']);
        static::assertSame(range(1, 25), array_column($rows, 'id'));
    }

    public function test_extracts_with_parameterized_query(): void
    {
        $rows = df()
            ->read(from_pgsql_cursor(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' WHERE id > $1 ORDER BY id',
                [10],
            )->withBatchSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(15, $rows);
        static::assertSame(11, $rows[0]['id']);
        static::assertSame(25, $rows[14]['id']);
        static::assertSame(range(11, 25), array_column($rows, 'id'));
    }

    public function test_extracts_with_raw_sql(): void
    {
        $rows = df()
            ->read(from_pgsql_cursor(
                $this->client,
                'SELECT id, name FROM ' . $this->tableName . ' ORDER BY id',
            )->withBatchSize(5))
            ->fetch()
            ->toArray();

        static::assertCount(25, $rows);
        static::assertSame(1, $rows[0]['id']);
        static::assertSame(25, $rows[24]['id']);
        static::assertSame(range(1, 25), array_column($rows, 'id'));
    }

    public function test_returns_empty_for_empty_table(): void
    {
        $this->client->execute(delete()->from($this->tableName));

        $rows = df()
            ->read(from_pgsql_cursor($this->client, select(star())->from(table($this->tableName))))
            ->fetch()
            ->toArray();

        static::assertSame([], $rows);
    }

    public function test_a_parameterised_query_describes(): void
    {
        $extractor = from_pgsql_cursor(
            $this->client,
            sprintf('SELECT id, name FROM %s WHERE id > $1 ORDER BY id', $this->tableName),
            [20],
        );

        static::assertSame(['id', 'name'], $extractor->schema()->references()->names());
        static::assertCount(5, df()->read($extractor)->fetch()->toArray());
    }

    public function test_a_missing_table_throws_the_database_error(): void
    {
        $query = select(col('id'))->from(table('flow_probe_missing'));

        try {
            df()->read(from_pgsql_cursor($this->autoCommitClient, $query))->fetch();
            static::fail('a read of a missing table must fail');
        } catch (QueryException $e) {
            static::assertSame('42P01', $e->error()->sqlState);
            static::assertSame($query->toSql(), $e->sql());
            static::assertSame(16, $e->error()->position);
        }

        static::assertSame(0, $this->autoCommitClient->getTransactionNestingLevel());
    }

    public function test_a_multi_statement_query_is_refused_before_any_round_trip(): void
    {
        // raw SQL on purpose: the query builder cannot express two statements
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('reads exactly one read-only SELECT or VALUES statement');

        df()->read(from_pgsql_cursor($this->client, 'SELECT id FROM ' . $this->tableName . '; SELECT 1'))->fetch();
    }

    public function test_a_non_select_statement_is_refused_even_with_a_declared_schema(): void
    {
        try {
            df()
                ->read(from_pgsql_cursor(
                    $this->client,
                    insert()
                        ->into($this->tableName)
                        ->columns('id', 'name')
                        ->values(literal(98), literal('x'))
                        ->returning(col('id')),
                )->withSchema(schema(int_schema('id', nullable: true))))
                ->fetch();
            static::fail('an INSERT must not be read');
        } catch (InvalidArgumentException $e) {
            static::assertStringContainsString(
                'reads exactly one read-only SELECT or VALUES statement',
                $e->getMessage(),
            );
        }

        static::assertCount(
            25,
            df()->read(from_pgsql_cursor($this->client, select(col('id'))->from(table($this->tableName))))->fetch(),
        );
    }

    public function test_a_runtime_error_throws_itself_and_leaves_the_client_usable(): void
    {
        // the zero-row probe never evaluates the division, so the failure comes from the FETCH
        try {
            df()
                ->read(from_pgsql_cursor(
                    $this->autoCommitClient,
                    select(binary_expr(col('generate_series'), '/', literal(0))->as('id'))
                        ->from(table_func(func('generate_series', [literal(1), literal(3)]))),
                ))
                ->fetch();
            static::fail('a division by zero must fail the read');
        } catch (QueryException $e) {
            static::assertSame('22012', $e->error()->sqlState);
        }

        static::assertSame(0, $this->autoCommitClient->getTransactionNestingLevel());
        static::assertSame(1, $this->autoCommitClient->fetchScalarInt(select(literal(1))));
    }

    public function test_a_table_with_a_geometric_column_refuses_the_read(): void
    {
        $this->client->execute(
            create()->temporaryTable('flow_pg_geometry')->column(column('loc', column_type_custom('point'))),
        );

        $extractor = from_pgsql_cursor($this->client, 'SELECT loc FROM flow_pg_geometry');

        try {
            $extractor->schema();
            static::fail('schema() should refuse a geometric column');
        } catch (SchemaNotDerivableException $e) {
            static::assertStringContainsString('has PostgreSQL type "point"', $e->getMessage());
        }

        // The read does not fall back to untyped strings, it stops.
        $this->expectException(SchemaNotDerivableException::class);
        df()->read($extractor)->fetch();
    }

    public function test_a_table_with_an_array_column_describes_and_extracts(): void
    {
        $this->client->execute(
            create()
                ->temporaryTable('flow_pg_arrays')
                ->column(column('id', column_type_integer()))
                ->column(column('tags', column_type_array(column_type_text()))),
        );
        $this->client->execute('INSERT INTO flow_pg_arrays (id, tags) VALUES (1, ARRAY[\'a\', \'b\'])');

        $extractor = from_pgsql_cursor($this->client, 'SELECT id, tags FROM flow_pg_arrays ORDER BY id');

        static::assertEquals(
            type_list(type_union(type_string(), type_null())),
            $extractor->schema()->findDefinition('tags')?->type(),
        );
        static::assertSame(['a', 'b'], df()->read($extractor)->fetch()->toArray()[0]['tags']);
    }

    public function test_a_table_with_an_interval_column_reads_as_text(): void
    {
        // interval takes the text floor: TimeType is DateInterval-backed and rejects the year and
        // month components that age() and any 'N years' literal produce.
        $this->client->execute(
            create()->temporaryTable('flow_pg_intervals')->column(column('span', column_type_custom('interval'))),
        );
        $this->client->execute("INSERT INTO flow_pg_intervals (span) VALUES (INTERVAL '2 years 3 mons')");

        $extractor = from_pgsql_cursor($this->client, 'SELECT span FROM flow_pg_intervals');

        static::assertEquals(type_string(), $extractor->schema()->findDefinition('span')?->type());
        static::assertSame('2 years 3 mons', df()->read($extractor)->fetch()->toArray()[0]['span']);
    }

    public function test_a_trailing_semicolon_query_still_describes(): void
    {
        static::assertSame(
            ['id'],
            from_pgsql_cursor($this->client, 'SELECT id FROM ' . $this->tableName . ';')
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
                    from_pgsql_cursor($this->client, sprintf('SELECT id, name FROM %s ORDER BY id', $this->tableName)),
                    from_array([['id' => 99, 'name' => 'from array']]),
                ))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_schema_and_extract_agree(): void
    {
        $extractor = from_pgsql_cursor($this->client, sprintf('SELECT id, name FROM %s ORDER BY id', $this->tableName));
        $batch = df()->read($extractor)->fetch();

        static::assertSame($extractor->schema()->references()->names(), $batch->first()->names());
        static::assertTrue($batch->schema()->isSame($extractor->schema()));
    }

    public function test_schema_comes_from_result_metadata(): void
    {
        $schema = from_pgsql_cursor($this->client, 'SELECT id, name FROM ' . $this->tableName)->schema();

        static::assertSame(['id', 'name'], $schema->references()->names());
        static::assertTrue($schema->findDefinition('id')?->isNullable());
        static::assertTrue($schema->findDefinition('name')?->isNullable());
    }

    private function insertTestData(int $count): void
    {
        $insert = insert()->into($this->tableName)->columns('id', 'name');

        for ($i = 1; $i <= $count; $i++) {
            $insert = $insert->values(literal($i), literal(sprintf('User_%02d', $i)));
        }

        $this->client->execute($insert);
    }

    public function test_statistics_are_planned_without_reading_a_row(): void
    {
        $this->client->execute(analyze()->table($this->tableName));

        static::assertEquals(
            Cardinality::approximately(5),
            from_pgsql_cursor(
                $this->client,
                select(star())->from(table($this->tableName))->where(gt(col('id'), param(1))),
                [20],
            )->statistics()->rows,
        );
    }
}
