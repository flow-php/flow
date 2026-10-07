<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\Tests\Context\NamedRowsContext;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use PHPUnit\Framework\Attributes\TestWith;

use function array_column;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_key_set;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_desc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_numeric;
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

        NamedRowsContext::insert($this->client, $this->tableName, 25);
    }

    public function test_a_data_modifying_cte_is_refused_before_it_runs(): void
    {
        try {
            df()
                ->read(from_pgsql_key_set(
                    $this->client,
                    // raw SQL on purpose: the query builder cannot express a data-modifying WITH
                    sprintf(
                        'WITH x AS (INSERT INTO %s (id, name) VALUES (98, \'x\') RETURNING id) SELECT id FROM x ORDER BY id',
                        $this->tableName,
                    ),
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                )->withSchema(schema(int_schema('id', nullable: true))))
                ->fetch();
            static::fail('a query that writes must not be read');
        } catch (InvalidArgumentException $e) {
            static::assertStringContainsString(
                'Expected a read-only SELECT - the query holds a data-modifying WITH or SELECT ... INTO',
                $e->getMessage(),
            );
        }

        static::assertCount(
            25,
            df()
                ->read(from_pgsql_key_set(
                    $this->client,
                    select(col('id'))->from(table($this->tableName)),
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                ))
                ->fetch(),
        );
    }

    public function test_a_non_select_statement_is_refused_before_it_runs(): void
    {
        try {
            df()
                ->read(from_pgsql_key_set(
                    $this->client,
                    insert()
                        ->into($this->tableName)
                        ->columns('id', 'name')
                        ->values(literal(98), literal('x'))
                        ->returning(col('id')),
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
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
            df()
                ->read(from_pgsql_key_set(
                    $this->client,
                    select(col('id'))->from(table($this->tableName)),
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                ))
                ->fetch(),
        );
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

    public function test_extracts_all_rows_of_a_union(): void
    {
        $odd = $this->tableName . '_odd';
        $even = $this->tableName . '_even';

        $this->client->execute(create()->table($odd)->column(column('id', column_type_integer())->primaryKey()));
        $this->client->execute(create()->table($even)->column(column('id', column_type_integer())->primaryKey()));
        $this->client->execute(
            insert()
                ->into($odd)
                ->columns('id')
                ->values(literal(1))
                ->values(literal(3))
                ->values(literal(5))
                ->values(literal(7))
                ->values(literal(9)),
        );
        $this->client->execute(
            insert()
                ->into($even)
                ->columns('id')
                ->values(literal(2))
                ->values(literal(4))
                ->values(literal(6))
                ->values(literal(8))
                ->values(literal(10)),
        );

        $rows = df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'))
                    ->from(table($odd))
                    ->union(select(col('id'))->from(table($even)))
                    ->orderBy(asc(col('id'))),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize(3))
            ->fetch()
            ->toArray();

        static::assertSame(range(1, 10), array_column($rows, 'id'));
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

        static::assertSame($extractor->schema()->references()->names(), array_keys($batch->values(0)));
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

    public function test_duplicate_keys_throw(): void
    {
        $this->client->execute(
            create()
                ->table('flow_postgresql_keyset_duplicates')
                ->column(column('id', column_type_integer())->primaryKey())
                ->column(column('k', column_type_integer())->notNull()),
        );
        $this->client->execute(
            insert()
                ->into('flow_postgresql_keyset_duplicates')
                ->columns('id', 'k')
                ->values(literal(1), literal(1))
                ->values(literal(2), literal(1))
                ->values(literal(3), literal(1))
                ->values(literal(4), literal(2))
                ->values(literal(5), literal(3))
                ->values(literal(6), literal(3)),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key');

        df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('id'), col('k'))->from(table('flow_postgresql_keyset_duplicates')),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('k')),
            )->withBatchSize(2))
            ->fetch();
    }

    public function test_equal_numeric_keys_with_different_scale_throw(): void
    {
        $this->client->execute(
            create()->table('flow_postgresql_keyset_numeric')->column(column('k', column_type_numeric())->notNull()),
        );
        $this->client->execute(
            insert()
                ->into('flow_postgresql_keyset_numeric')
                ->columns('k')
                ->values(literal('1.0'))
                ->values(literal('1.00'))
                ->values(literal('2')),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination expected the next page to start at the key');

        df()
            ->read(from_pgsql_key_set(
                $this->client,
                select(col('k'))->from(table('flow_postgresql_keyset_numeric')),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('k')),
            )->withBatchSize(1))
            ->fetch();
    }

    public function test_null_in_secondary_key_throws_before_first_row(): void
    {
        $this->client->execute(
            create()
                ->table('flow_postgresql_keyset_nulls')
                ->column(column('a', column_type_integer())->notNull())
                ->column(column('b', column_type_integer())->nullable()),
        );
        $this->client->execute(
            insert()
                ->into('flow_postgresql_keyset_nulls')
                ->columns('a', 'b')
                ->values(literal(1), literal(1))
                ->values(literal(1), literal(null))
                ->values(literal(2), literal(1)),
        );

        $generator = from_pgsql_key_set(
            $this->client,
            select(col('a'), col('b'))->from(table('flow_postgresql_keyset_nulls')),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('a'), pgsql_pagination_key_asc('b')),
        )
            ->withBatchSize(2)
            ->extract(flow_context());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "a", "b"',
        );

        $generator->current();
    }

    #[TestWith([2])]
    #[TestWith([3])]
    #[TestWith([5])]
    #[TestWith([6])]
    #[TestWith([10])]
    public function test_null_keys_throw_at_every_batch_size(int $batchSize): void
    {
        $this->client->execute(
            create()
                ->table('keyset_null_repro')
                ->column(column('id', column_type_integer())->primaryKey())
                ->column(column('k', column_type_integer())->nullable()),
        );
        $this->client->execute(
            insert()
                ->into('keyset_null_repro')
                ->columns('id', 'k')
                ->values(literal(1), literal(1))
                ->values(literal(2), literal(2))
                ->values(literal(3), literal(3))
                ->values(literal(4), literal(4))
                ->values(literal(5), literal(null))
                ->values(literal(6), literal(null)),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "k"',
        );

        df()
            ->read(from_pgsql_key_set(
                $this->client,
                'SELECT id, k FROM keyset_null_repro',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('k')),
            )->withBatchSize($batchSize))
            ->fetch();
    }
}
