<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\KeySet;
use Flow\ETL\Adapter\PostgreSql\Pagination\Order;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\SortedRowsClient;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\SpyClient;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\StubCursor;
use Flow\ETL\Adapter\PostgreSql\Tests\Mother\ColumnMother;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Exception\PaginationException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function extension_loaded;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_key_set;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_desc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;
use function range;

final class PostgreSqlKeySetExtractorTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{int}>
     */
    public static function batch_sizes(): Generator
    {
        yield '1' => [1];
        yield '2' => [2];
        yield '3' => [3];
        yield '4' => [4];
        yield '6' => [6];
        yield '10' => [10];
    }

    /**
     * @return Generator<string, array{string, KeySet, string}>
     */
    public static function null_checks(): Generator
    {
        yield 'single key' => [
            'SELECT id FROM t WHERE active = $1',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            'SELECT 1 FROM (SELECT id FROM t WHERE active = $1) _flow_keyset_nulls WHERE id IS NULL LIMIT 1',
        ];
        yield 'composite key' => [
            'SELECT id, created_at FROM t WHERE active = $1',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('created_at'), pgsql_pagination_key_asc('id')),
            'SELECT 1 FROM (SELECT id, created_at FROM t WHERE active = $1) _flow_keyset_nulls WHERE created_at IS NULL OR id IS NULL LIMIT 1',
        ];
        yield 'qualified key' => [
            'SELECT u.id FROM t u WHERE u.active = $1',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('u.id')),
            'SELECT 1 FROM (SELECT u.id FROM t u WHERE u.active = $1) _flow_keyset_nulls WHERE id IS NULL LIMIT 1',
        ];
    }

    /**
     * @return Generator<string, array{int, list<array{k: null|int}>, KeySet, Order}>
     */
    public static function null_keys(): Generator
    {
        foreach (self::batch_sizes() as [$batchSize]) {
            yield "ascending, batch {$batchSize}" => [
                $batchSize,
                [['k' => 1], ['k' => 2], ['k' => null]],
                pgsql_pagination_key_set(pgsql_pagination_key_asc('k')),
                Order::ASC,
            ];
            yield "descending, batch {$batchSize}" => [
                $batchSize,
                [['k' => null], ['k' => 2], ['k' => 1]],
                pgsql_pagination_key_set(pgsql_pagination_key_desc('k')),
                Order::DESC,
            ];
        }
    }

    public function test_a_failing_read_probes_once(): void
    {
        $client = (new SpyClient())->willRefuseDescribe(QueryException::executionFailed(
            'SELECT id FROM t ORDER BY id',
            PostgreSqlError::unknown('boom'),
        ));

        try {
            df()
                ->read(from_pgsql_key_set(
                    $client,
                    'SELECT id FROM t ORDER BY id',
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                ))
                ->fetch();
        } catch (SchemaNotDerivableException) {
        }

        static::assertSame(1, $client->callsTo('describe'));
    }

    public function test_a_non_select_statement_is_refused_before_any_round_trip(): void
    {
        $client = new SpyClient();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('reads exactly one read-only SELECT or VALUES statement');

        try {
            iterator_to_array(
                from_pgsql_key_set(
                    $client,
                    'INSERT INTO t VALUES (1) RETURNING id',
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                )
                    ->withSchema(schema(int_schema('id', nullable: true)))
                    ->extract(flow_context()),
            );
        } finally {
            static::assertSame([], $client->calls);
        }
    }

    public function test_every_page_sends_the_same_sql_with_new_parameters(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(
                new StubCursor([['id' => '1'], ['id' => '2'], ['id' => '3']]),
                new StubCursor([['id' => '3']]),
            );

        iterator_to_array(
            from_pgsql_key_set(
                $client,
                'SELECT id FROM t WHERE active = $1',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                [true],
            )
                ->withBatchSize(2)
                ->extract(flow_context()),
        );

        $nextPage = 'SELECT id FROM t WHERE active = $1 AND id > $3 ORDER BY id ASC LIMIT $2';

        static::assertSame(
            [
                ['sql' => 'SELECT id FROM t WHERE active = $1 ORDER BY id ASC LIMIT $2', 'parameters' => [true, 3]],
                ['sql' => $nextPage, 'parameters' => [true, 3, '2']],
            ],
            $client->cursorQueries,
        );
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            static::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true`',
            );
        }
    }

    public function test_a_declared_schema_runs_no_query(): void
    {
        $client = new SpyClient();

        from_pgsql_key_set($client, 'SELECT id FROM t', pgsql_pagination_key_set(pgsql_pagination_key_asc('id')))
            ->withSchema(schema(int_schema('id')))
            ->schema();

        static::assertSame([], $client->calls);
    }

    public function test_a_declared_schema_wins_over_the_probe(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            from_pgsql_key_set(
                (new SpyClient())->willDescribe(ColumnMother::of(['other' => 'text'])),
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_extract_casts_through_the_derived_schema(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8', 'amount' => 'numeric']))
            ->willReturnCursors(new StubCursor([['id' => '1', 'amount' => '10.5']]), new StubCursor());

        $rows = iterator_to_array(
            from_pgsql_key_set(
                $client,
                'SELECT id, amount FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->extract(flow_context()),
        )[0];

        static::assertSame(1, $rows->column('id')->value(0));
        static::assertSame(10.5, $rows->column('amount')->value(0));
    }

    public function test_extract_derives_the_schema_once_and_reuses_it_across_batches(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(
                new StubCursor([['id' => '1'], ['id' => '2'], ['id' => '3']]),
                new StubCursor([['id' => '3']]),
            );

        $extractor = from_pgsql_key_set(
            $client,
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->withBatchSize(2);
        $batches = iterator_to_array($extractor->extract(flow_context()));

        static::assertSame(1, $client->callsTo('describe'));

        foreach ($batches as $batch) {
            static::assertTrue($batch->schema()->isSame($extractor->schema()));
        }
    }

    public function test_extract_refuses_to_read_when_the_schema_cannot_be_derived(): void
    {
        $client = (new SpyClient())->willDescribe(ColumnMother::of(['location' => 'point']));

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('column "location" has PostgreSQL type "point", which Flow has no type for');

        try {
            iterator_to_array(
                from_pgsql_key_set(
                    $client,
                    'SELECT location FROM t',
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                )->extract(flow_context()),
            );
        } finally {
            static::assertSame(['describe'], $client->calls);
        }
    }

    public function test_schema_is_derived_from_result_metadata(): void
    {
        $schema = from_pgsql_key_set(
            (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8', 'label' => 'text'])),
            'SELECT id, label FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->schema();

        static::assertSame(['id', 'label'], $schema->references()->names());
        static::assertTrue($schema->findDefinition('id')?->isNullable());
    }

    public function test_the_derived_schema_is_memoised(): void
    {
        $client = (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8']));
        $extractor = from_pgsql_key_set(
            $client,
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        );

        $extractor->schema();
        $extractor->schema();

        static::assertSame(['describe'], $client->calls);
    }

    public function test_with_batch_size_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        );

        static::assertSame($extractor, $extractor->withBatchSize(10));
    }

    public function test_with_maximum_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        );

        static::assertSame($extractor, $extractor->withMaximum(10));
    }

    public function test_with_schema_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        );

        static::assertSame($extractor, $extractor->withSchema(schema(int_schema('id'))));
    }

    public function test_with_maximum_validates_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -1');

        from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->withMaximum(-1);
    }

    public function test_with_maximum_validates_positive_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got 0');

        from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->withMaximum(0);
    }

    public function test_with_batch_size_validates_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, got -1');

        from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->withBatchSize(-1);
    }

    public function test_with_batch_size_validates_positive_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, got 0');

        from_pgsql_key_set(
            new SpyClient(),
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        )->withBatchSize(0);
    }

    public function test_pushed_limit_issues_no_query_once_satisfied(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(
                new StubCursor(array_map(static fn(int $id): array => ['id' => (string) $id], range(1, 1001))),
                new StubCursor(array_map(static fn(int $id): array => ['id' => (string) $id], range(1001, 1501))),
            );
        $extractor = from_pgsql_key_set(
            $client,
            'SELECT id FROM t',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        );

        self::assertExtractedRowsCount(1500, $extractor, limit: 1500);
        // 1000, then a page narrowed to the 500 still wanted - and no third query for row 1501
        static::assertSame(2, $client->callsTo('cursor'));
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(
            from_pgsql_key_set(
                new SpyClient(),
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->isRepeatable(),
        );
    }

    public function test_statistics_are_the_planner_estimate_without_analyze(): void
    {
        $client = (new SpyClient())->willExplain(42);

        static::assertEquals(
            Cardinality::approximately(42),
            from_pgsql_key_set(
                $client,
                'SELECT * FROM t WHERE id > $1',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                [1],
            )->statistics()->rows,
        );
        static::assertEquals(
            [['sql' => 'SELECT * FROM t WHERE id > $1', 'parameters' => [1], 'config' => ExplainConfig::forEstimate()]],
            $client->explained,
        );
    }

    public function test_statistics_are_explained_once(): void
    {
        $client = (new SpyClient())->willExplain(42);
        $extractor = from_pgsql_key_set(
            $client,
            'SELECT * FROM t WHERE id > $1',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            [1],
        );

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertSame(1, $client->callsTo('explain'));
    }

    public function test_a_maximum_bounds_and_caps_the_estimate_and_a_new_one_drops_the_statistics(): void
    {
        $client = (new SpyClient())->willExplain(42);
        $extractor = from_pgsql_key_set(
            $client,
            'SELECT * FROM t WHERE id > $1',
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            [1],
        )->withMaximum(10);

        static::assertEquals(
            new Cardinality(atMost: 10, estimate: 10, relativeError: Cardinality::DEFAULT_RELATIVE_ERROR),
            $extractor->statistics()->rows,
        );

        $extractor->withMaximum(100);

        static::assertEquals(
            new Cardinality(atMost: 100, estimate: 42, relativeError: Cardinality::DEFAULT_RELATIVE_ERROR),
            $extractor->statistics()->rows,
        );
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1']]), new StubCursor());
        $backend = new SpyBackend();

        iterator_to_array(
            from_pgsql_key_set(
                $client,
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->extract(flow_context(config_builder()->backend($backend)->build())),
            false,
        );

        static::assertGreaterThanOrEqual(1, $backend->builders());
    }

    #[DataProvider('batch_sizes')]
    public function test_duplicate_keys_throw_at_every_batch_size(int $batchSize): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key');

        iterator_to_array(
            from_pgsql_key_set(
                new SortedRowsClient(
                    ['id'],
                    [['id' => 1], ['id' => 1], ['id' => 1], ['id' => 2], ['id' => 3], ['id' => 3]],
                    'id',
                    Order::ASC,
                ),
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )
                ->withBatchSize($batchSize)
                ->extract(flow_context()),
        );
    }

    /**
     * @param list<array{k: null|int}> $rows
     */
    #[DataProvider('null_keys')]
    public function test_null_key_throws_without_reading_a_page_at_every_batch_size(
        int $batchSize,
        array $rows,
        KeySet $keySet,
        Order $order,
    ): void {
        $client = new SortedRowsClient(['k'], $rows, 'k', $order);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "k"',
        );

        try {
            iterator_to_array(
                from_pgsql_key_set($client, 'SELECT k FROM t', $keySet)
                    ->withBatchSize($batchSize)
                    ->extract(flow_context()),
            );
        } finally {
            static::assertSame(0, $client->cursorCalls);
        }
    }

    #[DataProvider('batch_sizes')]
    public function test_unique_keys_read_every_row_at_every_batch_size(int $batchSize): void
    {
        self::assertExtractedRowsCount(
            5,
            from_pgsql_key_set(
                new SortedRowsClient(
                    ['id'],
                    [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]],
                    'id',
                    Order::ASC,
                ),
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize($batchSize),
        );
    }

    #[TestWith([1])]
    #[TestWith([2])]
    public function test_duplicate_keys_within_the_maximum_throw(int $batchSize): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key (id = 2)');

        iterator_to_array(
            from_pgsql_key_set(
                new SortedRowsClient(['id'], [['id' => 1], ['id' => 2], ['id' => 2]], 'id', Order::ASC),
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )
                ->withBatchSize($batchSize)
                ->withMaximum(2)
                ->extract(flow_context()),
        );
    }

    public function test_empty_result_issues_one_page(): void
    {
        $client = new SortedRowsClient(['id'], [], 'id', Order::ASC);

        static::assertSame(
            [],
            iterator_to_array(
                from_pgsql_key_set(
                    $client,
                    'SELECT id FROM t',
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                )->extract(flow_context()),
            ),
        );
        static::assertSame(1, $client->cursorCalls);
    }

    public function test_exact_multiple_ends_without_an_empty_page(): void
    {
        $client = new SortedRowsClient(['id'], [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]], 'id', Order::ASC);

        self::assertExtractedRowsCount(
            4,
            from_pgsql_key_set(
                $client,
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize(2),
        );
        static::assertSame(2, $client->cursorCalls);
    }

    #[DataProvider('null_checks')]
    public function test_null_check_runs_before_the_first_page(string $query, KeySet $keySet, string $nullCheck): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8', 'created_at' => 'int8']))
            ->willReturnCursors(
                new StubCursor([
                    ['id' => '1', 'created_at' => '1'],
                    ['id' => '2', 'created_at' => '2'],
                    ['id' => '3', 'created_at' => '3'],
                ]),
                new StubCursor([['id' => '3', 'created_at' => '3']]),
            );

        iterator_to_array(
            from_pgsql_key_set($client, $query, $keySet, [true])->withBatchSize(2)->extract(flow_context()),
        );

        static::assertSame([['sql' => $nullCheck, 'parameters' => [true]]], $client->fetchOneQueries);
        static::assertSame(['describe', 'fetchOne', 'cursor', 'cursor'], $client->calls);
    }

    public function test_null_key_throws_before_any_page(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willFindNullKey();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "created_at", "id"; filter them out with IS NOT NULL or choose non-null keys',
        );

        try {
            iterator_to_array(
                from_pgsql_key_set(
                    $client,
                    'SELECT id, created_at FROM t',
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('created_at'), pgsql_pagination_key_asc('id')),
                )->extract(flow_context()),
            );
        } finally {
            static::assertSame([], $client->cursorQueries);
        }
    }

    public function test_query_limit_is_rejected_before_any_query(): void
    {
        $client = new SpyClient();

        $this->expectException(PaginationException::class);

        try {
            iterator_to_array(
                from_pgsql_key_set(
                    $client,
                    'SELECT id FROM t ORDER BY id LIMIT 5',
                    pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
                )
                    ->withSchema(schema(int_schema('id', nullable: true)))
                    ->extract(flow_context()),
            );
        } finally {
            static::assertSame([], $client->calls);
        }
    }

    public function test_short_page_ends_the_read(): void
    {
        $client = new SortedRowsClient(['id'], [['id' => 1], ['id' => 2], ['id' => 3]], 'id', Order::ASC);

        self::assertExtractedRowsCount(
            3,
            from_pgsql_key_set(
                $client,
                'SELECT id FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withBatchSize(2),
        );
        static::assertSame(2, $client->cursorCalls);
    }

    public function test_short_first_page_sends_no_further_page_query(): void
    {
        $client = new SortedRowsClient(
            ['k'],
            [['k' => 1], ['k' => 2], ['k' => 3], ['k' => 4], ['k' => 5], ['k' => 6]],
            'k',
            Order::ASC,
        );

        self::assertExtractedRowsCount(
            6,
            from_pgsql_key_set(
                $client,
                'SELECT k FROM t',
                pgsql_pagination_key_set(pgsql_pagination_key_asc('k')),
            )->withBatchSize(10),
        );
        static::assertSame(1, $client->cursorCalls);
    }
}
