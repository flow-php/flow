<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Logging\Middleware;
use Flow\ETL\Adapter\Doctrine\DbalKeySetExtractor;
use Flow\ETL\Adapter\Doctrine\Pagination\KeySet;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Adapter\Doctrine\Tests\Context\KeySetSqlite;
use Flow\ETL\Adapter\Doctrine\Tests\Context\SelectQueryCounter;
use Flow\ETL\Adapter\Doctrine\Tests\Double\NativeHandleStub;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use stdClass;

use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_desc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class DbalKeySetExtractorTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{int}>
     */
    public static function batch_sizes(): Generator
    {
        yield '1' => [1];
        yield '2' => [2];
        yield '3' => [3];
        yield '5' => [5];
        yield '6' => [6];
        yield '10' => [10];
    }

    /**
     * @return Generator<string, array{int, KeySet}>
     */
    public static function null_keys(): Generator
    {
        foreach (self::batch_sizes() as [$batchSize]) {
            yield "ascending, batch {$batchSize}" => [$batchSize, pagination_key_set(pagination_key_asc('k'))];
            yield "descending, batch {$batchSize}" => [$batchSize, pagination_key_set(pagination_key_desc('k'))];
        }
    }

    /**
     * @return Generator<string, array{int, int, int}>
     */
    public static function short_pages(): Generator
    {
        yield '3 rows' => [3, 3, 3];
        yield '4 rows' => [4, 4, 3];
        yield '0 rows' => [0, 0, 2];
    }

    public function test_a_refused_probe_is_memoised(): void
    {
        // The native handle has no arm, so every derivation refuses.
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new NativeHandleStub(new stdClass())), 1);
        $extractor = new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
            pagination_key_set(pagination_key_asc('id')),
        );

        try {
            $extractor->schema();
            static::fail('an undescribable read must refuse');
        } catch (SchemaNotDerivableException $first) {
        }

        try {
            $extractor->schema();
            static::fail('an undescribable read must refuse');
        } catch (SchemaNotDerivableException $second) {
        }

        static::assertSame($first, $second);
    }

    public function test_schema_describes_the_base_builder_not_the_paged_sql(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 3);
        $extractor = (new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
            pagination_key_set(pagination_key_asc('id')),
        ))->withBatchSize(2);

        static::assertSame(['id', 'name', 'amount'], $extractor->schema()->references()->names());

        $rows = 0;

        foreach ($extractor->extract(flow_context()) as $batch) {
            $rows += $batch->count();
            static::assertSame(['id', 'name', 'amount'], $batch->schema()->references()->names());
        }

        static::assertSame(3, $rows);
    }

    public function test_schema_is_derived_when_it_was_not_declared(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 1);

        static::assertEquals(
            schema(str_schema('id', true), str_schema('name', true), str_schema('amount', true)),
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('*')->from('users'),
                pagination_key_set(pagination_key_asc('id')),
            ))->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        $connection = InMemorySqlite::connection();
        $extractor = new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
            pagination_key_set(pagination_key_asc('id')),
        );

        static::assertEquals(schema(int_schema('id')), $extractor->withSchema(schema(int_schema('id')))->schema());
    }

    public function test_pushed_limit_issues_no_query_once_satisfied(): void
    {
        $counter = new SelectQueryCounter();
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new Middleware($counter)), 3000);
        $extractor = new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
            pagination_key_set(pagination_key_asc('id')),
        );
        $counter->reset();

        self::assertExtractedRowsCount(1500, $extractor, limit: 1500);
        // the NULL check, 1000, then a page narrowed to the 500 still wanted - and no query for row 1501
        static::assertSame(3, $counter->count);
    }

    public function test_is_repeatable(): void
    {
        $connection = InMemorySqlite::connection();

        static::assertTrue(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('*')->from('users'),
                pagination_key_set(pagination_key_asc('id')),
            ))->isRepeatable(),
        );
    }

    public function test_a_new_maximum_drops_the_statistics(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 3);
        $extractor = (new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
            pagination_key_set(pagination_key_asc('id')),
        ))->withMaximum(2);

        static::assertSame($extractor->statistics(), $extractor->statistics());
        static::assertEquals(Cardinality::atMost(2), $extractor->statistics()->rows);

        $extractor->withMaximum(5);

        static::assertEquals(Cardinality::atMost(5), $extractor->statistics()->rows);
    }

    public function test_extract_builds_through_the_config_backend(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 1);
        $backend = new SpyBackend();

        iterator_to_array(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('*')->from('users'),
                pagination_key_set(pagination_key_asc('id')),
            ))->extract(flow_context(config_builder()->backend($backend)->build())),
            false,
        );

        static::assertGreaterThanOrEqual(1, $backend->builders());
    }

    #[DataProvider('batch_sizes')]
    public function test_duplicate_keys_throw_at_every_batch_size(int $batchSize): void
    {
        $connection = KeySetSqlite::withKeys(
            InMemorySqlite::connection(),
            'keyset',
            schema(int_schema('id'), int_schema('k')),
            [
                ['id' => 1, 'k' => 1],
                ['id' => 2, 'k' => 1],
                ['id' => 3, 'k' => 1],
                ['id' => 4, 'k' => 2],
                ['id' => 5, 'k' => 3],
                ['id' => 6, 'k' => 3],
            ],
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key');

        iterator_to_array(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id', 'k')->from('keyset'),
                pagination_key_set(pagination_key_asc('k')),
            ))->withBatchSize($batchSize)->extract(flow_context()),
        );
    }

    #[TestWith([1])]
    #[TestWith([2])]
    public function test_duplicate_keys_within_the_maximum_throw(int $batchSize): void
    {
        $connection = KeySetSqlite::withKeys(
            InMemorySqlite::connection(),
            'keyset',
            schema(int_schema('id'), int_schema('k')),
            [['id' => 1, 'k' => 1], ['id' => 2, 'k' => 2], ['id' => 3, 'k' => 2]],
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key');

        iterator_to_array(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id', 'k')->from('keyset'),
                pagination_key_set(pagination_key_asc('k')),
            ))
                ->withBatchSize($batchSize)
                ->withMaximum(2)
                ->extract(flow_context()),
        );
    }

    public function test_equal_keys_under_a_case_insensitive_collation_throw(): void
    {
        $connection = KeySetSqlite::withCaseInsensitiveKeys(
            InMemorySqlite::connection(),
            'keyset_nocase',
            ['a', 'A', 'b'],
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination expected the next page to start at the key');

        iterator_to_array(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id', 'k')->from('keyset_nocase'),
                pagination_key_set(pagination_key_asc('k')),
            ))->withBatchSize(1)->extract(flow_context()),
        );
    }

    public function test_mixed_direction_composite_duplicate_keys_throw(): void
    {
        $connection = KeySetSqlite::withKeys(
            InMemorySqlite::connection(),
            'keyset',
            schema(int_schema('a'), int_schema('b')),
            [['a' => 2, 'b' => 1], ['a' => 2, 'b' => 1], ['a' => 1, 'b' => 1]],
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key');

        iterator_to_array(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('a', 'b')->from('keyset'),
                pagination_key_set(pagination_key_asc('b'), pagination_key_desc('a')),
            ))->withBatchSize(1)->extract(flow_context()),
        );
    }

    public function test_null_check_adds_is_null_to_the_query(): void
    {
        $counter = new SelectQueryCounter();
        $connection = KeySetSqlite::withKeys(
            InMemorySqlite::connection(new Middleware($counter)),
            'keyset',
            schema(int_schema('a', true), int_schema('k', true)),
            [['a' => 1, 'k' => 1]],
        );
        $counter->reset();

        iterator_to_array(
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('a', 'k')->from('keyset'),
                pagination_key_set(pagination_key_asc('k'), pagination_key_asc('a')),
            ))->extract(flow_context()),
        );

        static::assertStringContainsString('WHERE (a IS NULL) OR (k IS NULL)', $counter->queries[0]);
        static::assertStringContainsString('LIMIT 1', $counter->queries[0]);
    }

    #[DataProvider('null_keys')]
    public function test_null_key_throws_with_one_query_at_every_batch_size(int $batchSize, KeySet $keySet): void
    {
        $counter = new SelectQueryCounter();
        $connection = KeySetSqlite::withKeys(
            InMemorySqlite::connection(new Middleware($counter)),
            'keyset',
            schema(int_schema('id'), int_schema('k', true)),
            [['id' => 1, 'k' => 1], ['id' => 2, 'k' => 2], ['id' => 3, 'k' => null]],
        );
        $generator = (new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('id', 'k')->from('keyset'),
            $keySet,
        ))
            ->withBatchSize($batchSize)
            ->withSchema(schema(int_schema('id'), int_schema('k', true)))
            ->extract(flow_context());
        $counter->reset();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires non-null keys, but a row has NULL in the key column(s) "k"',
        );

        try {
            $generator->current();
        } finally {
            static::assertSame(1, $counter->count);
        }
    }

    public function test_query_first_result_is_rejected(): void
    {
        $connection = InMemorySqlite::connection();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keyset pagination sets its own LIMIT and OFFSET, please remove setMaxResults()/setFirstResult() from Query Builder and use withMaximum() or DataFrame::limit()',
        );

        new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users')->setFirstResult(5),
            pagination_key_set(pagination_key_asc('id')),
        );
    }

    public function test_query_max_results_is_rejected(): void
    {
        $connection = InMemorySqlite::connection();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Keyset pagination sets its own LIMIT and OFFSET');

        new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users')->setMaxResults(5),
            pagination_key_set(pagination_key_asc('id')),
        );
    }

    #[DataProvider('short_pages')]
    public function test_short_page_ends_the_read(int $rows, int $expectedRows, int $expectedSelects): void
    {
        $counter = new SelectQueryCounter();
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new Middleware($counter)), $rows);
        $extractor = (new DbalKeySetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
            pagination_key_set(pagination_key_asc('id')),
        ))->withBatchSize(2)->withSchema(schema(int_schema('id'), str_schema('name'), str_schema('amount', true)));
        $counter->reset();

        self::assertExtractedRowsCount($expectedRows, $extractor);
        static::assertSame($expectedSelects, $counter->count);
    }

    #[DataProvider('batch_sizes')]
    public function test_unique_keys_read_every_row_at_every_batch_size(int $batchSize): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);

        self::assertExtractedRowsCount(
            5,
            (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('*')->from('users'),
                pagination_key_set(pagination_key_asc('id')),
            ))->withBatchSize($batchSize),
        );
    }
}
