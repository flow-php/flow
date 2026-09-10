<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Logging\Middleware;
use Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Adapter\Doctrine\Tests\Context\SelectQueryCounter;
use Flow\ETL\Adapter\Doctrine\Tests\Double\NativeHandleStub;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use stdClass;

use function array_map;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function implode;

final class DbalLimitOffsetExtractorTest extends FlowTestCase
{
    public function test_an_empty_page_is_not_yielded(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);
        $extractor = (new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users')->orderBy('id'),
        ))
            ->withBatchSize(5)
            ->withMaximum(10);

        // the second page asks for the 5 rows still allowed and gets none back
        static::assertSame(
            [5],
            array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array($extractor->extract(flow_context()), false),
            ),
        );
    }

    public function test_a_declared_schema_is_answered_without_probing(): void
    {
        // The native handle has no arm, so any derivation throws: only the short-circuit can answer.
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new NativeHandleStub(new stdClass())), 1);

        static::assertEquals(
            schema(int_schema('id')),
            (new DbalLimitOffsetExtractor($connection, $connection->createQueryBuilder()->select('*')->from('users')))
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_extract_derives_the_schema_once_and_reuses_it_across_batches(): void
    {
        $counter = new SelectQueryCounter();
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new Middleware($counter)), 5);
        $counter->reset();

        $extractor = (new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users')->orderBy('id'),
        ))->withBatchSize(2);

        $batches = 0;

        foreach ($extractor->extract(flow_context()) as $rows) {
            $batches++;
            static::assertTrue($rows->schema()->isSame($extractor->schema()));
        }

        // 5 rows at 2 per page: 2, 2, 1
        static::assertSame(3, $batches);
        static::assertSame($extractor->schema(), $extractor->schema());
        // COUNT(*) plus three pages; the SQLite name probe is native, so DBAL never sees it.
        static::assertSame(4, $counter->count);
    }

    public function test_extract_does_not_mutate_the_query_builder(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 3);
        $queryBuilder = $connection->createQueryBuilder()->select('*')->from('users')->orderBy('id');

        foreach ((new DbalLimitOffsetExtractor($connection, $queryBuilder))
            ->withBatchSize(2)
            ->extract(flow_context()) as $_rows) {
        }

        static::assertNull($queryBuilder->getMaxResults());
        static::assertSame(0, $queryBuilder->getFirstResult());
    }

    public function test_extract_refuses_to_read_when_the_schema_cannot_be_derived(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new NativeHandleStub(new stdClass())), 1);
        $extractor = new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
        );

        $batches = 0;

        try {
            foreach ($extractor->extract(flow_context()) as $_rows) {
                $batches++;
            }
            static::fail('a read that cannot be described must not run');
        } catch (SchemaNotDerivableException $e) {
            static::assertStringContainsString('the stdClass driver reports no column types', $e->getMessage());

            try {
                $extractor->schema();
                static::fail('schema() must refuse for the same reason');
            } catch (SchemaNotDerivableException $fromSchema) {
                static::assertSame($fromSchema->getMessage(), $e->getMessage());
            }
        }

        static::assertSame(0, $batches);
    }

    public function test_schema_does_not_mutate_the_query_builder(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 1);
        $queryBuilder = $connection->createQueryBuilder()->select('*')->from('users');

        (new DbalLimitOffsetExtractor($connection, $queryBuilder))->schema();

        static::assertNull($queryBuilder->getMaxResults());
        static::assertSame(0, $queryBuilder->getFirstResult());
    }

    public function test_schema_is_derived_when_it_was_not_declared(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 1);

        static::assertEquals(
            schema(str_schema('id', true), str_schema('name', true), str_schema('amount', true)),
            (new DbalLimitOffsetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('*')->from('users'),
            ))->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        $connection = InMemorySqlite::connection();
        $extractor = new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
        );

        static::assertEquals(schema(int_schema('id')), $extractor->withSchema(schema(int_schema('id')))->schema());
    }

    public function test_sqlite_values_are_cast_to_string(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 1);

        $rows = [];

        foreach ((new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users'),
        ))->extract(flow_context()) as $batch) {
            $rows = [...$rows, ...$batch->toArray()];
        }

        static::assertSame([['id' => '1', 'name' => 'name_1', 'amount' => '1.5']], $rows);
    }

    public function test_pushed_limit_skips_the_count_query(): void
    {
        $counter = new SelectQueryCounter();
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new Middleware($counter)), 5);
        $extractor = (new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users')->orderBy('id'),
        ))->withBatchSize(2);
        $extractor->pushLimit(5);
        $counter->reset();

        self::assertExtractedRowsCount(5, $extractor);
        // the three pages of the unlimited read, without the COUNT(*) in front of them
        static::assertSame(3, $counter->count);
        static::assertStringNotContainsString('COUNT(*)', implode(' ', $counter->queries));
    }

    public function test_stops_issuing_pages_when_a_page_comes_back_short(): void
    {
        $counter = new SelectQueryCounter();
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(new Middleware($counter)), 3);
        $extractor = (new DbalLimitOffsetExtractor(
            $connection,
            $connection->createQueryBuilder()->select('*')->from('users')->orderBy('id'),
        ))
            ->withBatchSize(2)
            ->withMaximum(1000);
        $counter->reset();

        self::assertExtractedRowsCount(3, $extractor);
        // not ceil(1000 / 2) pages: the second one came back short
        static::assertSame(2, $counter->count);
    }

    public function test_is_repeatable(): void
    {
        $connection = InMemorySqlite::connection();

        static::assertTrue(
            (new DbalLimitOffsetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('*')->from('users'),
            ))->isRepeatable(),
        );
    }
}
