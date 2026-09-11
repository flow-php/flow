<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Logging\Middleware;
use Flow\ETL\Adapter\Doctrine\DbalKeySetExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Adapter\Doctrine\Tests\Context\SelectQueryCounter;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DbalKeySetExtractorTest extends FlowTestCase
{
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
        $extractor->pushLimit(1500);
        $counter->reset();

        self::assertExtractedRowsCount(1500, $extractor);
        // 1000, then a page narrowed to the 500 still wanted - and no third query for row 1501
        static::assertSame(2, $counter->count);
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
}
