<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\DbalKeySetExtractor;
use Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

/**
 * Against 5 rows only: withBatchSize(1) is one round trip per row.
 */
final class DbalExtractorBatchContractTest extends FlowTestCase
{
    public function test_key_set_extractor_honours_its_maximum(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);

        self::assertExtractorHonoursMaximum(
            static fn(int $maximum): DbalKeySetExtractor => (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id')->from('users'),
                pagination_key_set(pagination_key_asc('id')),
            ))->withSchema(schema(int_schema('id')))->withMaximum($maximum),
            3,
        );
    }

    public function test_key_set_extractor_honours_the_batch_contract(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);

        self::assertExtractorHonoursBatchContract(
            static fn(): DbalKeySetExtractor => (new DbalKeySetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id')->from('users'),
                pagination_key_set(pagination_key_asc('id')),
            ))->withSchema(schema(int_schema('id'))),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_limit_offset_extractor_honours_its_maximum(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);

        self::assertExtractorHonoursMaximum(
            static fn(int $maximum): DbalLimitOffsetExtractor => (new DbalLimitOffsetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id')->from('users')->orderBy('id'),
            ))
                ->withSchema(schema(int_schema('id')))
                ->withMaximum($maximum),
            3,
        );
    }

    public function test_limit_offset_extractor_honours_the_batch_contract(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);

        self::assertExtractorHonoursBatchContract(
            static fn(): DbalLimitOffsetExtractor => (new DbalLimitOffsetExtractor(
                $connection,
                $connection->createQueryBuilder()->select('id')->from('users')->orderBy('id'),
            ))->withSchema(schema(int_schema('id'))),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_query_extractor_honours_the_batch_contract(): void
    {
        $connection = InMemorySqlite::withUsers(InMemorySqlite::connection(), 5);

        self::assertExtractorHonoursBatchContract(
            static fn(): DbalQueryExtractor => (new DbalQueryExtractor(
                $connection,
                'SELECT id FROM users ORDER BY id',
            ))->withSchema(schema(int_schema('id'))),
            RowsMother::sequentialIds(5),
        );
    }
}
