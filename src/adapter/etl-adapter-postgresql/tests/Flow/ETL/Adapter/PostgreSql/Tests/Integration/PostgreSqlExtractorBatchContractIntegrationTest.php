<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlCursorExtractor;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlKeySetExtractor;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlLimitOffsetExtractor;
use Flow\ETL\Adapter\PostgreSql\Tests\Context\IdsTableContext;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_key_set;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

/**
 * Against 5 rows only: withBatchSize(1) is one round trip per row, and on the cursor extractor one
 * FETCH FORWARD 1 per row inside an open transaction.
 */
final class PostgreSqlExtractorBatchContractIntegrationTest extends IntegrationTestCase
{
    public function test_cursor_extractor_honours_its_maximum(): void
    {
        $table = IdsTableContext::create($this->client, 'flow_postgresql_batch_contract_test', 5);

        self::assertExtractorHonoursMaximum(
            fn(int $maximum): PostgreSqlCursorExtractor => from_pgsql_cursor(
                $this->client,
                select(col('id'))->from(table($table))->orderBy(asc(col('id'))),
            )->withMaximum($maximum),
            3,
        );
    }

    public function test_cursor_extractor_honours_the_batch_contract(): void
    {
        $table = IdsTableContext::create($this->client, 'flow_postgresql_batch_contract_test', 5);

        self::assertExtractorHonoursBatchContract(
            fn(): PostgreSqlCursorExtractor => from_pgsql_cursor(
                $this->client,
                select(col('id'))->from(table($table))->orderBy(asc(col('id'))),
            ),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_key_set_extractor_honours_its_maximum(): void
    {
        $table = IdsTableContext::create($this->client, 'flow_postgresql_batch_contract_test', 5);

        self::assertExtractorHonoursMaximum(
            fn(int $maximum): PostgreSqlKeySetExtractor => from_pgsql_key_set(
                $this->client,
                select(col('id'))->from(table($table)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            )->withMaximum($maximum),
            3,
        );
    }

    public function test_key_set_extractor_honours_the_batch_contract(): void
    {
        $table = IdsTableContext::create($this->client, 'flow_postgresql_batch_contract_test', 5);

        self::assertExtractorHonoursBatchContract(
            fn(): PostgreSqlKeySetExtractor => from_pgsql_key_set(
                $this->client,
                select(col('id'))->from(table($table)),
                pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            ),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_limit_offset_extractor_honours_its_maximum(): void
    {
        $table = IdsTableContext::create($this->client, 'flow_postgresql_batch_contract_test', 5);

        self::assertExtractorHonoursMaximum(
            fn(int $maximum): PostgreSqlLimitOffsetExtractor => from_pgsql_limit_offset(
                $this->client,
                select(col('id'))->from(table($table))->orderBy(asc(col('id'))),
            )->withMaximum($maximum),
            3,
        );
    }

    public function test_limit_offset_extractor_honours_the_batch_contract(): void
    {
        $table = IdsTableContext::create($this->client, 'flow_postgresql_batch_contract_test', 5);

        self::assertExtractorHonoursBatchContract(
            fn(): PostgreSqlLimitOffsetExtractor => from_pgsql_limit_offset(
                $this->client,
                select(col('id'))->from(table($table))->orderBy(asc(col('id'))),
            ),
            RowsMother::sequentialIds(5),
        );
    }
}
