<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function range;

final class ParquetExtractorBatchContractTest extends FlowTestCase
{
    public function test_parquet_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): ParquetExtractor => from_parquet(__DIR__ . '/Fixtures/Pagination/05_15.parquet'),
            array_to_rows(
                array_map(static fn(int $id): array => ['id' => $id, 'name' => 'name_' . $id], range(1, 15)),
                schema(int_schema('id'), str_schema('name')),
            ),
        );
    }
}
