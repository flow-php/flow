<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CSVExtractorBatchContractTest extends FlowTestCase
{
    public function test_csv_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): CSVExtractor => from_csv(CSVFixtureContext::path('two_columns.csv'))->withHeader(false),
            array_to_rows(
                [['e00' => 1, 'e01' => 'a'], ['e00' => 2, 'e01' => 'b']],
                schema(int_schema('e00'), str_schema('e01')),
            ),
        );
    }
}
