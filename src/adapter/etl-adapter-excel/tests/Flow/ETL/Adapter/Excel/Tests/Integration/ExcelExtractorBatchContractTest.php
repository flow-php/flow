<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Adapter\Excel\ExcelExtractor;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Tests\Context\ExtractedRows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;

final class ExcelExtractorBatchContractTest extends FlowTestCase
{
    public function test_excel_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): ExcelExtractor => from_excel(ExcelFixtureContext::file('fixture.xlsx')),
            // one row per batch is the read the other sizes must reproduce
            ExtractedRows::of(from_excel(ExcelFixtureContext::file('fixture.xlsx'))->withBatchSize(1)),
        );
    }
}
