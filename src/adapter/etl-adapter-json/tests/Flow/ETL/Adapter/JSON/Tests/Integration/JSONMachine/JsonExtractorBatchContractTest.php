<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonLinesExtractor;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\from_json_lines;

final class JsonExtractorBatchContractTest extends FlowTestCase
{
    public function test_json_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): JsonExtractor => from_json(JsonFixtureContext::path('five_rows.json')),
            RowsMother::sequentialIds(5),
        );
    }

    public function test_json_lines_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): JsonLinesExtractor => from_json_lines(JsonFixtureContext::path('five_rows.jsonl')),
            RowsMother::sequentialIds(5),
        );
    }
}
