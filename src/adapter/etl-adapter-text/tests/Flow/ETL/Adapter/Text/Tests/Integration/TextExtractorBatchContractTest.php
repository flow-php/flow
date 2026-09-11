<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class TextExtractorBatchContractTest extends FlowTestCase
{
    public function test_text_extractor_honours_the_batch_contract(): void
    {
        self::assertExtractorHonoursBatchContract(
            static fn(): TextExtractor => from_text(__DIR__ . '/../Fixtures/parity_lines.txt'),
            rows(
                schema(str_schema('text')),
                row(['text' => 'alpha']),
                row(['text' => 'beta']),
                row(['text' => 'gamma']),
            ),
        );
    }
}
