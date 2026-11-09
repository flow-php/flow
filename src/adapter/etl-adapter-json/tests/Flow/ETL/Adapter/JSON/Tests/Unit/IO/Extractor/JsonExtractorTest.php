<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\IO\Extractor;

use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\JSON\from_json_lines;

final class JsonExtractorTest extends FlowTestCase
{
    public function test_schema_reports_the_sampled_rows_to_the_inference_progress(): void
    {
        $progress = new ProgressSpy();
        $extractor = from_json_lines(__DIR__ . '/../../../Fixtures/five_rows.jsonl');
        $extractor->reportInferenceTo($progress);

        $extractor->schema();

        static::assertSame([['', 5]], $progress->advancedCalls());
    }
}
