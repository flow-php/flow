<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Pipeline;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\from_pipeline;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class PipelineExtractorTest extends FlowTestCase
{
    public function test_pipeline_extractor(): void
    {
        $pipeline = new Pipeline(from_rows(
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
            rows(row(int_entry('id', 3)), row(int_entry('id', 4))),
            rows(row(int_entry('id', 5)), row(int_entry('id', 6))),
        ));

        $extractor = from_pipeline($pipeline);

        self::assertExtractedBatchesCount(3, $extractor);
    }
}
