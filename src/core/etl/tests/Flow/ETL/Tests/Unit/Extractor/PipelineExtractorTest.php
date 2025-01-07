<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{from_pipeline, from_rows, int_entry, row, rows};
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\{Tests\FlowTestCase};

final class PipelineExtractorTest extends FlowTestCase
{
    public function test_pipeline_extractor() : void
    {
        $pipeline = new SynchronousPipeline(from_rows(rows(row(int_entry('id', 1)), row(int_entry('id', 2))), rows(row(int_entry('id', 3)), row(int_entry('id', 4))), rows(row(int_entry('id', 5)), row(int_entry('id', 6)))));

        $extractor = from_pipeline($pipeline);

        self::assertExtractedBatchesCount(3, $extractor);
    }
}
