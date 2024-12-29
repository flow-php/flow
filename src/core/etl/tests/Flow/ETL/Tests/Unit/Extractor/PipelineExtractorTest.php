<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Extractor\{PipelineExtractor, RowsExtractor};
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\{Row, Rows, Tests\FlowTestCase};

final class PipelineExtractorTest extends FlowTestCase
{
    public function test_pipeline_extractor() : void
    {
        $pipeline = new SynchronousPipeline(new RowsExtractor(
            new Rows(Row::create(int_entry('id', 1)), Row::create(int_entry('id', 2))),
            new Rows(Row::create(int_entry('id', 3)), Row::create(int_entry('id', 4))),
            new Rows(Row::create(int_entry('id', 5)), Row::create(int_entry('id', 6))),
        ));

        $extractor = new PipelineExtractor($pipeline);

        self::assertExtractedBatchesCount(3, $extractor);
    }
}
