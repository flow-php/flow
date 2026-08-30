<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Pipeline;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\from_pipeline;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class PipelineExtractorTest extends FlowTestCase
{
    public function test_pipeline_extractor(): void
    {
        $pipeline = new Pipeline(from_rows(
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
            rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 4])),
            rows(schema(int_schema('id')), row(['id' => 5]), row(['id' => 6])),
        ));

        $extractor = from_pipeline($pipeline);

        self::assertExtractedBatchesCount(3, $extractor);
    }
}
