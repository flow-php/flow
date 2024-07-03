<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{bool_entry, int_entry, lit};
use Flow\ETL\Extractor\RowsExtractor;
use Flow\ETL\Pipeline\{LinkedPipeline, SynchronousPipeline};
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class LinkedPipelineTest extends TestCase
{
    public function test_linked_pipelines() : void
    {
        $pipeline = new LinkedPipeline(
            (new SynchronousPipeline(new RowsExtractor(
                new Rows(
                    Row::create(int_entry('id', 1)),
                    Row::create(int_entry('id', 2))
                )
            )))->add(new ScalarFunctionTransformer('active', lit(true))),
        );

        self::assertEquals(
            [
                new Rows(
                    Row::create(int_entry('id', 1), bool_entry('active', true)),
                    Row::create(int_entry('id', 2), bool_entry('active', true))
                ),
            ],
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
