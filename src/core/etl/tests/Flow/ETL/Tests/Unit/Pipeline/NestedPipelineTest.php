<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{bool_entry, int_entry, lit};
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Pipeline\{BatchingPipeline, NestedPipeline, SynchronousPipeline};
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class NestedPipelineTest extends TestCase
{
    public function test_nested_pipelines() : void
    {
        $pipeline = new NestedPipeline(
            (new SynchronousPipeline())->add(new ScalarFunctionTransformer('active', lit(true))),
            new BatchingPipeline(new SynchronousPipeline(), 1)
        );

        $pipeline->setSource(new ProcessExtractor(
            new Rows(
                Row::create(int_entry('id', 1)),
                Row::create(int_entry('id', 2))
            )
        ));

        $this->assertEquals(
            [
                new Rows(
                    Row::create(int_entry('id', 1), bool_entry('active', true)),
                ),
                new Rows(
                    Row::create(int_entry('id', 2), bool_entry('active', true))
                ),
            ],
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
