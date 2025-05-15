<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{bool_entry, int_entry, lit};
use function Flow\ETL\DSL\{config, flow_context, from_rows, ref, row, rows};
use Flow\ETL\Pipeline\{BatchingPipeline, LinkedPipeline, PartitioningPipeline, SynchronousPipeline};
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\{Pipeline, Tests\FlowTestCase};
use Flow\ETL\Transformer\ScalarFunctionTransformer;

final class LinkedPipelineTest extends FlowTestCase
{
    public function test_linked_pipelines() : void
    {
        $pipeline = new LinkedPipeline(
            (new SynchronousPipeline(from_rows(rows(row(int_entry('id', 1)), row(int_entry('id', 2))))))->add(new ScalarFunctionTransformer('active', lit(true))),
        );

        self::assertEquals(
            [
                rows(row(int_entry('id', 1), bool_entry('active', true)), row(int_entry('id', 2), bool_entry('active', true))),
            ],
            \iterator_to_array($pipeline->process(flow_context(config())))
        );
    }

    public function test_list_of_all_pipelines_linked_by_linked_pipeline() : void
    {
        $pipeline = new LinkedPipeline(
            new CollectingPipeline(
                new LinkedPipeline(
                    new PartitioningPipeline(
                        new LinkedPipeline(new BatchingPipeline(new SynchronousPipeline(), 100)),
                        [ref('id')]
                    )
                ),
            )
        );

        $pipelines = \array_map(
            fn (Pipeline $pipeline) => $pipeline::class,
            $pipeline->pipelines()
        );

        self::assertEquals(
            [
                CollectingPipeline::class,
                SynchronousPipeline::class,
            ],
            $pipelines
        );
    }
}
