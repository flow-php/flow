<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\DSL\{config, flow_context};
use function Flow\ETL\DSL\{from_all, from_array};
use Flow\ETL\Pipeline\{BatchingPipeline, SynchronousPipeline};
use Flow\ETL\{Rows, Tests\FlowTestCase};

final class BatchingPipelineTest extends FlowTestCase
{
    public function test_batching_rows() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(from_all(
            from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]),
            from_array([
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        )), size: 10);

        self::assertCount(
            1,
            \iterator_to_array($pipeline->process(flow_context(config())))
        );
    }

    public function test_that_rows_are_not_lost() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(from_all(
            from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        )), size: 7);

        self::assertEquals(
            [
                [
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                    ['id' => 4],
                    ['id' => 5],
                    ['id' => 6],
                    ['id' => 7],
                ],
                [
                    ['id' => 8],
                    ['id' => 9],
                    ['id' => 10],
                ],
            ],
            \array_map(
                static fn (Rows $r) => $r->toArray(),
                \iterator_to_array($pipeline->process(flow_context(config())))
            )
        );
    }

    public function test_using_bigger_batch_size_than_total_number_of_rows() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(from_all(
            from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]),
            from_array([
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        )), size: 11);

        self::assertCount(
            1,
            \iterator_to_array($pipeline->process(flow_context(config())))
        );
    }

    public function test_using_smaller_batch_size_than_total_number_of_rows() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(from_all(
            from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        )), size: 5);

        self::assertCount(
            2,
            \iterator_to_array($pipeline->process(flow_context(config())))
        );
    }
}
