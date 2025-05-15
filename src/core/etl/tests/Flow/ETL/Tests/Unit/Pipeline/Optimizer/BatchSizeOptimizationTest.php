<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\{GroupBy, Transformer};
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Pipeline\{BatchingPipeline,
    CollectingPipeline,
    GroupByPipeline,
    LinkedPipeline,
    PartitioningPipeline,
    SynchronousPipeline};
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Tests\FlowTestCase;

final class BatchSizeOptimizationTest extends FlowTestCase
{
    public function test_for_nested_pipeline_with_batching_pipeline() : void
    {
        $pipeline = new LinkedPipeline(new BatchingPipeline(new SynchronousPipeline(), 10));

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_for_synchronous_pipeline_with_loader() : void
    {
        $pipeline = new SynchronousPipeline();

        self::assertTrue(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_for_synchronous_pipeline_with_stream_loader() : void
    {
        $pipeline = new SynchronousPipeline();

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(StreamLoader::output(), $pipeline)
        );
    }

    public function test_for_synchronous_pipeline_without_loaders() : void
    {
        $pipeline = new SynchronousPipeline();

        self::assertFalse(
            (new BatchSizeOptimization())->isFor($this->createMock(Transformer::class), $pipeline)
        );
    }

    public function test_is_for_already_batching_pipeline() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(), 10);

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_is_for_already_deeply_nested_batching_pipeline() : void
    {
        $pipeline = new LinkedPipeline(
            new GroupByPipeline(
                new GroupBy(),
                new LinkedPipeline(
                    new PartitioningPipeline(
                        new LinkedPipeline(new BatchingPipeline(new SynchronousPipeline(), 100)),
                        [ref('id')]
                    )
                ),
            )
        );

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_is_for_collecting_pipeline() : void
    {
        $pipeline = new CollectingPipeline(new SynchronousPipeline());

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }
}
