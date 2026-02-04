<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use function Flow\ETL\DSL\{from_rows, ref, rows};
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\{Pipeline, Transformer};
use Flow\ETL\Processor\{BatchingProcessor, CollectingProcessor, PartitioningProcessor};
use Flow\ETL\Tests\FlowTestCase;

final class BatchSizeOptimizationTest extends FlowTestCase
{
    public function test_for_pipeline_with_batching_processor() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new BatchingProcessor(10));

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_for_pipeline_with_loader() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        self::assertTrue(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_for_pipeline_with_stream_loader() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(StreamLoader::output(), $pipeline)
        );
    }

    public function test_for_pipeline_without_loaders() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        self::assertFalse(
            (new BatchSizeOptimization())->isFor($this->createMock(Transformer::class), $pipeline)
        );
    }

    public function test_is_for_pipeline_with_collecting_processor() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new CollectingProcessor());

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_is_for_pipeline_with_partitioning_processor() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new PartitioningProcessor([ref('group')]));

        self::assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline)
        );
    }

    public function test_optimize_adds_batching_processor() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $loader = new DbalLoader('test', []);

        $optimizedPipeline = (new BatchSizeOptimization(500))->optimize($loader, $pipeline);

        self::assertCount(2, $optimizedPipeline->segments()->steps());
        self::assertInstanceOf(BatchingProcessor::class, $optimizedPipeline->segments()->steps()[0]);
        self::assertSame($loader, $optimizedPipeline->segments()->steps()[1]);
    }
}
