<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\PartitioningProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;

final class BatchSizeOptimizationTest extends FlowTestCase
{
    public function test_for_pipeline_with_batching_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new BatchingProcessor(10));

        static::assertFalse((new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline));
    }

    public function test_for_pipeline_with_loader(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        static::assertTrue((new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline));
    }

    public function test_for_pipeline_with_stream_loader(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        static::assertFalse((new BatchSizeOptimization())->isFor(StreamLoader::output(), $pipeline));
    }

    public function test_for_pipeline_without_loaders(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        static::assertFalse((new BatchSizeOptimization())->isFor($this->createMock(Transformer::class), $pipeline));
    }

    public function test_is_for_pipeline_with_collecting_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new CollectingProcessor());

        static::assertFalse((new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline));
    }

    public function test_is_for_pipeline_with_partitioning_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add(new PartitioningProcessor([ref('group')]));

        static::assertFalse((new BatchSizeOptimization())->isFor(new DbalLoader('test', []), $pipeline));
    }

    public function test_optimize_adds_batching_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $loader = new DbalLoader('test', []);

        $optimizedPipeline = (new BatchSizeOptimization(500))->optimize($loader, $pipeline);

        static::assertCount(2, $optimizedPipeline->segments()->steps());
        static::assertInstanceOf(BatchingProcessor::class, $optimizedPipeline->segments()->steps()[0]);
        static::assertSame($loader, $optimizedPipeline->segments()->steps()[1]);
    }
}
