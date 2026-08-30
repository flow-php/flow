<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\PartitioningProcessor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\WrappingLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class BatchSizeOptimizationTest extends FlowTestCase
{
    public function test_for_pipeline_with_batching_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));
        $pipeline->add(new BatchingProcessor(10));

        static::assertFalse((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new SpyLoader(),
            $pipeline,
        ));
    }

    public function test_for_pipeline_with_loader(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertTrue((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new SpyLoader(),
            $pipeline,
        ));
    }

    public function test_for_pipeline_with_stream_loader(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertFalse((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            StreamLoader::output(),
            $pipeline,
        ));
    }

    public function test_for_pipeline_with_wrapped_loader(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertTrue((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new WrappingLoader(new SpyLoader()),
            $pipeline,
        ));
    }

    public function test_for_pipeline_with_wrapped_loader_nested_three_levels_deep(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertTrue((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new WrappingLoader(new WrappingLoader(new WrappingLoader(new SpyLoader()))),
            $pipeline,
        ));
    }

    public function test_for_pipeline_with_wrapper_hiding_a_supported_loader_behind_an_unsupported_one(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertTrue((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new WrappingLoader(StreamLoader::output(), new SpyLoader()),
            $pipeline,
        ));
    }

    public function test_for_pipeline_with_wrapper_overriding_only_unsupported_loaders(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertFalse((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new WrappingLoader(StreamLoader::output()),
            $pipeline,
        ));
    }

    public function test_for_pipeline_without_loaders(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));

        static::assertFalse((new BatchSizeOptimization())->isFor($this->createStub(Transformer::class), $pipeline));
    }

    public function test_is_for_pipeline_with_collecting_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));
        $pipeline->add(new CollectingProcessor());

        static::assertFalse((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new SpyLoader(),
            $pipeline,
        ));
    }

    public function test_is_for_pipeline_with_partitioning_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));
        $pipeline->add(new PartitioningProcessor([ref('group')]));

        static::assertFalse((new BatchSizeOptimization(supportedLoaders: [SpyLoader::class]))->isFor(
            new SpyLoader(),
            $pipeline,
        ));
    }

    public function test_optimize_adds_batching_processor(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));
        $loader = new SpyLoader();

        $optimizedPipeline = (new BatchSizeOptimization(500, [SpyLoader::class]))->optimize($loader, $pipeline);

        static::assertCount(2, $optimizedPipeline->segments()->steps());
        static::assertInstanceOf(BatchingProcessor::class, $optimizedPipeline->segments()->steps()[0]);
        static::assertSame($loader, $optimizedPipeline->segments()->steps()[1]);
    }
}
