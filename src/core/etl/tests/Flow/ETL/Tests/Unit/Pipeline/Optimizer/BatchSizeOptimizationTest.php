<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Pipeline\BatchingPipeline;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\NestedPipeline;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Transformer;
use PHPUnit\Framework\TestCase;

final class BatchSizeOptimizationTest extends TestCase
{
    public function test_for_nested_pipeline_with_batching_pipeline() : void
    {
        $pipeline = new NestedPipeline(
            new BatchingPipeline(new SynchronousPipeline(), 10),
            new SynchronousPipeline()
        );

        $this->assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', [], []), $pipeline)
        );
    }

    public function test_for_synchronous_pipeline_with_loader() : void
    {
        $pipeline = new SynchronousPipeline();

        $this->assertTrue(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', [], []), $pipeline)
        );
    }

    public function test_for_synchronous_pipeline_with_stream_loader() : void
    {
        $pipeline = new SynchronousPipeline();

        $this->assertFalse(
            (new BatchSizeOptimization())->isFor(StreamLoader::output(), $pipeline)
        );
    }

    public function test_for_synchronous_pipeline_without_loaders() : void
    {
        $pipeline = new SynchronousPipeline();

        $this->assertFalse(
            (new BatchSizeOptimization())->isFor($this->createMock(Transformer::class), $pipeline)
        );
    }

    public function test_is_for_already_batching_pipeline() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(), 10);

        $this->assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', [], []), $pipeline)
        );
    }

    public function test_is_for_collecting_pipeline() : void
    {
        $pipeline = new CollectingPipeline(new SynchronousPipeline());

        $this->assertFalse(
            (new BatchSizeOptimization())->isFor(new DbalLoader('test', [], []), $pipeline)
        );
    }
}
