<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\constraint_unique;
use function Flow\ETL\DSL\df;

final class BatchSizeOptimizationTest extends FlowTestCase
{
    public function test_not_changing_default_batch_size_when_loader_is_not_supported_by_optimizer(): void
    {
        df()
            ->from(new FakeExtractor(100))
            ->write($loader = new SpyLoader())
            ->run();

        static::assertCount(100, $loader->loadedRows);
        static::assertSame(1, $loader->loadedRows[0]->count());
    }

    public function test_not_changing_explicitly_set_batch_size(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->batchSize(10)
            ->write($loader = new SpyLoader())
            ->run();

        static::assertCount(10, $loader->loadedRows);
        static::assertSame(10, $loader->loadedRows[0]->count());
    }

    public function test_not_changing_explicitly_set_batch_size_with_another_overriding_pipeline(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        $df = df($config)->from(new FakeExtractor(100));

        $df
            ->match(FakeExtractor::schema())
            ->limit(null)
            ->batchSize(10)
            ->constrain(constraint_unique('int'))
            ->write($loader = new SpyLoader())
            ->run();

        static::assertCount(10, $loader->loadedRows);
        static::assertSame(10, $loader->loadedRows[0]->count());
    }

    public function test_setting_batch_size_to_1k_when_another_overriding_pipeline_is_set(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->constrain(constraint_unique('int'))
            ->write($loader = new SpyLoader())
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
    }

    public function test_setting_batch_size_to_1k_when_none_was_set(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->write($loader = new SpyLoader())
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
    }
}
