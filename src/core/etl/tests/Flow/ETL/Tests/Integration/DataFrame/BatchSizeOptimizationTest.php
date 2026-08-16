<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\DataFrame;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\BatchSizeOptimization;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\WrappingLoader;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\constraint_unique;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\write_with_retries;

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

    public function test_not_changing_default_batch_size_when_the_wrapped_loader_is_not_supported_by_optimizer(): void
    {
        df()
            ->from(new FakeExtractor(100))
            ->write(new WrappingLoader($loader = new SpyLoader()))
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

    public function test_setting_batch_size_to_1k_when_only_one_of_the_wrapped_loaders_is_supported(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));
        $unsupported = [];

        df($config)
            ->from(new FakeExtractor(100))
            ->write(new WrappingLoader(to_array($unsupported), $loader = new SpyLoader()))
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
        static::assertCount(100, $unsupported);
    }

    public function test_setting_batch_size_to_1k_when_the_supported_loader_is_wrapped(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->write(write_with_retries($loader = new SpyLoader()))
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
    }

    public function test_setting_batch_size_to_1k_when_the_supported_loader_is_wrapped_by_a_branch(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->write(to_branch(ref('int')->isNotNull(), $loader = new SpyLoader()))
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
    }

    public function test_setting_batch_size_to_1k_when_the_supported_loader_is_wrapped_by_a_transformation(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->write(to_transformation(
                new CallbackTransformation(static fn(DataFrame $dataFrame): DataFrame => $dataFrame),
                $loader = new SpyLoader(),
            ))
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
    }

    public function test_setting_batch_size_to_1k_when_the_supported_loader_is_wrapped_twice(): void
    {
        $config = config_builder()->optimizer(new Optimizer(new BatchSizeOptimization(100, [SpyLoader::class])));

        df($config)
            ->from(new FakeExtractor(100))
            ->write(new WrappingLoader(write_with_retries($loader = new SpyLoader())))
            ->run();

        static::assertCount(1, $loader->loadedRows);
        static::assertSame(100, $loader->loadedRows[0]->count());
    }
}
