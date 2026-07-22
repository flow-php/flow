<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Floe\Tests\Double\SpyHydrator;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function sort;

final class FloeLoaderTest extends TestCase
{
    public function test_closure_writes_a_readable_non_torn_file(): void
    {
        $context = flow_context(config());
        $path = path('memory://closed.floe');

        $loader = to_floe($path);
        $loader->load(rows(row(int_entry('id', 1)), row(int_entry('id', 2))), $context);
        $loader->closure($context);

        $ids = [];

        foreach (from_floe($path)->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->valueOf('id');
            }
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_load_honors_the_context_hydrator(): void
    {
        $hydrator = new SpyHydrator();
        $context = flow_context(config_builder()->hydrator($hydrator)->build());
        $path = path('memory://hydrator.floe');

        $loader = to_floe($path);
        $loader->load(rows(row(int_entry('id', 1)), row(int_entry('id', 2))), $context);
        $loader->closure($context);

        static::assertGreaterThan(0, $hydrator->dehydrateCalls);
    }

    public function test_destination_returns_path(): void
    {
        static::assertSame('memory://out.floe', to_floe(path('memory://out.floe'))->destination()->uri());
    }

    public function test_load_to_path_without_extension_reports_failure(): void
    {
        $this->expectException(RuntimeException::class);

        to_floe(path('memory://no-extension'))->load(rows(row(int_entry('id', 1))), flow_context(config()));
    }

    public function test_partitioned_batches_write_one_file_per_partition(): void
    {
        $context = flow_context(config());
        $base = path('memory://parts/data.floe');

        $loader = to_floe($base);
        $loader->load(
            Rows::partitioned([row(int_entry('id', 1), str_entry('country', 'PL'))], [new Partition('country', 'PL')]),
            $context,
        );
        $loader->load(
            Rows::partitioned([row(int_entry('id', 2), str_entry('country', 'US'))], [new Partition('country', 'US')]),
            $context,
        );
        $loader->closure($context);

        $fs = $context->filesystem($base);

        static::assertNotNull($fs->status(path('memory://parts/country=PL/data.floe')));
        static::assertNotNull($fs->status(path('memory://parts/country=US/data.floe')));

        $ids = [];

        foreach (from_floe(path('memory://parts/**/*.floe'))->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->valueOf('id');
            }
        }

        sort($ids);

        static::assertSame([1, 2], $ids);
    }

    public function test_repeated_loads_write_a_single_file(): void
    {
        $context = flow_context(config());
        $path = path('memory://repeated.floe');

        $loader = to_floe($path);
        $loader->load(rows(row(int_entry('id', 1)), row(int_entry('id', 2))), $context);
        $loader->load(rows(row(int_entry('id', 3))), $context);
        $loader->closure($context);

        $ids = [];

        // reading the concrete (non-pattern) path proves all rows landed in one file
        foreach (from_floe($path)->extract($context) as $batch) {
            foreach ($batch->all() as $extractedRow) {
                $ids[] = $extractedRow->valueOf('id');
            }
        }

        static::assertSame([1, 2, 3], $ids);
    }
}
