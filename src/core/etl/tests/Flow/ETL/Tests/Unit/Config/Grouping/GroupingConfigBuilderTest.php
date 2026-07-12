<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Grouping;

use Flow\ETL\Config\Grouping\GroupingConfigBuilder;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\fstab;

final class GroupingConfigBuilderTest extends FlowTestCase
{
    public function test_default_keeps_aggregation_in_memory(): void
    {
        $config = (new GroupingConfigBuilder())->build(fstab(), Path::realpath(__DIR__));

        static::assertNull($config->cache);
        static::assertSame(64, $config->partitions);
        static::assertSame(1000, $config->batchSize);
    }

    public function test_filesystem_builds_the_default_buckets_cache(): void
    {
        $config = (new GroupingConfigBuilder())
            ->filesystem()
            ->partitions(4)
            ->batchSize(250)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBucketsCache::class, $config->cache);
        static::assertSame(4, $config->partitions);
        static::assertSame(250, $config->batchSize);
    }

    public function test_injected_buckets_cache_wins_over_the_default(): void
    {
        $cache = new FilesystemBucketsCache(new NativeLocalFilesystem(), Path::realpath(__DIR__));

        $config = (new GroupingConfigBuilder())
            ->filesystem($cache)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertSame($cache, $config->cache);
    }
}
