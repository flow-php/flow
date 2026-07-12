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
    public function test_default_builds_a_filesystem_buckets_cache(): void
    {
        $config = (new GroupingConfigBuilder())->build(fstab(), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBucketsCache::class, $config->cache);
        static::assertSame(64, $config->bucketsCount);
        static::assertSame(1000, $config->batchSize);
    }

    public function test_injected_cache_wins_over_the_default(): void
    {
        $cache = new FilesystemBucketsCache(new NativeLocalFilesystem(), Path::realpath(__DIR__));

        $config = (new GroupingConfigBuilder())
            ->cache($cache)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertSame($cache, $config->cache);
    }

    public function test_buckets_count_and_batch_size_are_set(): void
    {
        $config = (new GroupingConfigBuilder())
            ->bucketsCount(4)
            ->batchSize(250)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertSame(4, $config->bucketsCount);
        static::assertSame(250, $config->batchSize);
    }
}
