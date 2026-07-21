<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Join;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Config\Join\JoinConfigBuilder;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\fstab;

final class JoinConfigBuilderTest extends FlowTestCase
{
    public function test_buckets_count_and_batch_size_are_set(): void
    {
        $config = (new JoinConfigBuilder())
            ->bucketsCount(4)
            ->batchSize(250)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertSame(4, $config->bucketsCount);
        static::assertSame(250, $config->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = (new JoinConfigBuilder())->build(fstab(), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->cache);
        static::assertSame(64, $config->bucketsCount);
        static::assertSame(1000, $config->batchSize);
    }

    public function test_injected_cache_wins_over_the_default(): void
    {
        $cache = new MemoryBuckets();

        $config = (new JoinConfigBuilder())
            ->cache($cache)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertSame($cache, $config->cache);
    }
}
