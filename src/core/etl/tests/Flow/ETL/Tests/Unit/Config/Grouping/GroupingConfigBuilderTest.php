<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Grouping;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Config\Grouping\GroupingConfigBuilder;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\fstab;

final class GroupingConfigBuilderTest extends FlowTestCase
{
    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = (new GroupingConfigBuilder())->build(fstab(), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->storage);
        static::assertSame(64, $config->bucketsCount);
        static::assertSame(1000, $config->batchSize);
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryBuckets();

        $config = (new GroupingConfigBuilder())
            ->storage($storage)
            ->build(fstab(), Path::realpath(__DIR__));

        static::assertSame($storage, $config->storage);
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
