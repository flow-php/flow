<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Join;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\hash_join;

final class HashJoinBuilderTest extends FlowTestCase
{
    public function test_bucketing_options_are_set(): void
    {
        $config = hash_join()->bucketsCount(4)->batchSize(250)->build(Path::realpath(__DIR__));

        static::assertSame(4, $config->bucketing->bucketsCount);
        static::assertSame(250, $config->bucketing->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = hash_join()->build(Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->bucketing->storage);
        static::assertSame(64, $config->bucketing->bucketsCount);
        static::assertSame(1000, $config->bucketing->batchSize);
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryBuckets();

        static::assertSame(
            $storage,
            hash_join()->storage($storage)->build(Path::realpath(__DIR__))->bucketing->storage,
        );
    }
}
