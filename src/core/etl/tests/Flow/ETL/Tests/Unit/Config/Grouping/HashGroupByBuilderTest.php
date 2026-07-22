<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Grouping;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\hash_group_by;
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\native_local_filesystem;

final class HashGroupByBuilderTest extends FlowTestCase
{
    public function test_bucketing_options_are_set(): void
    {
        $config = hash_group_by()->bucketsCount(4)->batchSize(250)->build(fstab(), Path::realpath(__DIR__));

        static::assertSame(4, $config->bucketing->bucketsCount);
        static::assertSame(250, $config->bucketing->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = hash_group_by()->build(fstab(), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->bucketing->storage);
        static::assertSame(64, $config->bucketing->bucketsCount);
        static::assertSame(1000, $config->bucketing->batchSize);
    }

    public function test_filesystem_protocol_is_used_for_the_default_storage(): void
    {
        static::assertInstanceOf(
            FilesystemBuckets::class,
            hash_group_by()
                ->filesystemProtocol('custom-group-by')
                ->build(fstab(native_local_filesystem('custom-group-by')), Path::realpath(__DIR__))
                ->bucketing
                ->storage,
        );
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryBuckets();

        static::assertSame(
            $storage,
            hash_group_by()->storage($storage)->build(fstab(), Path::realpath(__DIR__))->bucketing->storage,
        );
    }
}
