<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Sort;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\external_sort;
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\native_local_filesystem;

final class ExternalSortBuilderTest extends FlowTestCase
{
    public function test_bucketing_options_are_set(): void
    {
        $config = external_sort()->bucketsCount(10)->batchSize(250)->build(fstab(), Path::realpath(__DIR__));

        static::assertSame(10, $config->bucketing->bucketsCount);
        static::assertSame(250, $config->bucketing->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = external_sort()->build(fstab(), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->bucketing->storage);
        static::assertSame(100, $config->bucketing->bucketsCount);
        static::assertSame(1000, $config->bucketing->batchSize);
        static::assertSame(10_000, $config->runSize);
    }

    public function test_filesystem_protocol_is_used_for_the_default_storage(): void
    {
        // the fstab has no 'file' mount, a successful build proves the custom protocol was used
        $config = external_sort()
            ->filesystemProtocol('custom-sort')
            ->build(fstab(native_local_filesystem('custom-sort')), Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->bucketing->storage);
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryBuckets();

        static::assertSame(
            $storage,
            external_sort()->storage($storage)->build(fstab(), Path::realpath(__DIR__))->bucketing->storage,
        );
    }

    public function test_run_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Run size must be greater than 0');

        // @mago-ignore analysis:invalid-argument
        external_sort()->runSize(0);
    }

    public function test_run_size_is_set(): void
    {
        static::assertSame(500, external_sort()->runSize(500)->build(fstab(), Path::realpath(__DIR__))->runSize);
    }
}
