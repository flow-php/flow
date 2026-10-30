<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Config\Sort;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\MemoryLimit;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\external_sort;

final class ExternalSortBuilderTest extends FlowTestCase
{
    public function test_bucketing_options_are_set(): void
    {
        $config = external_sort()->bucketsCount(10)->batchSize(250)->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertSame(10, $config->bucketing->bucketsCount);
        static::assertSame(250, $config->bucketing->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = external_sort()->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertInstanceOf(FilesystemRowsStorage::class, $config->bucketing->storage);
        static::assertSame(100, $config->bucketing->bucketsCount);
        static::assertSame(1000, $config->bucketing->batchSize);
        static::assertSame(MemoryLimit::default()->inBytes(), $config->bucketing->memoryLimit->inBytes());
    }

    public function test_merge_storage_defaults_to_null_so_the_spill_storage_is_reused(): void
    {
        static::assertNull(
            external_sort()->storage(new MemoryRowsStorage())->build(Path::realpath(__DIR__), new PhpBackend())->merge,
        );
    }

    public function test_merge_storage_is_carried_into_the_config(): void
    {
        $merge = new MemoryRowsStorage();

        static::assertSame(
            $merge,
            external_sort()->mergeStorage($merge)->build(Path::realpath(__DIR__), new PhpBackend())->merge,
        );
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertSame(
            $storage,
            external_sort()->storage($storage)->build(Path::realpath(__DIR__), new PhpBackend())->bucketing->storage,
        );
    }

    public function test_memory_limit_below_one_byte_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Bucketing memory limit must be greater than 0 bytes');

        external_sort()->memoryLimit(Unit::fromBytes(0));
    }

    public function test_memory_limit_is_set(): void
    {
        static::assertSame(
            Unit::fromMb(500)->inBytes(),
            external_sort()
                ->memoryLimit(Unit::fromMb(500))
                ->build(Path::realpath(__DIR__), new PhpBackend())
                ->bucketing->memoryLimit->inBytes(),
        );
    }
}
