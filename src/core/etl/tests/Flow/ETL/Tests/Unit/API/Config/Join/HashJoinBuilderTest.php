<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Config\Join;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\MemoryLimit;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\hash_join;
use function Flow\Filesystem\DSL\path;

final class HashJoinBuilderTest extends FlowTestCase
{
    public function test_broadcast_limit_defaults_to_10_mb(): void
    {
        static::assertSame(
            10 * 1000 * 1000,
            hash_join()->build(path('memory://spill'), new PhpBackend())->broadcastLimit->inBytes(),
        );
        static::assertSame(
            5,
            hash_join()
                ->broadcastLimit(Unit::fromBytes(5))
                ->build(path('memory://spill'), new PhpBackend())
                ->broadcastLimit->inBytes(),
        );

        $this->expectException(InvalidArgumentException::class);

        hash_join()->broadcastLimit(Unit::fromBytes(0));
    }

    public function test_bucketing_options_are_set(): void
    {
        $config = hash_join()->bucketsCount(4)->batchSize(250)->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertSame(4, $config->bucketing->bucketsCount);
        static::assertSame(250, $config->bucketing->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = hash_join()->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertInstanceOf(FilesystemRowsStorage::class, $config->bucketing->storage);
        static::assertSame(64, $config->bucketing->bucketsCount);
        static::assertSame(1000, $config->bucketing->batchSize);
        static::assertSame(MemoryLimit::default()->inBytes(), $config->bucketing->memoryLimit->inBytes());
    }

    public function test_memory_limit_below_one_byte_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Bucketing memory limit must be greater than 0 bytes');

        hash_join()->memoryLimit(Unit::fromBytes(0));
    }

    public function test_memory_limit_is_set(): void
    {
        static::assertSame(
            Unit::fromMb(128)->inBytes(),
            hash_join()
                ->memoryLimit(Unit::fromMb(128))
                ->build(Path::realpath(__DIR__), new PhpBackend())
                ->bucketing->memoryLimit->inBytes(),
        );
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertSame(
            $storage,
            hash_join()->storage($storage)->build(Path::realpath(__DIR__), new PhpBackend())->bucketing->storage,
        );
    }
}
