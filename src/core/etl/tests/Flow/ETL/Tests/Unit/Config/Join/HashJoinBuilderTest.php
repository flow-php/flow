<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Join;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Config\MemoryLimit;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\hash_join;

final class HashJoinBuilderTest extends FlowTestCase
{
    public function test_bucketing_options_are_set(): void
    {
        $config = hash_join()->bucketsCount(4)->batchSize(250)->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertSame(4, $config->bucketing->bucketsCount);
        static::assertSame(250, $config->bucketing->batchSize);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = hash_join()->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertInstanceOf(FilesystemBuckets::class, $config->bucketing->storage);
        static::assertSame(64, $config->bucketing->bucketsCount);
        static::assertSame(1000, $config->bucketing->batchSize);
        static::assertSame(MemoryLimit::default()->inBytes(), $config->memoryLimit->inBytes());
    }

    public function test_memory_limit_below_one_byte_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Join memory limit must be greater than 0 bytes');

        hash_join()->memoryLimit(Unit::fromBytes(0));
    }

    public function test_memory_limit_is_set(): void
    {
        static::assertSame(
            Unit::fromMb(128)->inBytes(),
            hash_join()
                ->memoryLimit(Unit::fromMb(128))
                ->build(Path::realpath(__DIR__), new PhpBackend())
                ->memoryLimit->inBytes(),
        );
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryBuckets();

        static::assertSame(
            $storage,
            hash_join()->storage($storage)->build(Path::realpath(__DIR__), new PhpBackend())->bucketing->storage,
        );
    }
}
