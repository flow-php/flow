<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Config\Bucketing;

use Flow\ETL\API\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\MemoryLimit;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Path;

use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\hash_repartition;

final class BucketingConfigBuilderTest extends FlowTestCase
{
    public function test_batch_size_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be at least 1');

        // @mago-ignore analysis:invalid-argument
        (new BucketingConfigBuilder('/flow-php-join/', 64))->batchSize(0);
    }

    public function test_buckets_count_and_batch_size_are_set(): void
    {
        $config = (new BucketingConfigBuilder('/flow-php-join/', 64))
            ->bucketsCount(4)
            ->batchSize(250)
            ->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertSame(4, $config->bucketsCount);
        static::assertSame(250, $config->batchSize);
    }

    public function test_buckets_count_below_one_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Buckets count must be greater than 0');

        // @mago-ignore analysis:invalid-argument
        (new BucketingConfigBuilder('/flow-php-join/', 64))->bucketsCount(0);
    }

    public function test_default_builds_a_filesystem_buckets_storage(): void
    {
        $config = (new BucketingConfigBuilder('/flow-php-join/', 64))->build(Path::realpath(__DIR__), new PhpBackend());

        static::assertInstanceOf(FilesystemRowsStorage::class, $config->storage);
        static::assertSame(64, $config->bucketsCount);
        static::assertSame(1000, $config->batchSize);
    }

    public function test_default_buckets_count_comes_from_the_constructor(): void
    {
        static::assertSame(
            100,
            (new BucketingConfigBuilder('/flow-php-sort/', 100))->build(
                Path::realpath(__DIR__),
                new PhpBackend(),
            )->bucketsCount,
        );
    }

    public function test_memory_limit_defaults_to_the_process_default(): void
    {
        static::assertSame(
            MemoryLimit::default()->inBytes(),
            (new BucketingConfigBuilder('/flow-php-join/', 64))
                ->build(Path::realpath(__DIR__), new PhpBackend())
                ->memoryLimit->inBytes(),
        );
    }

    public function test_memory_limit_is_set(): void
    {
        static::assertSame(
            Unit::fromMb(128)->inBytes(),
            (new BucketingConfigBuilder('/flow-php-join/', 64))
                ->memoryLimit(Unit::fromMb(128))
                ->build(Path::realpath(__DIR__), new PhpBackend())
                ->memoryLimit->inBytes(),
        );
    }

    public function test_memory_limit_below_one_byte_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Bucketing memory limit must be greater than 0 bytes');

        (new BucketingConfigBuilder('/flow-php-join/', 64))->memoryLimit(Unit::fromBytes(0));
    }

    public function test_group_by_and_repartition_defaults_are_64_buckets_of_1000_rows_on_the_filesystem(): void
    {
        foreach ([hash_group_by(), hash_repartition()] as $builder) {
            $config = $builder->build(Path::realpath(__DIR__), new PhpBackend());

            static::assertInstanceOf(FilesystemRowsStorage::class, $config->storage);
            static::assertSame(64, $config->bucketsCount);
            static::assertSame(1000, $config->batchSize);
        }
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertSame(
            $storage,
            (new BucketingConfigBuilder('/flow-php-join/', 64))
                ->storage($storage)
                ->build(Path::realpath(__DIR__), new PhpBackend())
                ->storage,
        );
    }
}
