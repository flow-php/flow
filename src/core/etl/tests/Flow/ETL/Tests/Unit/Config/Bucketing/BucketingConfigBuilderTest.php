<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Bucketing;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

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
            ->build(Path::realpath(__DIR__));

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
        $config = (new BucketingConfigBuilder('/flow-php-join/', 64))->build(Path::realpath(__DIR__));

        static::assertInstanceOf(FilesystemBuckets::class, $config->storage);
        static::assertSame(64, $config->bucketsCount);
        static::assertSame(1000, $config->batchSize);
    }

    public function test_default_buckets_count_comes_from_the_constructor(): void
    {
        $config = (new BucketingConfigBuilder('/flow-php-sort/', 100))->build(Path::realpath(__DIR__));

        static::assertSame(100, $config->bucketsCount);
    }

    public function test_injected_storage_wins_over_the_default(): void
    {
        $storage = new MemoryBuckets();

        $config = (new BucketingConfigBuilder('/flow-php-join/', 64))
            ->storage($storage)
            ->build(Path::realpath(__DIR__));

        static::assertSame($storage, $config->storage);
    }
}
