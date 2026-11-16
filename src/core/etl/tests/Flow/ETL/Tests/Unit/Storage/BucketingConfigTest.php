<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage;

use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\PsrRowsStorage;
use Flow\ETL\Storage\SpillingRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;

final class BucketingConfigTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $rebuilt = DescribeRoundTrip::rebuilt(
            new BucketingConfig(new MemoryRowsStorage(), 3, 500, Unit::fromBytes(12_345)),
        );

        static::assertSame(3, $rebuilt->bucketsCount);
        static::assertSame(500, $rebuilt->batchSize);
        static::assertSame(12_345, $rebuilt->memoryLimit->inBytes());
        static::assertInstanceOf(MemoryRowsStorage::class, $rebuilt->storage);
    }

    public function test_a_rebuilt_config_takes_its_share_of_the_memory_limit(): void
    {
        $config = new BucketingConfig(new MemoryRowsStorage(), 3, 500, Unit::fromBytes(800));

        static::assertSame(
            100,
            DescribeRoundTrip::rebuilt($config, new Rebuild(memoryShare: 8))->memoryLimit->inBytes(),
        );
        static::assertSame(800, DescribeRoundTrip::rebuilt($config, new Rebuild())->memoryLimit->inBytes());
    }

    public function test_a_share_never_takes_the_limit_to_zero(): void
    {
        static::assertSame(
            1,
            DescribeRoundTrip::rebuilt(
                new BucketingConfig(new MemoryRowsStorage(), 3, 500, Unit::fromBytes(3)),
                new Rebuild(memoryShare: 8),
            )->memoryLimit->inBytes(),
        );
    }

    public function test_spilling_keeps_a_resident_storage_and_wraps_any_other(): void
    {
        $resident = new MemoryRowsStorage();
        $disk = new SpyRowsStorage(new MemoryRowsStorage());

        static::assertSame($resident, (new BucketingConfig($resident, 64, 1000, Unit::fromMb(1)))->spilling(
            new PhpBackend(),
            new MemoryBudget(new PhpBackend(), Unit::fromMb(1), null),
        ));

        $wrapped = (new BucketingConfig($disk, 64, 1, Unit::fromGb(64)))->spilling(
            new PhpBackend(),
            new MemoryBudget(new PhpBackend(), Unit::fromBytes(1), null),
        );
        $wrapped->append('a', IdRows::batches([1, 2])->current());

        static::assertInstanceOf(SpillingRowsStorage::class, $wrapped);
        static::assertSame($disk, $wrapped->disk);
        static::assertNotSame([], $disk->appendedRows(), 'the wrapped storage spills under the budget it was given');
    }

    public function test_a_psr_cache_storage_does_not_describe(): void
    {
        static::assertNull(
            (new BucketingConfig(
                new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new PhpBackend())),
                3,
                500,
                Unit::fromMb(1),
            ))->describe(),
        );
    }
}
