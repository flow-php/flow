<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Sort;

use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\PsrRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;

final class ExternalSortConfigTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $rebuilt = DescribeRoundTrip::rebuilt(
            new ExternalSortConfig(
                new BucketingConfig(new MemoryRowsStorage(), 4, 100, Unit::fromBytes(12_345)),
                new MemoryRowsStorage(),
            ),
        );

        static::assertInstanceOf(MemoryRowsStorage::class, $rebuilt->merge);
        static::assertSame(12_345, $rebuilt->bucketing->memoryLimit->inBytes());
    }

    public function test_a_missing_merge_storage_round_trips_as_null(): void
    {
        static::assertNull(DescribeRoundTrip::rebuilt(new ExternalSortConfig(
            new BucketingConfig(new MemoryRowsStorage(), 4, 100, Unit::fromBytes(1)),
        ))->merge);
    }

    public function test_an_undescribable_merge_storage_makes_the_config_undescribable(): void
    {
        static::assertNull(
            (new ExternalSortConfig(
                new BucketingConfig(new MemoryRowsStorage(), 4, 100, Unit::fromBytes(1)),
                new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new PhpBackend())),
            ))->describe(),
        );
    }
}
