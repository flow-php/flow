<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Join;

use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\PsrRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;

final class HashJoinConfigTest extends FlowTestCase
{
    public function test_broadcast_limit_round_trips(): void
    {
        static::assertSame(
            99,
            DescribeRoundTrip::rebuilt(
                new HashJoinConfig(
                    new BucketingConfig(new MemoryRowsStorage(), 4, 100, Unit::fromBytes(12_345)),
                    Unit::fromBytes(99),
                ),
            )->broadcastLimit->inBytes(),
        );
    }

    public function test_describe_round_trips(): void
    {
        $rebuilt = DescribeRoundTrip::rebuilt(
            new HashJoinConfig(
                new BucketingConfig(new MemoryRowsStorage(), 4, 100, Unit::fromBytes(12_345)),
                Unit::fromBytes(77),
            ),
        );

        static::assertSame(12_345, $rebuilt->bucketing->memoryLimit->inBytes());
        static::assertSame(77, $rebuilt->broadcastLimit->inBytes());
        static::assertSame(4, $rebuilt->bucketing->bucketsCount);
    }

    public function test_an_undescribable_storage_makes_the_config_undescribable(): void
    {
        static::assertNull(
            (new HashJoinConfig(
                new BucketingConfig(
                    new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new PhpBackend())),
                    4,
                    100,
                    Unit::fromBytes(1),
                ),
                Unit::fromBytes(1),
            ))->describe(),
        );
    }
}
