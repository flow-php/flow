<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\Physical\DateTimePhysical;
use Flow\Types\Exception\CastingException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

final class DateTimePhysicalTest extends TestCase
{
    #[TestWith(['@0', 0])]
    #[TestWith(['@-0.000001', -1])]
    #[TestWith(['@-1.500000', -1_500_000])]
    #[TestWith(['@1.000001', 1_000_001])]
    #[TestWith(['9999-12-31 23:59:59.999999 UTC', 253_402_300_799_999_999])]
    public function test_microseconds_since_the_epoch(string $datetime, int $microseconds): void
    {
        $physical = new DateTimePhysical(new DateTimeZone('UTC'));
        $value = new DateTimeImmutable($datetime);

        static::assertSame($microseconds, $physical->toPhysical($value));
        static::assertEquals($value, $physical->fromPhysical($microseconds));
        static::assertEquals([$value, null], $physical->fromPhysicalAll([$microseconds, null]));
    }

    public function test_reads_back_in_the_zone(): void
    {
        $physical = new DateTimePhysical(new DateTimeZone('Asia/Tokyo'));
        $value = type_instance_of(DateTimeImmutable::class)->assert($physical->fromPhysical(0));
        $all = type_instance_of(DateTimeImmutable::class)->assert($physical->fromPhysicalAll([0])[0]);

        static::assertSame('1970-01-01 09:00:00 Asia/Tokyo', $value->format('Y-m-d H:i:s e'));
        static::assertSame('1970-01-01 09:00:00 Asia/Tokyo', $all->format('Y-m-d H:i:s e'));
    }

    public function test_refuses_a_value_outside_the_microsecond_range(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('outside the microsecond range of a datetime column');

        (new DateTimePhysical(new DateTimeZone('UTC')))->toPhysical(new DateTimeImmutable('@9223372036855'));
    }
}
