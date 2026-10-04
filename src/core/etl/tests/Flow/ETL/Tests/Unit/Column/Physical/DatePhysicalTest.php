<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\Physical\DatePhysical;
use Flow\Types\Exception\CastingException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

final class DatePhysicalTest extends TestCase
{
    public function test_the_calendar_day_in_the_value_zone(): void
    {
        static::assertSame(
            19_787,
            (new DatePhysical())->toPhysical(
                new DateTimeImmutable('2024-03-05 00:30:00', new DateTimeZone('Asia/Tokyo')),
            ),
        );
        static::assertSame(-1, (new DatePhysical())->toPhysical(new DateTimeImmutable('1969-12-31 23:59:59 UTC')));
    }

    public function test_reads_back_midnight_utc(): void
    {
        $physical = new DatePhysical();
        $value = type_instance_of(DateTimeImmutable::class)->assert($physical->fromPhysical(-1));
        $all = type_instance_of(DateTimeImmutable::class)->assert($physical->fromPhysicalAll([-1, null])[0]);

        static::assertSame('1969-12-31 00:00:00 UTC', $value->format('Y-m-d H:i:s e'));
        static::assertSame('1969-12-31 00:00:00 UTC', $all->format('Y-m-d H:i:s e'));
        static::assertNull($physical->fromPhysicalAll([-1, null])[1]);
    }

    public function test_refuses_a_day_outside_the_i32_range(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('outside the day range of a date column');

        (new DatePhysical())->toPhysical(new DateTimeImmutable('@' . ((2 ** 31) * 86_400)));
    }
}
