<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DateInterval;
use Flow\ETL\Column\Physical\TimePhysical;
use Flow\Types\Exception\CastingException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;

final class TimePhysicalTest extends TestCase
{
    public function test_microseconds_of_the_duration(): void
    {
        $interval = new DateInterval('P1DT2H3M4S');
        // @mago-ignore analysis:invalid-property-write
        $interval->f = 0.5;

        static::assertSame(93_784_500_000, (new TimePhysical())->toPhysical($interval));

        // @mago-ignore analysis:invalid-property-write
        $interval->invert = 1;

        static::assertSame(-93_784_500_000, (new TimePhysical())->toPhysical($interval));
    }

    public function test_reads_back_hours_minutes_seconds_and_fraction(): void
    {
        $physical = new TimePhysical();
        $intervals = type_list(type_instance_of(DateInterval::class))->assert([
            $physical->fromPhysical(-93_784_500_000),
            $physical->fromPhysicalAll([-93_784_500_000, null])[0],
        ]);

        foreach ($intervals as $interval) {
            static::assertSame([0, 26, 3, 4, 0.5, 1], [
                $interval->d,
                $interval->h,
                $interval->i,
                $interval->s,
                $interval->f,
                $interval->invert,
            ]);
        }

        static::assertNull($physical->fromPhysicalAll([-93_784_500_000, null])[1]);
    }

    public function test_refuses_months(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage("Relative DateInterval (with months/years) can't be stored in a time column");

        (new TimePhysical())->toPhysical(new DateInterval('P1Y'));
    }

    public function test_refuses_a_duration_outside_the_microsecond_range(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('outside the microsecond range of a time column');

        $interval = new DateInterval('PT0S');
        // @mago-ignore analysis:invalid-property-write
        $interval->s = 9_223_372_036_855;

        (new TimePhysical())->toPhysical($interval);
    }
}
