<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException as TypesInvalidArgumentException;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\to_timezone;

final class ToTimeZoneTest extends FlowTestCase
{
    public function test_abbreviation_zone_is_refused(): void
    {
        $this->expectException(TypesInvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Time zone "PST" cannot be a datetime column zone, use an IANA name like "Europe/Warsaw", "UTC" or an offset "+HH:MM"',
        );

        to_timezone(lit(new DateTimeImmutable('2020-01-01')), 'PST');
    }

    public function test_casting_date_time_pst_to_utc_time_zone(): void
    {
        // @mago-ignore analysis:mixed-assignment
        $result = to_timezone(
            lit(new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('PST'))),
            new DateTimeZone('UTC'),
        )->eval(row([]), flow_context());
        static::assertInstanceOf(DateTimeInterface::class, $result);
        static::assertSame('2020-01-01 08:00:00.000000', $result->format('Y-m-d H:i:s.u'));
    }

    public function test_casting_date_time_pst_to_utc_time_zone_from_string_tz(): void
    {
        // @mago-ignore analysis:mixed-assignment
        $result = to_timezone(lit(new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('PST'))), 'UTC')->eval(
            row([]),
            flow_context(),
        );
        static::assertInstanceOf(DateTimeInterface::class, $result);
        static::assertSame('2020-01-01 08:00:00.000000', $result->format('Y-m-d H:i:s.u'));
    }

    public function test_does_not_mutate_a_datetime(): void
    {
        $value = new DateTime('2020-01-01 00:00:00 UTC');

        to_timezone(lit($value), 'Europe/Warsaw')->eval(row([]), flow_context());

        static::assertSame('UTC', $value->getTimezone()->getName());
    }

    public function test_with_children_keeps_the_zone(): void
    {
        $function = to_timezone(ref('at'), 'Europe/Warsaw');

        static::assertSame(
            'datetime<Europe/Warsaw>',
            $function->withChildren($function->children())->returns()->toString(),
        );
    }

    public function test_null_value_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ToTimeZone function requires non-null values');

        to_timezone(ref('at'), 'UTC')->eval(row(['at' => null]), flow_context());
    }
}
