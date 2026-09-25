<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\now;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_datetime;

final class NowTest extends FlowTestCase
{
    public function test_constant_zone_is_the_column_zone(): void
    {
        static::assertSame('datetime<Europe/Warsaw>', now(new DateTimeZone('Europe/Warsaw'))->returns()->toString());
    }

    public function test_function_zone_returns_utc(): void
    {
        static::assertSame('datetime', now(ref('tz'))->returns()->toString());
    }

    public function test_value_is_in_the_zone(): void
    {
        static::assertSame(
            'Europe/Warsaw',
            type_datetime()
                ->assert(now(new DateTimeZone('Europe/Warsaw'))->eval(row([]), flow_context()))
                ->getTimezone()
                ->getName(),
        );
    }

    public function test_function_zone_value_is_in_the_declared_zone(): void
    {
        static::assertSame(
            'UTC',
            type_datetime()
                ->assert(now(ref('tz'))->eval(row(['tz' => new DateTimeZone('Europe/Warsaw')]), flow_context()))
                ->getTimezone()
                ->getName(),
        );
    }

    public function test_function_zone_null_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Now function requires valid DateTimeZone');

        now(ref('tz'))->eval(row(['tz' => null]), flow_context());
    }

    public function test_utc_alias_zone_lands_in_utc(): void
    {
        static::assertSame(
            'UTC',
            type_datetime()
                ->assert(now(new DateTimeZone('GMT'))->eval(row([]), flow_context()))
                ->getTimezone()
                ->getName(),
        );
    }
}
