<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\AutoCaster;
use PHPUnit\Framework\TestCase;

final class AutoCasterTest extends TestCase
{
    public function test_auto_casting_array_of_ints_and_floats_into_array_of_floats() : void
    {
        self::assertSame(
            [1.0, 2.0, 3.0],
            (new AutoCaster())->cast([1, 2, 3.0])
        );
    }

    public function test_auto_casting_non_timezone_string_returns_unchanged() : void
    {
        self::assertSame(
            'not a timezone',
            (new AutoCaster())->cast('not a timezone')
        );
    }

    public function test_auto_casting_timezone_offset_to_timezone() : void
    {
        self::assertEquals(
            new \DateTimeZone('+05:30'),
            (new AutoCaster())->cast('+05:30')
        );
    }

    public function test_auto_casting_timezone_string_america_to_timezone() : void
    {
        self::assertEquals(
            new \DateTimeZone('America/New_York'),
            (new AutoCaster())->cast('America/New_York')
        );
    }

    public function test_auto_casting_timezone_string_to_timezone() : void
    {
        self::assertEquals(
            new \DateTimeZone('UTC'),
            (new AutoCaster())->cast('UTC')
        );
    }
}
