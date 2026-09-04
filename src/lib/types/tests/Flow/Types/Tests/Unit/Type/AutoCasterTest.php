<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use DateTimeZone;
use Flow\Types\Type\AutoCaster;
use PHPUnit\Framework\TestCase;

final class AutoCasterTest extends TestCase
{
    public function test_auto_casting_array_of_ints_and_floats_into_array_of_floats(): void
    {
        static::assertSame([1.0, 2.0, 3.0], (new AutoCaster())->cast([1, 2, 3.0]));
    }

    public function test_auto_casting_non_timezone_string_returns_unchanged(): void
    {
        static::assertSame('not a timezone', (new AutoCaster())->cast('not a timezone'));
    }

    public function test_auto_casting_string_that_stays_string_keeps_original_whitespace(): void
    {
        static::assertSame(' abc ', (new AutoCaster())->cast(' abc '));
    }

    public function test_auto_casting_whitespace_padded_boolean_false(): void
    {
        static::assertFalse((new AutoCaster())->cast(' false '));
    }

    public function test_auto_casting_whitespace_padded_boolean_true(): void
    {
        static::assertTrue((new AutoCaster())->cast(' true '));
    }

    public function test_auto_casting_whitespace_padded_integer(): void
    {
        static::assertSame(123, (new AutoCaster())->cast(' 123 '));
    }

    public function test_auto_casting_timezone_offset_to_timezone(): void
    {
        static::assertEquals(new DateTimeZone('+05:30'), (new AutoCaster())->cast('+05:30'));
    }

    public function test_auto_casting_timezone_string_america_to_timezone(): void
    {
        static::assertEquals(new DateTimeZone('America/New_York'), (new AutoCaster())->cast('America/New_York'));
    }

    public function test_auto_casting_timezone_string_to_timezone(): void
    {
        static::assertEquals(new DateTimeZone('UTC'), (new AutoCaster())->cast('UTC'));
    }

    public function test_digit_strings_cast_to_integer_not_date(): void
    {
        static::assertSame(20240101, (new AutoCaster())->cast('20240101'));
        static::assertSame(19991231, (new AutoCaster())->cast('19991231'));
        static::assertSame(1012024, (new AutoCaster())->cast('1012024'));
    }
}
