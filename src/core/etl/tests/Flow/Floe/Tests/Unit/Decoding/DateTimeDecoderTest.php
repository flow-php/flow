<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Floe\Encoding\DateTimeEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Tests\Mother\DateTimeDecoderMother;
use PHPUnit\Framework\TestCase;

use function chr;
use function pack;
use function strlen;

final class DateTimeDecoderTest extends TestCase
{
    public function test_round_trip_preserves_class_timezone_and_microseconds(): void
    {
        $value = new DateTimeImmutable('2025-06-15 12:30:45.987654', new DateTimeZone('Australia/Eucla'));
        $encoded = (new DateTimeEncoder())->encode($value);
        $position = 0;

        $decoded = DateTimeDecoderMother::create()->decode($encoded, $position);

        static::assertInstanceOf(DateTimeImmutable::class, $decoded);
        static::assertEquals($value, $decoded);
        static::assertSame('Australia/Eucla', $decoded->getTimezone()->getName());
        static::assertSame('987654', $decoded->format('u'));
        static::assertSame(strlen($encoded), $position);
    }

    public function test_round_trip_of_mutable_datetime(): void
    {
        $value = new DateTime('2025-01-01 00:00:00 UTC');
        $position = 0;

        static::assertInstanceOf(DateTime::class, DateTimeDecoderMother::create()->decode(
            (new DateTimeEncoder())->encode($value),
            $position,
        ));
    }

    public function test_round_trip_before_epoch(): void
    {
        $value = new DateTimeImmutable('1969-07-20 20:17:00.500000 UTC');
        $position = 0;

        static::assertEquals($value, DateTimeDecoderMother::create()->decode(
            (new DateTimeEncoder())->encode($value),
            $position,
        ));
    }

    public function test_unknown_class_flag_throws(): void
    {
        $encoded = chr(0x02) . pack('V', 11) . 'NoSuchClass' . pack('P', 0) . pack('V', 0) . pack('V', 3) . 'UTC';
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe found unknown datetime flag 0x02');

        DateTimeDecoderMother::create()->decode($encoded, $position);
    }
}
