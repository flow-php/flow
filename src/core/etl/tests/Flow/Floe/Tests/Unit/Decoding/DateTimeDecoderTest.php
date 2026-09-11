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

use function strlen;

final class DateTimeDecoderTest extends TestCase
{
    public function test_round_trip_preserves_timezone_and_microseconds(): void
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

    public function test_a_mutable_datetime_decodes_as_immutable(): void
    {
        $value = new DateTime('2025-01-01 00:00:00 UTC');
        $position = 0;

        $decoded = DateTimeDecoderMother::create()->decode((new DateTimeEncoder())->encode($value), $position);

        static::assertInstanceOf(DateTimeImmutable::class, $decoded);
        static::assertSame($value->format('Y-m-d H:i:s.u e'), $decoded->format('Y-m-d H:i:s.u e'));
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

    public function test_a_truncated_datetime_value_throws(): void
    {
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe found a truncated datetime value');

        DateTimeDecoderMother::create()->decode("\xEE", $position);
    }
}
