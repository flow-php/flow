<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use DateTime;
use DateTimeImmutable;
use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\Floe\Encoding\DateTimeEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use PHPUnit\Framework\TestCase;

use function ord;
use function pack;

final class DateTimeEncoderTest extends TestCase
{
    public function test_heads_are_immutable_and_mutable_only(): void
    {
        $encoder = new DateTimeEncoder();

        static::assertSame(
            Format::DATETIME_IMMUTABLE,
            ord($encoder->encode(new DateTimeImmutable('2025-01-01 00:00:00 UTC'))[0]),
        );
        static::assertSame(Format::DATETIME_MUTABLE, ord($encoder->encode(new DateTime('2025-01-01 00:00:00 UTC'))[0]));
    }

    public function test_encodes_timestamp_microseconds_and_timezone(): void
    {
        $value = new DateTimeImmutable('2025-01-01 00:00:00.123456 UTC');

        static::assertSame(
            "\x00" . pack('P', $value->getTimestamp()) . pack('V', 123456) . pack('V', 3) . 'UTC',
            (new DateTimeEncoder())->encode($value),
        );
    }

    public function test_custom_datetime_class_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe supports only DateTime and DateTimeImmutable, got '
        . CustomDateTime::class);

        (new DateTimeEncoder())->encode(new CustomDateTime('2025-01-01 00:00:00 UTC'));
    }
}
