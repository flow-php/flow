<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\Floe\Decoding\EnumDecoder;
use Flow\Floe\Encoding\EnumEncoder;
use Flow\Floe\Exception\FloeException;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class EnumDecoderTest extends TestCase
{
    public function test_round_trip_restores_enum_case(): void
    {
        $position = 0;

        static::assertSame(BackedStringEnum::one, (new EnumDecoder())->decode(
            (new EnumEncoder())->encode(BackedStringEnum::one),
            $position,
        ));
    }

    public function test_unknown_case_throws(): void
    {
        $class = BackedStringEnum::class;
        $encoded = pack('V', strlen($class)) . $class . pack('V', 4) . 'nope';
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('cannot restore enum case');

        (new EnumDecoder())->decode($encoded, $position);
    }

    public function test_unknown_class_throws(): void
    {
        $encoded = pack('V', 10) . 'NoSuchEnum' . pack('V', 3) . 'one';
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('enum not found');

        (new EnumDecoder())->decode($encoded, $position);
    }
}
