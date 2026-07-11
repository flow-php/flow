<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\Floe\Encoding\DynamicEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use Flow\Floe\Tests\Mother\DynamicDecoderMother;
use PHPUnit\Framework\TestCase;
use stdClass;

use function chr;
use function pack;

use const PHP_INT_MIN;

final class DynamicEncoderTest extends TestCase
{
    public function test_round_trip_of_dynamic_values(): void
    {
        $encoder = new DynamicEncoder();
        $decoder = DynamicDecoderMother::create();

        $values = [
            'null' => null,
            'int' => 42,
            'negative_int' => PHP_INT_MIN,
            'float' => 3.5,
            'bool' => true,
            'string' => 'text',
            'array' => ['a' => 1, 0 => 'zero', 'nested' => ['x' => [1, 2]], -7 => 'negative key'],
        ];

        foreach ($values as $label => $value) {
            $position = 0;

            static::assertSame($value, $decoder->decode($encoder->encode($value), $position), $label);
        }
    }

    public function test_encodes_tagged_scalars(): void
    {
        $encoder = new DynamicEncoder();

        static::assertSame(chr(Format::TAG_NULL), $encoder->encode(null));
        static::assertSame(chr(Format::TAG_INTEGER) . pack('P', 42), $encoder->encode(42));
        static::assertSame(chr(Format::TAG_BOOLEAN) . "\x01", $encoder->encode(true));
        static::assertSame(chr(Format::TAG_STRING) . pack('V', 4) . 'text', $encoder->encode('text'));
    }

    public function test_unsupported_object_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not support values of type "stdClass" in mixed/union context');

        (new DynamicEncoder())->encode(new stdClass());
    }

    public function test_custom_datetime_nested_in_array_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe supports only DateTime and DateTimeImmutable');

        (new DynamicEncoder())->encode(['created_at' => new CustomDateTime('2025-01-01 00:00:00 UTC')]);
    }
}
