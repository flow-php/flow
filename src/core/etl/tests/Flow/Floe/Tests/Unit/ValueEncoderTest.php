<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DateTime;
use DateTimeImmutable;
use DOMElement;
use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\Floe\Encoding\DateTimeEncoder;
use Flow\Floe\Encoding\DynamicEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use Flow\Floe\Tests\Mother\DynamicDecoderMother;
use Flow\Floe\ValueEncoder;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function ord;
use function pack;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class ValueEncoderTest extends TestCase
{
    public function test_encoding_datetime_heads_are_immutable_and_mutable_only(): void
    {
        $encoder = new DateTimeEncoder();

        static::assertSame(
            Format::DATETIME_IMMUTABLE,
            ord($encoder->encode(new DateTimeImmutable('2025-01-01 00:00:00 UTC'))[0]),
        );
        static::assertSame(Format::DATETIME_MUTABLE, ord($encoder->encode(new DateTime('2025-01-01 00:00:00 UTC'))[0]));
    }

    public function test_encoding_datetime_of_custom_class_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe supports only DateTime and DateTimeImmutable, got '
        . CustomDateTime::class);

        (new DateTimeEncoder())->encode(new CustomDateTime('2025-01-01 00:00:00 UTC'));
    }

    public function test_encoding_datetime_of_custom_class_nested_in_dynamic_value_throws(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(type_list(type_mixed()));

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe supports only DateTime and DateTimeImmutable');

        $encoder->encode([['created_at' => new CustomDateTime('2025-01-01 00:00:00 UTC')]]);
    }

    public function test_encoding_dynamic_value_of_unsupported_type_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not support values of type "stdClass" in mixed/union context');

        (new DynamicEncoder())->encode(new stdClass());
    }

    public function test_encoding_list_of_mixed_values_with_object_throws(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(type_list(type_mixed()));

        $this->expectException(FloeException::class);

        $encoder->encode([new stdClass()]);
    }

    public function test_encoding_value_of_unsupported_type_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not support values of type');

        (new ValueEncoder())->encoderFor(type_callable());
    }

    public function test_encoding_xml_element_without_owner_document_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to convert DOMElement to XML string');

        ValueEncoder::xmlElementToString(new DOMElement('detached'));
    }

    public function test_integer_encoding_is_little_endian_for_signed_edges(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(type_integer());

        static::assertSame(pack('P', PHP_INT_MIN), $encoder->encode(PHP_INT_MIN));
        static::assertSame(pack('P', PHP_INT_MAX), $encoder->encode(PHP_INT_MAX));
        static::assertSame("\xFB\xFF\xFF\xFF\xFF\xFF\xFF\xFF", $encoder->encode(-5));
        static::assertSame("\x2A\x00\x00\x00\x00\x00\x00\x00", $encoder->encode(42));
    }

    public function test_integer_list_fast_path_is_bulk_packed(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(type_list(type_integer()));

        static::assertSame(pack('V', 3) . pack('P*', 10, -20, 30), $encoder->encode([10, -20, 30]));
        static::assertSame(pack('V', 0), $encoder->encode([]));
    }

    public function test_stateful_encoders_are_shared_across_calls(): void
    {
        $factory = new ValueEncoder();

        static::assertSame($factory->encoderFor(type_datetime()), $factory->encoderFor(type_datetime()));
        static::assertSame($factory->encoderFor(type_datetime()), $factory->encoderFor(type_date()));
        static::assertSame($factory->encoderFor(type_time_zone()), $factory->encoderFor(type_time_zone()));
        static::assertSame($factory->encoderFor(type_uuid()), $factory->encoderFor(type_uuid()));
        static::assertSame($factory->encoderFor(type_json()), $factory->encoderFor(type_json()));
    }

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
}
