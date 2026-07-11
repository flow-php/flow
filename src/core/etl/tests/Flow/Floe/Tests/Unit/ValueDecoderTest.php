<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Tests\Mother\DateTimeDecoderMother;
use Flow\Floe\Tests\Mother\DynamicDecoderMother;
use Flow\Floe\ValueDecoder;
use Flow\Floe\ValueEncoder;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

use function chr;
use function class_exists;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function pack;
use function strlen;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class ValueDecoderTest extends TestCase
{
    public function test_decoding_datetime_with_legacy_class_flag_throws(): void
    {
        $encoded = chr(0x02) . pack('V', 11) . 'NoSuchClass' . pack('P', 0) . pack('V', 0) . pack('V', 3) . 'UTC';
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe found unknown datetime flag 0x02');

        DateTimeDecoderMother::create()->decode($encoded, $position);
    }

    public function test_decoding_datetime_with_unknown_flag_throws(): void
    {
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown datetime flag 0xEE');

        DateTimeDecoderMother::create()->decode("\xEE", $position);
    }

    public function test_decoding_dynamic_value_with_unknown_tag_throws(): void
    {
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown dynamic value tag');

        DynamicDecoderMother::create()->decode("\xEE", $position);
    }

    public function test_decoding_enum_of_unknown_case_throws(): void
    {
        $class = BackedStringEnum::class;
        $encoded = pack('V', strlen($class)) . $class . pack('V', 4) . 'nope';
        $decoder = (new ValueDecoder())->decoderFor(type_enum(BackedStringEnum::class));
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('cannot restore enum case');

        $decoder->decode($encoded, $position);
    }

    public function test_decoding_enum_of_unknown_class_throws(): void
    {
        $encoded = pack('V', 10) . 'NoSuchEnum' . pack('V', 3) . 'one';
        $decoder = (new ValueDecoder())->decoderFor(type_enum(BackedStringEnum::class));
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('enum not found');

        $decoder->decode($encoded, $position);
    }

    public function test_decoding_html_document_below_php84_throws(): void
    {
        if (class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('requires PHP < 8.4');
        }

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires PHP 8.4+');

        ValueDecoder::htmlDocumentFromString('<p>x</p>');
    }

    public function test_decoding_html_element_below_php84_throws(): void
    {
        if (class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('requires PHP < 8.4');
        }

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires PHP 8.4+');

        ValueDecoder::htmlElementFromString('<p>x</p>');
    }

    public function test_decoding_structure_with_unknown_element_flag_throws(): void
    {
        $decoder = (new ValueDecoder())->decoderFor(type_structure(['name' => type_string()]));
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown structure element flag');

        $decoder->decode("\xEE", $position);
    }

    public function test_decoding_invalid_xml_throws(): void
    {
        $encoded = pack('V', 9) . 'not < xml';
        $decoder = (new ValueDecoder())->decoderFor(type_xml());
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to restore DOMDocument');

        $decoder->decode($encoded, $position);
    }

    public function test_decoding_value_of_unsupported_type_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not support values of type');

        (new ValueDecoder())->decoderFor(type_callable());
    }

    public function test_round_trip_of_datetime_preserves_timezone_and_microseconds(): void
    {
        $value = new DateTimeImmutable('2025-06-15 12:30:45.987654', new DateTimeZone('Australia/Eucla'));
        $encoded = (new ValueEncoder())
            ->encoderFor(type_datetime())
            ->encode($value);
        $position = 0;

        // @mago-ignore analysis:mixed-assignment
        $decoded = (new ValueDecoder())
            ->decoderFor(type_datetime())
            ->decode($encoded, $position);

        static::assertInstanceOf(DateTimeImmutable::class, $decoded);
        static::assertEquals($value, $decoded);
        static::assertSame('Australia/Eucla', $decoded->getTimezone()->getName());
        static::assertSame('987654', $decoded->format('u'));
        static::assertSame(strlen($encoded), $position);
    }

    public function test_round_trip_of_datetime_before_epoch(): void
    {
        $value = new DateTimeImmutable('1969-07-20 20:17:00.500000 UTC');
        $encoded = (new ValueEncoder())
            ->encoderFor(type_datetime())
            ->encode($value);
        $position = 0;

        static::assertEquals($value, (new ValueDecoder())
            ->decoderFor(type_datetime())
            ->decode($encoded, $position));
    }

    public function test_round_trip_of_integer_signed_edges(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(type_integer());
        $decoder = (new ValueDecoder())->decoderFor(type_integer());

        foreach ([PHP_INT_MIN, PHP_INT_MAX, -1, 0, 42] as $value) {
            $position = 0;

            static::assertSame($value, $decoder->decode($encoder->encode($value), $position));
        }
    }

    public function test_round_trip_of_interval_preserves_all_fields(): void
    {
        $value = new DateInterval('P1DT2H3M4S');
        // @mago-ignore analysis:invalid-property-write
        $value->invert = 1;
        // @mago-ignore analysis:invalid-property-write
        $value->f = 0.5;

        $encoded = (new ValueEncoder())
            ->encoderFor(type_time())
            ->encode($value);
        $position = 0;

        // @mago-ignore analysis:mixed-assignment
        $decoded = (new ValueDecoder())
            ->decoderFor(type_time())
            ->decode($encoded, $position);

        static::assertInstanceOf(DateInterval::class, $decoded);
        static::assertSame([1, 2, 3, 4, 0.5, 1], [
            $decoded->d,
            $decoded->h,
            $decoded->i,
            $decoded->s,
            $decoded->f,
            $decoded->invert,
        ]);
    }

    public function test_restoring_xml_element_from_string(): void
    {
        static::assertSame('item', ValueDecoder::xmlElementFromString('<item id="5">value</item>')->tagName);
    }

    public function test_round_trip_of_uuid_skips_validation(): void
    {
        $uuid = new Uuid('0196aecb-b568-7e57-a381-8ec8d3e4a531');
        $encoded = (new ValueEncoder())
            ->encoderFor(type_uuid())
            ->encode($uuid);
        $position = 0;

        // @mago-ignore analysis:mixed-assignment
        $decoded = (new ValueDecoder())
            ->decoderFor(type_uuid())
            ->decode($encoded, $position);

        static::assertInstanceOf(Uuid::class, $decoded);
        static::assertSame($uuid->toString(), $decoded->toString());
    }
}
