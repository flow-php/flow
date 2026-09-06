<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DOMElement;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\Floe\Encoding\DateTimeEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\ValueEncoder;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;
use function pack;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class ValueEncoderTest extends TestCase
{
    public function test_encoding_datetime_of_custom_class_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe supports only DateTime and DateTimeImmutable, got '
        . CustomDateTime::class);

        (new DateTimeEncoder())->encode(new CustomDateTime('2025-01-01 00:00:00 UTC'));
    }

    public function test_encoding_union_column_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe does not support columns of type "integer|string"');

        (new ValueEncoder())->encoderFor(new UnionDefinition('c', type_union(type_integer(), type_string())));
    }

    public function test_encoding_list_of_mixed_elements_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe does not support values of type "mixed"');

        (new ValueEncoder())->encoderFor(list_schema('c', type_list(type_mixed())));
    }

    public function test_encoding_structure_allowing_extra_values_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe does not support structures that allow extra values');

        (new ValueEncoder())->encoderFor(structure_schema('c', type_structure(['id' => type_integer()], true)));
    }

    public function test_encoding_map_with_unsupported_key_type_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe does not support map keys of type "non_empty_string"');

        (new ValueEncoder())->encoderFor(map_schema('c', type_map(type_non_empty_string(), type_string())));
    }

    public function test_encoding_xml_element_without_owner_document_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to convert DOMElement to XML string');

        ValueEncoder::xmlElementToString(new DOMElement('detached'));
    }

    public function test_integer_encoding_is_little_endian_for_signed_edges(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(int_schema('c'));

        static::assertSame(pack('P', PHP_INT_MIN), $encoder->encode(PHP_INT_MIN));
        static::assertSame(pack('P', PHP_INT_MAX), $encoder->encode(PHP_INT_MAX));
        static::assertSame("\xFB\xFF\xFF\xFF\xFF\xFF\xFF\xFF", $encoder->encode(-5));
        static::assertSame("\x2A\x00\x00\x00\x00\x00\x00\x00", $encoder->encode(42));
    }

    public function test_integer_list_fast_path_is_bulk_packed(): void
    {
        $encoder = (new ValueEncoder())->encoderFor(list_schema('c', type_list(type_integer())));

        static::assertSame(pack('V', 3) . pack('P*', 10, -20, 30), $encoder->encode([10, -20, 30]));
        static::assertSame(pack('V', 0), $encoder->encode([]));
    }

    public function test_stateful_encoders_are_shared_across_calls(): void
    {
        $factory = new ValueEncoder();

        static::assertSame($factory->encoderFor(datetime_schema('c')), $factory->encoderFor(datetime_schema('c')));
        static::assertSame($factory->encoderFor(datetime_schema('c')), $factory->encoderFor(date_schema('c')));
        static::assertSame($factory->encoderFor(uuid_schema('c')), $factory->encoderFor(uuid_schema('c')));
        static::assertSame($factory->encoderFor(json_schema('c')), $factory->encoderFor(json_schema('c')));
    }
}
