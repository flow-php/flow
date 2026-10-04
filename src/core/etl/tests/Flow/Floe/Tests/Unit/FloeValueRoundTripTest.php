<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

/**
 * Pins every value encoder/decoder pair through a full write/read cycle.
 */
final class FloeValueRoundTripTest extends TestCase
{
    public function test_datetime_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'immutable' => new DateTimeImmutable(
                        '2025-06-15 12:30:45.123456',
                        new DateTimeZone('Europe/Warsaw'),
                    ),
                    'mutable' => new DateTime('2025-06-15 12:30:45.654321', new DateTimeZone('America/New_York')),
                    'offset_timezone' => new DateTimeImmutable('2025-06-15 12:30:45', new DateTimeZone('+02:30')),
                    'before_epoch' => new DateTimeImmutable('1969-07-20 20:17:00 UTC'),
                    'date' => new DateTimeImmutable('2025-06-15 00:00:00', new DateTimeZone('UTC')),
                ]],
                schema(
                    datetime_schema('immutable', zone: 'Europe/Warsaw'),
                    datetime_schema('mutable', zone: 'America/New_York'),
                    datetime_schema('offset_timezone', zone: '+02:30'),
                    datetime_schema('before_epoch'),
                    date_schema('date'),
                ),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_entries_with_null_values(): void
    {
        $rows = array_to_rows(
            [[
                'int' => null,
                'float' => null,
                'bool' => null,
                'str' => null,
                'from_null' => null,
                'list' => null,
                'map' => null,
                'json' => null,
                'uuid' => null,
                'datetime' => null,
                'time' => null,
                'enum' => null,
                'xml' => null,
            ]],
            schema(
                int_schema('int', nullable: true),
                float_schema('float', nullable: true),
                bool_schema('bool', nullable: true),
                str_schema('str', nullable: true),
                null_schema('from_null'),
                list_schema('list', type_list(type_integer()), nullable: true),
                map_schema('map', type_map(type_string(), type_integer()), nullable: true),
                json_schema('json', nullable: true),
                uuid_schema('uuid', nullable: true),
                datetime_schema('datetime', nullable: true),
                time_schema('time', nullable: true),
                enum_schema('enum', BackedStringEnum::class, nullable: true),
                xml_schema('xml', nullable: true),
            ),
        );

        static::assertEquals($rows, FloeStreamReaderContext::roundTrip($rows));
    }

    public function test_enum_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [['backed' => BackedStringEnum::two, 'basic' => BasicEnum::three]],
                schema(enum_schema('backed', BackedStringEnum::class), enum_schema('basic', BasicEnum::class)),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_entries(): void
    {
        $rows = array_to_rows([[
            'html' => type_html()->cast('<!DOCTYPE html><html><head></head><body><p>hello</p></body></html>'),
        ]], schema(html_schema('html')));

        static::assertSame(
            type_string()->cast($rows->column('html')->value(0)),
            type_string()->cast(FloeStreamReaderContext::roundTrip($rows)->column('html')->value(0)),
        );
    }

    public function test_interval_entries(): void
    {
        $negative = new DateInterval('PT5H30M');
        // @mago-ignore analysis:invalid-property-write
        $negative->invert = 1;

        $fractional = new DateInterval('PT1S');
        // @mago-ignore analysis:invalid-property-write
        $fractional->f = 0.123456;

        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'time' => new DateInterval('PT2H30M15S'),
                    'negative' => $negative,
                    'fractional' => $fractional,
                    'days' => new DateInterval('P3D'),
                ]],
                schema(time_schema('time'), time_schema('negative'), time_schema('fractional'), time_schema('days')),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_json_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'list' => type_json()->cast('[1,2,3]'),
                    'object' => type_json()->cast('{"a":1,"b":[true,null]}'),
                    'empty_object' => type_json()->cast('{}'),
                    'empty_list' => type_json()->cast('[]'),
                ]],
                schema(
                    json_schema('list'),
                    json_schema('object'),
                    json_schema('empty_object'),
                    json_schema('empty_list'),
                ),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_list_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'int' => [1, -2, PHP_INT_MAX],
                    'int_empty' => [],
                    'float' => [1.5, -2.25],
                    'float_empty' => [],
                    'string' => ['a', 'b', ''],
                    'bool' => [true, false],
                    'nullable_int' => [1, null, 3],
                    'list_of_lists' => [[1, 2], [3]],
                ]],
                schema(
                    list_schema('int', type_list(type_integer())),
                    list_schema('int_empty', type_list(type_integer())),
                    list_schema('float', type_list(type_float())),
                    list_schema('float_empty', type_list(type_float())),
                    list_schema('string', type_list(type_string())),
                    list_schema('bool', type_list(type_boolean())),
                    list_schema('nullable_int', type_list(type_optional(type_integer()))),
                    list_schema('list_of_lists', type_list(type_list(type_integer()))),
                ),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_map_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [['string_keys' => ['a' => 1, 'b' => 2], 'int_keys' => [10 => 'x', 20 => 'y'], 'empty' => []]],
                schema(
                    map_schema('string_keys', type_map(type_string(), type_integer())),
                    map_schema('int_keys', type_map(type_integer(), type_string())),
                    map_schema('empty', type_map(type_string(), type_integer())),
                ),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_uuid_entries_round_trip_under_format_v2(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'uuid' => type_uuid()->cast('0196aecb-b568-7e57-a381-8ec8d3e4a531'),
                    'uuid_max' => type_uuid()->cast('ffffffff-ffff-ffff-ffff-ffffffffffff'),
                    'after' => 'not desynchronised',
                ]],
                schema(uuid_schema('uuid'), uuid_schema('uuid_max'), str_schema('after')),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_scalar_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'int' => 42,
                    'int_min' => PHP_INT_MIN,
                    'int_max' => PHP_INT_MAX,
                    'float' => 3.14159,
                    'float_negative' => -1.0E-10,
                    'bool_true' => true,
                    'bool_false' => false,
                    'string' => 'hello',
                    'string_empty' => '',
                    'string_binary' => "line\nbreak\x00null\xFFbyte",
                    'string_unicode' => 'zażółć gęślą jaźń 🚀',
                ]],
                schema(
                    int_schema('int'),
                    int_schema('int_min'),
                    int_schema('int_max'),
                    float_schema('float'),
                    float_schema('float_negative'),
                    bool_schema('bool_true'),
                    bool_schema('bool_false'),
                    str_schema('string'),
                    str_schema('string_empty'),
                    str_schema('string_binary'),
                    str_schema('string_unicode'),
                ),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_structure_entries(): void
    {
        $type = type_structure([
            'street' => type_string(),
            'nested' => type_structure(['count' => type_integer(), 'tags' => type_list(type_string())]),
            'optional_zip' => structure_element('optional_zip', type_string(), optional: true),
        ]);

        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'full' => [
                        'street' => 'Main',
                        'nested' => ['count' => 5, 'tags' => ['a']],
                        'optional_zip' => '00-001',
                    ],
                    'without_optional' => ['street' => 'Side', 'nested' => ['count' => 0, 'tags' => []]],
                    'nullable_element' => ['street' => null, 'nested' => ['count' => 1, 'tags' => []]],
                    'with_timezone' => ['tz' => new DateTimeZone('Europe/Warsaw')],
                    'with_null_type' => ['nothing' => null],
                ]],
                schema(
                    structure_schema('full', $type),
                    structure_schema('without_optional', $type),
                    structure_schema('nullable_element', type_structure([
                        'street' => type_optional(type_string()),
                        'nested' => type_structure(['count' => type_integer(), 'tags' => type_list(type_string())]),
                    ])),
                    structure_schema('with_timezone', type_structure([
                        'tz' => type_time_zone(),
                    ])),
                    structure_schema('with_null_type', type_structure(['nothing' => type_null()])),
                ),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_interleaved_structure_entries(): void
    {
        $type = type_structure([
            'z' => type_integer(),
            'a' => structure_element('a', type_string(), optional: true),
            'b' => type_string(),
        ]);

        static::assertEquals(
            $rows = array_to_rows(
                [['full' => ['z' => 1, 'a' => 'present', 'b' => 'x'], 'without_optional' => ['z' => 2, 'b' => 'y']]],
                schema(structure_schema('full', $type), structure_schema('without_optional', $type)),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_round_trip_rewrites_value_key_order_into_schema_order(): void
    {
        $type = type_structure(['a' => type_integer(), 'b' => type_string()]);

        $inOrder = array_to_rows([['s' => ['a' => 1, 'b' => 'x']]], schema(structure_schema('s', $type)));

        static::assertSame($inOrder->values(0), FloeStreamReaderContext::roundTrip($inOrder)->values(0));

        $outOfOrder = array_to_rows([['s' => ['b' => 'x', 'a' => 1]]], schema(structure_schema('s', $type)));
        $decoded = FloeStreamReaderContext::roundTrip($outOfOrder);

        static::assertSame(['a' => 1, 'b' => 'x'], $decoded->column('s')->value(0));
    }

    public function test_uuid_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows([[
                'uuid' => type_uuid()->cast('0196aecb-b568-7e57-a381-8ec8d3e4a531'),
            ]], schema(uuid_schema('uuid'))),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_xml_entries(): void
    {
        static::assertEquals(
            $rows = array_to_rows(
                [[
                    'xml' => type_xml()->cast('<root attr="1"><child>text &amp; entity</child></root>'),
                    'element' => type_xml_element()->cast('<item id="5">value</item>'),
                ]],
                schema(xml_schema('xml'), xml_element_schema('element')),
            ),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }
}
