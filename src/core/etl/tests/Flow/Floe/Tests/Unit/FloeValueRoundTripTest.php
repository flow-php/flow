<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Row;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\html_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\time_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_element_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;

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
            $rows = rows(row(
                datetime_entry(
                    'immutable',
                    new DateTimeImmutable('2025-06-15 12:30:45.123456', new DateTimeZone('Europe/Warsaw')),
                ),
                new Row\Entry\DateTimeEntry(
                    'mutable',
                    new DateTime('2025-06-15 12:30:45.654321', new DateTimeZone('America/New_York')),
                ),
                datetime_entry(
                    'offset_timezone',
                    new DateTimeImmutable('2025-06-15 12:30:45', new DateTimeZone('+02:30')),
                ),
                datetime_entry('before_epoch', new DateTimeImmutable('1969-07-20 20:17:00 UTC')),
                date_entry('date', new DateTimeImmutable('2025-06-15', new DateTimeZone('Europe/Warsaw'))),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_entries_with_null_values(): void
    {
        $rows = rows(row(
            int_entry('int', null),
            float_entry('float', null),
            bool_entry('bool', null),
            str_entry('str', null),
            null_entry('from_null'),
            list_entry('list', null, type_list(type_integer())),
            map_entry('map', null, type_map(type_string(), type_integer())),
            json_entry('json', null),
            uuid_entry('uuid', null),
            datetime_entry('datetime', null),
            time_entry('time', null),
            enum_entry('enum', null),
            xml_entry('xml', null),
        ));

        static::assertEquals($rows, FloeStreamReaderContext::roundTrip($rows));
    }

    public function test_enum_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(enum_entry('backed', BackedStringEnum::two), enum_entry('basic', BasicEnum::three))),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_entries(): void
    {
        $rows = rows(row(html_entry('html', '<html><body><p>hello</p></body></html>')));

        static::assertSame(
            $rows->first()->entries()['html']->toString(),
            FloeStreamReaderContext::roundTrip($rows)->first()->entries()['html']->toString(),
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
            $rows = rows(row(
                time_entry('time', new DateInterval('PT2H30M15S')),
                time_entry('negative', $negative),
                time_entry('fractional', $fractional),
                time_entry('days', new DateInterval('P3D')),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_json_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(
                json_entry('list', '[1,2,3]'),
                json_object_entry('object', '{"a":1,"b":[true,null]}'),
                json_object_entry('empty_object', '{}'),
                json_entry('empty_list', '[]'),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_list_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(
                list_entry('int', [1, -2, PHP_INT_MAX], type_list(type_integer())),
                list_entry('int_empty', [], type_list(type_integer())),
                list_entry('float', [1.5, -2.25], type_list(type_float())),
                list_entry('float_empty', [], type_list(type_float())),
                list_entry('string', ['a', 'b', ''], type_list(type_string())),
                list_entry('bool', [true, false], type_list(type_boolean())),
                list_entry('nullable_int', [1, null, 3], type_list(type_optional(type_integer()))),
                list_entry('list_of_lists', [[1, 2], [3]], type_list(type_list(type_integer()))),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_map_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(
                map_entry('string_keys', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer())),
                map_entry('int_keys', [10 => 'x', 20 => 'y'], type_map(type_integer(), type_string())),
                map_entry('empty', [], type_map(type_string(), type_integer())),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_uuid_entries_round_trip_under_format_v2(): void
    {
        static::assertEquals(
            $rows = rows(row(
                uuid_entry('uuid', '0196aecb-b568-7e57-a381-8ec8d3e4a531'),
                uuid_entry('uuid_max', 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
                str_entry('after', 'not desynchronised'),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_scalar_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(
                int_entry('int', 42),
                int_entry('int_min', PHP_INT_MIN),
                int_entry('int_max', PHP_INT_MAX),
                float_entry('float', 3.14159),
                float_entry('float_negative', -1.0E-10),
                bool_entry('bool_true', true),
                bool_entry('bool_false', false),
                str_entry('string', 'hello'),
                str_entry('string_empty', ''),
                str_entry('string_binary', "line\nbreak\x00null\xFFbyte"),
                str_entry('string_unicode', 'zażółć gęślą jaźń 🚀'),
            )),
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
            $rows = rows(row(
                structure_entry(
                    'full',
                    ['street' => 'Main', 'nested' => ['count' => 5, 'tags' => ['a']], 'optional_zip' => '00-001'],
                    $type,
                ),
                structure_entry(
                    'without_optional',
                    ['street' => 'Side', 'nested' => ['count' => 0, 'tags' => []]],
                    $type,
                ),
                // @mago-ignore analysis:possibly-invalid-argument
                structure_entry(
                    'nullable_element',
                    ['street' => null, 'nested' => ['count' => 1, 'tags' => []]],
                    type_structure([
                        'street' => type_optional(type_string()),
                        'nested' => type_structure(['count' => type_integer(), 'tags' => type_list(type_string())]),
                    ]),
                ),
                structure_entry('with_timezone', ['tz' => new DateTimeZone('Europe/Warsaw')], type_structure([
                    'tz' => type_time_zone(),
                ])),
                structure_entry('with_null_type', ['nothing' => null], type_structure(['nothing' => type_null()])),
            )),
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
            $rows = rows(row(
                structure_entry('full', ['z' => 1, 'a' => 'present', 'b' => 'x'], $type),
                structure_entry('without_optional', ['z' => 2, 'b' => 'y'], $type),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_round_trip_rewrites_value_key_order_into_schema_order(): void
    {
        $type = type_structure(['a' => type_integer(), 'b' => type_string()]);

        $inOrder = rows(row(structure_entry('s', ['a' => 1, 'b' => 'x'], $type)));

        static::assertTrue(FloeStreamReaderContext::roundTrip($inOrder)->first()->isEqual($inOrder->first()));

        $outOfOrder = rows(row(structure_entry('s', ['b' => 'x', 'a' => 1], $type)));
        $decoded = FloeStreamReaderContext::roundTrip($outOfOrder)->first();

        static::assertSame(['a' => 1, 'b' => 'x'], $decoded->get('s')->value());
        static::assertTrue($decoded->isEqual($outOfOrder->first()));
    }

    public function test_structure_allowing_extra_values_is_rejected(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('does not support structures that allow extra values');

        FloeStreamReaderContext::roundTrip(rows(row(structure_entry(
            'with_extra',
            ['id' => 1, 'custom' => 'x'],
            type_structure(['id' => type_integer()], true),
        ))));
    }

    public function test_uuid_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(uuid_entry('uuid', '0196aecb-b568-7e57-a381-8ec8d3e4a531'))),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }

    public function test_xml_entries(): void
    {
        static::assertEquals(
            $rows = rows(row(
                xml_entry('xml', '<root attr="1"><child>text &amp; entity</child></root>'),
                xml_element_entry('element', '<item id="5">value</item>'),
            )),
            FloeStreamReaderContext::roundTrip($rows),
        );
    }
}
