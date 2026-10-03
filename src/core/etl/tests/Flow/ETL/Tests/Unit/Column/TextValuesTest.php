<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\Types\Type;
use Flow\Types\Value\Uuid;
use Generator;
use JsonException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function hex2bin;
use function ini_get;
use function ini_set;
use function str_split;

final class TextValuesTest extends TestCase
{
    public const string UUID_BYTES = "\x00\x00\x00\x01\x00\x02\x40\x03\x80\x00\x00\x00\x00\x00\x00\xff";

    public const string UUID_TEXT = '00000001-0002-4003-8000-0000000000ff';

    /**
     * @return Generator<string, array{Type<mixed>, list<mixed>, list<mixed>}>
     */
    public static function plain_values(): Generator
    {
        yield 'integer' => [type_integer(), [1, -2], [1, -2]];
        yield 'float' => [type_float(), [1.5, -0.0], [1.5, -0.0]];
        yield 'boolean' => [type_boolean(), [true, false], [true, false]];
        yield 'string' => [type_string(), ['a', ''], ['a', '']];
        yield 'optional string' => [type_optional(type_string()), ['a', null], ['a', null]];
        yield 'datetime' => [type_datetime(), [1_767_323_045_123_456, null], ['2026-01-02T03:04:05+00:00', null]];
        yield 'optional datetime' => [type_optional(type_datetime()), [0, null], ['1970-01-01T00:00:00+00:00', null]];
        yield 'zoned datetime' => [
            type_datetime('Europe/Warsaw'),
            [1_767_323_045_000_000],
            ['2026-01-02T04:04:05+01:00'],
        ];
        yield 'date' => [type_date(), [20_455, null], ['2026-01-02', null]];
        yield 'time' => [type_time(), [3_600_000_000, null], [3_600_000_000, null]];
        yield 'uuid' => [type_uuid(), [self::UUID_BYTES, null], [self::UUID_TEXT, null]];
        yield 'enum' => [type_enum(BasicEnum::class), ['one', null], ['one', null]];
        yield 'time_zone' => [type_time_zone(), ['Europe/Warsaw', null], ['Europe/Warsaw', null]];
        yield 'json' => [
            type_json(),
            ['{"a":1}', '[1,2]', '5', null],
            [(object) ['a' => 1], [1, 2], 5, null],
        ];
        yield 'xml' => [
            type_xml(),
            ['<?xml version="1.0"?>' . "\n" . '<a b="1"><c/></a>' . "\n", null],
            ['<a b="1"><c/></a>', null],
        ];
        yield 'xml node documents' => [
            type_xml(),
            [
                XmlDocumentPhysical::DECLARATION . '<row a="żółć"><!-- c --></row>' . "\n",
                XmlDocumentPhysical::DECLARATION . "<a/>\n<!-- after the root -->\n",
                null,
            ],
            ['<row a="żółć"><!-- c --></row>', '<a/>', null],
        ];
        yield 'xml_element' => [type_xml_element(), ['<a b="1"><c/></a>', null], ['<a b="1"><c></c></a>', null]];
        yield 'html' => [type_html(), ['<html><body>x</body></html>', null], ['<html><body>x</body></html>', null]];
        yield 'html_element' => [type_html_element(), ['<p>x</p>', null], ['<p>x</p>', null]];
        yield 'null' => [type_null(), [null, null], [null, null]];
        yield 'list<datetime>' => [
            type_list(type_datetime()),
            [[1_767_323_045_000_000, null], null, []],
            [['2026-01-02T03:04:05+00:00', null], null, []],
        ];
        yield 'list<list<date>>' => [
            type_list(type_list(type_date())),
            [[[20_455], [], null], [[0, 1]]],
            [[['2026-01-02'], [], null], [['1970-01-01', '1970-01-02']]],
        ];
        yield 'map<string, uuid>' => [
            type_map(type_string(), type_uuid()),
            [['a' => self::UUID_BYTES, 'b' => null], null],
            [(object) ['a' => self::UUID_TEXT, 'b' => null], null],
        ];
        yield 'structure with an absent optional element' => [
            type_structure(['day' => type_date(), 'id' => structure_element('id', type_optional(type_uuid()), true)]),
            [['day' => 0, 'id' => self::UUID_BYTES], ['day' => 1], ['day' => 2, 'id' => null], null],
            [
                (object) ['day' => '1970-01-01', 'id' => self::UUID_TEXT],
                (object) ['day' => '1970-01-02'],
                (object) ['day' => '1970-01-03', 'id' => null],
                null,
            ],
        ];
        yield 'structure of a list and a map' => [
            type_structure(['l' => type_list(type_date()), 'm' => type_map(type_integer(), type_json())]),
            [['l' => [0], 'm' => [5 => '{}']], ['l' => [], 'm' => []]],
            [
                (object) ['l' => ['1970-01-01'], 'm' => (object) [5 => (object) []]],
                (object) ['l' => [], 'm' => (object) []],
            ],
        ];
    }

    /**
     * @return Generator<string, array{Type<mixed>, list<mixed>, string}>
     */
    public static function shapes(): Generator
    {
        yield 'a map keyed 0, 1' => [
            type_map(type_integer(), type_string()),
            [[0 => 'a', 1 => 'b']],
            '[{"0":"a","1":"b"}]',
        ];
        yield 'a map keyed 5, 7' => [
            type_map(type_integer(), type_string()),
            [[5 => 'a', 7 => 'b']],
            '[{"5":"a","7":"b"}]',
        ];
        yield 'a map keyed by numeric strings' => [
            type_map(type_string(), type_string()),
            [['0' => 'a', '1' => 'b']],
            '[{"0":"a","1":"b"}]',
        ];
        yield 'an empty map' => [type_map(type_string(), type_integer()), [[]], '[{}]'];
        yield 'a structure' => [type_structure(['a' => type_integer()]), [['a' => 1]], '[{"a":1}]'];
        yield 'a structure keyed 0, 1' => [
            type_structure([type_integer(), type_integer()]),
            [[0 => 1, 1 => 2]],
            '[{"0":1,"1":2}]',
        ];
        yield 'a list' => [type_list(type_integer()), [[1, 2], []], '[[1,2],[]]'];
        yield 'json objects and lists' => [type_json(), ['{}', '[]', '{"a":{}}'], '[{},[],{"a":{}}]'];
    }

    /**
     * @return Generator<string, array{Type<mixed>, list<mixed>, list<?string>}>
     */
    public static function scalar_texts(): Generator
    {
        yield 'integer' => [type_optional(type_integer()), [1, -2, null], ['1', '-2', null]];
        yield 'float' => [
            type_optional(type_float()),
            [
                1.0,
                -0.0,
                1.0e25,
                1.0e-7,
                PHP_FLOAT_MAX,
                PHP_FLOAT_MIN,
                5.0e-324,
                0.1 + 0.2,
                1 / 3,
                123_456_789_012_345.67,
                null,
            ],
            [
                '1.0',
                '-0.0',
                '1.0e+25',
                '1.0e-7',
                '1.7976931348623157e+308',
                '2.2250738585072014e-308',
                '5.0e-324',
                '0.30000000000000004',
                '0.3333333333333333',
                '123456789012345.67',
                null,
            ],
        ];
        yield 'a float that is not finite' => [type_float(), [NAN, INF, -INF], ['NAN', 'INF', '-INF']];
        yield 'boolean' => [type_optional(type_boolean()), [true, false, null], ['true', 'false', null]];
        yield 'string' => [type_optional(type_string()), ['Norbert', '', null], ['Norbert', '', null]];
        yield 'datetime' => [
            type_optional(type_datetime()),
            [1_767_323_045_123_456, null],
            ['2026-01-02T03:04:05+00:00', null],
        ];
        yield 'zoned datetime' => [
            type_datetime('Europe/Warsaw'),
            [1_767_323_045_000_000],
            ['2026-01-02T04:04:05+01:00'],
        ];
        yield 'date' => [type_optional(type_date()), [20_455, null], ['2026-01-02', null]];
        yield 'time' => [type_optional(type_time()), [3_600_000_000, null], ['3600000000', null]];
        yield 'uuid' => [type_optional(type_uuid()), [self::UUID_BYTES, null], [self::UUID_TEXT, null]];
        yield 'enum' => [type_optional(type_enum(BasicEnum::class)), ['two', null], ['two', null]];
        yield 'time_zone' => [type_optional(type_time_zone()), ['Europe/Warsaw', null], ['Europe/Warsaw', null]];
        yield 'json keeps its stored text' => [
            type_optional(type_json()),
            ['{"a": 1}', '[]', '{}', null],
            ['{"a": 1}', '[]', '{}', null],
        ];
        yield 'xml' => [
            type_optional(type_xml()),
            ['<?xml version="1.0"?>' . "\n" . '<root><a>1</a></root>' . "\n", null],
            ['<root><a>1</a></root>', null],
        ];
        yield 'xml_element' => [type_xml_element(), ['<a b="1"><c/></a>'], ['<a b="1"><c></c></a>']];
        yield 'html' => [
            type_html(),
            ['<!DOCTYPE html><html><body>x</body></html>'],
            ['<!DOCTYPE html><html><body>x</body></html>'],
        ];
        yield 'html_element' => [type_html_element(), ['<p>x</p>'], ['<p>x</p>']];
        yield 'null' => [type_null(), [null, null], [null, null]];
        yield 'list<integer>' => [
            type_optional(type_list(type_integer())),
            [[1, 2, 3], [], null],
            ['[1,2,3]', '[]', null],
        ];
        yield 'list<datetime>' => [
            type_list(type_datetime()),
            [[1_767_323_045_000_000]],
            ['["2026-01-02T03:04:05+00:00"]'],
        ];
        yield 'list<date>' => [type_list(type_date()), [[20_455]], ['["2026-01-02"]']];
        yield 'list<uuid>' => [type_list(type_uuid()), [[self::UUID_BYTES]], ['["' . self::UUID_TEXT . '"]']];
        yield 'list<time>' => [type_list(type_time()), [[3_600_000_000]], ['[3600000000]']];
        yield 'list<enum>' => [type_list(type_enum(BasicEnum::class)), [['one']], ['["one"]']];
        yield 'list<float> keeps the zero fraction' => [
            type_list(type_float()),
            [[1.0, 0.1 + 0.2, 1.0e25]],
            ['[1.0,0.30000000000000004,1.0e+25]'],
        ];
        yield 'list<json> holds the decoded documents' => [
            type_list(type_json()),
            [['{"a": {}}', '[]']],
            ['[{"a":{}},[]]'],
        ];
        yield 'a map keyed 0, 1 and an empty map' => [
            type_map(type_integer(), type_string()),
            [[0 => 'a', 1 => 'b'], []],
            ['{"0":"a","1":"b"}', '{}'],
        ];
        yield 'structure' => [
            type_structure(['id' => type_integer(), 'on' => type_date()]),
            [['id' => 1, 'on' => 20_455]],
            ['{"id":1,"on":"2026-01-02"}'],
        ];
    }

    /**
     * @return Generator<string, array{Type<mixed>, bool}>
     */
    public static function kept_physicals(): Generator
    {
        yield 'integer' => [type_integer(), true];
        yield 'optional float' => [type_optional(type_float()), true];
        yield 'boolean' => [type_boolean(), true];
        yield 'string' => [type_string(), true];
        yield 'time' => [type_time(), true];
        yield 'enum' => [type_enum(BasicEnum::class), true];
        yield 'time_zone' => [type_time_zone(), true];
        yield 'html' => [type_html(), true];
        yield 'null' => [type_null(), true];
        yield 'list<integer>' => [type_list(type_integer()), true];
        yield 'list<list<?string>>' => [type_list(type_list(type_optional(type_string()))), true];
        yield 'datetime' => [type_datetime(), false];
        yield 'optional date' => [type_optional(type_date()), false];
        yield 'uuid' => [type_uuid(), false];
        yield 'json' => [type_json(), false];
        yield 'xml' => [type_xml(), false];
        yield 'xml_element' => [type_xml_element(), false];
        yield 'list<date>' => [type_list(type_date()), false];
        yield 'list<list<uuid>>' => [type_list(type_list(type_uuid())), false];
        yield 'map<string, integer>' => [type_map(type_string(), type_integer()), false];
        yield 'structure{a: integer}' => [type_structure(['a' => type_integer()]), false];
        yield 'list<map<string, integer>>' => [type_list(type_map(type_string(), type_integer())), false];
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $physicals
     * @param list<?string> $expected
     */
    #[DataProvider('scalar_texts')]
    public function test_texts(Type $type, array $physicals, array $expected): void
    {
        static::assertSame($expected, (new TextValues())->texts($type, $physicals));
    }

    public function test_texts_use_the_configured_formats_at_every_depth(): void
    {
        $text = new TextValues('d/m/Y H:i', 'Y/m/d');

        static::assertSame(['02/01/2026 03:04'], $text->texts(type_datetime(), [1_767_323_045_000_000]));
        static::assertSame(['2026/01/02'], $text->texts(type_date(), [20_455]));
        static::assertSame(
            ['{"at":["02\/01\/2026 03:04"],"on":"2026\/01\/02"}'],
            $text->texts(type_structure(['at' => type_list(type_datetime()), 'on' => type_date()]), [[
                'at' => [1_767_323_045_000_000],
                'on' => 20_455,
            ]]),
        );
    }

    public function test_texts_refuse_a_float_that_is_not_finite_inside_a_nested_value(): void
    {
        $this->expectException(JsonException::class);
        $this->expectExceptionMessage('Inf and NaN cannot be JSON encoded');

        (new TextValues())->texts(type_list(type_float()), [[1.5, NAN]]);
    }

    #[TestWith(['serialize_precision', '17'])]
    #[TestWith(['precision', '5'])]
    public function test_nested_float_texts_never_depend_on_an_ini(string $ini, string $value): void
    {
        $previous = (string) ini_get($ini);
        ini_set($ini, $value);

        try {
            static::assertSame(['[0.1,1.0e+25]'], (new TextValues())->texts(type_list(type_float()), [[0.1, 1.0e25]]));
            static::assertSame($value, ini_get($ini));
        } finally {
            ini_set($ini, $previous);
        }
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('kept_physicals')]
    public function test_keeps_physical(Type $type, bool $kept): void
    {
        static::assertSame($kept, (new TextValues())->keepsPhysical($type));
    }

    public function test_of_returns_the_physicals_of_a_list_whose_elements_are_kept(): void
    {
        $physicals = [[1, null, 3], null, [], [[4]]];

        static::assertSame($physicals, (new TextValues())->of(type_list(type_optional(type_integer())), $physicals));
        static::assertSame(
            [[['a', null], []], null],
            (new TextValues())->of(type_list(type_list(type_optional(type_string()))), [[['a', null], []], null]),
        );
    }

    public function test_of_renders_only_the_structure_elements_that_differ_from_their_physical(): void
    {
        static::assertEquals(
            [
                (object) ['id' => 1, 'tags' => ['a', 'b'], 'on' => '2026-01-02', 'name' => null],
                (object) ['id' => 2, 'tags' => [], 'on' => '1970-01-01'],
                null,
            ],
            (new TextValues())->of(
                type_structure([
                    'id' => type_integer(),
                    'tags' => type_list(type_string()),
                    'on' => type_date(),
                    'name' => structure_element('name', type_optional(type_string()), true),
                ]),
                [
                    ['id' => 1, 'tags' => ['a', 'b'], 'on' => 20_455, 'name' => null],
                    ['id' => 2, 'tags' => [], 'on' => 0],
                    null,
                ],
            ),
        );
    }

    public function test_of_makes_a_map_and_a_structure_of_kept_values_an_object(): void
    {
        static::assertEquals(
            [(object) ['a' => 1, 'b' => 2], (object) [], null],
            (new TextValues())->of(type_map(type_string(), type_integer()), [['a' => 1, 'b' => 2], [], null]),
        );
        static::assertEquals(
            [(object) ['a' => 1, 'b' => 'x']],
            (new TextValues())->of(type_structure(['a' => type_integer(), 'b' => type_string()]), [[
                'a' => 1,
                'b' => 'x',
            ]]),
        );
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $physicals
     * @param list<mixed> $expected
     */
    #[DataProvider('plain_values')]
    public function test_of(Type $type, array $physicals, array $expected): void
    {
        static::assertEquals($expected, (new TextValues())->of($type, $physicals));
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $physicals
     */
    #[DataProvider('shapes')]
    public function test_a_plain_value_carries_the_shape_of_its_type(Type $type, array $physicals, string $json): void
    {
        static::assertSame($json, json_encode((new TextValues())->of($type, $physicals), JSON_THROW_ON_ERROR));
    }

    public function test_of_uses_the_configured_formats_at_every_depth(): void
    {
        static::assertEquals(
            [(object) ['at' => ['02/01/2026 03:04'], 'on' => '2026/01/02']],
            (new TextValues('d/m/Y H:i', 'Y/m/d'))->of(
                type_structure(['at' => type_list(type_datetime()), 'on' => type_date()]),
                [['at' => [1_767_323_045_000_000], 'on' => 20_455]],
            ),
        );
    }

    public function test_of_refuses_malformed_json_text(): void
    {
        $this->expectException(JsonException::class);

        (new TextValues())->of(type_json(), ['{oops']);
    }

    #[TestWith([0])]
    #[TestWith([1_767_323_045_123_456])]
    #[TestWith([-500_000])]
    #[TestWith([253_402_300_799_999_999])]
    public function test_utc_date_times_format_every_letter_as_a_date_time_does(int $micros): void
    {
        $seconds = intdiv($micros - ((($micros % 1_000_000) + 1_000_000) % 1_000_000), 1_000_000);
        $instant = (new DateTimeImmutable('@' . $seconds))
            ->modify('+' . ((($micros % 1_000_000) + 1_000_000) % 1_000_000) . ' usec')
            ->setTimezone(new DateTimeZone('UTC'));

        foreach (str_split('dDjlNSwzWFmMntLoYyaABgGhHisuveIOPpZcrU') as $letter) {
            static::assertSame(
                [$instant->format($letter . ' | ' . $letter)],
                (new TextValues())->dateTimes(type_datetime(), [$micros], $letter . ' | ' . $letter),
                $letter,
            );
        }
    }

    public function test_date_times_of_a_date_are_midnight_utc(): void
    {
        static::assertSame(
            ['2026-01-02 00:00:00.000000 000 +00:00 UTC', null, '1969-12-31 00:00:00.000000 000 +00:00 UTC'],
            (new TextValues())->dateTimes(type_date(), [20_455, null, -1], 'Y-m-d H:i:s.u v P e'),
        );
    }

    public function test_date_times_with_the_zone_abbreviation_letter_name_utc(): void
    {
        static::assertSame(['UTC 1970', 'UTC 1970'], [
            ...(new TextValues())->dateTimes(type_datetime(), [0], 'T Y'),
            ...(new TextValues())->dateTimes(type_date(), [0], 'T Y'),
        ]);
    }

    public function test_date_times_of_a_named_zone_cross_a_transition(): void
    {
        static::assertSame(
            ['2026-03-29T01:59:59.999999+01:00 CET', '2026-03-29T03:00:00.000000+02:00 CEST', null],
            (new TextValues())->dateTimes(
                type_datetime('Europe/Warsaw'),
                [1_774_745_999_999_999, 1_774_746_000_000_000, null],
                'Y-m-d\TH:i:s.uP T',
            ),
        );
    }

    public function test_date_times_keep_escaped_letters(): void
    {
        static::assertSame(
            ['u 123456 T v 123 \\'],
            (new TextValues())->dateTimes(type_datetime(), [1_767_323_045_123_456], '\u u \T \v v \\\\'),
        );
    }

    public function test_uuids(): void
    {
        $bytes = [
            self::UUID_BYTES,
            (string) hex2bin('ffffffffffffffffffffffffffffffff'),
            (string) hex2bin('0123456789abcdef0123456789abcdef'),
        ];

        static::assertSame(
            [
                Uuid::fromBytes($bytes[0])->toString(),
                Uuid::fromBytes($bytes[1])->toString(),
                Uuid::fromBytes($bytes[2])->toString(),
            ],
            (new TextValues())->uuids($bytes),
        );
        static::assertSame(
            [Uuid::fromBytes($bytes[0])->toString(), null, Uuid::fromBytes($bytes[2])->toString()],
            (new TextValues())->uuids([$bytes[0], null, $bytes[2]]),
        );
        static::assertSame([], (new TextValues())->uuids([]));
    }

    #[TestWith(['serialize_precision', '-1'])]
    #[TestWith(['serialize_precision', '17'])]
    #[TestWith(['precision', '5'])]
    public function test_floats_never_depend_on_an_ini(string $ini, string $value): void
    {
        $previous = (string) ini_get($ini);
        ini_set($ini, $value);

        try {
            static::assertSame(
                [
                    '1.0',
                    '-0.0',
                    '1.0e+25',
                    '1.0e-7',
                    '1.7976931348623157e+308',
                    '2.2250738585072014e-308',
                    '5.0e-324',
                    '0.30000000000000004',
                    null,
                ],
                (new TextValues())->floats([
                    1.0,
                    -0.0,
                    1.0e25,
                    1.0e-7,
                    PHP_FLOAT_MAX,
                    PHP_FLOAT_MIN,
                    5.0e-324,
                    0.1 + 0.2,
                    null,
                ]),
            );
            static::assertSame(
                ['NAN', 'INF', '-INF', '0.30000000000000004', '1.0e+25', null],
                (new TextValues())->floats([NAN, INF, -INF, 0.1 + 0.2, 1.0e25, null]),
            );
            static::assertSame($value, ini_get($ini));
        } finally {
            ini_set($ini, $previous);
        }
    }

    public function test_floats_of_an_empty_list(): void
    {
        static::assertSame([], (new TextValues())->floats([]));
    }

    public function test_bindable_renders_markup_from_its_physicals(): void
    {
        $column = ColumnMother::of(xml_schema('x'), ['<root><a/></root>']);

        static::assertSame(['<root><a/></root>'], (new TextValues())->bindable($column->type(), $column));
    }

    public function test_bindable_hands_over_every_other_type_as_logical_values(): void
    {
        $column = ColumnMother::of(datetime_schema('at'), [new DateTimeImmutable('2026-01-02 03:04:05 UTC')]);

        static::assertEquals(
            [new DateTimeImmutable('2026-01-02 03:04:05 UTC')],
            (new TextValues())->bindable($column->type(), $column),
        );
    }
}
