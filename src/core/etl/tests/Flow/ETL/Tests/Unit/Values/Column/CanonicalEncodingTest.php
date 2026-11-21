<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\MistypedColumnStub;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Tests\Mother\DateIntervalMother;
use Flow\ETL\Values\Column\CanonicalEncoding;
use Flow\ETL\Values\Column\Php\MapColumn;
use Flow\ETL\Values\Schema\Definition;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_unique;
use function array_values;
use function bin2hex;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_element_schema;
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
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_xml_element;

use const NAN;

final class CanonicalEncodingTest extends TestCase
{
    public static function provide_encodings(): Generator
    {
        yield 'null in the columns form' => [null_schema('v'), [null], ['00']];
        yield 'boolean' => [bool_schema('v', true), [true, false, null], ['0101', '0100', '00']];
        yield 'integer' => [int_schema('v'), [1, -1], ['020100000000000000', '02ffffffffffffffff']];
        yield 'float' => [float_schema('v'), [1.0, 1.5], ['03000000000000f03f', '03000000000000f83f']];
        yield 'string' => [str_schema('v'), ['1', ''], ['04010000000000000031', '040000000000000000']];
        yield 'json' => [json_schema('v'), ['{"a":1}'], ['0507000000000000007b2261223a317d']];
        yield 'enum' => [
            enum_schema('v', BackedStringEnum::class),
            [BackedStringEnum::one],
            ['0603000000000000006f6e65'],
        ];
        yield 'xml' => [
            xml_schema('v'),
            ['<a/>'],
            ['071b000000000000003c3f786d6c2076657273696f6e3d22312e30223f3e0a3c612f3e0a'],
        ];
        yield 'timezone' => [time_zone_schema('v'), [new DateTimeZone('UTC')], ['090300000000000000555443']];
        yield 'uuid' => [
            uuid_schema('v'),
            ['00000000-0000-0000-0000-000000000001'],
            ['0a00000000000000000000000000000001'],
        ];
        yield 'date' => [date_schema('v'), [new DateTimeImmutable('1970-01-02')], ['0b01000000']];
        yield 'datetime' => [
            datetime_schema('v'),
            [new DateTimeImmutable('1970-01-01 00:00:01.000002 UTC')],
            ['0c42420f0000000000'],
        ];
        yield 'time' => [
            time_schema('v'),
            [new DateInterval('PT1S'), DateIntervalMother::of('PT1S', 0.0, 1)],
            ['0d40420f0000000000', '0dc0bdf0ffffffffff'],
        ];
        yield 'list with a null element' => [
            list_schema('v', type_list(type_optional(type_integer()))),
            [[1, null]],
            ['0e020000000000000002010000000000000000'],
        ];
        yield 'map' => [
            map_schema('v', type_map(type_string(), type_integer())),
            [['b' => 2, 'a' => 1]],
            ['0f02000000000000000401000000000000006102010000000000000004010000000000000062020200000000000000'],
        ];
        yield 'nested structure' => [
            structure_schema('v', type_structure([
                'b' => type_integer(),
                'a' => type_optional(type_structure(['x' => type_string()])),
            ])),
            [['b' => 1, 'a' => ['x' => 'y']], ['b' => 1, 'a' => null]],
            [
                '10020000000000000001000000000000006110010000000000000001000000000000007804010000000000000079010000000000000062020100000000000000',
                '100100000000000000010000000000000062020100000000000000',
            ],
        ];
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     * @param list<string> $expected
     */
    #[DataProvider('provide_encodings')]
    public function test_ordered_writes_the_tag_and_payload_of_each_type(
        Definition $definition,
        array $values,
        array $expected,
    ): void {
        $rows = array_to_rows(array_map(static fn(mixed $value): array => [
            'v' => $value,
        ], $values), schema($definition));

        static::assertSame($expected, array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column(
            'v',
        )], $rows->count())));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_takes_tag_08(): void
    {
        $rows = array_to_rows([[
            'v' => '<!DOCTYPE html><html><head></head><body><p>a</p></body></html>',
        ]], schema(html_schema('v')));

        static::assertSame(
            [
                '083e000000000000003c21444f43545950452068746d6c3e3c68746d6c3e3c686561643e3c2f686561643e3c626f64793e3c703e613c2f703e3c2f626f64793e3c2f68746d6c3e',
            ],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 1)),
        );
    }

    public function test_xml_element_is_encoded_by_its_markup_wherever_it_lives(): void
    {
        $rows = array_to_rows([
            ['v' => type_xml_element()->cast('<div><a x="1"/></div>')->firstElementChild],
            ['v' => type_xml_element()->cast('<section><b/><a x="1"/></section>')->lastElementChild],
            ['v' => type_xml_element()->cast('<div><a/></div>')->firstElementChild],
            ['v' => (new DOMDocument())->createElement('a')],
        ], schema(xml_element_schema('v')));

        static::assertSame(
            [
                '110d000000000000003c6120783d2231223e3c2f613e',
                '110d000000000000003c6120783d2231223e3c2f613e',
                '1107000000000000003c613e3c2f613e',
                '1107000000000000003c613e3c2f613e',
            ],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 4)),
        );
    }

    public function test_xml_element_differs_from_xml_document_with_the_same_markup(): void
    {
        $rows = array_to_rows(
            [['d' => '<a/>', 'e' => type_xml_element()->cast('<a/>')]],
            schema(xml_schema('d'), xml_element_schema('e')),
        );

        static::assertNotSame(
            (new CanonicalEncoding())->ordered([$rows->column('d')], 1),
            (new CanonicalEncoding())->ordered([$rows->column('e')], 1),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_element_is_encoded_by_its_markup_wherever_it_lives(): void
    {
        $rows = array_to_rows([
            ['v' => type_html_element()->cast('<div><p>a</p></div>')->firstElementChild],
            ['v' => type_html_element()->cast('<section><b></b><p>a</p></section>')->lastElementChild],
        ], schema(html_element_schema('v')));

        static::assertSame(
            ['1208000000000000003c703e613c2f703e', '1208000000000000003c703e613c2f703e'],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 2)),
        );
    }

    public function test_ordered_concatenates_the_columns_in_list_order(): void
    {
        $rows = array_to_rows([['a' => 1, 'b' => true]], schema(int_schema('a'), bool_schema('b')));

        static::assertSame(['0201000000000000000101'], array_map(bin2hex(...), (new CanonicalEncoding())->ordered([
            $rows->column('a'),
            $rows->column('b'),
        ], 1)));
        static::assertSame(['0101020100000000000000'], array_map(bin2hex(...), (new CanonicalEncoding())->ordered([
            $rows->column('b'),
            $rows->column('a'),
        ], 1)));
    }

    public function test_same_text_of_different_types_differs(): void
    {
        $rows = array_to_rows(
            [['i' => 1, 'f' => 1.0, 's' => '1', 'j' => '[1]', 'e' => BackedStringEnum::one]],
            schema(
                int_schema('i'),
                float_schema('f'),
                str_schema('s'),
                json_schema('j'),
                enum_schema('e', BackedStringEnum::class),
            ),
        );

        static::assertCount(
            5,
            array_unique(array_map(
                static fn($column): string => (new CanonicalEncoding())->ordered([$column], 1)[0],
                array_values($rows->columns()),
            )),
        );
    }

    public function test_null_differs_from_empty_string(): void
    {
        $rows = array_to_rows([['v' => null], ['v' => '']], schema(str_schema('v', true)));

        static::assertSame(['00', '040000000000000000'], array_map(bin2hex(...), (new CanonicalEncoding())->ordered(
            [$rows->column('v')],
            2,
        )));
    }

    public function test_negative_zero_equals_zero_and_every_nan_is_one_value(): void
    {
        $rows = array_to_rows([['v' => -0.0], ['v' => 0.0], ['v' => NAN], ['v' => -NAN]], schema(float_schema('v')));

        static::assertSame(
            ['030000000000000000', '030000000000000000', '03000000000000f87f', '03000000000000f87f'],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 4)),
        );
    }

    public function test_float_nan_differs_from_string_nan(): void
    {
        $rows = array_to_rows([['f' => NAN, 's' => 'NaN']], schema(float_schema('f'), str_schema('s')));

        static::assertNotSame(
            (new CanonicalEncoding())->ordered([$rows->column('f')], 1),
            (new CanonicalEncoding())->ordered([$rows->column('s')], 1),
        );
    }

    public function test_same_instant_in_two_zones_is_equal(): void
    {
        $rows = array_to_rows([
            ['v' => new DateTimeImmutable('2026-01-01 01:00:00', new DateTimeZone('Europe/Warsaw'))],
            ['v' => new DateTimeImmutable('2026-01-01 00:00:00', new DateTimeZone('UTC'))],
        ], schema(datetime_schema('v')));

        static::assertSame(
            ['0c0040204648470600', '0c0040204648470600'],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 2)),
        );
    }

    public function test_date_differs_from_datetime_at_midnight(): void
    {
        $rows = array_to_rows(
            [['d' => new DateTimeImmutable('1970-01-02'), 't' => new DateTimeImmutable('1970-01-02 00:00:00 UTC')]],
            schema(date_schema('d'), datetime_schema('t')),
        );

        static::assertSame(['0b01000000', '0c0060d71d14000000'], [
            bin2hex((new CanonicalEncoding())->ordered([$rows->column('d')], 1)[0]),
            bin2hex((new CanonicalEncoding())->ordered([$rows->column('t')], 1)[0]),
        ]);
    }

    public function test_map_entry_order_does_not_count(): void
    {
        $rows = array_to_rows([
            ['v' => ['a' => 1, 'b' => 2]],
            ['v' => ['b' => 2, 'a' => 1]],
        ], schema(map_schema('v', type_map(type_string(), type_integer()))));
        $encoded = (new CanonicalEncoding())->ordered([$rows->column('v')], 2);

        static::assertSame($encoded[0], $encoded[1]);
    }

    public function test_map_with_integer_keys_takes_tag_02_for_the_keys(): void
    {
        $rows = array_to_rows([['v' => [1 => 'a']]], schema(map_schema('v', type_map(type_integer(), type_string()))));

        static::assertSame(
            ['0f010000000000000002010000000000000004010000000000000061'],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 1)),
        );
    }

    public function test_map_string_key_that_php_turned_into_an_integer_takes_tag_04(): void
    {
        $column = new MapColumn(
            type_map(type_string(), type_integer()),
            [0, 1],
            ColumnMother::of(str_schema('k'), ['1']),
            ColumnMother::of(int_schema('v'), [1]),
            [],
        );

        static::assertSame(
            ['0f010000000000000004010000000000000031020100000000000000'],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$column], 1)),
        );
    }

    public function test_list_order_counts(): void
    {
        $rows = array_to_rows([['v' => [1, 2]], ['v' => [2, 1]]], schema(list_schema('v', type_list(type_integer()))));
        $encoded = (new CanonicalEncoding())->ordered([$rows->column('v')], 2);

        static::assertNotSame($encoded[0], $encoded[1]);
    }

    public function test_structure_element_order_does_not_count(): void
    {
        $rows = array_to_rows(
            [['l' => ['a' => 1, 'b' => 'x'], 'r' => ['b' => 'x', 'a' => 1]]],
            schema(
                structure_schema('l', type_structure(['a' => type_integer(), 'b' => type_string()])),
                structure_schema('r', type_structure(['b' => type_string(), 'a' => type_integer()])),
            ),
        );

        static::assertSame(
            (new CanonicalEncoding())->ordered([$rows->column('l')], 1),
            (new CanonicalEncoding())->ordered([$rows->column('r')], 1),
        );
    }

    public function test_renamed_nested_element_differs(): void
    {
        $rows = array_to_rows(
            [['l' => ['s' => ['a' => 1]], 'r' => ['s' => ['b' => 1]]]],
            schema(
                structure_schema('l', type_structure(['s' => type_structure(['a' => type_integer()])])),
                structure_schema('r', type_structure(['s' => type_structure(['b' => type_integer()])])),
            ),
        );

        static::assertNotSame(
            (new CanonicalEncoding())->ordered([$rows->column('l')], 1),
            (new CanonicalEncoding())->ordered([$rows->column('r')], 1),
        );
    }

    public function test_null_structure_element_equals_missing_optional_element(): void
    {
        $rows = array_to_rows([
            ['v' => ['a' => 1, 'b' => null]],
            ['v' => ['a' => 1]],
        ], schema(structure_schema('v', type_structure([
            'a' => type_integer(),
            'b' => structure_element('b', type_optional(type_string()), true),
        ]))));

        static::assertSame(
            [
                '100100000000000000010000000000000061020100000000000000',
                '100100000000000000010000000000000061020100000000000000',
            ],
            array_map(bin2hex(...), (new CanonicalEncoding())->ordered([$rows->column('v')], 2)),
        );
    }

    public function test_json_whitespace_counts(): void
    {
        $rows = array_to_rows([['v' => '{"a":1}'], ['v' => '{"a": 1}']], schema(json_schema('v')));
        $encoded = (new CanonicalEncoding())->ordered([$rows->column('v')], 2);

        static::assertNotSame($encoded[0], $encoded[1]);
    }

    public function test_named_writes_name_parts_sorted_by_name(): void
    {
        $rows = array_to_rows([['b' => true, 'a' => 1]], schema(bool_schema('b'), int_schema('a')));

        static::assertSame(
            ['0100000000000000610201000000000000000100000000000000620101'],
            array_map(bin2hex(...), (new CanonicalEncoding())->named($rows->columns(), 1)),
        );
    }

    public function test_named_ignores_the_column_order(): void
    {
        $rows = array_to_rows([['a' => 1, 'b' => 'x']], schema(int_schema('a'), str_schema('b')));

        static::assertSame(
            (new CanonicalEncoding())->named(['a' => $rows->column('a'), 'b' => $rows->column('b')], 1),
            (new CanonicalEncoding())->named(['b' => $rows->column('b'), 'a' => $rows->column('a')], 1),
        );
    }

    public function test_named_renamed_column_differs(): void
    {
        $rows = array_to_rows([['a' => 1]], schema(int_schema('a')));

        static::assertNotSame(
            (new CanonicalEncoding())->named(['a' => $rows->column('a')], 1),
            (new CanonicalEncoding())->named(['c' => $rows->column('a')], 1),
        );
    }

    public function test_named_null_column_equals_missing_column(): void
    {
        $with = array_to_rows([['a' => 1, 'b' => null]], schema(int_schema('a'), str_schema('b', true)));
        $without = array_to_rows([['a' => 1]], schema(int_schema('a')));

        static::assertSame(
            (new CanonicalEncoding())->named($with->columns(), 1),
            (new CanonicalEncoding())->named($without->columns(), 1),
        );
    }

    public function test_named_of_only_nulls_is_empty(): void
    {
        $rows = array_to_rows([['a' => null, 'b' => null]], schema(int_schema('a', true), null_schema('b')));

        static::assertSame([''], (new CanonicalEncoding())->named($rows->columns(), 1));
    }

    public function test_type_outside_the_table_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('type mixed has no canonical encoding, declare the type of the column');

        (new CanonicalEncoding())->ordered([new MistypedColumnStub(
            ColumnMother::of(int_schema('a'), [1]),
            type_mixed(),
        )], 1);
    }

    public function test_no_rows_encode_to_no_values(): void
    {
        $rows = array_to_rows([], schema(int_schema('a')));

        static::assertSame([], (new CanonicalEncoding())->named($rows->columns(), 0));
        static::assertSame([], (new CanonicalEncoding())->ordered([$rows->column('a')], 0));
    }
}
