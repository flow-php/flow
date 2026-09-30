<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
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
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

/**
 * The batches the text writers' lanes are compared on, by name: a data provider yields names() and the test body
 * builds the batch on the backend it needs.
 */
final class TextWriterBatches
{
    /**
     * @return list<string>
     */
    public static function names(): array
    {
        return [
            'scalars with nulls',
            'floats',
            'not finite floats',
            'a not finite float inside a list',
            'strings that need quoting or escaping',
            'text that is not UTF-8',
            'temporal kinds',
            'a named zone across a transition',
            'uuid, enum and time zone',
            'a json column',
            'markup columns',
            'nested kinds rendered natively',
            'maps by key',
            'empty containers',
            'a list of json documents',
            'nested markup leaves',
            'awkward column names',
            'no rows',
        ];
    }

    /**
     * @return array{Schema, list<array<array-key, mixed>>}
     */
    public static function of(string $name): array
    {
        return match ($name) {
            'scalars with nulls' => [
                schema(
                    int_schema('id', nullable: true),
                    str_schema('name', nullable: true),
                    bool_schema('active', nullable: true),
                    null_schema('nothing'),
                ),
                [
                    ['id' => 1, 'name' => 'Norbert', 'active' => true, 'nothing' => null],
                    ['id' => null, 'name' => null, 'active' => null, 'nothing' => null],
                    ['id' => -2, 'name' => '', 'active' => false, 'nothing' => null],
                ],
            ],
            'floats' => [
                schema(float_schema('f', nullable: true)),
                [
                    ['f' => 1.0],
                    ['f' => -0.0],
                    ['f' => 1.0e25],
                    ['f' => 1.0e-7],
                    ['f' => PHP_FLOAT_MAX],
                    ['f' => PHP_FLOAT_MIN],
                    ['f' => 5.0e-324],
                    ['f' => 4.9e-324],
                    ['f' => 0.1 + 0.2],
                    ['f' => null],
                ],
            ],
            'not finite floats' => [
                schema(int_schema('id'), float_schema('f')),
                [['id' => 1, 'f' => NAN], ['id' => 2, 'f' => INF], ['id' => 3, 'f' => -INF]],
            ],
            'a not finite float inside a list' => [
                schema(int_schema('id'), list_schema('l', type_list(type_float()))),
                [['id' => 1, 'l' => [1.5]], ['id' => 2, 'l' => [2.0, NAN]]],
            ],
            'strings that need quoting or escaping' => [
                schema(str_schema('s')),
                [
                    ['s' => 'a,b'],
                    ['s' => 'say "hi"'],
                    ['s' => "two\nlines"],
                    ['s' => 'back\slash"q'],
                    ['s' => ' lead'],
                    ['s' => "tab\t"],
                    ['s' => 'a;b|c'],
                    ['s' => "it's"],
                    ['s' => 'path/to'],
                    ['s' => "control \x01 and \x7f"],
                    ['s' => 'zażółć 😀'],
                ],
            ],
            'text that is not UTF-8' => [
                schema(int_schema('id'), str_schema('s')),
                [['id' => 1, 's' => 'fine'], ['id' => 2, 's' => "a\xffb"]],
            ],
            'temporal kinds' => [
                schema(
                    datetime_schema('at', nullable: true),
                    datetime_schema('offset', zone: '+02:30'),
                    date_schema('on', nullable: true),
                    time_schema('took', nullable: true),
                ),
                [
                    [
                        'at' => new DateTimeImmutable('2026-01-02 03:04:05.123456 UTC'),
                        'offset' => new DateTimeImmutable('2026-01-02 03:04:05 UTC'),
                        'on' => new DateTimeImmutable('2026-01-02'),
                        'took' => new DateInterval('PT1H'),
                    ],
                    [
                        'at' => null,
                        'offset' => new DateTimeImmutable('1969-12-31 23:59:59.5 UTC'),
                        'on' => null,
                        'took' => null,
                    ],
                ],
            ],
            'a named zone across a transition' => [
                schema(datetime_schema('at', zone: 'Europe/Warsaw')),
                [
                    ['at' => new DateTimeImmutable('@1774745999')],
                    ['at' => new DateTimeImmutable('@1774746000')],
                    ['at' => new DateTimeImmutable('@1792890000')],
                ],
            ],
            'uuid, enum and time zone' => [
                schema(
                    uuid_schema('id', nullable: true),
                    enum_schema('backed', BackedStringEnum::class),
                    enum_schema('basic', BasicEnum::class, nullable: true),
                    time_zone_schema('zone', nullable: true),
                ),
                [
                    [
                        'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
                        'backed' => BackedStringEnum::one,
                        'basic' => BasicEnum::two,
                        'zone' => new DateTimeZone('Europe/Warsaw'),
                    ],
                    ['id' => null, 'backed' => BackedStringEnum::two, 'basic' => null, 'zone' => null],
                ],
            ],
            'a json column' => [
                schema(int_schema('id'), json_schema('j', nullable: true)),
                [
                    ['id' => 1, 'j' => '{"a": 1, "b": [1.0, "x/y"]}'],
                    ['id' => 2, 'j' => '{}'],
                    ['id' => 3, 'j' => '[]'],
                    ['id' => 4, 'j' => null],
                ],
            ],
            'markup columns' => [
                schema(int_schema('id'), xml_schema('x', nullable: true), xml_element_schema('e')),
                [
                    ['id' => 1, 'x' => '<root><a>1</a></root>', 'e' => '<a b="1"><c/></a>'],
                    ['id' => 2, 'x' => null, 'e' => '<b/>'],
                ],
            ],
            'nested kinds rendered natively' => [
                schema(
                    list_schema('l', type_list(type_datetime()), nullable: true),
                    map_schema('m', type_map(type_string(), type_float())),
                    structure_schema('s', type_structure([
                        'a' => type_integer(),
                        'b' => structure_element('b', type_date(), true),
                    ])),
                    list_schema('deep', type_list(type_list(type_optional(type_string())))),
                ),
                [
                    [
                        'l' => [new DateTimeImmutable('2026-01-02 03:04:05 UTC')],
                        'm' => ['a b' => 1.0, 'c/d' => 0.1 + 0.2],
                        's' => ['a' => 1, 'b' => new DateTimeImmutable('2026-01-02')],
                        'deep' => [['x', null], []],
                    ],
                    ['l' => null, 'm' => ['k' => -0.0], 's' => ['a' => 2], 'deep' => []],
                ],
            ],
            'maps by key' => [
                schema(map_schema('m', type_map(type_integer(), type_string()))),
                [['m' => [0 => 'a', 1 => 'b']], ['m' => [5 => 'a', 7 => 'b']], ['m' => [-1 => 'z']]],
            ],
            'empty containers' => [
                schema(
                    map_schema('m', type_map(type_string(), type_integer())),
                    list_schema('l', type_list(type_integer())),
                ),
                [['m' => [], 'l' => []]],
            ],
            'a list of json documents' => [
                schema(int_schema('id'), list_schema('l', type_list(type_json()))),
                [['id' => 1, 'l' => ['{"a": {}}', '[]']], ['id' => 2, 'l' => []]],
            ],
            'nested markup leaves' => [
                schema(
                    int_schema('id'),
                    list_schema('l', type_list(type_xml())),
                    structure_schema('s', type_structure(['x' => type_xml_element()])),
                ),
                [['id' => 1, 'l' => ['<a>1</a>'], 's' => ['x' => '<b c="d"/>']]],
            ],
            'awkward column names' => [
                schema(
                    int_schema('first name'),
                    int_schema('a"b'),
                    int_schema('x/y'),
                    int_schema('ż'),
                    int_schema('0'),
                    int_schema('7'),
                ),
                [['first name' => 1, 'a"b' => 2, 'x/y' => 3, 'ż' => 4, '0' => 5, '7' => 6]],
            ],
            'no rows' => [schema(int_schema('id'), str_schema('name')), []],
            default => throw new InvalidArgumentException('There is no text writer batch named "' . $name . '"'),
        };
    }
}
