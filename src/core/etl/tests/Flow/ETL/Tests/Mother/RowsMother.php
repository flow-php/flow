<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
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
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function range;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class RowsMother
{
    /**
     * 3 batches of 2 rows, ids descending: [5,4][3,2][1,0].
     *
     * @return list<Rows>
     */
    public static function descendingIdBatches(): array
    {
        return [
            array_to_rows([['id' => 5], ['id' => 4]], schema(int_schema('id'))),
            array_to_rows([['id' => 3], ['id' => 2]], schema(int_schema('id'))),
            array_to_rows([['id' => 1], ['id' => 0]], schema(int_schema('id'))),
        ];
    }

    /**
     * 3 batches of 2 rows, groups interleaved: [a1,b10][a2,b20][a3,b30].
     *
     * @return list<Rows>
     */
    public static function interleavedGroupBatches(): array
    {
        return [
            array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'b', 'v' => 10]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'a', 'v' => 2], ['g' => 'b', 'v' => 20]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'a', 'v' => 3], ['g' => 'b', 'v' => 30]], schema(str_schema('g'), int_schema('v'))),
        ];
    }

    /**
     * 3 batches of 2 rows, groups already consecutive but straddling batch boundaries:
     * [a1,a2][a3,b10][b20,b30].
     *
     * @return list<Rows>
     */
    public static function sortedGroupBatches(): array
    {
        return [
            array_to_rows([['g' => 'a', 'v' => 1], ['g' => 'a', 'v' => 2]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'a', 'v' => 3], ['g' => 'b', 'v' => 10]], schema(str_schema('g'), int_schema('v'))),
            array_to_rows([['g' => 'b', 'v' => 20], ['g' => 'b', 'v' => 30]], schema(str_schema('g'), int_schema('v'))),
        ];
    }

    public static function sequentialIds(int $count): Rows
    {
        $sequence = [];

        for ($id = 1; $id <= $count; $id++) {
            $sequence[] = ['id' => $id];
        }

        return array_to_rows($sequence, schema(int_schema('id')));
    }

    /**
     * One row whose "array_entry" structure nests a structure: {id, status, enabled, array: {foo}}.
     */
    public static function arrayColumn(): Rows
    {
        return array_to_rows([[
            'array_entry' => ['id' => 1, 'status' => 'PENDING', 'enabled' => true, 'array' => ['foo' => 'bar']],
        ]], schema(structure_schema('array_entry', type_structure([
            'id' => type_integer(),
            'status' => type_string(),
            'enabled' => type_boolean(),
            'array' => type_structure(['foo' => type_string()]),
        ]))));
    }

    public static function empty(): Rows
    {
        return rows(schema());
    }

    public static function workedExample(): Rows
    {
        return array_to_rows(
            [['id' => 1, 'name' => 'ab'], ['id' => 2, 'name' => null]],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );
    }

    public static function ids(int $from, int $to): Rows
    {
        return array_to_rows(
            array_map(static fn(int $id) => ['id' => $id], range($from, $to)),
            schema(int_schema('id')),
        );
    }

    /**
     * Ids 1..$count, every fourth name null, one datetime an hour apart per row.
     */
    public static function numbered(int $count): Rows
    {
        return array_to_rows(
            array_map(
                static fn(int $id) => [
                    'id' => $id,
                    'name' => ($id % 4) === 0 ? null : 'user_' . $id,
                    'at' => (new DateTimeImmutable('2026-01-01 00:00:00.000001 +00:00'))->modify("+{$id} hours"),
                ],
                range(1, $count),
            ),
            schema(int_schema('id'), str_schema('name', nullable: true), datetime_schema('at')),
        );
    }

    public static function heterogeneous(): Rows
    {
        return array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y'], ['a' => 3, 'c' => 1.5], ['b' => 'z', 'a' => 4]],
            schema(int_schema('a'), str_schema('b', nullable: true), float_schema('c', nullable: true)),
        );
    }

    /**
     * Every column type except html/html_element, which require PHP 8.4.
     */
    public static function withAllColumnTypes(): Rows
    {
        $structure = type_structure([
            'street' => type_string(),
            'nested' => type_structure(['count' => type_integer(), 'tags' => type_list(type_string())]),
        ]);
        $interleaved = type_structure([
            'z' => type_integer(),
            'a' => structure_element('a', type_string(), optional: true),
            'b' => type_string(),
        ]);

        return array_to_rows(
            [[
                'int' => 42,
                'int_min' => PHP_INT_MIN,
                'int_max' => PHP_INT_MAX,
                'float' => 3.14159,
                'bool' => true,
                'string' => 'hello',
                'string_binary' => "line\nbreak\x00null\xFFbyte",
                'string_unicode' => 'zażółć gęślą jaźń 🚀',
                'string_null' => null,
                'string_from_null' => null,
                'datetime_immutable' => new DateTimeImmutable(
                    '2025-06-15 12:30:45.123456',
                    new DateTimeZone('Europe/Warsaw'),
                ),
                'datetime_mutable' => new DateTime('2025-06-15 12:30:45.654321', new DateTimeZone('America/New_York')),
                'datetime_before_epoch' => new DateTimeImmutable('1969-07-20 20:17:00.500000 UTC'),
                'date' => new DateTimeImmutable('2025-06-15 00:00:00', new DateTimeZone('UTC')),
                'time' => new DateInterval('PT2H30M15S'),
                'uuid' => type_uuid()->cast('0196aecb-b568-7e57-a381-8ec8d3e4a531'),
                'json' => type_json()->cast('[1,2,3]'),
                'json_object' => type_json()->cast('{"a":1,"b":[true,null]}'),
                'enum_backed' => BackedStringEnum::two,
                'enum_basic' => BasicEnum::three,
                'list' => [1, -2, PHP_INT_MAX],
                'map' => ['a' => 1, 'b' => 2],
                'structure' => $structure->cast(['street' => 'Main', 'nested' => ['count' => 5, 'tags' => ['a']]]),
                'structure_interleaved' => $interleaved->cast(['z' => 1, 'b' => 'x']),
                'xml' => type_xml()->cast('<root attr="1"><child>text &amp; entity</child></root>'),
                'xml_element' => type_xml_element()->cast('<item id="5">value</item>'),
            ]],
            schema(
                int_schema('int'),
                int_schema('int_min'),
                int_schema('int_max'),
                float_schema('float'),
                bool_schema('bool'),
                str_schema('string'),
                str_schema('string_binary'),
                str_schema('string_unicode'),
                str_schema('string_null', nullable: true),
                null_schema('string_from_null'),
                datetime_schema('datetime_immutable', zone: 'Europe/Warsaw'),
                datetime_schema('datetime_mutable', zone: 'America/New_York'),
                datetime_schema('datetime_before_epoch'),
                date_schema('date'),
                time_schema('time'),
                uuid_schema('uuid'),
                json_schema('json'),
                json_schema('json_object'),
                enum_schema('enum_backed', BackedStringEnum::class),
                enum_schema('enum_basic', BasicEnum::class),
                list_schema('list', type_list(type_integer())),
                map_schema('map', type_map(type_string(), type_integer())),
                structure_schema('structure', $structure),
                structure_schema('structure_interleaved', $interleaved),
                xml_schema('xml'),
                xml_element_schema('xml_element'),
            ),
        );
    }

    public static function orders(): Rows
    {
        return array_to_rows(
            [
                ['id' => 1, 'total' => 100, 'seller_id' => 10],
                ['id' => 2, 'total' => 250, 'seller_id' => 20],
                ['id' => 3, 'total' => 75, 'seller_id' => 10],
                ['id' => 4, 'total' => 300, 'seller_id' => 30],
                ['id' => 5, 'total' => 120, 'seller_id' => 20],
                ['id' => 6, 'total' => 90, 'seller_id' => 10],
            ],
            schema(int_schema('id'), int_schema('total'), int_schema('seller_id')),
        );
    }

    public static function sellers(): Rows
    {
        return array_to_rows(
            [['id' => 10, 'name' => 'Alice'], ['id' => 20, 'name' => 'Bob'], ['id' => 30, 'name' => 'Carol']],
            schema(int_schema('id'), str_schema('name')),
        );
    }
}
