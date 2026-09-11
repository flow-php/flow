<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;

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
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class RowsMother
{
    public static function empty(): Rows
    {
        return rows(schema());
    }

    public static function heterogeneous(): Rows
    {
        return rows(
            schema(int_schema('a'), str_schema('b', nullable: true), float_schema('c', nullable: true)),
            row(['a' => 1, 'b' => 'x']),
            row(['a' => 2, 'b' => 'y']),
            row(['a' => 3, 'c' => 1.5]),
            row(['b' => 'z', 'a' => 4]),
        );
    }

    /**
     * Every column type except html/html_element, which require PHP 8.4.
     */
    public static function withAllEntryTypes(): Rows
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

        return rows(
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
                datetime_schema('datetime_immutable'),
                datetime_schema('datetime_mutable'),
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
            row([
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
                'date' => new DateTimeImmutable('2025-06-15 00:00:00', new DateTimeZone('Europe/Warsaw')),
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
            ]),
        );
    }
}
