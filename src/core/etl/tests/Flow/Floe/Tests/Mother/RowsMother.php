<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Filesystem\Partition;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\time_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_element_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class RowsMother
{
    public static function heterogeneous(): Rows
    {
        return rows(
            row(int_entry('a', 1), str_entry('b', 'x')),
            row(int_entry('a', 2), str_entry('b', 'y')),
            row(int_entry('a', 3), float_entry('c', 1.5)),
            row(str_entry('b', 'z'), int_entry('a', 4)),
        );
    }

    public static function partitioned(): Rows
    {
        return Rows::partitioned([
            row(int_entry('id', 1), str_entry('country', 'PL')),
            row(int_entry('id', 2), str_entry('country', 'PL')),
        ], [new Partition('country', 'PL'), new Partition('year', '2025')]);
    }

    /**
     * Every entry type except html/html_element, which require PHP 8.4.
     */
    public static function withAllEntryTypes(): Rows
    {
        return rows(row(
            int_entry('int', 42),
            int_entry('int_min', PHP_INT_MIN),
            int_entry('int_max', PHP_INT_MAX),
            float_entry('float', 3.14159),
            bool_entry('bool', true),
            str_entry('string', 'hello'),
            str_entry('string_binary', "line\nbreak\x00null\xFFbyte"),
            str_entry('string_unicode', 'zażółć gęślą jaźń 🚀'),
            str_entry('string_null', null),
            StringEntry::fromNull('string_from_null'),
            datetime_entry(
                'datetime_immutable',
                new DateTimeImmutable('2025-06-15 12:30:45.123456', new DateTimeZone('Europe/Warsaw')),
            ),
            new Row\Entry\DateTimeEntry(
                'datetime_mutable',
                new DateTime('2025-06-15 12:30:45.654321', new DateTimeZone('America/New_York')),
            ),
            datetime_entry('datetime_before_epoch', new DateTimeImmutable('1969-07-20 20:17:00.500000 UTC')),
            date_entry('date', new DateTimeImmutable('2025-06-15', new DateTimeZone('Europe/Warsaw'))),
            time_entry('time', new DateInterval('PT2H30M15S')),
            uuid_entry('uuid', '0196aecb-b568-7e57-a381-8ec8d3e4a531'),
            json_entry('json', '[1,2,3]'),
            json_object_entry('json_object', '{"a":1,"b":[true,null]}'),
            enum_entry('enum_backed', BackedStringEnum::two),
            enum_entry('enum_basic', BasicEnum::three),
            list_entry('list', [1, -2, PHP_INT_MAX], type_list(type_integer())),
            map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer())),
            structure_entry(
                'structure',
                ['street' => 'Main', 'nested' => ['count' => 5, 'tags' => ['a']]],
                type_structure([
                    'street' => type_string(),
                    'nested' => type_structure(['count' => type_integer(), 'tags' => type_list(type_string())]),
                ]),
            ),
            xml_entry('xml', '<root attr="1"><child>text &amp; entity</child></root>'),
            xml_element_entry('xml_element', '<item id="5">value</item>'),
        ));
    }
}
