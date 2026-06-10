<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit\RowsNormalizer;

use DateTimeImmutable;
use Flow\ETL\Adapter\Seal\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Row\Entry;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class EntryNormalizerTest extends FlowTestCase
{
    public static function entries_provider(): Generator
    {
        yield 'string' => [str_entry('string', 'value'), 'value'];
        yield 'string_nullable' => [str_entry('string', null), null];
        yield 'int' => [int_entry('integer', 1), 1];
        yield 'int_nullable' => [int_entry('integer', null), null];
        yield 'float' => [float_entry('float', 1.1), 1.1];
        yield 'float_nullable' => [float_entry('float', null), null];
        yield 'bool' => [bool_entry('bool', true), true];
        yield 'bool_nullable' => [bool_entry('bool', null), null];
        yield 'uuid' => [
            uuid_entry('uuid', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            'f47ac10b-58cc-4372-a567-0e02b2c3d479',
        ];
        yield 'datetime' => [
            datetime_entry('datetime', new DateTimeImmutable('2023-10-01 12:02:01')),
            '2023-10-01T12:02:01+00:00',
        ];
        yield 'datetime_nullable' => [datetime_entry('datetime', null), null];
        yield 'date' => [date_entry('date', new DateTimeImmutable('2023-10-01 12:02:01')), '2023-10-01'];
        yield 'date_nullable' => [date_entry('date', null), null];
        yield 'enum' => [enum_entry('enum', BackedStringEnum::one), 'one'];
        yield 'enum_nullable' => [enum_entry('enum', null), null];
        yield 'xml' => [xml_entry('xml', '<root><a>1</a></root>'), '<root><a>1</a></root>'];
        yield 'json' => [json_entry('json', ['a' => 1, 'b' => 'two']), ['a' => 1, 'b' => 'two']];
        yield 'json_nullable' => [json_entry('json', null), null];
        yield 'list' => [list_entry('list', ['a', 'b'], type_list(type_string())), ['a', 'b']];
        yield 'list_nullable' => [list_entry('list', null, type_list(type_string())), null];
        yield 'list_of_datetimes' => [
            list_entry('list', [new DateTimeImmutable('2023-10-01 12:02:01')], type_list(type_datetime())),
            ['2023-10-01T12:02:01+00:00'],
        ];
        yield 'map' => [
            map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_integer())),
            ['a' => 1, 'b' => 2],
        ];
        yield 'structure' => [
            struct_entry('structure', ['name' => 'John', 'age' => 30], type_structure([
                'name' => type_string(),
                'age' => type_integer(),
            ])),
            ['name' => 'John', 'age' => 30],
        ];
        yield 'structure_with_nested_datetime' => [
            struct_entry(
                'structure',
                ['name' => 'John', 'created_at' => new DateTimeImmutable('2023-10-01 12:02:01')],
                type_structure(['name' => type_string(), 'created_at' => type_datetime()]),
            ),
            ['name' => 'John', 'created_at' => '2023-10-01T12:02:01+00:00'],
        ];
        yield 'structure_nullable' => [
            struct_entry('structure', null, type_structure(['name' => type_string()])),
            null,
        ];
    }

    /**
     * @param Entry<mixed> $entry
     */
    #[DataProvider('entries_provider')]
    public function test_normalizing_entries(Entry $entry, mixed $expected): void
    {
        static::assertSame($expected, (new EntryNormalizer())->normalize($entry));
    }

    public function test_normalizing_a_date_with_a_custom_format(): void
    {
        $normalizer = new EntryNormalizer(dateFormat: 'd/m/Y');

        static::assertSame(
            '01/10/2023',
            $normalizer->normalize(date_entry('date', new DateTimeImmutable('2023-10-01'))),
        );
    }

    public function test_normalizing_a_datetime_with_a_custom_format(): void
    {
        $normalizer = new EntryNormalizer(dateTimeFormat: 'Y-m-d H:i:s');

        static::assertSame(
            '2023-10-01 12:02:01',
            $normalizer->normalize(datetime_entry('datetime', new DateTimeImmutable('2023-10-01 12:02:01'))),
        );
    }
}
