<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\OrderEntriesTransformer;
use Ramsey\Uuid\Uuid;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\compare_entries_by_name;
use function Flow\ETL\DSL\compare_entries_by_name_desc;
use function Flow\ETL\DSL\compare_entries_by_type;
use function Flow\ETL\DSL\compare_entries_by_type_and_name;
use function Flow\ETL\DSL\compare_entries_by_type_desc;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class OrderEntriesTransformerTest extends FlowTestCase
{
    public function test_ordering_entries_by_name_and_type(): void
    {
        $rows = rows(row(
            int_entry('int_a', 1),
            int_entry('int_b', 1),
            float_entry('float_a', generate_random_int(100, 100000) / 100),
            float_entry('float_b', generate_random_int(100, 100000) / 100),
            bool_entry('bool', false),
            bool_entry('bool_a', false),
            bool_entry('bool_c', false),
            datetime_entry('datetime_d', new \DateTimeImmutable('now')),
            datetime_entry('datetime_z', new \DateTimeImmutable('now')),
            str_entry('string_a', 'string'),
            str_entry('string_b', 'string'),
            uuid_entry('uuid', new \Flow\Types\Value\Uuid(Uuid::uuid4())),
            json_entry('json', ['id' => 1, 'status' => 'NEW']),
            list_entry('list', [1, 2, 3], type_list(type_integer())),
            map_entry('map', [0 => 'zero', 1 => 'one', 2 => 'two'], type_map(type_integer(), type_string())),
            struct_entry(
                'struct',
                [
                    'street' => 'street',
                    'city' => 'city',
                    'zip' => 'zip',
                    'country' => 'country',
                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                ],
                type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                    'location' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                ]),
            ),
            enum_entry('enum_a', BackedStringEnum::three),
            enum_entry('enum_b', BackedStringEnum::one),
        ));

        static::assertSame(
            [
                'uuid',
                'int_a',
                'int_b',
                'bool',
                'bool_a',
                'bool_c',
                'float_a',
                'float_b',
                'datetime_d',
                'datetime_z',
                'string_a',
                'string_b',
                'enum_a',
                'enum_b',
                'list',
                'json',
                'map',
                'struct',
            ],
            \array_keys(
                (new OrderEntriesTransformer(compare_entries_by_type_and_name()))
                    ->transform($rows, flow_context(config()))
                    ->toArray()[0],
            ),
        );
    }

    public function test_ordering_entries_by_name_asc(): void
    {
        $rows = rows(
            row(int_entry('b', 2), int_entry('d', 4), int_entry('a', 1), int_entry('c', 3), int_entry('e', 5)),
            row(int_entry('e', 5), int_entry('a', 1), int_entry('c', 3), int_entry('b', 2), int_entry('d', 4)),
        );

        static::assertSame(
            [
                ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5],
                ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5],
            ],
            (new OrderEntriesTransformer(compare_entries_by_name()))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
        static::assertSame(
            [
                ['e' => 5, 'd' => 4, 'c' => 3, 'b' => 2, 'a' => 1],
                ['e' => 5, 'd' => 4, 'c' => 3, 'b' => 2, 'a' => 1],
            ],
            (new OrderEntriesTransformer(compare_entries_by_name_desc()))
                ->transform($rows, flow_context(config()))
                ->toArray(),
        );
    }

    public function test_ordering_entries_by_type(): void
    {
        $rows = rows(row(
            int_entry('int', 1),
            float_entry('float', generate_random_int(100, 100000) / 100),
            bool_entry('bool', false),
            datetime_entry('datetime', new \DateTimeImmutable('now')),
            str_entry('null', null),
            uuid_entry('uuid', new \Flow\Types\Value\Uuid(Uuid::uuid4())),
            json_entry('json', ['id' => 1, 'status' => 'NEW']),
            list_entry('list', [1, 2, 3], type_list(type_integer())),
            map_entry('map', [0 => 'zero', 1 => 'one', 2 => 'two'], type_map(type_integer(), type_string())),
            struct_entry(
                'struct',
                [
                    'street' => 'street',
                    'city' => 'city',
                    'zip' => 'zip',
                    'country' => 'country',
                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                ],
                type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                    'location' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                ]),
            ),
            enum_entry('enum', BackedStringEnum::three),
        ));

        static::assertSame(
            ['uuid', 'int', 'bool', 'float', 'datetime', 'null', 'enum', 'list', 'json', 'map', 'struct'],
            \array_keys(
                (new OrderEntriesTransformer(compare_entries_by_type()))
                    ->transform($rows, flow_context(config()))
                    ->toArray()[0],
            ),
        );
        static::assertSame(
            array_reverse([
                'uuid',
                'int',
                'bool',
                'float',
                'datetime',
                'null',
                'enum',
                'list',
                'json',
                'map',
                'struct',
            ]),
            \array_keys(
                (new OrderEntriesTransformer(compare_entries_by_type_desc()))
                    ->transform($rows, flow_context(config()))
                    ->toArray()[0],
            ),
        );
    }
}
