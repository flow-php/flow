<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use DateTimeImmutable;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\OrderEntriesTransformer;
use Flow\Types\Value\Uuid as FlowUuid;
use Ramsey\Uuid\Uuid;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_sort_by_name;
use function Flow\ETL\DSL\schema_sort_by_type;
use function Flow\ETL\DSL\schema_sort_by_type_and_name;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;

final class OrderEntriesTransformerTest extends FlowTestCase
{
    public function test_ordering_entries_by_name_and_type(): void
    {
        $rows = rows(
            schema(
                int_schema('int_a'),
                int_schema('int_b'),
                float_schema('float_a'),
                float_schema('float_b'),
                bool_schema('bool'),
                bool_schema('bool_a'),
                bool_schema('bool_c'),
                datetime_schema('datetime_d'),
                datetime_schema('datetime_z'),
                str_schema('string_a'),
                str_schema('string_b'),
                uuid_schema('uuid'),
                json_schema('json'),
                list_schema('list', type_list(type_integer())),
                map_schema('map', type_map(type_integer(), type_string())),
                structure_schema('struct', type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                    'location' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                ])),
                enum_schema('enum_a', BackedStringEnum::class),
                enum_schema('enum_b', BackedStringEnum::class),
            ),
            row([
                'int_a' => 1,
                'int_b' => 1,
                'float_a' => generate_random_int(100, 100000) / 100,
                'float_b' => generate_random_int(100, 100000) / 100,
                'bool' => false,
                'bool_a' => false,
                'bool_c' => false,
                'datetime_d' => new DateTimeImmutable('now'),
                'datetime_z' => new DateTimeImmutable('now'),
                'string_a' => 'string',
                'string_b' => 'string',
                'uuid' => new FlowUuid(Uuid::uuid4()),
                'json' => type_json()->cast(['id' => 1, 'status' => 'NEW']),
                'list' => [1, 2, 3],
                'map' => [0 => 'zero', 1 => 'one', 2 => 'two'],
                'struct' => [
                    'street' => 'street',
                    'city' => 'city',
                    'zip' => 'zip',
                    'country' => 'country',
                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                ],
                'enum_a' => BackedStringEnum::three,
                'enum_b' => BackedStringEnum::one,
            ]),
        );

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
            (new OrderEntriesTransformer(schema_sort_by_type_and_name()))
                ->transform($rows, flow_context(config()))
                ->schema()
                ->references()
                ->names(),
        );
    }

    /**
     * TimeZoneEntry was APPENDED at the end of the priority table. Inserting it would have
     * renumbered thirteen shipped rows and silently changed existing sort output.
     */
    public function test_a_time_zone_entry_sorts_after_every_previously_known_type(): void
    {
        static::assertSame(
            ['int', 'tz'],
            (new OrderEntriesTransformer(schema_sort_by_type_and_name()))
                ->transform(
                    rows(
                        schema(time_zone_schema('tz'), int_schema('int')),
                        row(['tz' => type_time_zone()->cast('UTC'), 'int' => 1]),
                    ),
                    flow_context(config()),
                )
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_ordering_entries_by_name_asc(): void
    {
        $rows = rows(
            schema(int_schema('b'), int_schema('d'), int_schema('a'), int_schema('c'), int_schema('e')),
            row(['b' => 2, 'd' => 4, 'a' => 1, 'c' => 3, 'e' => 5]),
            row(['e' => 5, 'a' => 1, 'c' => 3, 'b' => 2, 'd' => 4]),
        );

        $sorted = (new OrderEntriesTransformer(schema_sort_by_name()))->transform($rows, flow_context(config()));

        static::assertSame(['a', 'b', 'c', 'd', 'e'], $sorted->schema()->references()->names());
        // only the schema reorders, row storage keeps the order it was written in
        static::assertSame(
            [
                ['b' => 2, 'd' => 4, 'a' => 1, 'c' => 3, 'e' => 5],
                ['e' => 5, 'a' => 1, 'c' => 3, 'b' => 2, 'd' => 4],
            ],
            $sorted->toArray(),
        );
        static::assertSame(
            ['e', 'd', 'c', 'b', 'a'],
            (new OrderEntriesTransformer(schema_sort_by_name(SortOrder::DESC)))
                ->transform($rows, flow_context(config()))
                ->schema()
                ->references()
                ->names(),
        );
    }

    public function test_ordering_entries_by_type(): void
    {
        $rows = rows(
            schema(
                int_schema('int'),
                float_schema('float'),
                bool_schema('bool'),
                datetime_schema('datetime'),
                str_schema('null', nullable: true),
                uuid_schema('uuid'),
                json_schema('json'),
                list_schema('list', type_list(type_integer())),
                map_schema('map', type_map(type_integer(), type_string())),
                structure_schema('struct', type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                    'location' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                ])),
                enum_schema('enum', BackedStringEnum::class),
            ),
            row([
                'int' => 1,
                'float' => generate_random_int(100, 100000) / 100,
                'bool' => false,
                'datetime' => new DateTimeImmutable('now'),
                'null' => null,
                'uuid' => new FlowUuid(Uuid::uuid4()),
                'json' => type_json()->cast(['id' => 1, 'status' => 'NEW']),
                'list' => [1, 2, 3],
                'map' => [0 => 'zero', 1 => 'one', 2 => 'two'],
                'struct' => [
                    'street' => 'street',
                    'city' => 'city',
                    'zip' => 'zip',
                    'country' => 'country',
                    'location' => ['lat' => 1.5, 'lon' => 1.5],
                ],
                'enum' => BackedStringEnum::three,
            ]),
        );

        static::assertSame(
            ['uuid', 'int', 'bool', 'float', 'datetime', 'null', 'enum', 'list', 'json', 'map', 'struct'],
            (new OrderEntriesTransformer(schema_sort_by_type()))
                ->transform($rows, flow_context(config()))
                ->schema()
                ->references()
                ->names(),
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
            (new OrderEntriesTransformer(schema_sort_by_type(order: SortOrder::DESC)))
                ->transform($rows, flow_context(config()))
                ->schema()
                ->references()
                ->names(),
        );
    }
}
