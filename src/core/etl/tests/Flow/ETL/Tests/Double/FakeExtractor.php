<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use function Flow\ETL\DSL\{bool_entry,
    bool_schema,
    datetime_entry,
    datetime_schema,
    enum_entry,
    enum_schema,
    float_entry,
    float_schema,
    generate_random_int,
    int_entry,
    int_schema,
    json_entry,
    json_schema,
    list_entry,
    list_schema,
    map_entry,
    map_schema,
    null_entry,
    row,
    rows,
    string_schema,
    struct_entry,
    structure_schema,
    uuid_entry,
    uuid_schema,
    xml_entry,
    xml_schema};
use function Flow\Types\DSL\{type_datetime, type_float, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\{Extractor, FlowContext, Rows, Schema};
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Ramsey\Uuid\Uuid;

final readonly class FakeExtractor implements Extractor
{
    public function __construct(private int $total)
    {
    }

    public static function schema() : Schema
    {
        return \Flow\ETL\DSL\schema(
            int_schema('int'),
            float_schema('float'),
            bool_schema('bool'),
            datetime_schema('datetime'),
            string_schema('null', nullable: true),
            uuid_schema('uuid'),
            json_schema('json'),
            list_schema('list', type_list(type_integer())),
            list_schema('list_of_datetimes', type_list(type_datetime())),
            map_schema(
                'map',
                type_map(type_integer(), type_string())
            ),
            structure_schema(
                'struct',
                type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                    'location' => type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                ])
            ),
            enum_schema('enum', BackedStringEnum::class),
            xml_schema('xml'),
        );
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context) : \Generator
    {
        for ($i = 0; $i < $this->total; $i++) {
            $id = $i;

            yield rows(
                row(
                    int_entry('int', $id),
                    float_entry('float', generate_random_int(100, 100000) / 100),
                    bool_entry('bool', \random_int(0, 1) === 1),
                    datetime_entry('datetime', new \DateTimeImmutable('now')),
                    null_entry('null'),
                    uuid_entry('uuid', new \Flow\Types\Value\Uuid(Uuid::uuid4())),
                    json_entry('json', ['id' => $id, 'status' => 'NEW']),
                    list_entry('list', [1, 2, 3], type_list(type_integer())),
                    list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_datetime())),
                    map_entry(
                        'map',
                        ['NEW', 'PENDING'],
                        type_map(type_integer(), type_string())
                    ),
                    struct_entry(
                        'struct',
                        [
                            'street' => 'street_' . $id,
                            'city' => 'city_' . $id,
                            'zip' => 'zip_' . $id,
                            'country' => 'country_' . $id,
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
                    xml_entry('xml', '<xml><node id="' . $id . '">node-' . $id . '</node></xml>'),
                )
            );
        }
    }
}
