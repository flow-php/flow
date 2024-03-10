<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use function Flow\ETL\DSL\{array_entry, bool_entry, datetime_entry, enum_entry, float_entry, int_entry, json_entry, list_entry, map_entry, null_entry, object_entry, row, rows, struct_element, struct_entry, struct_type, type_datetime, type_float, type_int, type_list, type_map, type_string, uuid_entry};
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\{Extractor, FlowContext};
use Ramsey\Uuid\Uuid;

final class FakeExtractor implements Extractor
{
    public function __construct(private readonly int $total)
    {
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
                    float_entry('float', \random_int(100, 100000) / 100),
                    bool_entry('bool', false),
                    datetime_entry('datetime', new \DateTimeImmutable('now')),
                    null_entry('null'),
                    uuid_entry('uuid', new \Flow\ETL\Row\Entry\Type\Uuid(Uuid::uuid4())),
                    json_entry('json', ['id' => $id, 'status' => 'NEW']),
                    array_entry(
                        'array',
                        [
                            ['id' => 1, 'status' => 'NEW'],
                            ['id' => 2, 'status' => 'PENDING'],
                        ]
                    ),
                    list_entry('list', [1, 2, 3], type_list(type_int())),
                    list_entry('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()], type_list(type_datetime())),
                    map_entry(
                        'map',
                        ['NEW', 'PENDING'],
                        type_map(type_int(), type_string())
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
                        struct_type([
                            struct_element('street', type_string()),
                            struct_element('city', type_string()),
                            struct_element('zip', type_string()),
                            struct_element('country', type_string()),
                            struct_element(
                                'location',
                                struct_type([
                                    struct_element('lat', type_float()),
                                    struct_element('lon', type_float()),
                                ])
                            ),
                        ]),
                    ),
                    object_entry('object', new \ArrayIterator([1, 2, 3])),
                    enum_entry('enum', BackedStringEnum::three)
                )
            );
        }
    }
}
