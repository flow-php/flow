<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\uuid_entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
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
