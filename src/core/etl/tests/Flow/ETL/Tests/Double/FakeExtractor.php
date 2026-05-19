<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateTimeImmutable;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\Types\Value\Uuid as FlowUuid;
use Generator;
use Ramsey\Uuid\Uuid;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_entry;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function random_int;

// @mago-ignore analysis:less-specific-argument
final readonly class FakeExtractor implements Extractor
{
    public function __construct(
        private int $total,
    ) {}

    /**
     * @return Schema
     */
    public static function schema(): Schema
    {
        return schema(
            int_schema('int'),
            float_schema('float'),
            bool_schema('bool'),
            datetime_schema('datetime'),
            string_schema('null', nullable: true),
            uuid_schema('uuid'),
            json_schema('json'),
            list_schema('list', type_list(type_integer())),
            list_schema('list_of_datetimes', type_list(type_datetime())),
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
            xml_schema('xml'),
        );
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context): Generator
    {
        for ($i = 0; $i < $this->total; $i++) {
            $id = $i;

            yield rows(row(
                int_entry('int', $id),
                float_entry('float', generate_random_int(100, 100000) / 100),
                bool_entry('bool', random_int(0, 1) === 1),
                datetime_entry('datetime', new DateTimeImmutable('now')),
                null_entry('null'),
                uuid_entry('uuid', new FlowUuid(Uuid::uuid4())),
                json_entry('json', ['id' => $id, 'status' => 'NEW']),
                list_entry('list', [1, 2, 3], type_list(type_integer())),
                list_entry(
                    'list_of_datetimes',
                    [new DateTimeImmutable(), new DateTimeImmutable(), new DateTimeImmutable()],
                    type_list(type_datetime()),
                ),
                map_entry('map', ['NEW', 'PENDING'], type_map(type_integer(), type_string())),
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
            ));
        }
    }
}
