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

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_xml;
use function random_int;

final readonly class FakeExtractor implements Extractor
{
    public function __construct(
        private int $total,
    ) {}

    /**
     * @return Schema
     */
    public function schema(): Schema
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

            yield rows(
                schema(
                    int_schema('int'),
                    float_schema('float'),
                    bool_schema('bool'),
                    datetime_schema('datetime'),
                    null_schema('null'),
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
                ),
                row([
                    'int' => $id,
                    'float' => type_float()->cast(generate_random_int(100, 100000) / 100),
                    'bool' => random_int(0, 1) === 1,
                    'datetime' => new DateTimeImmutable('now'),
                    'null' => null,
                    'uuid' => new FlowUuid(Uuid::uuid4()),
                    'json' => type_json()->cast(['id' => $id, 'status' => 'NEW']),
                    'list' => [1, 2, 3],
                    'list_of_datetimes' => [new DateTimeImmutable(), new DateTimeImmutable(), new DateTimeImmutable()],
                    'map' => ['NEW', 'PENDING'],
                    'struct' => [
                        'street' => 'street_' . $id,
                        'city' => 'city_' . $id,
                        'zip' => 'zip_' . $id,
                        'country' => 'country_' . $id,
                        'location' => ['lat' => 1.5, 'lon' => 1.5],
                    ],
                    'enum' => BackedStringEnum::three,
                    'xml' => type_xml()->cast('<xml><node id="' . $id . '">node-' . $id . '</node></xml>'),
                ]),
            );
        }
    }

    public function withSchema(Schema $schema): static
    {
        return $this;
    }
}
