<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\RowsNormalizer;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_entry;
use function Flow\ETL\DSL\html_schema;
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

final class RowsNormalizerTest extends FlowTestCase
{
    public function test_normalization_nullable_entries(): void
    {
        $rows = rows(row(
            int_entry('int', null),
            float_entry('float', null),
            bool_entry('bool', null),
            datetime_entry('datetime', null),
            null_entry('null'),
            uuid_entry('uuid', null),
            json_entry('json', null),
            list_entry('list', null, type_list(type_integer())),
            list_entry('list_of_datetimes', null, type_list(type_datetime())),
            map_entry('map', null, type_map(type_integer(), type_string())),
            struct_entry('struct', null, type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'zip' => type_string(),
                'country' => type_string(),
                'location' => type_structure([
                    'lat' => type_float(),
                    'lon' => type_float(),
                ]),
            ])),
            enum_entry('enum', null),
            xml_entry('xml', null),
            html_entry('html', null),
        ));
        $schema = schema(
            int_schema('int', true),
            float_schema('float', true),
            bool_schema('bool', true),
            datetime_schema('datetime', true),
            string_schema('null', nullable: true),
            uuid_schema('uuid', true),
            json_schema('json', true),
            list_schema('list', type_list(type_integer()), true),
            list_schema('list_of_datetimes', type_list(type_datetime()), true),
            map_schema('map', type_map(type_integer(), type_string()), true),
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
                ]),
                true,
            ),
            enum_schema('enum', BackedStringEnum::class, true),
            xml_schema('xml', true),
            html_schema('html', true),
        );

        static::assertEquals(
            [
                [
                    'int' => null,
                    'float' => null,
                    'bool' => null,
                    'datetime' => null,
                    'null' => null,
                    'uuid' => null,
                    'json' => null,
                    'list' => null,
                    'list_of_datetimes' => null,
                    'map' => null,
                    'struct' => null,
                    'enum' => null,
                    'xml' => null,
                    'html' => null,
                ],
            ],
            (new RowsNormalizer())->normalize($rows, $schema),
        );
    }
}
