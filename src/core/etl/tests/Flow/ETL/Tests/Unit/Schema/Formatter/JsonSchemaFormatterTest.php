<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter;

use function Flow\ETL\DSL\{bool_schema,
    datetime_schema,
    enum_schema,
    float_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    schema,
    string_schema,
    structure_schema,
    uuid_schema,
    xml_schema};
use function Flow\Types\DSL\{type_datetime, type_float, type_integer, type_list, type_map, type_string, type_structure};
use Flow\ETL\Schema\Formatter\JsonSchemaFormatter;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;

final class JsonSchemaFormatterTest extends FlowTestCase
{
    public function test_formatting_empty_schema() : void
    {
        self::assertEquals(
            '[]',
            (new JsonSchemaFormatter())->format(schema())
        );
    }

    public function test_formatting_schema() : void
    {
        self::assertEquals(
            <<<'JSON'
[{"ref":"int","type":{"type":"integer"},"nullable":false,"metadata":[]},{"ref":"float","type":{"type":"float"},"nullable":false,"metadata":[]},{"ref":"bool","type":{"type":"boolean"},"nullable":false,"metadata":[]},{"ref":"datetime","type":{"type":"datetime"},"nullable":false,"metadata":[]},{"ref":"null","type":{"type":"string"},"nullable":true,"metadata":[]},{"ref":"uuid","type":{"type":"uuid"},"nullable":false,"metadata":[]},{"ref":"json","type":{"type":"json"},"nullable":false,"metadata":[]},{"ref":"list","type":{"type":"list","element":{"type":"integer"}},"nullable":false,"metadata":{"foo":"bar"}},{"ref":"list_of_datetimes","type":{"type":"list","element":{"type":"datetime"}},"nullable":false,"metadata":[]},{"ref":"map","type":{"type":"map","key":{"type":"integer"},"value":{"type":"string"}},"nullable":false,"metadata":[]},{"ref":"struct","type":{"type":"structure","elements":{"street":{"type":"string"},"city":{"type":"string"},"zip":{"type":"string"},"country":{"type":"string"},"location":{"type":"structure","elements":{"lat":{"type":"float"},"lon":{"type":"float"}}}}},"nullable":false,"metadata":[]},{"ref":"enum","type":{"type":"enum","class":"Flow\\ETL\\Tests\\Fixtures\\Enum\\BackedStringEnum"},"nullable":false,"metadata":[]},{"ref":"xml","type":{"type":"xml"},"nullable":false,"metadata":[]}]
JSON,
            (new JsonSchemaFormatter())->format(
                schema(
                    int_schema('int'),
                    float_schema('float'),
                    bool_schema('bool'),
                    datetime_schema('datetime'),
                    string_schema('null', nullable: true),
                    uuid_schema('uuid'),
                    json_schema('json'),
                    list_schema('list', type_list(type_integer()), metadata: Metadata::fromArray(['foo' => 'bar'])),
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
                )
            )
        );
    }
}
