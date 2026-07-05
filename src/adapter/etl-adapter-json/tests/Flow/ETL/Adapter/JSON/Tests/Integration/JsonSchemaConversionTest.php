<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\schema_from_json_schema;
use function Flow\ETL\Adapter\JSON\schema_to_json_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\Filesystem\DSL\path;

final class JsonSchemaConversionTest extends FlowTestCase
{
    public function test_extracting_json_file_with_schema_converted_from_json_schema(): void
    {
        $schema = schema_from_json_schema(path(__DIR__ . '/../Fixtures/json-schema/person.json'));

        $rows = data_frame()
            ->read(from_json(__DIR__ . '/../Fixtures/json-schema/people.json', schema: $schema))
            ->fetch();

        static::assertCount(2, $rows);
        static::assertSame(
            [
                [
                    'name' => 'John',
                    'age' => 30,
                    'address' => ['street' => 'Main Street 1', 'city' => 'New York', 'zip' => '10001'],
                    'location' => ['lat' => 40.7, 'lon' => -74.0],
                ],
                [
                    'name' => 'Jane',
                    'age' => null,
                    'address' => ['street' => 'Broad Street 2', 'city' => 'Los Angeles', 'zip' => '90001'],
                    'location' => ['lat' => 34.0, 'lon' => -118.2],
                ],
            ],
            $rows->toArray(),
        );
    }

    public function test_flow_schema_from_json_schema_file_converted_back_to_json_schema(): void
    {
        $jsonSchema = schema_to_json_schema(schema_from_json_schema(path(__DIR__
        . '/../Fixtures/json-schema/person.json')));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'name' => ['type' => 'string'],
                    'age' => ['type' => ['integer', 'null']],
                    'address' => [
                        'type' => 'object',
                        'properties' => [
                            'street' => ['type' => 'string'],
                            'city' => ['type' => 'string'],
                            'zip' => ['type' => 'string'],
                        ],
                        'required' => ['street', 'city'],
                    ],
                    'location' => [
                        'type' => ['object', 'null'],
                        'properties' => [
                            'lat' => ['type' => 'number'],
                            'lon' => ['type' => 'number'],
                        ],
                        'required' => ['lat', 'lon'],
                    ],
                ],
                'required' => ['name', 'address'],
            ],
            $jsonSchema,
        );
    }
}
