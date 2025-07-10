<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use function Flow\Bridge\OpenAPI\Specification\DSL\schema_to_openapi_specification;
use function Flow\ETL\DSL\{bool_schema, date_schema, datetime_schema, enum_schema, float_schema, int_schema, json_schema, list_schema, map_schema, schema, str_schema, structure_schema, time_schema, uuid_schema, xml_element_schema, xml_schema};
use function Flow\Types\DSL\{type_array, type_callable, type_integer, type_list, type_map, type_string, type_structure};
use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\ETL\Schema\{Definition, Metadata};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FlowToOpenAPIConverterTest extends TestCase
{
    public static function basic_types_provider() : \Generator
    {
        yield 'boolean non-nullable' => [
            bool_schema('active', false),
            ['type' => 'boolean', 'nullable' => false],
        ];

        yield 'boolean nullable' => [
            bool_schema('active', true),
            ['type' => 'boolean', 'nullable' => true],
        ];

        yield 'integer non-nullable' => [
            int_schema('id', false),
            ['type' => 'integer', 'nullable' => false],
        ];

        yield 'integer nullable' => [
            int_schema('id', true),
            ['type' => 'integer', 'nullable' => true],
        ];

        yield 'float non-nullable' => [
            float_schema('price', false),
            ['type' => 'number', 'nullable' => false],
        ];

        yield 'float nullable' => [
            float_schema('price', true),
            ['type' => 'number', 'nullable' => true],
        ];

        yield 'string non-nullable' => [
            str_schema('name', false),
            ['type' => 'string', 'nullable' => false],
        ];

        yield 'string nullable' => [
            str_schema('name', true),
            ['type' => 'string', 'nullable' => true],
        ];
    }

    public static function special_types_provider() : \Generator
    {
        yield 'date non-nullable' => [
            date_schema('birth_date', false),
            ['type' => 'string', 'format' => 'date', 'nullable' => false],
        ];

        yield 'date nullable' => [
            date_schema('birth_date', true),
            ['type' => 'string', 'format' => 'date', 'nullable' => true],
        ];

        yield 'datetime non-nullable' => [
            datetime_schema('created_at', false),
            ['type' => 'string', 'format' => 'date-time', 'nullable' => false],
        ];

        yield 'datetime nullable' => [
            datetime_schema('created_at', true),
            ['type' => 'string', 'format' => 'date-time', 'nullable' => true],
        ];

        yield 'time non-nullable' => [
            time_schema('duration', false),
            ['type' => 'string', 'format' => 'time', 'nullable' => false],
        ];

        yield 'time nullable' => [
            time_schema('duration', true),
            ['type' => 'string', 'format' => 'time', 'nullable' => true],
        ];

        yield 'uuid non-nullable' => [
            uuid_schema('uuid', false),
            ['type' => 'string', 'format' => 'uuid', 'nullable' => false],
        ];

        yield 'uuid nullable' => [
            uuid_schema('uuid', true),
            ['type' => 'string', 'format' => 'uuid', 'nullable' => true],
        ];

        yield 'json non-nullable' => [
            json_schema('data', false),
            ['type' => 'string', 'format' => 'json', 'nullable' => false],
        ];

        yield 'json nullable' => [
            json_schema('data', true),
            ['type' => 'string', 'format' => 'json', 'nullable' => true],
        ];

        yield 'xml non-nullable' => [
            xml_schema('xml', false),
            ['type' => 'string', 'format' => 'xml', 'nullable' => false],
        ];

        yield 'xml nullable' => [
            xml_schema('xml', true),
            ['type' => 'string', 'format' => 'xml', 'nullable' => true],
        ];

        yield 'xml_element non-nullable' => [
            xml_element_schema('xml_element', false),
            ['type' => 'string', 'format' => 'xml', 'nullable' => false],
        ];

        yield 'xml_element nullable' => [
            xml_element_schema('xml_element', true),
            ['type' => 'string', 'format' => 'xml', 'nullable' => true],
        ];
    }

    public function test_to_open_api_enum_edge_cases() : void
    {
        $converter = new OpenAPIConverter();
        $definition = enum_schema('single_enum', FlowTestSingleEnum::class, false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'single_enum' => [
                    'type' => 'string',
                    'enum' => ['ONLY'],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_preserves_definition_order() : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            str_schema('c'),
            str_schema('a'),
            str_schema('b')
        );

        $result = $converter->toOpenAPI($schema);

        $properties = type_array()->assert($result['properties']);
        self::assertSame(['c', 'a', 'b'], \array_keys($properties));
    }

    public function test_to_open_api_with_array_type() : void
    {
        $converter = new OpenAPIConverter();
        $arrayType = type_array();
        $definition = map_schema('items', $arrayType, false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'items' => [
                    'type' => 'array',
                    'items' => ['type' => 'string'],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_backed_enum() : void
    {
        $converter = new OpenAPIConverter();
        $definition = enum_schema('priority', FlowTestBackedEnum::class, false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'priority' => [
                    'type' => 'string',
                    'enum' => ['high', 'low', 'medium'],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_backed_enum_different_types() : void
    {
        $converter = new OpenAPIConverter();
        $definition = enum_schema('int_enum', FlowTestIntBackedEnum::class, false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'int_enum' => [
                    'type' => 'string',
                    'enum' => [1, 2, 3],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    /**
     * @param Definition<mixed> $definition
     * @param array<string, mixed> $expected
     */
    #[DataProvider('basic_types_provider')]
    public function test_to_open_api_with_basic_types(Definition $definition, array $expected) : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                $definition->entry()->name() => $expected,
            ],
        ], $result);
    }

    public function test_to_open_api_with_complex_schema() : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true, Metadata::empty()->add('description', 'User name')),
            bool_schema('active', false, Metadata::empty()->add('example', true)),
            enum_schema('status', FlowTestUnitEnum::class, false),
            list_schema('tags', type_list(type_string()), true),
            structure_schema('address', type_structure([
                'street' => type_string(),
            ], [
                'city' => type_string(),
            ]), false)
        );

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true, 'description' => 'User name'],
                'active' => ['type' => 'boolean', 'nullable' => false, 'example' => true],
                'status' => ['type' => 'string', 'enum' => ['ACTIVE', 'INACTIVE', 'PENDING'], 'nullable' => false],
                'tags' => ['type' => 'array', 'items' => ['type' => 'string'], 'nullable' => true],
                'address' => [
                    'type' => 'object',
                    'properties' => [
                        'street' => ['type' => 'string', 'nullable' => false],
                        'city' => ['type' => 'string', 'nullable' => true],
                    ],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_deeply_nested_structures() : void
    {
        $converter = new OpenAPIConverter();
        $definition = structure_schema('nested', type_structure([
            'level1' => type_structure([
                'level2' => type_structure([
                    'value' => type_integer(),
                ]),
            ]),
        ]), false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'nested' => [
                    'type' => 'object',
                    'properties' => [
                        'level1' => [
                            'type' => 'object',
                            'properties' => [
                                'level2' => [
                                    'type' => 'object',
                                    'properties' => [
                                        'value' => ['type' => 'integer', 'nullable' => false],
                                    ],
                                    'nullable' => false,
                                ],
                            ],
                            'nullable' => false,
                        ],
                    ],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_empty_schema() : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema();

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [],
        ], $result);
    }

    public function test_to_open_api_with_list_type() : void
    {
        $converter = new OpenAPIConverter();
        $definition = list_schema('tags', type_list(type_string()), false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'tags' => [
                    'type' => 'array',
                    'items' => ['type' => 'string'],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_map_type() : void
    {
        $converter = new OpenAPIConverter();
        $definition = map_schema('metadata', type_map(type_string(), type_integer()), false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'metadata' => [
                    'type' => 'object',
                    'additionalProperties' => ['type' => 'integer'],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_metadata() : void
    {
        $converter = new OpenAPIConverter();
        $definition = str_schema(
            'name',
            false,
            Metadata::empty()
                ->add('description', 'User full name')
                ->add('example', 'John Doe')
        );
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'name' => [
                    'type' => 'string',
                    'nullable' => false,
                    'description' => 'User full name',
                    'example' => 'John Doe',
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_metadata_description_only() : void
    {
        $converter = new OpenAPIConverter();
        $definition = str_schema(
            'name',
            false,
            Metadata::empty()->add('description', 'User name')
        );
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'name' => [
                    'type' => 'string',
                    'nullable' => false,
                    'description' => 'User name',
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_metadata_example_only() : void
    {
        $converter = new OpenAPIConverter();
        $definition = int_schema(
            'age',
            false,
            Metadata::empty()->add('example', 25)
        );
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'age' => [
                    'type' => 'integer',
                    'nullable' => false,
                    'example' => 25,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_metadata_other_keys() : void
    {
        $converter = new OpenAPIConverter();
        $definition = str_schema(
            'name',
            false,
            Metadata::empty()->add('custom_key', 'custom_value')
        );
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'name' => [
                    'type' => 'string',
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_mixed_metadata_values() : void
    {
        $converter = new OpenAPIConverter();
        $definition = str_schema(
            'mixed_meta',
            false,
            Metadata::empty()
                ->add('description', 'A description')
                ->add('example', 42) // Integer example for string field
                ->add('other_prop', ['array', 'value'])
                ->add('bool_prop', true)
        );
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'mixed_meta' => [
                    'type' => 'string',
                    'nullable' => false,
                    'description' => 'A description',
                    'example' => 42,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_multiple_definitions() : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true),
            bool_schema('active', false),
            float_schema('price', true)
        );

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
                'active' => ['type' => 'boolean', 'nullable' => false],
                'price' => ['type' => 'number', 'nullable' => true],
            ],
        ], $result);
    }

    public function test_to_open_api_with_nested_list_type() : void
    {
        $converter = new OpenAPIConverter();
        $definition = list_schema('nested_tags', type_list(type_list(type_integer())), false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'nested_tags' => [
                    'type' => 'array',
                    'items' => [
                        'type' => 'array',
                        'items' => ['type' => 'integer'],
                    ],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_special_field_names() : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            str_schema('field-with-dashes', false),
            str_schema('field_with_underscores', false),
            str_schema('fieldWithCamelCase', false),
            str_schema('FIELD_WITH_CAPS', false),
            str_schema('field123', false),
            str_schema('123field', false), // Edge case: field starting with number
        );

        $result = $converter->toOpenAPI($schema);

        $properties = type_array()->assert($result['properties']);
        self::assertArrayHasKey('field-with-dashes', $properties);
        self::assertArrayHasKey('field_with_underscores', $properties);
        self::assertArrayHasKey('fieldWithCamelCase', $properties);
        self::assertArrayHasKey('FIELD_WITH_CAPS', $properties);
        self::assertArrayHasKey('field123', $properties);
        self::assertArrayHasKey('123field', $properties);
    }

    /**
     * @param Definition<mixed> $definition
     * @param array<string, mixed> $expected
     */
    #[DataProvider('special_types_provider')]
    public function test_to_open_api_with_special_types(Definition $definition, array $expected) : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                $definition->entry()->name() => $expected,
            ],
        ], $result);
    }

    public function test_to_open_api_with_structure_only_optional_elements() : void
    {
        $converter = new OpenAPIConverter();
        $definition = structure_schema('optional_only', type_structure([], [
            'optional_field' => type_string(),
        ]), false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'optional_only' => [
                    'type' => 'object',
                    'properties' => [
                        'optional_field' => ['type' => 'string', 'nullable' => true],
                    ],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_structure_type() : void
    {
        $converter = new OpenAPIConverter();
        $definition = structure_schema('address', type_structure([
            'street' => type_string(),
            'city' => type_string(),
        ], [
            'postal_code' => type_string(),
        ]), false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'address' => [
                    'type' => 'object',
                    'properties' => [
                        'street' => ['type' => 'string', 'nullable' => false],
                        'city' => ['type' => 'string', 'nullable' => false],
                        'postal_code' => ['type' => 'string', 'nullable' => true],
                    ],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_task_example_schema() : void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true),
            bool_schema('active', false, Metadata::empty()->add('key', 'value'))
        );

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
                'active' => ['type' => 'boolean', 'nullable' => false],
            ],
        ], $result);
    }

    public function test_to_open_api_with_unit_enum() : void
    {
        $converter = new OpenAPIConverter();
        $definition = enum_schema('status', FlowTestUnitEnum::class, false);
        $schema = schema($definition);

        $result = $converter->toOpenAPI($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'status' => [
                    'type' => 'string',
                    'enum' => ['ACTIVE', 'INACTIVE', 'PENDING'],
                    'nullable' => false,
                ],
            ],
        ], $result);
    }

    public function test_to_open_api_with_unsupported_type() : void
    {
        $converter = new OpenAPIConverter();
        $unsupportedType = type_callable();
        $definition = map_schema('callback', type_map(type_string(), $unsupportedType), false);
        $schema = schema($definition);

        $this->expectExceptionMessage('Unsupported type: Flow\Types\Type\Native\CallableType');
        $converter->toOpenAPI($schema);
    }

    public function test_to_openapi_spec_dsl_function() : void
    {
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true)
        );

        $result = schema_to_openapi_specification($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
            ],
        ], $result);
    }

    public function test_to_openapi_spec_dsl_usage_example() : void
    {
        // This test demonstrates practical usage of the DSL function
        // for creating API documentation from Flow schemas
        $userSchema = schema(
            int_schema(
                'id',
                false,
                Metadata::empty()
                ->add('description', 'Unique user identifier')
                ->add('example', 123)
            ),
            str_schema(
                'username',
                false,
                Metadata::empty()
                ->add('description', 'Username for login')
                ->add('example', 'johndoe')
            ),
            str_schema(
                'email',
                false,
                Metadata::empty()
                ->add('description', 'User email address')
                ->add('example', 'john@example.com')
            ),
            bool_schema(
                'is_active',
                false,
                Metadata::empty()
                ->add('description', 'Whether the user account is active')
                ->add('example', true)
            ),
            enum_schema(
                'role',
                FlowTestBackedEnum::class,
                false,
                Metadata::empty()
                ->add('description', 'User role level')
            ),
            list_schema(
                'permissions',
                type_list(type_string()),
                true,
                Metadata::empty()
                ->add('description', 'List of user permissions')
                ->add('example', ['read', 'write'])
            )
        );

        $openApiSpec = schema_to_openapi_specification($userSchema);

        self::assertSame('object', $openApiSpec['type']);
        $properties = type_array()->assert($openApiSpec['properties']);
        self::assertCount(6, $properties);

        $id = type_array()->assert($properties['id']);
        self::assertSame('integer', $id['type']);
        self::assertSame('Unique user identifier', $id['description']);
        self::assertSame(123, $id['example']);

        $role = type_array()->assert($properties['role']);
        self::assertSame('string', $role['type']);
        self::assertSame(['high', 'low', 'medium'], $role['enum']);
        self::assertSame('User role level', $role['description']);

        $permissions = type_array()->assert($properties['permissions']);
        self::assertSame('array', $permissions['type']);
        self::assertTrue($permissions['nullable']);
        self::assertSame('List of user permissions', $permissions['description']);
    }

    public function test_to_openapi_spec_dsl_with_complex_schema() : void
    {
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true, Metadata::empty()->add('description', 'User name')),
            enum_schema('status', FlowTestUnitEnum::class, false),
            list_schema('tags', type_list(type_string()), true)
        );

        $result = schema_to_openapi_specification($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true, 'description' => 'User name'],
                'status' => ['type' => 'string', 'enum' => ['ACTIVE', 'INACTIVE', 'PENDING'], 'nullable' => false],
                'tags' => ['type' => 'array', 'items' => ['type' => 'string'], 'nullable' => true],
            ],
        ], $result);
    }

    public function test_to_openapi_spec_dsl_with_metadata() : void
    {
        $schema = schema(
            str_schema(
                'email',
                false,
                Metadata::empty()
                ->add('description', 'User email address')
                ->add('example', 'user@example.com')
            )
        );

        $result = schema_to_openapi_specification($schema);

        self::assertSame([
            'type' => 'object',
            'properties' => [
                'email' => [
                    'type' => 'string',
                    'nullable' => false,
                    'description' => 'User email address',
                    'example' => 'user@example.com',
                ],
            ],
        ], $result);
    }
}

enum FlowTestUnitEnum
{
    case ACTIVE;
    case INACTIVE;
    case PENDING;
}

enum FlowTestBackedEnum : string
{
    case HIGH = 'high';
    case LOW = 'low';
    case MEDIUM = 'medium';
}

enum FlowTestSingleEnum
{
    case ONLY;
}

enum FlowTestIntBackedEnum : int
{
    case FIRST = 1;
    case SECOND = 2;
    case THIRD = 3;
}
