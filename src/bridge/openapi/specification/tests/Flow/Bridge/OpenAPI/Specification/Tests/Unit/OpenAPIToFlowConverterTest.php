<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use function Flow\Bridge\OpenAPI\Specification\DSL\{schema_from_openapi_specification, schema_to_openapi_specification};
use function Flow\ETL\DSL\{bool_schema, int_schema, schema, str_schema};
use Flow\Bridge\OpenAPI\Specification\Exception\InvalidArgumentException;
use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\Types\Type\Logical\{DateTimeType, DateType, JsonType, ListType, MapType, StructureType, TimeType, UuidType, XMLType};
use Flow\Types\Type\Native\{BooleanType, IntegerType, StringType};
use PHPUnit\Framework\TestCase;

final class OpenAPIToFlowConverterTest extends TestCase
{
    public function test_bidirectional_conversion_consistency() : void
    {
        $originalSchema = schema(
            int_schema('id', false),
            str_schema('name', true),
            bool_schema('active', false)
        );

        $openApiSpec = schema_to_openapi_specification($originalSchema);
        $convertedSchema = schema_from_openapi_specification($openApiSpec);

        self::assertCount(3, $convertedSchema->definitions());
        $definitions = \array_values($convertedSchema->definitions());
        self::assertSame('id', $definitions[0]->entry()->name());
        self::assertSame(IntegerType::class, $definitions[0]->type()::class);
        self::assertFalse($definitions[0]->isNullable());

        self::assertSame('name', $definitions[1]->entry()->name());
        self::assertSame(StringType::class, $definitions[1]->type()::class);
        self::assertTrue($definitions[1]->isNullable());

        self::assertSame('active', $definitions[2]->entry()->name());
        self::assertSame(BooleanType::class, $definitions[2]->type()::class);
        self::assertFalse($definitions[2]->isNullable());
    }

    public function test_from_openapi_spec_dsl_function() : void
    {
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
            ],
        ];

        $schema = schema_from_openapi_specification($openApiSpec);

        self::assertCount(2, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        self::assertSame('id', $definitions[0]->entry()->name());
        self::assertSame('name', $definitions[1]->entry()->name());
    }

    public function test_from_openapi_throws_exception_for_invalid_property_spec() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'invalid_prop' => 'not_an_array',
            ],
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Property 'invalid_prop' specification must be an array");

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_throws_exception_for_invalid_type() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'array',
            'properties' => [],
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('OpenAPI specification must have type "object"');

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_throws_exception_for_missing_properties() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('OpenAPI specification must have properties array');

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_throws_exception_for_missing_property_type() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'no_type' => ['nullable' => false],
            ],
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Property 'no_type' must have a type");

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_throws_exception_for_unsupported_type() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'unsupported' => ['type' => 'binary'],
            ],
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported OpenAPI type: binary');

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_with_array_type() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'tags' => [
                    'type' => 'array',
                    'items' => ['type' => 'string'],
                    'nullable' => false,
                ],
            ],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(1, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        $definition = $definitions[0];
        self::assertSame('tags', $definition->entry()->name());
        self::assertSame(ListType::class, $definition->type()::class);
        self::assertFalse($definition->isNullable());
    }

    public function test_from_openapi_with_basic_types() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
                'active' => ['type' => 'boolean', 'nullable' => false],
                'price' => ['type' => 'number', 'nullable' => true],
            ],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(4, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        self::assertSame('id', $definitions[0]->entry()->name());
        self::assertSame(IntegerType::class, $definitions[0]->type()::class);
        self::assertFalse($definitions[0]->isNullable());

        self::assertSame('name', $definitions[1]->entry()->name());
        self::assertSame(StringType::class, $definitions[1]->type()::class);
        self::assertTrue($definitions[1]->isNullable());
    }

    public function test_from_openapi_with_complex_schema() : void
    {
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true, 'description' => 'User name'],
                'active' => ['type' => 'boolean', 'nullable' => false, 'example' => true],
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
        ];

        $schema = schema_from_openapi_specification($openApiSpec);

        self::assertCount(5, $schema->definitions());
        $definitions = \array_values($schema->definitions());

        self::assertSame('id', $definitions[0]->entry()->name());
        self::assertSame(IntegerType::class, $definitions[0]->type()::class);
        self::assertFalse($definitions[0]->isNullable());

        self::assertSame('name', $definitions[1]->entry()->name());
        self::assertSame(StringType::class, $definitions[1]->type()::class);
        self::assertTrue($definitions[1]->isNullable());
        self::assertTrue($definitions[1]->metadata()->has('description'));
        self::assertSame('User name', $definitions[1]->metadata()->get('description'));

        self::assertSame('active', $definitions[2]->entry()->name());
        self::assertSame(BooleanType::class, $definitions[2]->type()::class);
        self::assertFalse($definitions[2]->isNullable());
        self::assertTrue($definitions[2]->metadata()->has('example'));
        self::assertTrue($definitions[2]->metadata()->get('example'));

        self::assertSame('tags', $definitions[3]->entry()->name());
        self::assertSame(ListType::class, $definitions[3]->type()::class);
        self::assertTrue($definitions[3]->isNullable());

        self::assertSame('address', $definitions[4]->entry()->name());
        self::assertSame(StructureType::class, $definitions[4]->type()::class);
        self::assertFalse($definitions[4]->isNullable());
    }

    public function test_from_openapi_with_empty_properties() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(0, $schema->definitions());
    }

    public function test_from_openapi_with_metadata() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'name' => [
                    'type' => 'string',
                    'nullable' => false,
                    'description' => 'User name',
                    'example' => 'John Doe',
                ],
            ],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(1, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        $definition = $definitions[0];
        self::assertSame('name', $definition->entry()->name());
        self::assertTrue($definition->metadata()->has('description'));
        self::assertSame('User name', $definition->metadata()->get('description'));
        self::assertTrue($definition->metadata()->has('example'));
        self::assertSame('John Doe', $definition->metadata()->get('example'));
    }

    public function test_from_openapi_with_nested_structure() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'address' => [
                    'type' => 'object',
                    'properties' => [
                        'street' => ['type' => 'string', 'nullable' => false],
                        'city' => ['type' => 'string', 'nullable' => true],
                    ],
                    'nullable' => false,
                ],
            ],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(1, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        $definition = $definitions[0];
        self::assertSame('address', $definition->entry()->name());
        self::assertSame(StructureType::class, $definition->type()::class);
        self::assertFalse($definition->isNullable());
    }

    public function test_from_openapi_with_object_with_additional_properties() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'metadata' => [
                    'type' => 'object',
                    'additionalProperties' => ['type' => 'integer'],
                    'nullable' => false,
                ],
            ],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(1, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        $definition = $definitions[0];
        self::assertSame('metadata', $definition->entry()->name());
        self::assertSame(MapType::class, $definition->type()::class);
        self::assertFalse($definition->isNullable());
    }

    public function test_from_openapi_with_special_string_formats() : void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'birth_date' => ['type' => 'string', 'format' => 'date', 'nullable' => false],
                'created_at' => ['type' => 'string', 'format' => 'date-time', 'nullable' => false],
                'duration' => ['type' => 'string', 'format' => 'time', 'nullable' => false],
                'uuid' => ['type' => 'string', 'format' => 'uuid', 'nullable' => false],
                'data' => ['type' => 'string', 'format' => 'json', 'nullable' => false],
                'xml' => ['type' => 'string', 'format' => 'xml', 'nullable' => false],
            ],
        ];

        $schema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(6, $schema->definitions());
        $definitions = \array_values($schema->definitions());
        self::assertSame(DateType::class, $definitions[0]->type()::class);
        self::assertSame(DateTimeType::class, $definitions[1]->type()::class);
        self::assertSame(TimeType::class, $definitions[2]->type()::class);
        self::assertSame(UuidType::class, $definitions[3]->type()::class);
        self::assertSame(JsonType::class, $definitions[4]->type()::class);
        self::assertSame(XMLType::class, $definitions[5]->type()::class);
    }
}
