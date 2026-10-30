<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use Flow\Bridge\OpenAPI\Specification\Exception\InvalidArgumentException;
use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use PHPUnit\Framework\TestCase;

use function array_values;
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_from_openapi_specification;
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_to_openapi_specification;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class OpenAPIToFlowConverterTest extends TestCase
{
    public function test_bidirectional_conversion_consistency(): void
    {
        $convertedSchema = schema_from_openapi_specification(schema_to_openapi_specification(schema(
            int_schema('id', false),
            str_schema('name', true),
            bool_schema('active', false),
        )));

        static::assertCount(3, $convertedSchema->definitions());
        $definitions = array_values($convertedSchema->definitions());
        static::assertSame('id', $definitions[0]->name());
        static::assertSame(IntegerType::class, $definitions[0]->type()::class);
        static::assertFalse($definitions[0]->isNullable());

        static::assertSame('name', $definitions[1]->name());
        static::assertSame(StringType::class, $definitions[1]->type()::class);
        static::assertTrue($definitions[1]->isNullable());

        static::assertSame('active', $definitions[2]->name());
        static::assertSame(BooleanType::class, $definitions[2]->type()::class);
        static::assertFalse($definitions[2]->isNullable());
    }

    public function test_from_openapi_spec_dsl_function(): void
    {
        $schema = schema_from_openapi_specification([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
            ],
        ]);

        static::assertCount(2, $schema->definitions());
        $definitions = array_values($schema->definitions());
        static::assertSame('id', $definitions[0]->name());
        static::assertSame('name', $definitions[1]->name());
    }

    public function test_from_openapi_throws_exception_for_invalid_property_spec(): void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
            'properties' => [
                'invalid_prop' => 'not_an_array',
            ],
        ];

        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage(
            'Expected type "map<string, array<mixed>>", got "structure{invalid_prop: string}".',
        );

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_throws_exception_for_invalid_type(): void
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

    public function test_from_openapi_throws_exception_for_missing_properties(): void
    {
        $converter = new OpenAPIConverter();
        $openApiSpec = [
            'type' => 'object',
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('OpenAPI specification must have properties array');

        $converter->fromOpenAPI($openApiSpec);
    }

    public function test_from_openapi_throws_exception_for_missing_property_type(): void
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

    public function test_from_openapi_throws_exception_for_unsupported_type(): void
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

    public function test_from_openapi_with_array_type(): void
    {
        $schema = (new OpenAPIConverter())->fromOpenAPI([
            'type' => 'object',
            'properties' => [
                'tags' => [
                    'type' => 'array',
                    'items' => ['type' => 'string'],
                    'nullable' => false,
                ],
            ],
        ]);

        static::assertCount(1, $schema->definitions());
        $definition = array_values($schema->definitions())[0];
        static::assertSame('tags', $definition->name());
        static::assertSame(ListType::class, $definition->type()::class);
        static::assertFalse($definition->isNullable());
    }

    public function test_from_openapi_with_basic_types(): void
    {
        $schema = (new OpenAPIConverter())->fromOpenAPI([
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer', 'nullable' => false],
                'name' => ['type' => 'string', 'nullable' => true],
                'active' => ['type' => 'boolean', 'nullable' => false],
                'price' => ['type' => 'number', 'nullable' => true],
            ],
        ]);

        static::assertCount(4, $schema->definitions());
        $definitions = array_values($schema->definitions());
        static::assertSame('id', $definitions[0]->name());
        static::assertSame(IntegerType::class, $definitions[0]->type()::class);
        static::assertFalse($definitions[0]->isNullable());

        static::assertSame('name', $definitions[1]->name());
        static::assertSame(StringType::class, $definitions[1]->type()::class);
        static::assertTrue($definitions[1]->isNullable());
    }

    public function test_from_openapi_with_complex_schema(): void
    {
        $schema = schema_from_openapi_specification([
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
        ]);

        static::assertCount(5, $schema->definitions());
        $definitions = array_values($schema->definitions());

        static::assertSame('id', $definitions[0]->name());
        static::assertSame(IntegerType::class, $definitions[0]->type()::class);
        static::assertFalse($definitions[0]->isNullable());

        static::assertSame('name', $definitions[1]->name());
        static::assertSame(StringType::class, $definitions[1]->type()::class);
        static::assertTrue($definitions[1]->isNullable());
        static::assertTrue($definitions[1]->metadata()->has('description'));
        static::assertSame('User name', $definitions[1]->metadata()->get('description'));

        static::assertSame('active', $definitions[2]->name());
        static::assertSame(BooleanType::class, $definitions[2]->type()::class);
        static::assertFalse($definitions[2]->isNullable());
        static::assertTrue($definitions[2]->metadata()->has('example'));
        static::assertTrue($definitions[2]->metadata()->get('example'));

        static::assertSame('tags', $definitions[3]->name());
        static::assertSame(ListType::class, $definitions[3]->type()::class);
        static::assertTrue($definitions[3]->isNullable());

        static::assertSame('address', $definitions[4]->name());
        static::assertSame(StructureType::class, $definitions[4]->type()::class);
        static::assertFalse($definitions[4]->isNullable());
    }

    public function test_from_openapi_with_empty_properties(): void
    {
        static::assertCount(
            0,
            (new OpenAPIConverter())->fromOpenAPI([
                'type' => 'object',
                'properties' => [],
            ])->definitions(),
        );
    }

    public function test_from_openapi_with_metadata(): void
    {
        $schema = (new OpenAPIConverter())->fromOpenAPI([
            'type' => 'object',
            'properties' => [
                'name' => [
                    'type' => 'string',
                    'nullable' => false,
                    'description' => 'User name',
                    'example' => 'John Doe',
                ],
            ],
        ]);

        static::assertCount(1, $schema->definitions());
        $definition = array_values($schema->definitions())[0];
        static::assertSame('name', $definition->name());
        static::assertTrue($definition->metadata()->has('description'));
        static::assertSame('User name', $definition->metadata()->get('description'));
        static::assertTrue($definition->metadata()->has('example'));
        static::assertSame('John Doe', $definition->metadata()->get('example'));
    }

    public function test_from_openapi_with_nested_structure(): void
    {
        $schema = (new OpenAPIConverter())->fromOpenAPI([
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
        ]);

        static::assertCount(1, $schema->definitions());
        $definition = array_values($schema->definitions())[0];
        static::assertSame('address', $definition->name());
        static::assertSame(StructureType::class, $definition->type()::class);
        static::assertFalse($definition->isNullable());
    }

    public function test_from_openapi_with_object_with_additional_properties(): void
    {
        $schema = (new OpenAPIConverter())->fromOpenAPI([
            'type' => 'object',
            'properties' => [
                'metadata' => [
                    'type' => 'object',
                    'additionalProperties' => ['type' => 'integer'],
                    'nullable' => false,
                ],
            ],
        ]);

        static::assertCount(1, $schema->definitions());
        $definition = array_values($schema->definitions())[0];
        static::assertSame('metadata', $definition->name());
        static::assertSame(MapType::class, $definition->type()::class);
        static::assertFalse($definition->isNullable());
    }

    public function test_from_openapi_with_special_string_formats(): void
    {
        $schema = (new OpenAPIConverter())->fromOpenAPI([
            'type' => 'object',
            'properties' => [
                'birth_date' => ['type' => 'string', 'format' => 'date', 'nullable' => false],
                'created_at' => ['type' => 'string', 'format' => 'date-time', 'nullable' => false],
                'duration' => ['type' => 'string', 'format' => 'time', 'nullable' => false],
                'uuid' => ['type' => 'string', 'format' => 'uuid', 'nullable' => false],
                'data' => ['type' => 'string', 'format' => 'json', 'nullable' => false],
                'xml' => ['type' => 'string', 'format' => 'xml', 'nullable' => false],
            ],
        ]);

        static::assertCount(6, $schema->definitions());
        $definitions = array_values($schema->definitions());
        static::assertSame(DateType::class, $definitions[0]->type()::class);
        static::assertSame(DateTimeType::class, $definitions[1]->type()::class);
        static::assertSame(TimeType::class, $definitions[2]->type()::class);
        static::assertSame(UuidType::class, $definitions[3]->type()::class);
        static::assertSame(JsonType::class, $definitions[4]->type()::class);
        static::assertSame(XMLType::class, $definitions[5]->type()::class);
    }
}
