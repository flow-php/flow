<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use function Flow\Bridge\OpenAPI\Specification\DSL\{schema_from_openapi_specification, schema_to_openapi_specification};
use function Flow\ETL\DSL\{bool_schema, enum_schema, int_schema, list_schema, schema, str_schema, structure_schema};
use function Flow\Types\DSL\{type_array, type_list, type_string, type_structure};
use Flow\Bridge\OpenAPI\Specification\Exception\InvalidArgumentException;
use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\{ListType, StructureType};
use Flow\Types\Type\Native\{BooleanType, IntegerType, StringType};
use PHPUnit\Framework\TestCase;

final class OpenAPIConverterTest extends TestCase
{
    public function test_bidirectional_conversion_with_complex_schema() : void
    {
        $converter = new OpenAPIConverter();

        $originalSchema = schema(
            int_schema('id', false, Metadata::empty()->add('description', 'User ID')->add('example', 123)),
            str_schema('name', true, Metadata::empty()->add('description', 'User name')),
            bool_schema('active', false),
            enum_schema('status', IntegrationTestUnitEnum::class, false),
            list_schema('tags', type_list(type_string()), true),
            structure_schema('address', type_structure([
                'street' => type_string(),
            ], [
                'city' => type_string(),
            ]), false)
        );

        $openApiSpec = $converter->toOpenAPI($originalSchema);
        $convertedSchema = $converter->fromOpenAPI($openApiSpec);

        self::assertCount(6, $convertedSchema->definitions());

        $definitions = \array_values($convertedSchema->definitions());

        self::assertSame('id', $definitions[0]->entry()->name());
        self::assertSame(IntegerType::class, $definitions[0]->type()::class);
        self::assertFalse($definitions[0]->isNullable());
        self::assertTrue($definitions[0]->metadata()->has('description'));
        self::assertTrue($definitions[0]->metadata()->has('example'));

        self::assertSame('name', $definitions[1]->entry()->name());
        self::assertSame(StringType::class, $definitions[1]->type()::class);
        self::assertTrue($definitions[1]->isNullable());
        self::assertTrue($definitions[1]->metadata()->has('description'));

        self::assertSame('active', $definitions[2]->entry()->name());
        self::assertSame(BooleanType::class, $definitions[2]->type()::class);
        self::assertFalse($definitions[2]->isNullable());

        self::assertSame('tags', $definitions[4]->entry()->name());
        self::assertSame(ListType::class, $definitions[4]->type()::class);
        self::assertTrue($definitions[4]->isNullable());

        self::assertSame('address', $definitions[5]->entry()->name());
        self::assertSame(StructureType::class, $definitions[5]->type()::class);
        self::assertFalse($definitions[5]->isNullable());
    }

    public function test_converter_error_handling() : void
    {
        $converter = new OpenAPIConverter();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('OpenAPI specification must have type "object"');

        $converter->fromOpenAPI(['type' => 'array']);
    }

    public function test_dsl_functions_integration() : void
    {
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true, Metadata::empty()->add('description', 'User name')),
            bool_schema('active', false)
        );

        $openApiSpec = schema_to_openapi_specification($schema);

        self::assertSame('object', $openApiSpec['type']);
        $properties = type_array()->assert($openApiSpec['properties']);
        self::assertCount(3, $properties);
        self::assertArrayHasKey('id', $properties);
        self::assertArrayHasKey('name', $properties);
        self::assertArrayHasKey('active', $properties);

        $convertedSchema = schema_from_openapi_specification($openApiSpec);

        self::assertCount(3, $convertedSchema->definitions());
        $definitions = \array_values($convertedSchema->definitions());
        self::assertSame('id', $definitions[0]->entry()->name());
        self::assertSame('name', $definitions[1]->entry()->name());
        self::assertSame('active', $definitions[2]->entry()->name());

        self::assertTrue($definitions[1]->metadata()->has('description'));
        self::assertSame('User name', $definitions[1]->metadata()->get('description'));
    }

    public function test_task_requirement_example() : void
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

        $convertedSchema = $converter->fromOpenAPI($result);
        self::assertCount(3, $convertedSchema->definitions());
    }
}

enum IntegrationTestUnitEnum
{
    case ACTIVE;
    case INACTIVE;
    case PENDING;
}
