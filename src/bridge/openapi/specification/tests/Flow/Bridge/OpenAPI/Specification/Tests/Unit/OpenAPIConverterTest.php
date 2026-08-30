<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use Flow\Bridge\OpenAPI\Specification\Exception\InvalidArgumentException;
use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use PHPUnit\Framework\TestCase;

use function array_values;
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_from_openapi_specification;
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_to_openapi_specification;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class OpenAPIConverterTest extends TestCase
{
    public function test_bidirectional_conversion_with_complex_schema(): void
    {
        $converter = new OpenAPIConverter();

        $originalSchema = schema(
            int_schema('id', false, Metadata::empty()->add('description', 'User ID')->add('example', 123)),
            str_schema('name', true, Metadata::empty()->add('description', 'User name')),
            bool_schema('active', false),
            enum_schema('status', IntegrationTestUnitEnum::class, false),
            list_schema('tags', type_list(type_string()), true),
            structure_schema(
                'address',
                type_structure([
                    'street' => type_string(),
                    'city' => structure_element('city', type_string(), optional: true),
                ]),
                false,
            ),
        );

        $openApiSpec = $converter->toOpenAPI($originalSchema);
        $convertedSchema = $converter->fromOpenAPI($openApiSpec);

        static::assertCount(6, $convertedSchema->definitions());

        $definitions = array_values($convertedSchema->definitions());

        static::assertSame('id', $definitions[0]->entry()->name());
        static::assertSame(IntegerType::class, $definitions[0]->type()::class);
        static::assertFalse($definitions[0]->isNullable());
        static::assertTrue($definitions[0]->metadata()->has('description'));
        static::assertTrue($definitions[0]->metadata()->has('example'));

        static::assertSame('name', $definitions[1]->entry()->name());
        static::assertSame(StringType::class, $definitions[1]->type()::class);
        static::assertTrue($definitions[1]->isNullable());
        static::assertTrue($definitions[1]->metadata()->has('description'));

        static::assertSame('active', $definitions[2]->entry()->name());
        static::assertSame(BooleanType::class, $definitions[2]->type()::class);
        static::assertFalse($definitions[2]->isNullable());

        static::assertSame('tags', $definitions[4]->entry()->name());
        static::assertSame(ListType::class, $definitions[4]->type()::class);
        static::assertTrue($definitions[4]->isNullable());

        static::assertSame('address', $definitions[5]->entry()->name());
        static::assertSame(StructureType::class, $definitions[5]->type()::class);
        static::assertFalse($definitions[5]->isNullable());
    }

    public function test_converter_error_handling(): void
    {
        $converter = new OpenAPIConverter();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('OpenAPI specification must have type "object"');

        $converter->fromOpenAPI(['type' => 'array']);
    }

    public function test_dsl_functions_integration(): void
    {
        $schema = schema(
            int_schema('id', false),
            str_schema('name', true, Metadata::empty()->add('description', 'User name')),
            bool_schema('active', false),
        );

        $openApiSpec = schema_to_openapi_specification($schema);

        static::assertSame('object', $openApiSpec['type']);
        $properties = type_array()->assert($openApiSpec['properties']);
        static::assertCount(3, $properties);
        static::assertArrayHasKey('id', $properties);
        static::assertArrayHasKey('name', $properties);
        static::assertArrayHasKey('active', $properties);

        $convertedSchema = schema_from_openapi_specification($openApiSpec);

        static::assertCount(3, $convertedSchema->definitions());
        $definitions = array_values($convertedSchema->definitions());
        static::assertSame('id', $definitions[0]->entry()->name());
        static::assertSame('name', $definitions[1]->entry()->name());
        static::assertSame('active', $definitions[2]->entry()->name());

        static::assertTrue($definitions[1]->metadata()->has('description'));
        static::assertSame('User name', $definitions[1]->metadata()->get('description'));
    }

    public function test_task_requirement_example(): void
    {
        $converter = new OpenAPIConverter();

        $schema = schema(
            int_schema('id', false),
            str_schema('name', true),
            bool_schema('active', false, Metadata::empty()->add('key', 'value')),
        );

        $result = $converter->toOpenAPI($schema);

        static::assertSame(
            [
                'type' => 'object',
                'properties' => [
                    'id' => ['type' => 'integer', 'nullable' => false],
                    'name' => ['type' => 'string', 'nullable' => true],
                    'active' => ['type' => 'boolean', 'nullable' => false],
                ],
            ],
            $result,
        );

        $convertedSchema = $converter->fromOpenAPI($result);
        static::assertCount(3, $convertedSchema->definitions());
    }
}

enum IntegrationTestUnitEnum
{
    case ACTIVE;
    case INACTIVE;
    case PENDING;
}
