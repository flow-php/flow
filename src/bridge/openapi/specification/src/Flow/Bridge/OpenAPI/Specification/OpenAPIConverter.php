<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification;

use BackedEnum;
use Flow\Bridge\OpenAPI\Specification\Exception\InvalidArgumentException;
use Flow\Bridge\OpenAPI\Specification\Exception\RuntimeException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use UnitEnum;

use function array_map;
use function enum_exists;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

/**
 * Bidirectional converter between Flow PHP schemas and OpenAPI 3.0 specifications.
 *
 * This converter enables you to:
 * - Generate OpenAPI documentation from Flow schemas
 * - Parse OpenAPI specifications to create Flow schemas
 * - Preserve metadata (descriptions, examples) during conversion
 * - Support all common data types and structures
 *
 * @example Basic Flow to OpenAPI conversion
 * ```php
 * $schema = schema(
 *     int_schema('id', false),
 *     str_schema('name', true)
 * );
 * $openApi = $converter->toOpenAPI($schema);
 * ```
 * @example Basic OpenAPI to Flow conversion
 * ```php
 * $openApiSpec = [
 *     'type' => 'object',
 *     'properties' => [
 *         'id' => ['type' => 'integer', 'nullable' => false],
 *         'name' => ['type' => 'string', 'nullable' => true]
 *     ]
 * ];
 * $schema = $converter->fromOpenAPI($openApiSpec);
 * ```
 */
final class OpenAPIConverter
{
    /**
     * @param array<string, mixed> $openApiSpec OpenAPI object specification with 'type' and 'properties'
     *
     * @throws InvalidArgumentException When the specification is invalid or unsupported
     */
    public function fromOpenAPI(array $openApiSpec): Schema
    {
        if (!isset($openApiSpec['type']) || $openApiSpec['type'] !== 'object') {
            throw new InvalidArgumentException('OpenAPI specification must have type "object"');
        }

        if (!isset($openApiSpec['properties']) || !is_array($openApiSpec['properties'])) {
            throw new InvalidArgumentException('OpenAPI specification must have properties array');
        }

        $properties = type_map(type_string(), type_array())->assert($openApiSpec['properties']);
        $definitions = [];

        foreach ($properties as $propertyName => $propertySpec) {
            /** @var array<string, mixed> $propertySpec */
            $definitions[] = $this->convertOpenAPIPropertyToDefinition($propertyName, $propertySpec);
        }

        return new Schema(...$definitions);
    }

    /**
     * @param Schema $schema Flow schema containing field definitions
     *
     * @return array<string, mixed> OpenAPI object specification with 'type' and 'properties'
     */
    public function toOpenAPI(Schema $schema): array
    {
        $properties = [];

        foreach ($schema->definitions() as $definition) {
            $properties[$definition->entry()->name()] = $this->convertDefinitionToOpenAPI($definition);
        }

        return [
            'type' => 'object',
            'properties' => $properties,
        ];
    }

    /**
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function convertArrayToOpenAPI(Type $type): array
    {
        return [
            'type' => 'array',
            'items' => ['type' => 'string'],
        ];
    }

    /**
     * Convert a single Flow Schema Definition to OpenAPI property format.
     *
     * @param Definition<mixed> $definition
     *
     * @return array<string, mixed>
     */
    private function convertDefinitionToOpenAPI(Definition $definition): array
    {
        $property = $this->convertTypeToOpenAPI($definition->type());
        $property['nullable'] = $definition->isNullable();

        if ($definition->metadata()->has(OpenAPIMetadata::DESCRIPTION->value)) {
            $property['description'] = $definition->metadata()->get(OpenAPIMetadata::DESCRIPTION->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::FORMAT->value)) {
            $property['format'] = $definition->metadata()->get(OpenAPIMetadata::FORMAT->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::EXAMPLE->value)) {
            $property['example'] = $definition->metadata()->get(OpenAPIMetadata::EXAMPLE->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::EXAMPLES->value)) {
            $property['examples'] = $definition->metadata()->get(OpenAPIMetadata::EXAMPLES->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::DEPRECATED->value)) {
            $property['deprecated'] = $definition->metadata()->get(OpenAPIMetadata::DEPRECATED->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::TITLE->value)) {
            $property['title'] = $definition->metadata()->get(OpenAPIMetadata::TITLE->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::DEFAULT->value)) {
            $property['default'] = $definition->metadata()->get(OpenAPIMetadata::DEFAULT->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::READ_ONLY->value)) {
            $property['readOnly'] = $definition->metadata()->get(OpenAPIMetadata::READ_ONLY->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::WRITE_ONLY->value)) {
            $property['writeOnly'] = $definition->metadata()->get(OpenAPIMetadata::WRITE_ONLY->value);
        }

        if ($definition->metadata()->has(OpenAPIMetadata::NULLABLE->value)) {
            $property['nullable'] = $definition->metadata()->get(OpenAPIMetadata::NULLABLE->value);
        }

        if (!isset($property['description']) && $definition->metadata()->has('description')) {
            $property['description'] = $definition->metadata()->get('description');
        }

        if (!isset($property['example']) && $definition->metadata()->has('example')) {
            $property['example'] = $definition->metadata()->get('example');
        }

        return $property;
    }

    /**
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function convertEnumToOpenAPI(Type $type): array
    {
        if (!$type instanceof EnumType) {
            return ['type' => 'string'];
        }

        $values = [];

        if (enum_exists($type->class)) {
            $cases = [$type->class, 'cases'];
            /** @var array<UnitEnum> $allCases */
            $allCases = $cases();
            $values = array_map(static fn(UnitEnum $case) => $case instanceof BackedEnum
                ? $case->value
                : $case->name, $allCases);
        }

        return [
            'type' => 'string',
            'enum' => $values,
        ];
    }

    /**
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function convertListToOpenAPI(Type $type): array
    {
        if (!$type instanceof ListType) {
            return ['type' => 'array', 'items' => ['type' => 'string']];
        }

        return [
            'type' => 'array',
            'items' => $this->convertTypeToOpenAPI($type->element()),
        ];
    }

    /**
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function convertMapToOpenAPI(Type $type): array
    {
        if (!$type instanceof MapType) {
            return ['type' => 'object'];
        }

        return [
            'type' => 'object',
            'additionalProperties' => $this->convertTypeToOpenAPI($type->value()),
        ];
    }

    /**
     * Convert OpenAPI array type to Flow ListType.
     *
     * @param array<string, mixed> $typeSpec
     *
     * @return Type<mixed>
     */
    private function convertOpenAPIArrayToFlowType(array $typeSpec): Type
    {
        if (!isset($typeSpec['items'])) {
            return type_list(type_string());
        }

        if (!is_array($typeSpec['items'])) {
            throw new InvalidArgumentException('OpenAPI array items must be an array specification');
        }

        /** @var array<string, mixed> $items */
        $items = $typeSpec['items'];
        $itemType = $this->convertOpenAPITypeToFlowType($items);

        return type_list($itemType);
    }

    /**
     * Convert OpenAPI object type to Flow StructureType or MapType.
     *
     * @param array<string, mixed> $typeSpec
     *
     * @return Type<mixed>
     */
    private function convertOpenAPIObjectToFlowType(array $typeSpec): Type
    {
        // If it has additionalProperties, it's a map
        if (isset($typeSpec['additionalProperties'])) {
            if (!is_array($typeSpec['additionalProperties'])) {
                throw new InvalidArgumentException('OpenAPI additionalProperties must be an array specification');
            }

            /** @var array<string, mixed> $additionalProperties */
            $additionalProperties = $typeSpec['additionalProperties'];
            $valueType = $this->convertOpenAPITypeToFlowType($additionalProperties);

            return type_map(type_string(), $valueType);
        }

        if (isset($typeSpec['properties']) && is_array($typeSpec['properties'])) {
            $elements = [];
            $optionalElements = [];
            $properties = type_map(type_string(), type_array())->assert($typeSpec['properties']);

            foreach ($properties as $propName => $propSpec) {
                /** @var array<string, mixed> $propSpec */
                $propType = $this->convertOpenAPITypeToFlowType($propSpec);
                $isNullable = isset($propSpec['nullable']) && is_bool($propSpec['nullable'])
                    ? $propSpec['nullable']
                    : false;

                if ($isNullable) {
                    $optionalElements[$propName] = $propType;
                } else {
                    $elements[$propName] = $propType;
                }
            }

            return type_structure($elements, $optionalElements);
        }

        return type_map(type_string(), type_string());
    }

    /**
     * Convert OpenAPI property specification to Flow Schema Definition.
     *
     * @param array<string, mixed> $propertySpec
     *
     * @return Definition<mixed>
     */
    private function convertOpenAPIPropertyToDefinition(string $propertyName, array $propertySpec): Definition
    {
        if (!isset($propertySpec['type'])) {
            throw new InvalidArgumentException("Property '{$propertyName}' must have a type");
        }

        $nullable = isset($propertySpec['nullable']) && is_bool($propertySpec['nullable'])
            ? $propertySpec['nullable']
            : false;
        $metadata = Metadata::empty();

        if (isset($propertySpec['description']) && is_string($propertySpec['description'])) {
            $metadata = $metadata->add('description', $propertySpec['description']);
        }

        if (
            isset($propertySpec['example'])
            && (
                is_string($propertySpec['example'])
                || is_int($propertySpec['example'])
                || is_float($propertySpec['example'])
                || is_bool($propertySpec['example'])
                || is_array($propertySpec['example'])
            )
        ) {
            $metadata = $metadata->add('example', $propertySpec['example']);
        }

        $type = $this->convertOpenAPITypeToFlowType($propertySpec);

        return definition_from_type($propertyName, $type, $nullable, $metadata);
    }

    /**
     * Convert OpenAPI string type to Flow Type based on format.
     *
     * @param array<string, mixed> $typeSpec
     *
     * @return Type<mixed>
     */
    private function convertOpenAPIStringToFlowType(array $typeSpec): Type
    {
        return match ($typeSpec['format'] ?? null) {
            'date' => type_date(),
            'date-time' => type_datetime(),
            'time' => type_time(),
            'uuid' => type_uuid(),
            'json' => type_json(),
            'xml' => type_xml(),
            default => type_string(),
        };
    }

    /**
     * Convert OpenAPI type specification to Flow Type.
     *
     * @param array<string, mixed> $typeSpec
     *
     * @return Type<mixed>
     */
    private function convertOpenAPITypeToFlowType(array $typeSpec): Type
    {
        if (!isset($typeSpec['type']) || !is_string($typeSpec['type'])) {
            throw new InvalidArgumentException('OpenAPI type specification must have a string type');
        }

        $openApiType = $typeSpec['type'];

        return match ($openApiType) {
            'boolean' => type_boolean(),
            'integer' => type_integer(),
            'number' => type_float(),
            'string' => $this->convertOpenAPIStringToFlowType($typeSpec),
            'array' => $this->convertOpenAPIArrayToFlowType($typeSpec),
            'object' => $this->convertOpenAPIObjectToFlowType($typeSpec),
            default => throw new InvalidArgumentException("Unsupported OpenAPI type: {$openApiType}"),
        };
    }

    /**
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function convertStructureToOpenAPI(Type $type): array
    {
        if (!$type instanceof StructureType) {
            return ['type' => 'object'];
        }

        $properties = [];

        foreach ($type->elements() as $name => $elementType) {
            $properties[$name] = $this->convertTypeToOpenAPI($elementType);
            $properties[$name]['nullable'] = false;
        }

        foreach ($type->optionalElements() as $name => $elementType) {
            $properties[$name] = $this->convertTypeToOpenAPI($elementType);
            $properties[$name]['nullable'] = true;
        }

        return [
            'type' => 'object',
            'properties' => $properties,
        ];
    }

    /**
     * Convert Flow Type to OpenAPI type format.
     *
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function convertTypeToOpenAPI(Type $type): array
    {
        return match ($type::class) {
            BooleanType::class => ['type' => 'boolean'],
            IntegerType::class => ['type' => 'integer'],
            FloatType::class => ['type' => 'number'],
            StringType::class, HTMLType::class, HTMLElementType::class => ['type' => 'string'],
            DateType::class => ['type' => 'string', 'format' => 'date'],
            DateTimeType::class => ['type' => 'string', 'format' => 'date-time'],
            TimeType::class => ['type' => 'string', 'format' => 'time'],
            UuidType::class => ['type' => 'string', 'format' => 'uuid'],
            TimeZoneType::class => ['type' => 'string'],
            JsonType::class => ['type' => 'string', 'format' => 'json'],
            XMLType::class, XMLElementType::class => ['type' => 'string', 'format' => 'xml'],
            EnumType::class => $this->convertEnumToOpenAPI($type),
            ArrayType::class => $this->convertArrayToOpenAPI($type),
            ListType::class => $this->convertListToOpenAPI($type),
            MapType::class => $this->convertMapToOpenAPI($type),
            StructureType::class => $this->convertStructureToOpenAPI($type),
            default => throw new RuntimeException('Unsupported type: ' . $type::class),
        };
    }
}
