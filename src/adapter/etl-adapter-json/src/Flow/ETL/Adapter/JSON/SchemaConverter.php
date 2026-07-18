<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use BackedEnum;
use Flow\ETL\Adapter\JSON\JsonSchema\Exception\CircularReferenceException;
use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnsupportedKeywordException;
use Flow\ETL\Adapter\JSON\JsonSchema\JsonSchemaMetadata;
use Flow\ETL\Adapter\JSON\JsonSchema\ReferenceResolver;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Path;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;
use JsonException;
use UnitEnum;

use function array_filter;
use function array_key_exists;
use function array_map;
use function array_slice;
use function array_unique;
use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_union;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function json_decode;
use function sort;
use function sprintf;

use const JSON_THROW_ON_ERROR;

final class SchemaConverter
{
    private const ANNOTATION_KEYWORDS = [
        'description' => JsonSchemaMetadata::DESCRIPTION,
        'title' => JsonSchemaMetadata::TITLE,
        'default' => JsonSchemaMetadata::DEFAULT,
        'examples' => JsonSchemaMetadata::EXAMPLES,
        'enum' => JsonSchemaMetadata::ENUM,
        'pattern' => JsonSchemaMetadata::PATTERN,
        'minimum' => JsonSchemaMetadata::MINIMUM,
        'maximum' => JsonSchemaMetadata::MAXIMUM,
        'exclusiveMinimum' => JsonSchemaMetadata::EXCLUSIVE_MINIMUM,
        'exclusiveMaximum' => JsonSchemaMetadata::EXCLUSIVE_MAXIMUM,
        'minLength' => JsonSchemaMetadata::MIN_LENGTH,
        'maxLength' => JsonSchemaMetadata::MAX_LENGTH,
        'minItems' => JsonSchemaMetadata::MIN_ITEMS,
        'maxItems' => JsonSchemaMetadata::MAX_ITEMS,
        'prefixItems' => JsonSchemaMetadata::PREFIX_ITEMS,
    ];

    private const KNOWN_STRING_FORMATS = ['date', 'date-time', 'time', 'uuid', 'html', 'xml'];

    private const UNSUPPORTED_KEYWORDS = [
        'not',
        'if',
        'then',
        'else',
        'contains',
        'patternProperties',
        'unevaluatedProperties',
        'unevaluatedItems',
        'dependentSchemas',
        'dependentRequired',
        '$dynamicRef',
        '$dynamicAnchor',
        'propertyNames',
    ];

    private readonly ReferenceResolver $resolver;

    public function __construct(?ReferenceResolver $resolver = null)
    {
        $this->resolver = $resolver ?? new ReferenceResolver();
    }

    /**
     * Convert a JSON Schema (https://json-schema.org) document into a Flow Schema.
     *
     * @param array<string, mixed>|Path|string $jsonSchema decoded document, raw JSON document or a path to a schema file
     */
    public function toFlow(array|string|Path $jsonSchema): Schema
    {
        $baseUri = '';

        if ($jsonSchema instanceof Path) {
            $loaded = $this->resolver->load($jsonSchema);
            $document = $loaded->schema;
            $baseUri = $loaded->baseUri;
        } elseif (is_string($jsonSchema)) {
            try {
                // @mago-ignore analysis:mixed-assignment
                $document = json_decode($jsonSchema, true, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new InvalidArgumentException(
                    'JSON Schema document is not valid JSON: ' . $e->getMessage(),
                    previous: $e,
                );
            }

            if (!is_array($document)) {
                throw new InvalidArgumentException('JSON Schema document must decode to an object');
            }
        } else {
            $document = $jsonSchema;
        }

        /** @var array<string, mixed> $document */
        $root = $this->inline($document, $document, $baseUri, []);

        if (array_key_exists('allOf', $root)) {
            $root = $this->mergeAllOf($root, 'root');
        }

        if (!array_key_exists('type', $root) || $root['type'] !== 'object') {
            throw new InvalidArgumentException('JSON Schema root must have type "object"');
        }

        if (!array_key_exists('properties', $root) || !is_array($root['properties'])) {
            throw new InvalidArgumentException('JSON Schema root must have "properties"');
        }

        /** @var array<string> $required */
        $required = array_key_exists('required', $root) && is_array($root['required']) ? $root['required'] : [];

        $definitions = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($root['properties'] as $name => $propertySchema) {
            if (!is_string($name) || !is_array($propertySchema) && !is_bool($propertySchema)) {
                throw new InvalidArgumentException(
                    'JSON Schema properties must be a map of property names to schema objects',
                );
            }

            /** @var array<string, mixed>|bool $propertySchema */
            $definitions[] = $this->propertyDefinition(
                $name,
                $this->booleanSchema($propertySchema, $name),
                in_array($name, $required, true),
            );
        }

        return schema(...$definitions);
    }

    /**
     * Convert a Flow Schema into a JSON Schema (https://json-schema.org, draft 2020-12) document.
     *
     * @return array<string, mixed>
     */
    public function toJsonSchema(Schema $schema): array
    {
        if (!count($schema->definitions())) {
            throw new InvalidArgumentException('Cannot convert empty Flow schema to JSON Schema');
        }

        $properties = [];
        $required = [];

        foreach ($schema->definitions() as $definition) {
            $name = $definition->entry()->name();
            $properties[$name] = $this->definitionToJsonSchema($definition);

            if (!$definition->isNullable()) {
                $required[] = $name;
            }
        }

        $jsonSchema = [
            '$schema' => 'https://json-schema.org/draft/2020-12/schema',
            'type' => 'object',
            'properties' => $properties,
        ];

        if (count($required)) {
            $jsonSchema['required'] = $required;
        }

        return $jsonSchema;
    }

    /**
     * Boolean schemas are valid since draft-06, true accepts any value and is equivalent to an empty schema.
     *
     * @param array<string, mixed>|bool $schema
     *
     * @return array<string, mixed>
     */
    private function booleanSchema(array|bool $schema, string $path): array
    {
        if ($schema === false) {
            throw new InvalidArgumentException(sprintf(
                'JSON Schema "false" at path "%s" rejects all values and cannot be represented in a Flow schema',
                $path,
            ));
        }

        if ($schema === true) {
            return [];
        }

        return $schema;
    }

    /**
     * @param array<string, mixed> $schema fully inlined subschema
     *
     * @return Type<mixed>
     */
    private function convert(array $schema, string $path): Type
    {
        foreach (self::UNSUPPORTED_KEYWORDS as $keyword) {
            if (array_key_exists($keyword, $schema)) {
                throw new UnsupportedKeywordException($keyword, $path);
            }
        }

        if (array_key_exists('allOf', $schema)) {
            $schema = $this->mergeAllOf($schema, $path);
        }

        foreach (['anyOf', 'oneOf'] as $combinator) {
            if (array_key_exists($combinator, $schema)) {
                return $this->convertCombinator($schema, $combinator, $path);
            }
        }

        if (array_key_exists('enum', $schema) || array_key_exists('const', $schema)) {
            return $this->convertEnum($schema, $path);
        }

        if (!array_key_exists('type', $schema)) {
            return type_json();
        }

        if (is_array($schema['type'])) {
            return $this->convertTypeList($schema, $path);
        }

        if (!is_string($schema['type'])) {
            throw new InvalidArgumentException(sprintf(
                'JSON Schema "type" at path "%s" must be a string or an array of strings',
                $path,
            ));
        }

        return $this->convertSingleType($schema, $schema['type'], $path);
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertArray(array $schema, string $path): Type
    {
        if (array_key_exists('prefixItems', $schema)) {
            return type_array();
        }

        if (array_key_exists('items', $schema) && is_array($schema['items'])) {
            /** @var array<string, mixed> $items */
            $items = $schema['items'];

            return type_list($this->convert($items, $path . '.items'));
        }

        return type_list(type_mixed());
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertCombinator(array $schema, string $combinator, string $path): Type
    {
        if (!is_array($schema[$combinator]) || !count($schema[$combinator])) {
            throw new InvalidArgumentException(sprintf(
                'JSON Schema "%s" at path "%s" must be a non empty array of schema objects',
                $combinator,
                $path,
            ));
        }

        $nullable = false;
        $types = [];

        // @mago-ignore analysis:mixed-assignment
        foreach (array_values($schema[$combinator]) as $index => $memberSchema) {
            if (!is_array($memberSchema)) {
                throw new InvalidArgumentException(sprintf(
                    'JSON Schema "%s" at path "%s" must be a non empty array of schema objects',
                    $combinator,
                    $path,
                ));
            }

            /** @var array<string, mixed> $memberSchema */
            $memberType = $this->convert($memberSchema, sprintf('%s.%s[%d]', $path, $combinator, $index));

            if ($memberType instanceof NullType) {
                $nullable = true;

                continue;
            }

            if ($memberType instanceof OptionalType) {
                $nullable = true;
                $memberType = $memberType->base();
            }

            $types[] = $memberType;
        }

        if (!count($types)) {
            return type_null();
        }

        $type = $this->union($types);

        return $nullable ? $this->nullableType($type) : $type;
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertEnum(array $schema, string $path): Type
    {
        // @mago-ignore analysis:mixed-assignment
        $values = array_key_exists('enum', $schema) ? $schema['enum'] : [$schema['const']];

        if (!is_array($values) || !count($values)) {
            throw new InvalidArgumentException(sprintf(
                'JSON Schema "enum" at path "%s" must be a non empty array',
                $path,
            ));
        }

        $nullable = false;
        $types = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if ($value === null) {
                $nullable = true;
            } elseif (is_string($value)) {
                $types['string'] = type_string();
            } elseif (is_int($value)) {
                $types['integer'] = type_integer();
            } elseif (is_float($value)) {
                $types['float'] = type_float();
            } elseif (is_bool($value)) {
                $types['boolean'] = type_boolean();
            } else {
                throw new InvalidArgumentException(sprintf(
                    'JSON Schema "enum" at path "%s" must contain only scalar or null values',
                    $path,
                ));
            }
        }

        if (!count($types)) {
            return type_null();
        }

        $type = $this->union(array_values($types));

        return $nullable ? $this->nullableType($type) : $type;
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertObject(array $schema, string $path): Type
    {
        if (array_key_exists('properties', $schema) && is_array($schema['properties'])) {
            /** @var array<string> $required */
            $required = array_key_exists('required', $schema) && is_array($schema['required'])
                ? $schema['required']
                : [];

            $elements = [];
            $optionalElements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($schema['properties'] as $name => $propertySchema) {
                if (!is_string($name) || !is_array($propertySchema) && !is_bool($propertySchema)) {
                    throw new InvalidArgumentException(sprintf(
                        'JSON Schema "properties" at path "%s" must be a map of property names to schema objects',
                        $path,
                    ));
                }

                /** @var array<string, mixed>|bool $propertySchema */
                $type = $this->convert($this->booleanSchema($propertySchema, $path . '.' . $name), $path . '.' . $name);

                if (in_array($name, $required, true)) {
                    $elements[$name] = $type;
                } else {
                    $optionalElements[$name] = $type;
                }
            }

            return type_structure($elements, $optionalElements);
        }

        if (array_key_exists('additionalProperties', $schema) && is_array($schema['additionalProperties'])) {
            /** @var array<string, mixed> $additionalProperties */
            $additionalProperties = $schema['additionalProperties'];

            return type_map(type_string(), $this->convert($additionalProperties, $path . '.additionalProperties'));
        }

        return type_map(type_string(), type_mixed());
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertSingleType(array $schema, string $type, string $path): Type
    {
        return match ($type) {
            'string' => $this->convertString($schema),
            'integer' => type_integer(),
            'number' => type_float(),
            'boolean' => type_boolean(),
            'null' => type_null(),
            'array' => $this->convertArray($schema, $path),
            'object' => $this->convertObject($schema, $path),
            default => throw new InvalidArgumentException(sprintf(
                'Unsupported JSON Schema type "%s" at path "%s"',
                $type,
                $path,
            )),
        };
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertString(array $schema): Type
    {
        return match ($schema['format'] ?? null) {
            'date' => type_date(),
            'date-time' => type_datetime(),
            'time' => type_time(),
            'uuid' => type_uuid(),
            'html' => type_html(),
            'xml' => type_xml(),
            default => type_string(),
        };
    }

    /**
     * @param array<string, mixed> $schema
     *
     * @return Type<mixed>
     */
    private function convertTypeList(array $schema, string $path): Type
    {
        /** @var array<mixed> $names */
        $names = $schema['type'];
        $nullable = false;
        $members = [];

        // @mago-ignore analysis:mixed-assignment
        foreach (array_unique($names) as $name) {
            if (!is_string($name)) {
                throw new InvalidArgumentException(sprintf(
                    'JSON Schema "type" at path "%s" must be a string or an array of strings',
                    $path,
                ));
            }

            if ($name === 'null') {
                $nullable = true;

                continue;
            }

            $members[] = $name;
        }

        sort($members);

        if ($members === ['array', 'object']) {
            return $nullable ? type_optional(type_json()) : type_json();
        }

        if (!count($members)) {
            return type_null();
        }

        $type = $this->union(array_map(fn(string $name): Type => $this->convertSingleType(
            $schema,
            $name,
            $path,
        ), $members));

        return $nullable ? $this->nullableType($type) : $type;
    }

    /**
     * @param Definition<mixed> $definition
     *
     * @return array<string, mixed>|bool
     */
    private function definitionToJsonSchema(Definition $definition): array|bool
    {
        $metadata = $definition->metadata();

        if ($metadata->has(JsonSchemaMetadata::ANY->value)) {
            $property = [];
        } elseif ($definition->type() instanceof NullType) {
            $property = ['type' => 'null'];
        } else {
            $property = $metadata->has(JsonSchemaMetadata::PREFIX_ITEMS->value)
                ? ['type' => 'array']
                : $this->typeToJsonSchema($definition->type());

            if ($definition->isNullable()) {
                $property = $this->nullify($property);
            }
        }

        foreach (JsonSchemaMetadata::cases() as $metadataKey) {
            if ($metadataKey === JsonSchemaMetadata::ANY || !$metadata->has($metadataKey->value)) {
                continue;
            }

            $property[$metadataKey->keyword()] = $metadata->get($metadataKey->value);
        }

        // an empty array would serialize to json list, boolean true is the spec form of an accept anything schema
        if ($property === [] && $metadata->has(JsonSchemaMetadata::ANY->value)) {
            return true;
        }

        return $property;
    }

    /**
     * @param EnumType<UnitEnum> $type
     *
     * @return array<string, mixed>
     */
    private function enumToJsonSchema(EnumType $type): array
    {
        $values = [];
        $backedByInt = false;

        $cases = [$type->class, 'cases'];

        /** @var array<UnitEnum> $allCases */
        $allCases = $cases();

        foreach ($allCases as $case) {
            if ($case instanceof BackedEnum) {
                $values[] = $case->value;
                $backedByInt = is_int($case->value);
            } else {
                $values[] = $case->name;
            }
        }

        return [
            'type' => $backedByInt ? 'integer' : 'string',
            'enum' => $values,
        ];
    }

    /**
     * Recursively inline every $ref so that conversion operates on a reference free tree.
     *
     * @param array<string, mixed> $schema
     * @param array<string, mixed> $document
     * @param array<string> $refStack
     *
     * @return array<string, mixed>
     */
    private function inline(array $schema, array $document, string $baseUri, array $refStack): array
    {
        if (array_key_exists('$ref', $schema) && is_string($schema['$ref'])) {
            $resolved = $this->resolver->resolve($schema['$ref'], $baseUri, $document);

            if (in_array($resolved->identity, $refStack, true)) {
                throw new CircularReferenceException($schema['$ref'], [...$refStack, $resolved->identity]);
            }

            $target = $this->inline($resolved->schema, $resolved->document, $resolved->baseUri, [
                ...$refStack,
                $resolved->identity,
            ]);

            $siblings = $schema;
            unset($siblings['$ref']);

            if (!count($siblings)) {
                return $target;
            }

            return [...$target, ...$this->inline($siblings, $document, $baseUri, $refStack)];
        }

        foreach (['properties'] as $keyword) {
            if (array_key_exists($keyword, $schema) && is_array($schema[$keyword])) {
                // @mago-ignore analysis:mixed-assignment
                foreach ($schema[$keyword] as $name => $subSchema) {
                    if (is_array($subSchema)) {
                        /** @var array<string, mixed> $subSchema */
                        $schema[$keyword][$name] = $this->inline($subSchema, $document, $baseUri, $refStack);
                    }
                }
            }
        }

        foreach (['items', 'additionalProperties'] as $keyword) {
            if (array_key_exists($keyword, $schema) && is_array($schema[$keyword])) {
                /** @var array<string, mixed> $subSchema */
                $subSchema = $schema[$keyword];
                $schema[$keyword] = $this->inline($subSchema, $document, $baseUri, $refStack);
            }
        }

        foreach (['prefixItems', 'allOf', 'anyOf', 'oneOf'] as $keyword) {
            if (array_key_exists($keyword, $schema) && is_array($schema[$keyword])) {
                // @mago-ignore analysis:mixed-assignment
                foreach ($schema[$keyword] as $index => $subSchema) {
                    if (is_array($subSchema)) {
                        /** @var array<string, mixed> $subSchema */
                        $schema[$keyword][$index] = $this->inline($subSchema, $document, $baseUri, $refStack);
                    }
                }
            }
        }

        return $schema;
    }

    /**
     * @param array<string, mixed> $schema
     */
    private function isAny(array $schema): bool
    {
        foreach (['type', 'allOf', 'anyOf', 'oneOf', 'enum', 'const'] as $keyword) {
            if (array_key_exists($keyword, $schema)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Metadata accepts scalars and arrays of scalars, recursively, without nulls.
     */
    private function isMetadataSafe(mixed $value): bool
    {
        if (is_array($value)) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $element) {
                if (!$this->isMetadataSafe($element)) {
                    return false;
                }
            }

            return true;
        }

        return is_int($value) || is_string($value) || is_bool($value) || is_float($value);
    }

    /**
     * @param array<string, mixed> $schema schema containing an allOf keyword
     *
     * @return array<string, mixed>
     */
    private function mergeAllOf(array $schema, string $path): array
    {
        if (!is_array($schema['allOf']) || !count($schema['allOf'])) {
            throw new InvalidArgumentException(sprintf(
                'JSON Schema "allOf" at path "%s" must be a non empty array of schema objects',
                $path,
            ));
        }

        $members = $schema['allOf'];
        $merged = $schema;
        unset($merged['allOf']);

        // @mago-ignore analysis:mixed-assignment
        foreach ($members as $member) {
            if (!is_array($member)) {
                throw new InvalidArgumentException(sprintf(
                    'JSON Schema "allOf" at path "%s" must be a non empty array of schema objects',
                    $path,
                ));
            }

            /** @var array<string, mixed> $member */
            if (array_key_exists('allOf', $member)) {
                $member = $this->mergeAllOf($member, $path);
            }

            $merged = $this->mergeSchemas($merged, $member, $path);
        }

        return $merged;
    }

    /**
     * @param array<string, mixed> $base
     * @param array<string, mixed> $overlay
     *
     * @return array<string, mixed>
     */
    private function mergeSchemas(array $base, array $overlay, string $path): array
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($overlay as $keyword => $value) {
            if (
                $keyword === 'properties'
                && is_array($value)
                && array_key_exists('properties', $base)
                && is_array($base['properties'])
            ) {
                // @mago-ignore analysis:mixed-assignment
                foreach ($value as $name => $propertySchema) {
                    if (
                        array_key_exists($name, $base['properties'])
                        && is_array($base['properties'][$name])
                        && is_array($propertySchema)
                    ) {
                        /** @var array<string, mixed> $basePropertySchema */
                        $basePropertySchema = $base['properties'][$name];

                        /** @var array<string, mixed> $propertySchema */
                        $base['properties'][$name] = $this->mergeSchemas(
                            $basePropertySchema,
                            $propertySchema,
                            $path . '.' . $name,
                        );
                    } else {
                        $base['properties'][$name] = $propertySchema;
                    }
                }

                continue;
            }

            if (
                $keyword === 'required'
                && is_array($value)
                && array_key_exists('required', $base)
                && is_array($base['required'])
            ) {
                $base['required'] = array_values(array_unique([...$base['required'], ...$value]));

                continue;
            }

            if ($keyword === 'type' && array_key_exists('type', $base) && $base['type'] !== $value) {
                throw new UnsupportedKeywordException('allOf', $path);
            }

            $base[$keyword] = $value;
        }

        return $base;
    }

    /**
     * @param array<string, mixed> $schema fully inlined property schema
     */
    private function metadata(array $schema): Metadata
    {
        $metadata = Metadata::empty();

        foreach (self::ANNOTATION_KEYWORDS as $keyword => $metadataKey) {
            if (!array_key_exists($keyword, $schema)) {
                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            $value = $schema[$keyword];

            // nullability is already represented by the definition, Metadata does not accept null values
            if ($keyword === 'enum' && is_array($value)) {
                $value = array_values(array_filter($value, static fn(mixed $enumValue): bool => $enumValue !== null));
            }

            if ($this->isMetadataSafe($value)) {
                // @mago-ignore analysis:mixed-argument
                $metadata = $metadata->add($metadataKey->value, $value);
            }
        }

        if (
            array_key_exists('const', $schema)
            && !array_key_exists('enum', $schema)
            && $this->isMetadataSafe($schema['const'])
        ) {
            $metadata = $metadata->add(JsonSchemaMetadata::ENUM->value, [$schema['const']]);
        }

        if (
            array_key_exists('format', $schema)
            && is_string($schema['format'])
            && !in_array($schema['format'], self::KNOWN_STRING_FORMATS, true)
        ) {
            $metadata = $metadata->add(JsonSchemaMetadata::FORMAT->value, $schema['format']);
        }

        if ($this->isAny($schema)) {
            $metadata = $metadata->add(JsonSchemaMetadata::ANY->value, true);
        }

        return $metadata;
    }

    /**
     * Add null to an already emitted JSON Schema property.
     *
     * @param array<string, mixed> $property
     *
     * @return array<string, mixed>
     */
    private function nullify(array $property): array
    {
        if (array_key_exists('anyOf', $property) && is_array($property['anyOf'])) {
            $property['anyOf'][] = ['type' => 'null'];

            return $property;
        }

        if (array_key_exists('type', $property) && is_array($property['type'])) {
            if (!in_array('null', $property['type'], true)) {
                $property['type'][] = 'null';
            }

            return $property;
        }

        if (array_key_exists('type', $property) && is_string($property['type']) && $property['type'] !== 'null') {
            $property['type'] = [$property['type'], 'null'];
        }

        return $property;
    }

    /**
     * OptionalType cannot wrap a UnionType, nullable unions are represented as a union with a null member.
     *
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    private function nullableType(Type $type): Type
    {
        if ($type instanceof UnionType) {
            return type_union($type, type_null());
        }

        return type_optional($type);
    }

    /**
     * @param array<string, mixed> $schema fully inlined property schema
     *
     * @return Definition<mixed>
     */
    private function propertyDefinition(string $name, array $schema, bool $required): Definition
    {
        $type = $this->convert($schema, $name);
        $nullable = !$required;
        $metadata = $this->metadata($schema);

        if ($type instanceof OptionalType) {
            $type = $type->base();
            $nullable = true;
        }

        if ($type instanceof UnionType) {
            $members = [];

            foreach ($type->types()->all() as $member) {
                if ($member instanceof NullType) {
                    $nullable = true;

                    continue;
                }

                $members[] = $member;
            }

            if (count($members) < count($type->types())) {
                $type = count($members) ? $this->union($members) : type_null();
            }
        }

        if ($type instanceof NullType) {
            return null_schema($name, $metadata);
        }

        return definition_from_type($name, $type, $nullable, $metadata);
    }

    /**
     * @param non-empty-list<Type<mixed>> $types
     *
     * @return Type<mixed>
     */
    private function union(array $types): Type
    {
        if (count($types) === 1) {
            return $types[0];
        }

        return type_union($types[0], $types[1], ...array_slice($types, 2));
    }

    /**
     * @param Type<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function typeToJsonSchema(Type $type): array
    {
        return match (true) {
            $type instanceof OptionalType => $this->nullify($this->typeToJsonSchema($type->base())),
            $type instanceof StringType => ['type' => 'string'],
            $type instanceof IntegerType => ['type' => 'integer'],
            $type instanceof FloatType => ['type' => 'number'],
            $type instanceof BooleanType => ['type' => 'boolean'],
            $type instanceof NullType => ['type' => 'null'],
            $type instanceof DateType => ['type' => 'string', 'format' => 'date'],
            $type instanceof DateTimeType => ['type' => 'string', 'format' => 'date-time'],
            $type instanceof TimeType => ['type' => 'string', 'format' => 'time'],
            $type instanceof UuidType => ['type' => 'string', 'format' => 'uuid'],
            $type instanceof HTMLType, $type instanceof HTMLElementType => ['type' => 'string', 'format' => 'html'],
            $type instanceof XMLType, $type instanceof XMLElementType => ['type' => 'string', 'format' => 'xml'],
            $type instanceof EnumType => $this->enumToJsonSchema($type),
            $type instanceof JsonType => ['type' => ['object', 'array']],
            $type instanceof ArrayType => ['type' => 'array'],
            $type instanceof ListType => $type->element() instanceof MixedType
                ? ['type' => 'array']
                : ['type' => 'array', 'items' => $this->typeToJsonSchema($type->element())],
            $type instanceof MapType => $this->mapToJsonSchema($type),
            $type instanceof StructureType => $this->structureToJsonSchema($type),
            $type instanceof UnionType => [
                'anyOf' => array_map(fn(Type $member): array => $this->typeToJsonSchema(
                    $member,
                ), $type->types()->all()),
            ],
            default => throw new RuntimeException(sprintf('Type %s cannot be converted to JSON Schema', $type::class)),
        };
    }

    /**
     * @param MapType<array-key, mixed> $type
     *
     * @return array<string, mixed>
     */
    private function mapToJsonSchema(MapType $type): array
    {
        if (!$type->key() instanceof StringType) {
            throw new RuntimeException(sprintf(
                'Only maps with string keys can be converted to JSON Schema, got %s',
                $type->key()->toString(),
            ));
        }

        if ($type->value() instanceof MixedType) {
            return ['type' => 'object'];
        }

        return ['type' => 'object', 'additionalProperties' => $this->typeToJsonSchema($type->value())];
    }

    /**
     * @param StructureType<mixed> $type
     *
     * @return array<string, mixed>
     */
    private function structureToJsonSchema(StructureType $type): array
    {
        $properties = [];
        $required = [];

        foreach ($type->elements() as $name => $elementType) {
            $properties[$name] = $this->typeToJsonSchema($elementType);
            $required[] = $name;
        }

        foreach ($type->optionalElements() as $name => $elementType) {
            $properties[$name] = $this->typeToJsonSchema($elementType);
        }

        $jsonSchema = ['type' => 'object', 'properties' => $properties];

        if (count($required)) {
            $jsonSchema['required'] = $required;
        }

        return $jsonSchema;
    }
}
