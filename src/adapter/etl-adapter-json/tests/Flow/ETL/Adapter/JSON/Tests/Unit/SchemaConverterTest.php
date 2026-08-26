<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use Flow\ETL\Adapter\JSON\JsonSchema\Exception\CircularReferenceException;
use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnresolvableReferenceException;
use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnsupportedKeywordException;
use Flow\ETL\Adapter\JSON\JsonSchema\JsonSchemaMetadata;
use Flow\ETL\Adapter\JSON\SchemaConverter;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;
use function sprintf;

final class SchemaConverterTest extends FlowTestCase
{
    public function test_to_flow_all_of_merges_object_schemas(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['profile'],
            'properties' => [
                'profile' => [
                    'allOf' => [
                        ['type' => 'object', 'required' => ['name'], 'properties' => ['name' => ['type' => 'string']]],
                        ['type' => 'object', 'required' => ['age'], 'properties' => ['age' => ['type' => 'integer']]],
                    ],
                ],
            ],
        ]);

        static::assertEquals(
            schema(structure_schema('profile', type_structure(['name' => type_string(), 'age' => type_integer()]))),
            $flowSchema,
        );
    }

    public function test_to_flow_all_of_with_conflicting_types_throws_exception(): void
    {
        $this->expectException(UnsupportedKeywordException::class);
        $this->expectExceptionMessage('JSON Schema keyword "allOf" at path "value"');

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'value' => ['allOf' => [['type' => 'string'], ['type' => 'integer']]],
            ],
        ]);
    }

    public function test_to_flow_any_of_with_heterogeneous_members_is_refused(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['value'],
            'properties' => [
                'value' => ['anyOf' => [['type' => 'string'], ['type' => 'integer']]],
            ],
        ]);
    }

    public function test_to_flow_any_of_with_null_member_becomes_nullable(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['value'],
            'properties' => [
                'value' => ['anyOf' => [['type' => 'string'], ['type' => 'null']]],
            ],
        ]);

        static::assertEquals(schema(str_schema('value', true)), $flowSchema);
    }

    public function test_to_flow_any_of_with_only_null_members_becomes_null_definition(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'value' => ['anyOf' => [['type' => 'null']]],
            ],
        ]);

        static::assertEquals(schema(null_schema('value')), $flowSchema);
    }

    public function test_to_flow_array_without_items_becomes_list_of_mixed(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['values'],
            'properties' => [
                'values' => ['type' => 'array'],
            ],
        ]);

        static::assertEquals(schema(list_schema('values', type_list(type_mixed()))), $flowSchema);
    }

    public function test_to_flow_basic_types(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['id', 'name', 'price', 'active'],
            'properties' => [
                'id' => ['type' => 'integer'],
                'name' => ['type' => 'string'],
                'price' => ['type' => 'number'],
                'active' => ['type' => 'boolean'],
            ],
        ]);

        static::assertEquals(
            schema(int_schema('id'), str_schema('name'), float_schema('price'), bool_schema('active')),
            $flowSchema,
        );
    }

    public function test_to_flow_circular_reference_throws_exception(): void
    {
        $this->expectException(CircularReferenceException::class);
        $this->expectExceptionMessage('Circular reference detected: "#/$defs/node"');

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'node' => ['$ref' => '#/$defs/node'],
            ],
            '$defs' => [
                'node' => [
                    'type' => 'object',
                    'properties' => ['child' => ['$ref' => '#/$defs/node']],
                ],
            ],
        ]);
    }

    public function test_to_flow_const_becomes_scalar_with_enum_metadata(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['status'],
            'properties' => [
                'status' => ['const' => 'fixed'],
            ],
        ]);

        static::assertEquals(
            schema(str_schema('status', metadata: Metadata::with(JsonSchemaMetadata::ENUM->value, ['fixed']))),
            $flowSchema,
        );
    }

    public function test_to_flow_boolean_false_property_schema_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JSON Schema "false" at path "nothing" rejects all values');

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'nothing' => false,
            ],
        ]);
    }

    public function test_to_flow_boolean_true_property_schema_becomes_json_with_any_marker(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'anything' => true,
            ],
        ]);

        static::assertEquals(
            schema(json_schema('anything', true, Metadata::with(JsonSchemaMetadata::ANY->value, true))),
            $flowSchema,
        );
    }

    public function test_to_flow_boolean_true_property_schema_inside_nested_structure(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['profile'],
            'properties' => [
                'profile' => [
                    'type' => 'object',
                    'required' => ['name', 'payload'],
                    'properties' => [
                        'name' => ['type' => 'string'],
                        'payload' => true,
                    ],
                ],
            ],
        ]);

        static::assertEquals(
            schema(structure_schema('profile', type_structure([
                'name' => type_string(),
                'payload' => type_json(),
            ]))),
            $flowSchema,
        );
    }

    public function test_to_flow_empty_property_schema_becomes_json_with_any_marker(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'anything' => [],
            ],
        ]);

        static::assertEquals(
            schema(json_schema('anything', true, Metadata::with(JsonSchemaMetadata::ANY->value, true))),
            $flowSchema,
        );
    }

    public function test_to_flow_enum_with_heterogeneous_values_is_refused(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['value'],
            'properties' => [
                'value' => ['enum' => ['auto', 1, 2]],
            ],
        ]);
    }

    public function test_to_flow_enum_with_homogeneous_values_becomes_scalar(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['status'],
            'properties' => [
                'status' => ['enum' => ['on', 'off']],
            ],
        ]);

        static::assertEquals(
            schema(str_schema('status', metadata: Metadata::with(JsonSchemaMetadata::ENUM->value, ['on', 'off']))),
            $flowSchema,
        );
    }

    public function test_to_flow_enum_with_non_scalar_values_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JSON Schema "enum" at path "value" must contain only scalar or null values');

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'value' => ['enum' => [['nested' => 'array']]],
            ],
        ]);
    }

    public function test_to_flow_enum_with_null_value_becomes_nullable(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['status'],
            'properties' => [
                'status' => ['enum' => ['on', 'off', null]],
            ],
        ]);

        static::assertEquals(
            schema(str_schema('status', true, Metadata::with(JsonSchemaMetadata::ENUM->value, ['on', 'off']))),
            $flowSchema,
        );
    }

    public function test_to_flow_free_form_object_becomes_map_of_mixed(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['attributes'],
            'properties' => [
                'attributes' => ['type' => 'object'],
            ],
        ]);

        static::assertEquals(schema(map_schema('attributes', type_map(type_string(), type_mixed()))), $flowSchema);
    }

    public function test_to_flow_internal_reference_via_defs(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['money'],
            'properties' => [
                'money' => ['$ref' => '#/$defs/money'],
            ],
            '$defs' => [
                'money' => [
                    'type' => 'object',
                    'required' => ['amount'],
                    'properties' => [
                        'amount' => ['type' => 'number'],
                        'currency' => ['type' => 'string'],
                    ],
                ],
            ],
        ]);

        static::assertEquals(
            schema(structure_schema('money', type_structure(['amount' => type_float()], [
                'currency' => type_string(),
            ]))),
            $flowSchema,
        );
    }

    public function test_to_flow_internal_reference_via_legacy_definitions(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['status'],
            'properties' => [
                'status' => ['$ref' => '#/definitions/status'],
            ],
            'definitions' => [
                'status' => ['type' => 'string'],
            ],
        ]);

        static::assertEquals(schema(str_schema('status')), $flowSchema);
    }

    public function test_to_flow_invalid_json_string_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JSON Schema document is not valid JSON');

        (new SchemaConverter())->toFlow('{invalid');
    }

    public function test_to_flow_nested_structures_lists_and_maps(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['orders'],
            'properties' => [
                'orders' => [
                    'type' => 'array',
                    'items' => [
                        'type' => 'object',
                        'required' => ['sku', 'attributes'],
                        'properties' => [
                            'sku' => ['type' => 'string'],
                            'attributes' => ['type' => 'object', 'additionalProperties' => ['type' => 'string']],
                            'quantity' => ['type' => 'integer'],
                        ],
                    ],
                ],
            ],
        ]);

        static::assertEquals(
            schema(list_schema(
                'orders',
                type_list(type_structure([
                    'sku' => type_string(),
                    'attributes' => type_map(type_string(), type_string()),
                ], ['quantity' => type_integer()])),
            )),
            $flowSchema,
        );
    }

    public function test_to_flow_nested_union_inside_structure(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['config'],
            'properties' => [
                'config' => [
                    'type' => 'object',
                    'required' => ['value'],
                    'properties' => [
                        'value' => ['type' => ['string', 'integer']],
                    ],
                ],
            ],
        ]);

        static::assertEquals(
            schema(structure_schema('config', type_structure([
                'value' => type_union(type_integer(), type_string()),
            ]))),
            $flowSchema,
        );
    }

    public function test_to_flow_object_and_array_type_union_becomes_json(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['document'],
            'properties' => [
                'document' => ['type' => ['object', 'array']],
            ],
        ]);

        static::assertEquals(schema(json_schema('document')), $flowSchema);
    }

    public function test_to_flow_prefix_items_becomes_json_with_metadata(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['pair'],
            'properties' => [
                'pair' => ['type' => 'array', 'prefixItems' => [['type' => 'string'], ['type' => 'integer']]],
            ],
        ]);

        static::assertEquals(
            schema(json_schema('pair', metadata: Metadata::with(JsonSchemaMetadata::PREFIX_ITEMS->value, [
                ['type' => 'string'],
                ['type' => 'integer'],
            ]))),
            $flowSchema,
        );
    }

    public function test_to_flow_property_annotations_are_stored_in_metadata(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['name'],
            'properties' => [
                'name' => [
                    'type' => 'string',
                    'title' => 'Name',
                    'description' => 'Full name',
                    'default' => 'anonymous',
                    'examples' => ['John'],
                    'pattern' => '^[a-z]+$',
                    'minLength' => 1,
                    'maxLength' => 64,
                ],
            ],
        ]);

        static::assertEquals(
            schema(str_schema('name', metadata: Metadata::empty()
                ->add(JsonSchemaMetadata::DESCRIPTION->value, 'Full name')
                ->add(JsonSchemaMetadata::TITLE->value, 'Name')
                ->add(JsonSchemaMetadata::DEFAULT->value, 'anonymous')
                ->add(JsonSchemaMetadata::EXAMPLES->value, ['John'])
                ->add(JsonSchemaMetadata::PATTERN->value, '^[a-z]+$')
                ->add(JsonSchemaMetadata::MIN_LENGTH->value, 1)
                ->add(JsonSchemaMetadata::MAX_LENGTH->value, 64))),
            $flowSchema,
        );
    }

    public function test_to_flow_path_document_with_cross_file_references(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow(path(__DIR__ . '/../Fixtures/json-schema/person.json'));

        static::assertEquals(
            schema(
                str_schema('name'),
                int_schema('age', true),
                structure_schema('address', type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                ], ['zip' => type_string()])),
                structure_schema(
                    'location',
                    type_structure([
                        'lat' => type_float(),
                        'lon' => type_float(),
                    ]),
                    true,
                ),
            ),
            $flowSchema,
        );
    }

    public function test_to_flow_property_not_in_required_becomes_nullable(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['id'],
            'properties' => [
                'id' => ['type' => 'integer'],
                'note' => ['type' => 'string'],
            ],
        ]);

        static::assertEquals(schema(int_schema('id'), str_schema('note', true)), $flowSchema);
    }

    public function test_to_flow_raw_json_string_document(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow(
            '{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}}}',
        );

        static::assertEquals(schema(int_schema('id')), $flowSchema);
    }

    public function test_to_flow_remote_reference_without_client_throws_exception(): void
    {
        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage('resolving remote references requires a PSR-18 http client');

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'remote' => ['$ref' => 'https://example.com/schema.json#/$defs/x'],
            ],
        ]);
    }

    public function test_to_flow_root_without_object_type_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JSON Schema root must have type "object"');

        (new SchemaConverter())->toFlow(['type' => 'array', 'items' => ['type' => 'string']]);
    }

    public function test_to_flow_root_without_properties_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JSON Schema root must have "properties"');

        (new SchemaConverter())->toFlow(['type' => 'object']);
    }

    public function test_to_flow_string_formats(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['created_date', 'created_at', 'created_time', 'uuid', 'email'],
            'properties' => [
                'created_date' => ['type' => 'string', 'format' => 'date'],
                'created_at' => ['type' => 'string', 'format' => 'date-time'],
                'created_time' => ['type' => 'string', 'format' => 'time'],
                'uuid' => ['type' => 'string', 'format' => 'uuid'],
                'email' => ['type' => 'string', 'format' => 'email'],
            ],
        ]);

        static::assertEquals(
            schema(
                date_schema('created_date'),
                datetime_schema('created_at'),
                time_schema('created_time'),
                uuid_schema('uuid'),
                str_schema('email', metadata: Metadata::with(JsonSchemaMetadata::FORMAT->value, 'email')),
            ),
            $flowSchema,
        );
    }

    public function test_to_flow_type_list_with_null_becomes_nullable(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['score'],
            'properties' => [
                'score' => ['type' => ['number', 'null']],
            ],
        ]);

        static::assertEquals(schema(float_schema('score', true)), $flowSchema);
    }

    public function test_to_flow_type_list_with_null_and_multiple_types_is_refused(): void
    {
        $this->expectException(UnsupportedUnionTypeException::class);

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['value'],
            'properties' => [
                'value' => ['type' => ['string', 'integer', 'null']],
            ],
        ]);
    }

    public function test_to_flow_type_null_becomes_null_definition(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'nothing' => ['type' => 'null'],
            ],
        ]);

        static::assertEquals(schema(null_schema('nothing')), $flowSchema);
    }

    public function test_to_flow_unknown_type_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported JSON Schema type "money" at path "value"');

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'value' => ['type' => 'money'],
            ],
        ]);
    }

    #[TestWith(['not'])]
    #[TestWith(['if'])]
    #[TestWith(['then'])]
    #[TestWith(['else'])]
    #[TestWith(['contains'])]
    #[TestWith(['patternProperties'])]
    #[TestWith(['unevaluatedProperties'])]
    #[TestWith(['unevaluatedItems'])]
    #[TestWith(['dependentSchemas'])]
    #[TestWith(['dependentRequired'])]
    #[TestWith(['$dynamicRef'])]
    #[TestWith(['$dynamicAnchor'])]
    #[TestWith(['propertyNames'])]
    public function test_to_flow_unsupported_keywords_throw_exception(string $keyword): void
    {
        $this->expectException(UnsupportedKeywordException::class);
        $this->expectExceptionMessage(sprintf('JSON Schema keyword "%s" at path "value"', $keyword));

        (new SchemaConverter())->toFlow([
            'type' => 'object',
            'properties' => [
                'value' => ['type' => 'string', $keyword => []],
            ],
        ]);
    }

    public function test_round_trip_flow_to_json_schema_and_back(): void
    {
        $flowSchema = schema(
            int_schema('id'),
            str_schema('name', true),
            datetime_schema('created_at'),
            list_schema('tags', type_list(type_string())),
            map_schema('attributes', type_map(type_string(), type_string()), true),
            structure_schema('address', type_structure(['street' => type_string()], [
                'zip' => type_optional(type_string()),
            ])),
            json_schema('document'),
            json_schema('anything', true, Metadata::with(JsonSchemaMetadata::ANY->value, true)),
            null_schema('nothing'),
        );

        $converter = new SchemaConverter();

        static::assertEquals($flowSchema, $converter->toFlow($converter->toJsonSchema($flowSchema)));
    }

    public function test_a_union_column_cannot_round_trip_because_it_cannot_be_read_back(): void
    {
        $converter = new SchemaConverter();
        $jsonSchema = $converter->toJsonSchema(schema(
            new UnionDefinition('value', type_union(type_integer(), type_string()), true),
        ));

        $this->expectException(UnsupportedUnionTypeException::class);

        $converter->toFlow($jsonSchema);
    }

    public function test_round_trip_json_schema_to_flow_and_back(): void
    {
        $jsonSchema = [
            '$schema' => 'https://json-schema.org/draft/2020-12/schema',
            'type' => 'object',
            'properties' => [
                'id' => ['type' => 'integer'],
                'name' => ['type' => ['string', 'null']],
                'tags' => ['type' => 'array', 'items' => ['type' => 'string']],
                'address' => [
                    'type' => 'object',
                    'properties' => [
                        'street' => ['type' => 'string'],
                        'zip' => ['type' => ['string', 'null']],
                    ],
                    'required' => ['street'],
                ],
            ],
            'required' => ['id', 'tags', 'address'],
        ];

        $converter = new SchemaConverter();

        static::assertSame($jsonSchema, $converter->toJsonSchema($converter->toFlow($jsonSchema)));
    }

    public function test_to_json_schema_any_marked_definition_becomes_boolean_true_schema(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(json_schema(
            'anything',
            true,
            Metadata::with(JsonSchemaMetadata::ANY->value, true),
        )));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => ['anything' => true],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_any_marked_definition_with_annotations_keeps_object_form(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(json_schema(
            'anything',
            true,
            Metadata::with(JsonSchemaMetadata::ANY->value, true)->add(
                JsonSchemaMetadata::DESCRIPTION->value,
                'accepts anything',
            ),
        )));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => ['anything' => ['description' => 'accepts anything']],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_empty_schema_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot convert empty Flow schema to JSON Schema');

        (new SchemaConverter())->toJsonSchema(schema());
    }

    public function test_to_json_schema_basic_types_and_required(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(
            int_schema('id'),
            str_schema('name', true),
            float_schema('price'),
            bool_schema('active'),
        ));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'id' => ['type' => 'integer'],
                    'name' => ['type' => ['string', 'null']],
                    'price' => ['type' => 'number'],
                    'active' => ['type' => 'boolean'],
                ],
                'required' => ['id', 'price', 'active'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_enum_definitions(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(
            enum_schema('string_enum', BackedStringEnum::class),
            enum_schema('int_enum', BackedIntEnum::class),
            enum_schema('unit_enum', BasicEnum::class),
        ));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'string_enum' => ['type' => 'string', 'enum' => ['one', 'three', 'two']],
                    'int_enum' => ['type' => 'integer', 'enum' => [1, 3, 2]],
                    'unit_enum' => ['type' => 'string', 'enum' => ['one', 'three', 'two']],
                ],
                'required' => ['string_enum', 'int_enum', 'unit_enum'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_list_of_mixed_omits_items(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(list_schema('values', type_list(type_mixed()))));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => ['values' => ['type' => 'array']],
                'required' => ['values'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_logical_string_types_use_formats(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(
            date_schema('created_date'),
            datetime_schema('created_at'),
            time_schema('created_time'),
            uuid_schema('uuid'),
        ));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'created_date' => ['type' => 'string', 'format' => 'date'],
                    'created_at' => ['type' => 'string', 'format' => 'date-time'],
                    'created_time' => ['type' => 'string', 'format' => 'time'],
                    'uuid' => ['type' => 'string', 'format' => 'uuid'],
                ],
                'required' => ['created_date', 'created_at', 'created_time', 'uuid'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_map_of_mixed_omits_additional_properties(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(map_schema('attributes', type_map(
            type_string(),
            type_mixed(),
        ))));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => ['attributes' => ['type' => 'object']],
                'required' => ['attributes'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_map_with_non_string_keys_throws_exception(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Only maps with string keys can be converted to JSON Schema');

        (new SchemaConverter())->toJsonSchema(schema(map_schema('lookup', type_map(type_integer(), type_string()))));
    }

    public function test_to_json_schema_metadata_is_reemitted_as_keywords(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(str_schema('name', metadata: Metadata::empty()->add(
            JsonSchemaMetadata::DESCRIPTION->value,
            'Full name',
        )->add(JsonSchemaMetadata::PATTERN->value, '^[a-z]+$')->add(JsonSchemaMetadata::FORMAT->value, 'email'))));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'name' => [
                        'type' => 'string',
                        'description' => 'Full name',
                        'format' => 'email',
                        'pattern' => '^[a-z]+$',
                    ],
                ],
                'required' => ['name'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_null_definition(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(null_schema('nothing')));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => ['nothing' => ['type' => 'null']],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_nullable_union_appends_null_member(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(
            new UnionDefinition('value', type_union(type_integer(), type_string()), true),
        ));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'value' => ['anyOf' => [['type' => 'integer'], ['type' => 'string'], ['type' => 'null']]],
                ],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_prefix_items_metadata_restores_tuple(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(json_schema('pair', metadata: Metadata::with(
            JsonSchemaMetadata::PREFIX_ITEMS->value,
            [['type' => 'string'], ['type' => 'integer']],
        ))));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'pair' => ['type' => 'array', 'prefixItems' => [['type' => 'string'], ['type' => 'integer']]],
                ],
                'required' => ['pair'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_json_schema_structure_with_optional_elements(): void
    {
        $jsonSchema = (new SchemaConverter())->toJsonSchema(schema(structure_schema('address', type_structure([
            'street' => type_string(),
        ], ['zip' => type_optional(type_string())]))));

        static::assertSame(
            [
                '$schema' => 'https://json-schema.org/draft/2020-12/schema',
                'type' => 'object',
                'properties' => [
                    'address' => [
                        'type' => 'object',
                        'properties' => [
                            'street' => ['type' => 'string'],
                            'zip' => ['type' => ['string', 'null']],
                        ],
                        'required' => ['street'],
                    ],
                ],
                'required' => ['address'],
            ],
            $jsonSchema,
        );
    }

    public function test_to_flow_nullable_structure_member_uses_optional_type(): void
    {
        $flowSchema = (new SchemaConverter())->toFlow([
            'type' => 'object',
            'required' => ['profile'],
            'properties' => [
                'profile' => [
                    'type' => 'object',
                    'required' => ['name', 'age'],
                    'properties' => [
                        'name' => ['type' => 'string'],
                        'age' => ['type' => ['integer', 'null']],
                    ],
                ],
            ],
        ]);

        static::assertEquals(
            schema(structure_schema('profile', type_structure([
                'name' => type_string(),
                'age' => type_optional(type_integer()),
            ]))),
            $flowSchema,
        );
    }
}
