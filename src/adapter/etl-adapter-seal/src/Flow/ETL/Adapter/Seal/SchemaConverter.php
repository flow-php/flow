<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use CmsIg\Seal\Schema\Field;
use CmsIg\Seal\Schema\Field\AbstractField;
use CmsIg\Seal\Schema\Index;
use CmsIg\Seal\Schema\Schema as SealSchema;
use Flow\ETL\Exception\RuntimeException;
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
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function array_map;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class SchemaConverter
{
    public function toFlowSchema(SealSchema $schema): Schema
    {
        $definitions = [];

        foreach ($schema->indexes as $index) {
            foreach ($index->fields as $field) {
                $definitions[] = $this->sealFieldToFlowDefinition($field);
            }
        }

        return new Schema(...$definitions);
    }

    public function toSealSchema(Schema $schema, string $indexName, ?string $identifier = null): SealSchema
    {
        $fields = [];
        $identifiers = 0;

        foreach ($schema->definitions() as $definition) {
            $name = $definition->entry()->name();
            $metadata = $definition->metadata();

            if ($name === $identifier || $metadata->has(SealMetadata::IDENTIFIER->value)) {
                $fields[$name] = new Field\IdentifierField($name);
                $identifiers++;

                continue;
            }

            $fields[$name] = $this->flowToSealField($name, $definition->type(), false, $metadata);
        }

        if ($identifiers !== 1) {
            throw new RuntimeException(
                'A SEAL index requires exactly one identifier field, '
                . $identifiers
                . ' given. '
                . 'Pass the identifier field name or mark a definition with SealMetadata::identifier().',
            );
        }

        return new SealSchema([$indexName => new Index($indexName, $fields)]);
    }

    private function flag(Metadata $metadata, SealMetadata $key, bool $default): bool
    {
        if (!$metadata->has($key->value)) {
            return $default;
        }

        return (bool) $metadata->getAs($key->value, type_boolean(), $default);
    }

    /**
     * @param Type<mixed> $type
     */
    private function flowToSealField(string $name, Type $type, bool $multiple, Metadata $metadata): AbstractField
    {
        if ($type instanceof StructureType) {
            foreach ($type->elements() as $element) {
                if ($element->optional) {
                    throw new RuntimeException(sprintf(
                        'Seal schema does not support structure optional elements, given: %s',
                        $type->toString(),
                    ));
                }
            }
        }

        return match ($type::class) {
            EnumType::class,
            HTMLElementType::class,
            HTMLType::class,
            StringType::class,
            TimeZoneType::class,
            UuidType::class,
            XMLElementType::class,
            XMLType::class,
                => new Field\TextField(
                $name,
                multiple: $multiple,
                searchable: $this->flag($metadata, SealMetadata::SEARCHABLE, true),
                filterable: $this->flag($metadata, SealMetadata::FILTERABLE, false),
                sortable: $this->flag($metadata, SealMetadata::SORTABLE, false),
                distinct: $this->flag($metadata, SealMetadata::DISTINCT, false),
                facet: $this->flag($metadata, SealMetadata::FACET, false),
            ),
            IntegerType::class => new Field\IntegerField(
                $name,
                multiple: $multiple,
                filterable: $this->flag($metadata, SealMetadata::FILTERABLE, true),
                sortable: $this->flag($metadata, SealMetadata::SORTABLE, true),
                distinct: $this->flag($metadata, SealMetadata::DISTINCT, false),
                facet: $this->flag($metadata, SealMetadata::FACET, false),
            ),
            FloatType::class => new Field\FloatField(
                $name,
                multiple: $multiple,
                filterable: $this->flag($metadata, SealMetadata::FILTERABLE, true),
                sortable: $this->flag($metadata, SealMetadata::SORTABLE, true),
                distinct: $this->flag($metadata, SealMetadata::DISTINCT, false),
                facet: $this->flag($metadata, SealMetadata::FACET, false),
            ),
            BooleanType::class => new Field\BooleanField(
                $name,
                multiple: $multiple,
                filterable: $this->flag($metadata, SealMetadata::FILTERABLE, true),
                sortable: $this->flag($metadata, SealMetadata::SORTABLE, false),
                distinct: $this->flag($metadata, SealMetadata::DISTINCT, false),
                facet: $this->flag($metadata, SealMetadata::FACET, false),
            ),
            DateTimeType::class, DateType::class => new Field\DateTimeField(
                $name,
                multiple: $multiple,
                filterable: $this->flag($metadata, SealMetadata::FILTERABLE, true),
                sortable: $this->flag($metadata, SealMetadata::SORTABLE, true),
                distinct: $this->flag($metadata, SealMetadata::DISTINCT, false),
                facet: $this->flag($metadata, SealMetadata::FACET, false),
            ),
            JsonType::class, MapType::class => new Field\JsonObjectField($name),
            ListType::class => $this->flowToSealField($name, $this->unwrapOptional($type->element()), true, $metadata),
            StructureType::class => new Field\ObjectField($name, $this->structureFields($type), multiple: $multiple),
            default => throw new RuntimeException($type::class . ' is not supported.'),
        };
    }

    /**
     * @return Definition<mixed>
     */
    private function sealFieldToFlowDefinition(AbstractField $field): Definition
    {
        return definition_from_type(
            $field->name,
            $this->sealFieldToFlowType($field),
            !$field instanceof Field\IdentifierField,
        );
    }

    /**
     * @return Type<mixed>
     */
    private function sealFieldToFlowType(AbstractField $field): Type
    {
        if ($field instanceof Field\ObjectField) {
            $type = type_structure(array_map(fn(AbstractField $nested): Type => $this->sealFieldToFlowType(
                $nested,
            ), $field->fields));
        } else {
            $type = match (true) {
                $field instanceof Field\IdentifierField, $field instanceof Field\TextField => type_string(),
                $field instanceof Field\IntegerField => type_integer(),
                $field instanceof Field\FloatField => type_float(),
                $field instanceof Field\BooleanField => type_boolean(),
                $field instanceof Field\DateTimeField => type_datetime(),
                $field instanceof Field\JsonObjectField => type_json(),
                default => throw new RuntimeException($field::class . ' is not supported.'),
            };
        }

        return $field->multiple ? type_list($type) : $type;
    }

    /**
     * @return array<string, AbstractField>
     */
    private function structureFields(StructureType $type): array
    {
        $fields = [];

        foreach ($type->elements() as $element) {
            $elementType = $element->type instanceof OptionalType ? $element->type->base() : $element->type;
            $fields[(string) $element->name] = $this->flowToSealField(
                (string) $element->name,
                $elementType,
                false,
                Metadata::empty(),
            );
        }

        return $fields;
    }

    /**
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    private function unwrapOptional(Type $type): Type
    {
        return $type instanceof OptionalType ? $type->base() : $type;
    }
}
