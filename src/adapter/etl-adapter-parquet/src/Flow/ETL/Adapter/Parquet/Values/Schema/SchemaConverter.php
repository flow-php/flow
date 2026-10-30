<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Values\Schema;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\DateDefinition;
use Flow\ETL\Values\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Values\Schema\Definition\FloatDefinition;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Definition\JsonDefinition;
use Flow\ETL\Values\Schema\Definition\ListDefinition;
use Flow\ETL\Values\Schema\Definition\MapDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definition\StructureDefinition;
use Flow\ETL\Values\Schema\Definition\TimeDefinition;
use Flow\ETL\Values\Schema\Definition\UuidDefinition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;

use function array_map;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;

final class SchemaConverter
{
    /**
     * @return Schema
     */
    public function toFlowSchema(ParquetSchema $schema): Schema
    {
        return new Schema(...array_map(fn(Column $parquetColumn) => $this->parquetToFlowDefinition(
            $parquetColumn,
        ), $schema->columns()));
    }

    /**
     * @param Schema $schema
     */
    public function toParquetSchema(Schema $schema): ParquetSchema
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $columns[] = $this->flowToParquet($definition->name(), $definition->type(), $definition->isNullable());
        }

        return ParquetSchema::with(...$columns);
    }

    /**
     * @param Type<mixed> $type
     */
    private function flowToParquet(string $name, Type $type, bool $nullable): Column
    {
        if ($type instanceof StructureType) {
            foreach ($type->elements() as $element) {
                if ($element->optional) {
                    throw new RuntimeException(sprintf(
                        'Parquet schema does not support structure optional elements, given: %s',
                        $type->toString(),
                    ));
                }
            }
        }

        $repetition = $nullable ? ParquetSchema\Repetition::OPTIONAL : ParquetSchema\Repetition::REQUIRED;

        return match ($type::class) {
            FloatType::class => FlatColumn::double($name, $repetition),
            IntegerType::class => FlatColumn::int64($name, $repetition),
            HTMLType::class,
            HTMLElementType::class,
            XMLElementType::class,
            XMLType::class,
            StringType::class,
            NumericStringType::class,
            NullType::class,
                => FlatColumn::string($name, $repetition),
            BooleanType::class => FlatColumn::boolean($name, $repetition),
            TimeType::class => FlatColumn::time($name, $repetition),
            DateType::class => FlatColumn::date($name, $repetition),
            DateTimeType::class => FlatColumn::datetime($name, $repetition),
            UuidType::class => FlatColumn::uuid($name, $repetition),
            // Parquet has no TIMEZONE logical type, so there is no marker to read back:
            // a timezone column written to parquet returns as a plain string.
            TimeZoneType::class => FlatColumn::string($name, $repetition),
            JsonType::class => FlatColumn::json($name, $repetition),
            ListType::class => NestedColumn::list(
                $name,
                new ListElement($this->flowToParquet(
                    'element',
                    $this->unwrapOptional($type->element()),
                    $type->element() instanceof OptionalType,
                )),
                $repetition,
            ),
            MapType::class => NestedColumn::map(
                $name,
                new ParquetSchema\MapKey($this->flowToParquet('key', $type->key(), false)),
                new ParquetSchema\MapValue($this->flowToParquet(
                    'value',
                    $this->unwrapOptional($type->value()),
                    $type->value() instanceof OptionalType,
                )),
                $repetition,
            ),
            StructureType::class => NestedColumn::struct($name, array_map(function (StructureElement $element) {
                $elementNullable = $element->type instanceof OptionalType;

                return $this->flowToParquet(
                    (string) $element->name,
                    $elementNullable ? $element->type->base() : $element->type,
                    $elementNullable,
                );
            }, $type->elements()), $repetition),
            default => throw new RuntimeException($type::class . ' is not supported.'),
        };
    }

    /**
     * @return Definition<mixed>
     */
    private function parquetToFlowDefinition(Column $column): Definition
    {
        if ($column instanceof FlatColumn) {
            $logicalType = $column->logicalType();

            $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

            if (Integer::forColumn($column)?->is(64, false) === true) {
                return Definitions::of($column->name(), type_numeric_string(), $nullable);
            }

            if ($logicalType === null) {
                return match ($column->type()) {
                    ParquetSchema\PhysicalType::INT32 => match ($column->convertedType()) {
                        ParquetSchema\ConvertedType::DATE => new DateDefinition($column->name(), $nullable),
                        default => new IntegerDefinition($column->name(), $nullable),
                    },
                    ParquetSchema\PhysicalType::INT64 => new IntegerDefinition($column->name(), $nullable),
                    ParquetSchema\PhysicalType::BOOLEAN => new BooleanDefinition($column->name(), $nullable),
                    ParquetSchema\PhysicalType::DOUBLE => new FloatDefinition($column->name(), $nullable),
                    ParquetSchema\PhysicalType::FLOAT => new FloatDefinition($column->name(), $nullable),
                    ParquetSchema\PhysicalType::BYTE_ARRAY => new StringDefinition($column->name(), $nullable),
                    default => throw new RuntimeException($column->type()->name . ' is not supported.'),
                };
            }

            return match ($logicalType->name()) {
                ParquetSchema\LogicalType::STRING, ParquetSchema\LogicalType::ENUM => new StringDefinition(
                    $column->name(),
                    $nullable,
                ),
                ParquetSchema\LogicalType::TIME => new TimeDefinition($column->name(), $nullable),
                ParquetSchema\LogicalType::DATE => new DateDefinition($column->name(), $nullable),
                ParquetSchema\LogicalType::TIMESTAMP => new DateTimeDefinition($column->name(), nullable: $nullable),
                ParquetSchema\LogicalType::UUID => new UuidDefinition($column->name(), $nullable),
                ParquetSchema\LogicalType::JSON => new JsonDefinition($column->name(), $nullable),
                ParquetSchema\LogicalType::DECIMAL => new FloatDefinition($column->name(), $nullable),
                ParquetSchema\LogicalType::INTEGER => new IntegerDefinition($column->name(), $nullable),
                default => throw new RuntimeException($logicalType->name() . ' is not supported.'),
            };
        }

        /** @var NestedColumn $column */
        $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

        if ($column->isList()) {
            return new ListDefinition(
                $column->name(),
                type_list($this->parquetToFlowType($column->getListElement())),
                $nullable,
            );
        }

        if ($column->isMap()) {
            $keyType = $this->parquetToFlowType($column->getMapKeyColumn());

            if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
                throw new RuntimeException('Flow expects map key type to be string or integer type.');
            }

            $mapValueColumn = $column->getMapValueColumn();
            $valueType = $mapValueColumn !== null
                ? $this->parquetToFlowType($mapValueColumn)
                : type_optional(type_string());

            return new MapDefinition($column->name(), type_map($keyType, $valueType), $nullable);
        }

        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[$structColumn->name()] = $this->parquetToFlowType($structColumn);
        }

        return new StructureDefinition($column->name(), type_structure($elements), $nullable);
    }

    /**
     * @return Type<mixed>
     */
    public function parquetToFlowType(Column $column): Type
    {
        if ($column instanceof FlatColumn) {
            $logicalType = $column->logicalType();

            $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

            if (Integer::forColumn($column)?->is(64, false) === true) {
                return $nullable ? type_optional(type_numeric_string()) : type_numeric_string();
            }

            if ($logicalType === null) {
                $type = match ($column->type()) {
                    ParquetSchema\PhysicalType::INT32 => match ($column->convertedType()) {
                        ParquetSchema\ConvertedType::DATE => type_date(),
                        default => type_integer(),
                    },
                    ParquetSchema\PhysicalType::INT64 => type_integer(),
                    ParquetSchema\PhysicalType::BOOLEAN => type_boolean(),
                    ParquetSchema\PhysicalType::DOUBLE => type_float(),
                    ParquetSchema\PhysicalType::FLOAT => type_float(),
                    ParquetSchema\PhysicalType::BYTE_ARRAY => type_string(),
                    default => throw new RuntimeException($column->type()->name . ' is not supported.'),
                };

                return $nullable ? type_optional($type) : $type;
            }

            $type = match ($logicalType->name()) {
                ParquetSchema\LogicalType::STRING, ParquetSchema\LogicalType::ENUM => type_string(),
                ParquetSchema\LogicalType::TIME => type_time(),
                ParquetSchema\LogicalType::DATE => type_date(),
                ParquetSchema\LogicalType::TIMESTAMP => type_datetime(),
                ParquetSchema\LogicalType::UUID => type_uuid(),
                ParquetSchema\LogicalType::JSON => type_json(),
                ParquetSchema\LogicalType::DECIMAL => type_float(),
                ParquetSchema\LogicalType::INTEGER => type_integer(),
                default => throw new RuntimeException($logicalType->name() . ' is not supported.'),
            };

            return $nullable ? type_optional($type) : $type;
        }

        /** @var NestedColumn $column */
        $nullable = $column->repetition() === ParquetSchema\Repetition::OPTIONAL;

        if ($column->isList()) {
            return $nullable
                ? type_optional(type_list($this->parquetToFlowType($column->getListElement())))
                : type_list($this->parquetToFlowType($column->getListElement()));
        }

        if ($column->isMap()) {
            $keyType = $this->parquetToFlowType($column->getMapKeyColumn());

            if (!$keyType instanceof IntegerType && !$keyType instanceof StringType) {
                throw new RuntimeException('Flow expects map key type to be string or integer type.');
            }

            $mapValueColumn = $column->getMapValueColumn();
            $valueType = $mapValueColumn !== null
                ? $this->parquetToFlowType($mapValueColumn)
                : type_optional(type_string());

            return $nullable ? type_optional(type_map($keyType, $valueType)) : type_map($keyType, $valueType);
        }

        $elements = [];

        foreach ($column->children() as $structColumn) {
            $elements[$structColumn->name()] = $this->parquetToFlowType($structColumn);
        }

        return $nullable ? type_optional(type_structure($elements)) : type_structure($elements);
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
