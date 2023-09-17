<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Thrift\SchemaElement;

/**
 * @psalm-suppress RedundantCastGivenDocblockType
 * @psalm-suppress RedundantConditionGivenDocblockType
 * @psalm-suppress DocblockTypeContradiction
 */
final class FlatColumn implements Column
{
    public function __construct(
        private readonly string $name,
        private readonly PhysicalType $type,
        private readonly ?LogicalType $logicalType,
        private readonly ?Repetition $repetition,
        private readonly ?int $precision,
        private readonly ?int $scale,
        private readonly ?int $typeLength,
        private readonly int $maxDefinitionsLevel,
        private readonly int $maxRepetitionsLevel,
        private readonly ?string $rootPath,
        private ?Column $parent
    ) {
    }

    public static function fromThrift(
        SchemaElement $schemaElement,
        int $maxDefinitionsLevel,
        int $maxRepetitionsLevel,
        ?string $rootPath = null,
        ?Column $parent = null
    ) : self {
        return new self(
            $schemaElement->name,
            PhysicalType::from((int) $schemaElement->type),
            $schemaElement->logicalType === null ? null : LogicalType::fromThrift($schemaElement->logicalType),
            $schemaElement->repetition_type === null ? null : Repetition::from($schemaElement->repetition_type),
            $schemaElement->precision,
            $schemaElement->scale,
            $schemaElement->type_length,
            $maxDefinitionsLevel,
            $maxRepetitionsLevel,
            $rootPath,
            $parent
        );
    }

    public function __debugInfo() : ?array
    {
        return $this->normalize();
    }

    /** @psalm-suppress PossiblyNullOperand */
    public function ddl() : array
    {
        return [
            /** @phpstan-ignore-next-line */
            'type' => $this->type()->name . ($this->logicalType()?->name() !== null ? ' (' . $this->logicalType()?->name() . ')' : ''),
            'optional' => $this->repetition()?->value === Repetition::OPTIONAL->value,
        ];
    }

    public function flatPath() : string
    {
        if ($this->rootPath !== null) {
            return $this->rootPath . '.' . $this->name;
        }

        return $this->name;
    }

    public function isList() : bool
    {
        return false;
    }

    public function isListElement() : bool
    {
        if ($this->parent !== null) {
            // element
            if ($this->parent->logicalType()?->name() === 'LIST') {
                return true;
            }

            // list.element
            if ($this->parent->parent()?->logicalType()?->name() === 'LIST') {
                return true;
            }

            // list.element.{column}
            if ($this->parent->parent()?->parent()?->logicalType()?->name() === 'LIST') {
                return true;
            }
        }

        return false;
    }

    public function isMap() : bool
    {
        return false;
    }

    public function isMapElement() : bool
    {
        if ($this->parent === null) {
            return false;
        }

        if ($this->parent()?->logicalType()?->name() === 'MAP') {
            return true;
        }

        if ($this->parent()?->parent()?->logicalType()?->name() === 'MAP') {
            return true;
        }

        if ($this->parent()?->parent()?->parent()?->logicalType()?->name() === 'MAP') {
            return true;
        }

        return false;
    }

    public function isStruct() : bool
    {
        return false;
    }

    public function isStructElement() : bool
    {
        $parent = $this->parent();

        if ($parent === null) {
            return false;
        }

        /** @var NestedColumn $parent */
        if ($parent->isList()) {
            return false;
        }

        if ($parent->isMap()) {
            return false;
        }

        return true;
    }

    public function logicalType() : ?LogicalType
    {
        return $this->logicalType;
    }

    public function maxDefinitionsLevel() : int
    {
        return $this->maxDefinitionsLevel;
    }

    public function maxRepetitionsLevel() : int
    {
        return $this->maxRepetitionsLevel;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function normalize() : array
    {
        return [
            'type' => 'flat',
            'name' => $this->name(),
            'flat_path' => $this->flatPath(),
            'physical_type' => $this->type()->name,
            'logical_type' => $this->logicalType()?->name(),
            'repetition' => $this->repetition()?->name,
            'precision' => $this->precision(),
            'scale' => $this->scale(),
            'max_definition_level' => $this->maxDefinitionsLevel,
            'max_repetition_level' => $this->maxRepetitionsLevel,
            'children' => null,
            'is_map' => $this->isMap(),
            'is_list' => $this->isList(),
            'is_struct' => $this->isStruct(),
            'is_list_element' => $this->isListElement(),
            'is_map_element' => $this->isMapElement(),
            'is_struct_element' => $this->isStructElement(),
        ];
    }

    public function parent() : ?Column
    {
        return $this->parent;
    }

    public function precision() : ?int
    {
        return $this->precision;
    }

    public function repetition() : ?Repetition
    {
        return $this->repetition;
    }

    public function scale() : ?int
    {
        return $this->scale;
    }

    public function type() : PhysicalType
    {
        return $this->type;
    }

    public function typeLength() : ?int
    {
        return $this->typeLength;
    }
}
