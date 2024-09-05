<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Thrift\SchemaElement;

final class NestedColumn implements Column
{
    private ?string $flatPath = null;

    private ?self $parent = null;

    /**
     * @param array<Column> $children
     */
    public function __construct(
        private readonly string $name,
        private ?Repetition $repetition,
        private readonly array $children,
        private readonly ?ConvertedType $convertedType = null,
        private readonly ?LogicalType $logicalType = null,
        public readonly bool $schemaRoot = false,
    ) {
        foreach ($children as $child) {
            $child->setParent($this);
        }
    }

    /**
     * @param array<Column> $columns
     */
    public static function create(string $name, array $columns) : self
    {
        return new self($name, Repetition::OPTIONAL, $columns);
    }

    /**
     * @param array<Column> $children
     */
    public static function fromThrift(SchemaElement $schemaElement, array $children) : self
    {
        return new self(
            $schemaElement->name,
            $schemaElement->repetition_type ? Repetition::from($schemaElement->repetition_type) : null,
            $children,
            $schemaElement->converted_type ? ConvertedType::from($schemaElement->converted_type) : null,
            $schemaElement->logicalType ? LogicalType::fromThrift($schemaElement->logicalType) : null
        );
    }

    public static function list(string $name, ListElement $element, Repetition $repetition = Repetition::OPTIONAL) : self
    {
        return new self(
            $name,
            $repetition,
            [
                new self(
                    'list',
                    Repetition::REPEATED,
                    [$element->element]
                ),
            ],
            ConvertedType::LIST,
            new LogicalType(LogicalType::LIST)
        );
    }

    public static function map(string $name, MapKey $key, MapValue $value, Repetition $repetition = Repetition::OPTIONAL) : self
    {
        return new self(
            $name,
            $repetition,
            [
                new self(
                    'key_value',
                    Repetition::REPEATED,
                    [
                        $key->key,
                        $value->value,
                    ],
                ),
            ],
            ConvertedType::MAP,
            new LogicalType(LogicalType::MAP)
        );
    }

    /**
     * @param array<Column> $children
     */
    public static function schemaRoot(string $name, array $children) : self
    {
        return new self($name, Repetition::REQUIRED, $children, null, null, true);
    }

    /**
     * @param array<Column> $children
     */
    public static function struct(string $name, array $children, Repetition $repetition = Repetition::OPTIONAL) : self
    {
        return new self($name, $repetition, $children);
    }

    public static function structure(string $name, array $children, Repetition $repetition = Repetition::OPTIONAL) : self
    {
        return new self($name, $repetition, $children);
    }

    public function __debugInfo() : ?array
    {
        return [
            'name' => $this->name,
            'type' => 'nested_column',
            'flat_path' => $this->flatPath(),
            'parent' => $this->parent ? [
                'name' => $this->parent->name(),
                'flat_path' => $this->parent->flatPath(),
            ] : null,
            'physical_type' => $this->type(),
            'logical_type' => $this->logicalType,
            'converted_type' => $this->convertedType,
            'repetition' => $this->repetition,
            'max_definitions_level' => $this->maxDefinitionsLevel(),
            'max_repetitions_level' => $this->maxRepetitionsLevel(),
        ];
    }

    /**
     * @return array<Column>
     */
    public function children() : array
    {
        return $this->children;
    }

    /**
     * @return array<string, FlatColumn>
     */
    public function childrenFlat() : array
    {
        $flat = [];

        foreach ($this->children as $child) {
            if ($child instanceof self) {
                $flat = \array_merge($flat, $child->childrenFlat());
            } else {
                /** @var FlatColumn $child */
                $flat[$child->flatPath()] = $child;
            }
        }

        return $flat;
    }

    public function convertedType() : ?ConvertedType
    {
        return $this->convertedType;
    }

    public function ddl() : array
    {
        $ddlArray = [
            'type' => 'group',
            'optional' => $this->repetition()?->value === Repetition::OPTIONAL->value,
            'children' => [],
        ];

        foreach ($this->children as $column) {
            $ddlArray['children'][$column->name()] = $column->ddl();
        }

        return $ddlArray;
    }

    public function flatPath() : string
    {
        if ($this->flatPath !== null) {
            return $this->flatPath;
        }

        $parent = $this->parent();

        if ($parent?->schemaRoot) {
            $this->flatPath = $this->name;

            return $this->flatPath;
        }

        $path = [$this->name];

        while ($parent) {
            $path[] = $parent->name();
            $parent = $parent->parent();

            if ($parent && $parent->schemaRoot) {
                break;
            }
        }

        $path = \array_reverse($path);
        $this->flatPath = \implode('.', $path);

        return $this->flatPath;
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function getListElement() : Column
    {
        if ($this->isList()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[0];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a list');
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function getMapKeyColumn() : FlatColumn
    {
        if ($this->isMap()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[0];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a map');
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     */
    public function getMapValueColumn() : Column
    {
        if ($this->isMap()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[1];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a map');
    }

    public function isList() : bool
    {
        return $this->logicalType()?->name() === 'LIST' || $this->convertedType() === ConvertedType::LIST;
    }

    public function isListElement() : bool
    {
        if ($this->parent !== null) {
            // element
            if ($this->parent->logicalType()?->name() === 'LIST' || $this->parent->convertedType() === ConvertedType::LIST) {
                return true;
            }

            // list.element
            if ($this->parent->parent()?->logicalType()?->name() === 'LIST' || $this->parent->parent()?->convertedType() === ConvertedType::LIST) {
                return true;
            }
        }

        return false;
    }

    public function isMap() : bool
    {
        return $this->logicalType()?->name() === 'MAP' || $this->convertedType() === ConvertedType::MAP;
    }

    public function isMapElement() : bool
    {
        if ($this->parent === null) {
            return false;
        }

        if ($this->parent()?->logicalType()?->name() === 'MAP' || $this->parent()?->convertedType() === ConvertedType::MAP) {
            return true;
        }

        if ($this->parent()?->parent()?->logicalType()?->name() === 'MAP' || $this->parent()?->parent()?->convertedType() === ConvertedType::MAP) {
            return true;
        }

        return false;
    }

    public function isRequired() : bool
    {
        return $this->repetition !== Repetition::OPTIONAL;
    }

    public function isStruct() : bool
    {
        if ($this->isMap()) {
            return false;
        }

        if ($this->isList()) {
            return false;
        }

        return true;
    }

    public function isStructElement() : bool
    {
        if ($this->isMapElement()) {
            return false;
        }

        if ($this->isListElement()) {
            return false;
        }

        return true;
    }

    public function logicalType() : ?LogicalType
    {
        return $this->logicalType;
    }

    public function makeRequired() : self
    {
        $this->repetition = Repetition::REQUIRED;

        return $this;
    }

    public function maxDefinitionsLevel() : int
    {
        if ($this->repetition === null) {
            $level = 0;
        } else {
            $level = $this->repetition() === Repetition::REQUIRED ? 0 : 1;
        }

        return $this->parent ? $level + $this->parent->maxDefinitionsLevel() : $level;
    }

    public function maxRepetitionsLevel() : int
    {
        if ($this->repetition === null) {
            $level = 0;
        } else {
            $level = $this->repetition() === Repetition::REPEATED ? 1 : 0;
        }

        return $this->parent ? $level + $this->parent->maxRepetitionsLevel() : $level;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function parent() : ?self
    {
        return $this->parent;
    }

    public function path() : array
    {
        return \explode('.', $this->flatPath());
    }

    public function repetition() : ?Repetition
    {
        return $this->repetition;
    }

    public function setParent(self $parent) : void
    {
        $this->flatPath = null;
        $this->parent = $parent;

        foreach ($this->children as $child) {
            $child->setParent($this);
        }
    }

    /**
     * @return array<SchemaElement>
     */
    public function toThrift() : array
    {
        $elements = [
            new SchemaElement([
                'name' => $this->name(),
                'num_children' => \count($this->children),
                'converted_type' => $this->convertedType?->value,
                'repetition_type' => $this->repetition()?->value,
                'logicalType' => $this->logicalType()?->toThrift(),
            ]),
        ];

        foreach ($this->children as $child) {
            if ($child instanceof FlatColumn) {
                $elements[] = $child->toThrift();
            }

            if ($child instanceof self) {
                $elements = \array_merge($elements, $child->toThrift());
            }
        }

        return $elements;
    }

    public function type() : ?PhysicalType
    {
        return null;
    }

    public function typeLength() : ?int
    {
        return null;
    }
}
