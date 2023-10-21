<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;

final class NestedColumn implements Column
{
    /**
     * @var array<Column>
     */
    private array $children;

    /**
     * @param array<Column> $columns
     */
    public function __construct(
        private readonly FlatColumn $root,
        array $columns,
        private ?Column $parent = null
    ) {
        $this->children = $columns;

        foreach ($columns as $column) {
            if ($column->parent() === null) {
                $column->setParent($this);
            }
        }
    }

    public static function list(string $name, PhysicalType $type, ?LogicalType $logicalType = null) : self
    {
        return new self(
            new FlatColumn($name, PhysicalType::BOOLEAN, new LogicalType(LogicalType::LIST)),
            [
                new self(
                    new FlatColumn('list', PhysicalType::BOOLEAN, repetition: Repetition::REPEATED),
                    [
                        new FlatColumn('element', $type, $logicalType),
                    ]
                ),
            ]
        );
    }

    public static function map(string $name, PhysicalType $type, ?LogicalType $logicalType = null) : self
    {
        return new self(
            new FlatColumn($name, PhysicalType::BOOLEAN, new LogicalType(LogicalType::MAP)),
            [
                new self(
                    new FlatColumn('key_value', PhysicalType::BOOLEAN, repetition: Repetition::REPEATED),
                    [
                        new FlatColumn('key', PhysicalType::BYTE_ARRAY, new LogicalType(LogicalType::STRING), repetition: Repetition::REQUIRED),
                        new FlatColumn('value', $type, $logicalType),
                    ]
                ),
            ]
        );
    }

    /**
     * @param array<Column> $columns
     */
    public static function struct(string $name, array $columns) : self
    {
        return new self(
            new FlatColumn($name, PhysicalType::BOOLEAN, repetition: Repetition::OPTIONAL),
            $columns
        );
    }

    public function __debugInfo() : ?array
    {
        return $this->normalize();
    }

    /**
     * @return array<Column>
     */
    public function children() : array
    {
        return $this->children;
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
        return $this->root->flatPath();
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
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
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
     */
    public function getMapKeyElement() : Column
    {
        if ($this->isMap()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[0];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a map');
    }

    /**
     * @psalm-suppress UndefinedInterfaceMethod
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
     */
    public function getMapValueElement() : Column
    {
        if ($this->isMap()) {
            /** @phpstan-ignore-next-line */
            return $this->children()[0]->children()[1];
        }

        throw new InvalidArgumentException('Column ' . $this->flatPath() . ' is not a map');
    }

    public function isList() : bool
    {
        return $this->logicalType()?->name() === 'LIST';
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
        }

        return false;
    }

    public function isMap() : bool
    {
        return $this->logicalType()?->name() === 'MAP';
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

        return false;
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
        return $this->root->logicalType();
    }

    public function maxDefinitionsLevel() : int
    {
        $level = $this->root->repetition() === Repetition::REQUIRED ? 0 : 1;

        return $this->parent ? $level + $this->parent->maxDefinitionsLevel() : $level;
    }

    public function maxRepetitionsLevel() : int
    {
        $level = $this->root->repetition() === Repetition::REPEATED ? 1 : 0;

        return $this->parent ? $level + $this->parent->maxRepetitionsLevel() : $level;
    }

    public function name() : string
    {
        return $this->root->name();
    }

    public function normalize() : array
    {
        return [
            'type' => 'nested',
            'name' => $this->name(),
            'flat_path' => $this->flatPath(),
            'physical_type' => $this->type()->name,
            'logical_type' => $this->logicalType()?->name(),
            'repetition' => $this->repetition()?->name,
            'max_definition_level' => $this->maxDefinitionsLevel(),
            'max_repetition_level' => $this->maxRepetitionsLevel(),
            'is_map' => $this->isMap(),
            'is_list' => $this->isList(),
            'is_struct' => $this->isStruct(),
            'is_list_element' => $this->isListElement(),
            'is_map_element' => $this->isMapElement(),
            'is_struct_element' => $this->isStructElement(),
            'children' => $this->normalizeChildren(),
        ];
    }

    public function normalizeChildren() : array
    {
        $normalized = [];

        foreach ($this->children as $child) {
            $childData = [
                'type' => $child->type()->name,
                'flat_path' => $child->flatPath(),
                'logical_type' => $child->logicalType()?->name(),
                'repetition' => $child->repetition()?->name,
                'is_list_element' => $child->isListElement(),
                'is_map_element' => $child->isMapElement(),
                'is_struct_element' => $child->isStructElement(),
                'max_repetition_level' => $child->maxRepetitionsLevel(),
                'max_definition_level' => $child->maxDefinitionsLevel(),
            ];

            if ($child instanceof self) {
                $childData['children'] = $child->normalizeChildren();
            }

            $normalized[$child->flatPath()] = $childData;
        }

        return $normalized;
    }

    public function parent() : ?Column
    {
        return $this->parent;
    }

    public function repetition() : ?Repetition
    {
        return $this->root->repetition();
    }

    /**
     * @param array<Column> $columns
     */
    public function setChildren(array $columns) : void
    {
        $this->children = $columns;
    }

    public function setParent(self $parent) : void
    {
        $this->root->setParent($parent);
        $this->parent = $parent;

        foreach ($this->children as $child) {
            $child->setParent($this);
        }
    }

    public function type() : PhysicalType
    {
        return $this->root->type();
    }

    public function typeLength() : ?int
    {
        return $this->root->typeLength();
    }
}
