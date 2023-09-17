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
        private readonly int $maxDefinitionsLevel,
        private readonly int $maxRepetitionsLevel,
        private ?Column $parent = null
    ) {
        $this->children = $columns;
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
        return $this->maxDefinitionsLevel;
    }

    public function maxRepetitionsLevel() : int
    {
        return $this->maxRepetitionsLevel;
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
                'logical_type' => $child->logicalType()?->name(),
                'optional' => $child->repetition() === Repetition::OPTIONAL,
                'repeated' => $child->repetition() === Repetition::REPEATED,
                'is_list_element' => $child->isListElement(),
                'is_map_element' => $child->isMapElement(),
                'is_struct_element' => $child->isStructElement(),
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

    public function type() : PhysicalType
    {
        return $this->root->type();
    }
}
