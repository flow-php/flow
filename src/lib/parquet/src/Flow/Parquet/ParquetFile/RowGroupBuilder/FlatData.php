<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\FlatData\FlatValue;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final class FlatData
{
    /**
     * @var array<FlatValue>
     */
    private array $children = [];

    public function __construct(public readonly Column $column)
    {
        if ($this->column instanceof FlatColumn) {
            $this->children[$this->column->flatPath()] = new FlatValue($this->column, [], [], []);
        }

        if ($this->column instanceof NestedColumn) {
            foreach ($this->column->childrenFlat() as $columnChild) {
                $this->children[$columnChild->flatPath()] = new FlatValue($columnChild, [], [], []);
            }
        }
    }

    public function add(FlatValue ...$flatData) : void
    {
        foreach ($flatData as $data) {
            $this->children[$data->column->flatPath()]->merge($data);
        }
    }

    public function isEmpty(FlatColumn $column) : bool
    {
        foreach ($this->children as $child) {
            if ($child->column->flatPath() === $column->flatPath()) {
                return $child->isEmpty();
            }
        }

        throw new RuntimeException('Column ' . $column->flatPath() . ' not found in FlatData');
    }

    /**
     * @return array<string, {repetition_levels: array<int>, definition_levels: array<int>, values: array<mixed>}>
     */
    public function normalize() : array
    {
        $normalized = [];

        foreach ($this->children as $child) {
            $normalized[$child->column->flatPath()] = [
                'repetition_levels' => $child->repetitionLevels(),
                'definition_levels' => $child->definitionLevels(),
                'values' => $child->values(),
            ];
        }

        return $normalized;
    }
}
