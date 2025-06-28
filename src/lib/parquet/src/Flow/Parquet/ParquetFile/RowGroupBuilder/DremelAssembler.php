<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use function Flow\Parquet\{array_combine_recursive, array_iterate_at_level};
use function Flow\Parquet\array_merge_recursive;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{DefinitionConverter, NullLevel, Stack};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class DremelAssembler
{
    public function __construct(
        private DataConverter $dataConverter,
        private DefinitionConverter $definitionConverter = new DefinitionConverter(),
    ) {
    }

    /**
     * @return array<array-key, mixed>
     */
    public function assemble(Column $column, ReadFlatColumnData $flatData) : array
    {
        $depth = 0;

        if ($column instanceof FlatColumn) {
            $rows = [];

            foreach ($this->assemblyFlat($column, $flatData) as $value) {
                $rows[] = $this->processRowNullLevels([$column->name() => $value]);
            }

            return $rows;
        }

        /**
         * @var NestedColumn $column
         */
        if ($column->isList()) {
            $rows = [];

            foreach ($this->assemblyList($column, $flatData, $depth) as $value) {
                $rows[] = $this->processRowNullLevels([$column->name() => $value]);
            }

            return $rows;
        }

        if ($column->isMap()) {
            $rows = [];

            foreach ($this->assemblyMap($column, $flatData, $depth) as $value) {
                $rows[] = $this->processRowNullLevels([$column->name() => $value]);
            }

            return $rows;
        }

        $rows = [];

        foreach ($this->assemblyStructure($column, $flatData, $depth) as $value) {
            $rows[] = $this->processRowNullLevels([$column->name() => $value instanceof NullLevel ? null : $value]);
        }

        return $rows;
    }

    /**
     * @return \Generator<array-key, mixed>
     */
    private function assemblyFlat(FlatColumn $column, ReadFlatColumnData $flatData) : \Generator
    {
        $stack = new Stack($column->repetitions()->maxRepetitionLevel());

        foreach ($flatData->iterator($column) as $value) {
            if ($value->repetitionLevel === 0) {
                foreach ($stack->dump() as $row) {
                    yield $row;
                }
                $stack = new Stack($column->repetitions()->maxRepetitionLevel());
            }

            $stack->push(
                $value->repetitionLevel,
                $this->definitionConverter->toValue(
                    $column->repetitions(),
                    $value->definitionLevel,
                    $this->dataConverter->fromParquetType($column, $value->value)
                )
            );
        }

        foreach ($stack->dump() as $row) {
            yield $row;
        }
    }

    /**
     * @return array<mixed>
     */
    private function assemblyList(NestedColumn $column, ReadFlatColumnData $flatData, int $depth) : array
    {
        $depth++;

        $rows = [];
        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            foreach ($this->assemblyFlat($listElementColumn, $flatData) as $row) {
                $rows[] = $row;
            }

            return $rows;
        }

        /**
         * @var NestedColumn $listElementColumn
         */
        if ($listElementColumn->isList()) {
            return \array_merge($rows, $this->assemblyList($listElementColumn, $flatData, $depth));
        }

        if ($listElementColumn->isMap()) {
            return \array_merge($rows, $this->assemblyMap($listElementColumn, $flatData, $depth));
        }

        return \array_merge($rows, $this->assemblyStructure($listElementColumn, $flatData, $depth, repeated: true));
    }

    /**
     * @return array<mixed>
     */
    private function assemblyMap(NestedColumn $column, ReadFlatColumnData $flatData, int $depth) : array
    {
        $depth++;
        $rows = [];
        $mapKeyColumn = $column->getMapKeyColumn();
        $mapValueColumn = $column->getMapValueColumn();

        if ($mapValueColumn instanceof FlatColumn) {
            $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);
            $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
            $iterator->attachIterator($this->assemblyFlat($mapValueColumn, $flatData), 'value');

            foreach ($iterator as $iteration) {
                if ($iteration['key'] instanceof NullLevel) {
                    $rows[] = $iteration['key'];

                    continue;
                }

                $rows[] = array_combine_recursive($iteration['key'], $iteration['value']);
            }

            return $rows;
        }

        /**
         * @var NestedColumn $mapValueColumn
         */
        if ($mapValueColumn->isList()) {

            $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

            $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
            $iterator->attachIterator(new \ArrayIterator($this->assemblyList($mapValueColumn, $flatData, $depth)), 'value');

            foreach ($iterator as $iteration) {
                if ($iteration['key'] instanceof NullLevel) {
                    $rows[] = $iteration['key'];

                    continue;
                }

                $rows[] = array_combine_recursive($iteration['key'], $iteration['value']);
            }

            return $rows;
        }

        if ($mapValueColumn->isMap()) {
            $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

            $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
            $iterator->attachIterator(new \ArrayIterator($this->assemblyMap($mapValueColumn, $flatData, $depth)), 'value');

            foreach ($iterator as $iteration) {
                if ($iteration['key'] instanceof NullLevel) {
                    $rows[] = $iteration['key'];

                    continue;
                }

                $rows[] = array_combine_recursive($iteration['key'], $iteration['value']);
            }

            return $rows;
        }

        $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);
        $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
        $iterator->attachIterator(new \ArrayIterator($this->assemblyStructure($mapValueColumn, $flatData, $depth, repeated: true)), 'value');

        foreach ($iterator as $iteration) {
            if ($iteration['key'] instanceof NullLevel) {
                $rows[] = $iteration['key'];

                continue;
            }

            $rows[] = array_combine_recursive($iteration['key'], $iteration['value']);
        }

        return $rows;
    }

    /**
     * @return array<array-key, mixed>
     */
    private function assemblyStructure(NestedColumn $column, ReadFlatColumnData $flatData, int $depth, bool $repeated = false) : array
    {
        $depth++;
        $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($column->children() as $child) {
            if ($child instanceof FlatColumn) {
                $iterator->attachIterator($this->assemblyFlat($child, $flatData), $child->name());

                continue;
            }

            /**
             * @var NestedColumn $child
             */
            if ($child->isList()) {
                $iterator->attachIterator(new \ArrayIterator($this->assemblyList($child, $flatData, $depth)), $child->name());

                continue;
            }

            if ($child->isMap()) {
                $iterator->attachIterator(new \ArrayIterator($this->assemblyMap($child, $flatData, $depth)), $child->name());

                continue;
            }

            $iterator->attachIterator(new \ArrayIterator($this->assemblyStructure($child, $flatData, $depth, $repeated)), $child->name());
        }

        if (!$repeated) {
            $rows = [];

            foreach ($iterator as $iteration) {
                $structure = [];

                foreach ($iteration as $propertyName => $propertyValue) {

                    if ($propertyValue instanceof NullLevel && $propertyValue->level < $depth) {
                        $rows[] = new NullLevel($propertyValue->level);

                        continue 2;
                    }

                    $structure[$propertyName] = $propertyValue;
                }

                $rows[] = $structure;
            }

            return $rows;
        }

        $rows = [];

        foreach ($iterator as $iteration) {
            $structures = [];

            foreach ($iteration as $propertyName => $propertyValues) {

                if ($propertyValues instanceof NullLevel && $propertyValues->level <= $depth) {
                    $rows[] = new NullLevel($propertyValues->level);

                    continue 2;
                }

                array_iterate_at_level(
                    $propertyValues,
                    $column->repetitions()->maxRepetitionLevel(),
                    static function (mixed &$value) use ($propertyName, $column) : void {

                        if ($value instanceof NullLevel && $value->level + 1 === $column->repetitions()->maxDefinitionLevel()) {
                            return;
                        }

                        $value = [$propertyName => $value];
                    }
                );

                $structures = array_merge_recursive($structures, $propertyValues);
            }

            $rows[] = $structures;
        }

        return $rows;
    }

    /**
     * @param array<array-key, mixed> $row
     *
     * @return array<array-key, mixed>
     */
    private function processRowNullLevels(array $row) : array
    {
        foreach ($row as &$value) {
            if (is_array($value)) {
                $value = $this->processRowNullLevels($value);
            } elseif ($value instanceof NullLevel) {
                $value = null;
            }
        }
        unset($value);

        return $row;
    }
}
