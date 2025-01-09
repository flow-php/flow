<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use function Flow\Parquet\{array_combine_recursive, array_iterate_at_level};
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

    public function assemble(Column $column, FlatColumnData $flatData) : array
    {
        $depth = 0;

        if ($column instanceof FlatColumn) {
            $rows = [];

            foreach ($this->assemblyFlat($column, $flatData) as $value) {
                $rows[] = [$column->name() => $value];
            }

            $this->nullLevelToNull($rows);

            return $rows;
        }

        /**
         * @var NestedColumn $column
         */
        if ($column->isList()) {
            $rows = [];

            foreach ($this->assemblyList($column, $flatData, $depth) as $value) {
                $rows[] = [$column->name() => $value];
            }

            $this->nullLevelToNull($rows);

            return $rows;
        }

        if ($column->isMap()) {
            $rows = [];

            foreach ($this->assemblyMap($column, $flatData, $depth) as $value) {
                $rows[] = [$column->name() => $value];
            }

            $this->nullLevelToNull($rows);

            return $rows;
        }

        $rows = [];

        foreach ($this->assemblyStructure($column, $flatData, $depth) as $value) {
            $rows[] = [$column->name() => $value instanceof NullLevel ? null : $value];
        }

        $this->nullLevelToNull($rows);

        return $rows;
    }

    private function assemblyFlat(FlatColumn $column, FlatColumnData $flatData) : array
    {
        $stack = new Stack($column->repetitions()->maxRepetitionLevel());

        foreach ($flatData->iterator($column) as $i => $value) {
            $stack->push(
                $value->repetitionLevel,
                $this->definitionConverter->toValue(
                    $column->repetitions(),
                    $value->definitionLevel,
                    $this->dataConverter->fromParquetType($column, $value->value)
                )
            );
        }

        return $stack->dump();
    }

    private function assemblyList(NestedColumn $column, FlatColumnData $flatData, int $depth) : array
    {
        $depth++;

        $rows = [];
        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            return \array_merge($rows, $this->assemblyFlat($listElementColumn, $flatData));
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

    private function assemblyMap(NestedColumn $column, FlatColumnData $flatData, int $depth) : array
    {
        $depth++;
        $rows = [];
        $mapKeyColumn = $column->getMapKeyColumn();
        $mapValueColumn = $column->getMapValueColumn();

        if ($mapValueColumn instanceof FlatColumn) {
            $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);
            $iterator->attachIterator(new \ArrayIterator($this->assemblyFlat($mapKeyColumn, $flatData)), 'key');
            $iterator->attachIterator(new \ArrayIterator($this->assemblyFlat($mapValueColumn, $flatData)), 'value');

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

            $iterator->attachIterator(new \ArrayIterator($this->assemblyFlat($mapKeyColumn, $flatData)), 'key');
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

            $iterator->attachIterator(new \ArrayIterator($this->assemblyFlat($mapKeyColumn, $flatData)), 'key');
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
        $iterator->attachIterator(new \ArrayIterator($this->assemblyFlat($mapKeyColumn, $flatData)), 'key');
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

    private function assemblyStructure(NestedColumn $column, FlatColumnData $flatData, int $depth, bool $repeated = false) : array
    {
        $depth++;
        $iterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($column->children() as $child) {
            if ($child instanceof FlatColumn) {
                $iterator->attachIterator(new \ArrayIterator($this->assemblyFlat($child, $flatData)), $child->name());

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

                $structures = \Flow\Parquet\array_merge_recursive($structures, $propertyValues);
            }

            $rows[] = $structures;
        }

        return $rows;
    }

    private function nullLevelToNull(array &$array) : void
    {
        foreach ($array as &$value) {
            if (is_array($value)) {
                $this->nullLevelToNull($value);
            } elseif ($value instanceof NullLevel) {
                $value = null;
            }
        }

        unset($value);
    }
}
