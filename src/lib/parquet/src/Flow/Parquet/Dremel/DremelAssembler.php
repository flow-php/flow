<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\DefinitionConverter;
use Flow\Parquet\Dremel\ColumnData\NullLevel;
use Flow\Parquet\Dremel\ColumnData\Stack;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Generator;
use MultipleIterator;

use function Flow\Parquet\array_iterate_at_level;
use function Flow\Parquet\array_merge_recursive;
use function Flow\Parquet\dremel_array_combine_recursive;
use function Flow\Types\DSL\type_array;
use function gettype;
use function is_array;

final readonly class DremelAssembler
{
    public function __construct(
        private DataConverter $dataConverter,
        private DefinitionConverter $definitionConverter = new DefinitionConverter(),
    ) {}

    /**
     * @return \Generator<array-key, mixed>
     */
    public function assemble(Column $column, ReadColumnData $flatData): Generator
    {
        $depth = 0;

        if ($column instanceof FlatColumn) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyFlat($column, $flatData) as $value) {
                yield $this->processRowNullLevels([$column->name() => $value]);
            }

            return;
        }

        /**
         * @var NestedColumn $column
         */
        if ($column->isList()) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyList($column, $flatData, $depth) as $value) {
                yield $this->processRowNullLevels([$column->name() => $value]);
            }

            return;
        }

        if ($column->isMap()) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyMap($column, $flatData, $depth) as $value) {
                yield $this->processRowNullLevels([$column->name() => $value]);
            }

            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->assemblyStructure($column, $flatData, $depth) as $value) {
            yield $this->processRowNullLevels([$column->name() => $value instanceof NullLevel ? null : $value]);
        }
    }

    /**
     * @return \Generator<array-key, mixed>
     */
    private function assemblyFlat(FlatColumn $column, ReadColumnData $flatData): Generator
    {
        if ($column->repetitions()->maxRepetitionLevel() === 0) {
            yield from $flatData->flatValues[$column->flatPath()]->assembleFlat($this->dataConverter);

            return;
        }

        $stack = new Stack($column->repetitions()->maxRepetitionLevel());

        foreach ($flatData->iterator($column) as $value) {
            if ($value->repetitionLevel === 0) {
                // @mago-ignore analysis:mixed-assignment
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
                    $this->dataConverter->fromParquetType($column, $value->value),
                ),
            );
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($stack->dump() as $row) {
            yield $row;
        }
    }

    /**
     * @return \Generator<mixed>
     */
    private function assemblyList(NestedColumn $column, ReadColumnData $flatData, int $depth): Generator
    {
        $depth++;

        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyFlat($listElementColumn, $flatData) as $row) {
                yield $row;
            }

            return;
        }

        /**
         * @var NestedColumn $listElementColumn
         */
        if ($listElementColumn->isList()) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyList($listElementColumn, $flatData, $depth) as $row) {
                yield $row;
            }

            return;
        }

        if ($listElementColumn->isMap()) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyMap($listElementColumn, $flatData, $depth) as $row) {
                yield $row;
            }

            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->assemblyStructure($listElementColumn, $flatData, $depth, repeated: true) as $row) {
            yield $row;
        }
    }

    /**
     * @return \Generator<mixed>
     */
    private function assemblyMap(NestedColumn $column, ReadColumnData $flatData, int $depth): Generator
    {
        $depth++;
        $mapKeyColumn = $column->getMapKeyColumn();
        $mapValueColumn = $column->getMapValueColumn();

        if ($mapValueColumn === null) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($this->assemblyFlat($mapKeyColumn, $flatData) as $key) {
                if ($key instanceof NullLevel) {
                    yield $key;

                    continue;
                }

                if (is_array($key)) {
                    $result = [];

                    // @mago-ignore analysis:mixed-assignment
                    foreach ($key as $k) {
                        /** @var int|string $k */
                        $result[$k] = null;
                    }

                    yield $result;
                } else {
                    /** @var int|string $key */
                    yield [$key => null];
                }
            }

            return;
        }

        if ($mapValueColumn instanceof FlatColumn) {
            $iterator = new MultipleIterator(MultipleIterator::MIT_KEYS_ASSOC);
            $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
            $iterator->attachIterator($this->assemblyFlat($mapValueColumn, $flatData), 'value');

            foreach ($iterator as $iteration) {
                if ($iteration['key'] instanceof NullLevel) {
                    yield $iteration['key'];

                    continue;
                }

                yield dremel_array_combine_recursive(
                    type_array()->assert($iteration['key']),
                    type_array()->assert($iteration['value']),
                );
            }

            return;
        }

        /**
         * @var NestedColumn $mapValueColumn
         */
        if ($mapValueColumn->isList()) {
            $iterator = new MultipleIterator(MultipleIterator::MIT_KEYS_ASSOC);

            $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
            $iterator->attachIterator($this->assemblyList($mapValueColumn, $flatData, $depth), 'value');

            foreach ($iterator as $iteration) {
                if ($iteration['key'] instanceof NullLevel) {
                    yield $iteration['key'];

                    continue;
                }

                yield dremel_array_combine_recursive(
                    type_array()->assert($iteration['key']),
                    type_array()->assert($iteration['value']),
                );
            }

            return;
        }

        if ($mapValueColumn->isMap()) {
            $iterator = new MultipleIterator(MultipleIterator::MIT_KEYS_ASSOC);

            $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
            $iterator->attachIterator($this->assemblyMap($mapValueColumn, $flatData, $depth), 'value');

            foreach ($iterator as $iteration) {
                if ($iteration['key'] instanceof NullLevel) {
                    yield $iteration['key'];

                    continue;
                }

                yield dremel_array_combine_recursive(
                    type_array()->assert($iteration['key']),
                    type_array()->assert($iteration['value']),
                );
            }

            return;
        }

        $iterator = new MultipleIterator(MultipleIterator::MIT_KEYS_ASSOC);
        $iterator->attachIterator($this->assemblyFlat($mapKeyColumn, $flatData), 'key');
        $iterator->attachIterator(
            $this->assemblyStructure($mapValueColumn, $flatData, $depth, repeated: true),
            'value',
        );

        foreach ($iterator as $iteration) {
            if ($iteration['key'] instanceof NullLevel) {
                yield $iteration['key'];

                continue;
            }

            yield dremel_array_combine_recursive(
                type_array()->assert($iteration['key']),
                type_array()->assert($iteration['value']),
            );
        }
    }

    /**
     * @return \Generator<array-key, mixed>
     */
    private function assemblyStructure(
        NestedColumn $column,
        ReadColumnData $flatData,
        int $depth,
        bool $repeated = false,
    ): Generator {
        $depth++;
        $iterator = new MultipleIterator(MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($column->children() as $child) {
            if ($child instanceof FlatColumn) {
                $iterator->attachIterator($this->assemblyFlat($child, $flatData), $child->name());

                continue;
            }

            /**
             * @var NestedColumn $child
             */
            if ($child->isList()) {
                $iterator->attachIterator($this->assemblyList($child, $flatData, $depth), $child->name());

                continue;
            }

            if ($child->isMap()) {
                $iterator->attachIterator($this->assemblyMap($child, $flatData, $depth), $child->name());

                continue;
            }

            $iterator->attachIterator($this->assemblyStructure($child, $flatData, $depth, $repeated), $child->name());
        }

        if (!$repeated) {
            foreach ($iterator as $iteration) {
                $structure = [];

                // @mago-ignore analysis:mixed-assignment
                foreach ($iteration as $propertyName => $propertyValue) {
                    if ($propertyValue instanceof NullLevel && $propertyValue->level < $depth) {
                        yield new NullLevel($propertyValue->level);

                        continue 2;
                    }

                    $structure[$propertyName] = $propertyValue;
                }

                yield $structure;
            }

            return;
        }

        foreach ($iterator as $iteration) {
            $structures = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($iteration as $propertyName => $propertyValues) {
                if ($propertyValues instanceof NullLevel && $propertyValues->level <= $depth) {
                    yield new NullLevel($propertyValues->level);

                    continue 2;
                }

                if (!is_array($propertyValues)) {
                    throw new InvalidArgumentException(
                        'Expected property values to be an array, got ' . gettype($propertyValues),
                    );
                }

                array_iterate_at_level(
                    $propertyValues,
                    $column->repetitions()->maxRepetitionLevel(),
                    static function (mixed &$value) use ($propertyName, $column): void {
                        if (
                            $value instanceof NullLevel
                            && ($value->level + 1) === $column->repetitions()->maxDefinitionLevel()
                        ) {
                            return;
                        }

                        $value = [$propertyName => $value];
                    },
                );

                $structures = array_merge_recursive($structures, $propertyValues);
            }

            yield $structures;
        }
    }

    /**
     * @param array<array-key, mixed> $row
     *
     * @return array<array-key, mixed>
     */
    private function processRowNullLevels(array $row): array
    {
        // @mago-ignore analysis:mixed-assignment
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
