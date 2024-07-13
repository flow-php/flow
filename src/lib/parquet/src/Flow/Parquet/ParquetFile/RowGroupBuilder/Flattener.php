<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final class Flattener
{
    public function __construct(private readonly Validator $validator)
    {
    }

    /**
     * @param Column $column
     * @param array<mixed> $row
     *
     * @return array<mixed>
     */
    public function flattenColumn(Column $column, array $row) : array
    {
        if (!\array_key_exists($column->name(), $row)) {
            $this->validator->validate($column, null);

            return [$column->name() => null];
        }

        /**
         * @var mixed $columnData
         */
        $columnData = $row[$column->name()];

        if ($columnData === null) {
            $this->validator->validate($column, null);
        }

        if ($column instanceof FlatColumn) {
            $this->validator->validate($column, $columnData);

            return [
                $column->flatPath() => $columnData,
            ];
        }

        /** @var NestedColumn $column */
        if ($column->isList()) {
            return $this->flattenList($column, $columnData);
        }

        if ($column->isMap()) {
            return $this->flattenMap($column, $columnData);
        }

        if ($column->isStruct()) {
            return $this->flattenStructure($column, $columnData);
        }

        throw new RuntimeException('Unknown column type');
    }

    private function flattenList(NestedColumn $column, mixed $columnData) : array
    {
        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            $this->validator->validate($listElementColumn, $columnData);

            return [
                $listElementColumn->flatPath() => $columnData,
            ];
        }

        /** @var NestedColumn $listElementColumn */
        if ($listElementColumn->isList()) {
            $data = [];

            if (\is_array($columnData)) {
                if (\count($columnData)) {
                    foreach ($columnData as $listElement) {
                        $data[] = $this->flattenColumn($listElementColumn, [$listElementColumn->name() => [$listElement]]);
                    }
                } else {
                    $data[] = $this->flattenColumn($listElementColumn, [$listElementColumn->name() => []]);
                }
            }

            if ($columnData === null) {
                $data[] = $this->flattenColumn($listElementColumn, [$listElementColumn->name() => null]);
            }

            return \array_merge_recursive(...$data);
        }

        if ($listElementColumn->isMap()) {
            $data = [];

            if (\is_array($columnData)) {
                if (\count($columnData)) {
                    foreach ($columnData as $listMapElementData) {
                        foreach ($this->flattenMap($listElementColumn, $listMapElementData) as $key => $value) {
                            $data[$key][] = $value;
                        }
                    }
                } else {
                    foreach ($this->flattenColumn($listElementColumn, [$listElementColumn->name() => []]) as $key => $value) {
                        $data[$key] = $value;
                    }
                }
            }

            if ($columnData === null) {
                foreach ($this->flattenColumn($listElementColumn, [$listElementColumn->name() => null]) as $key => $value) {
                    $data[$key] = $value;
                }
            }

            return $data;
        }

        $data = [];

        if ($columnData === null) {
            foreach ($this->flattenStructure($listElementColumn, null) as $key => $value) {
                $data[$key] = $value;
            }
        }

        if (\is_array($columnData)) {
            if (\count($columnData)) {
                foreach ($columnData as $listStructureElementData) {
                    foreach ($this->flattenStructure($listElementColumn, $listStructureElementData) as $key => $value) {
                        $data[$key][] = $value;
                    }
                }
            } else {
                foreach ($this->flattenStructure($listElementColumn, []) as $key => $value) {
                    $data[$key] = [];
                }
            }
        }

        return $data;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     * @psalm-suppress NamedArgumentNotAllowed
     *
     * @return array<mixed>
     */
    private function flattenMap(NestedColumn $column, mixed $columnData) : array
    {
        $keyColumn = $column->getMapKeyColumn();
        $valueColumn = $column->getMapValueColumn();

        if ($valueColumn instanceof FlatColumn) {
            if ($columnData === null) {
                return [
                    $keyColumn->flatPath() => null,
                    $valueColumn->flatPath() => null,
                ];
            }

            $this->validator->validate($keyColumn, \array_keys($columnData));
            $this->validator->validate($valueColumn, \array_values($columnData));

            return [
                $keyColumn->flatPath() => \array_keys($columnData),
                $valueColumn->flatPath() => \array_values($columnData),
            ];
        }

        if ($valueColumn->isList()) {
            if ($columnData === null) {
                $data = [
                    $keyColumn->flatPath() => null,
                ];
            } else {
                $data = [
                    $keyColumn->flatPath() => \array_keys($columnData),
                ];
            }

            if (\is_array($columnData)) {
                if (\count($columnData) === 0) {
                    foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => []]) as $key => $value) {
                        $data[$key] = $value;
                    }
                } else {
                    foreach ($columnData as $listElement) {
                        foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => $listElement]) as $key => $value) {
                            $data[$key][] = $value;
                        }
                    }
                }
            }

            if ($columnData === null) {
                foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => null]) as $key => $value) {
                    $data[$key] = $value;
                }
            }

            return $data;
        }

        if ($valueColumn->isMap()) {
            if ($columnData === null) {
                $data = [
                    $keyColumn->flatPath() => null,
                ];
            } else {
                $data = [
                    $keyColumn->flatPath() => \array_keys($columnData),
                ];
            }

            if (\is_array($columnData)) {
                if (\count($columnData)) {
                    foreach ($columnData as $mapValue) {
                        foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => $mapValue]) as $key => $value) {
                            $data[$key][] = $value;
                        }
                    }
                } else {
                    foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => []]) as $key => $value) {
                        $data[$key] = $value;
                    }
                }
            }

            if ($columnData === null) {
                foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => null]) as $key => $value) {
                    $data[$key] = $value;
                }
            }

            return $data;
        }

        $data = [];

        if ($columnData === null) {
            $data = [
                $keyColumn->flatPath() => null,
            ];
        }

        if (\is_array($columnData)) {
            if (\count($columnData)) {
                foreach ($columnData as $structElement) {
                    $data[] = $this->flattenColumn($valueColumn, [$valueColumn->name() => $structElement]);
                }
            } else {
                foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => []]) as $key => $value) {
                    $data[$key] = [];
                }

                return \array_merge(
                    [
                        $keyColumn->flatPath() => \array_keys($columnData),
                    ],
                    $data
                );
            }
        } else {
            foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => null]) as $key => $value) {
                $data[$key] = $value;
            }

            return \array_merge(
                [
                    $keyColumn->flatPath() => $columnData,
                ],
                $data
            );
        }

        return \array_merge(
            [
                $keyColumn->flatPath() => \array_keys($columnData),
            ],
            /** @phpstan-ignore-next-line */
            \array_merge_recursive(...$data)
        );
    }

    private function flattenStructure(NestedColumn $column, mixed $columnData) : array
    {
        $data = [];

        if ($columnData === null) {
            foreach ($column->children() as $child) {
                $data = \array_merge($data, $this->flattenColumn($child, [$child->name() => null]));
            }

            return $data;
        }

        foreach ($column->children() as $child) {
            $fieldData = [$child->name() => $columnData[$child->name()] ?? null];

            $data = \array_merge($data, $this->flattenColumn($child, $fieldData));
        }

        return $data;
    }
}
