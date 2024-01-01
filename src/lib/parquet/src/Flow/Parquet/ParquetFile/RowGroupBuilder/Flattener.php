<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

final class Flattener
{
    public function __construct(private readonly Validator $validator)
    {
    }

    public function flattenColumn(Column $column, array $row) : array
    {
        if (!\array_key_exists($column->name(), $row)) {
            $this->validator->validate($column, null);

            return [];
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

        //        if ($columnData === null) {
        //            return [
        //                $listElementColumn->flatPath() => null,
        //            ];
        //        }

        if ($listElementColumn instanceof FlatColumn) {
            $this->validator->validate($listElementColumn, $columnData);

            return [
                $listElementColumn->flatPath() => $columnData,
            ];
        }

        /** @var NestedColumn $listElementColumn */
        if ($listElementColumn->isList()) {
            $data = [];

            foreach ($columnData as $listElement) {
                $data[] = $this->flattenColumn($listElementColumn, [$listElementColumn->name() => [$listElement]]);
            }

            return \array_merge_recursive(...$data);
        }

        if ($listElementColumn->isMap()) {
            $data = [];

            foreach ($columnData as $listMapElementData) {
                foreach ($this->flattenMap($listElementColumn, $listMapElementData) as $key => $value) {
                    $data[$key][] = $value;
                }
            }

            return $data;
        }

        $data = [];

        if ($columnData === null) {
            foreach ($this->flattenStructure($listElementColumn, null) as $key => $value) {
                $data[$key][] = $value;
            }
        } else {
            foreach ($columnData as $listStructureElementData) {
                foreach ($this->flattenStructure($listElementColumn, $listStructureElementData) as $key => $value) {
                    $data[$key][] = $value;
                }
            }
        }

        return $data;
    }

    private function flattenMap(NestedColumn $column, mixed $columnData) : array
    {
        $keyColumn = $column->getMapKeyColumn();
        $valueColumn = $column->getMapValueColumn();

        if ($columnData === null) {
            return [
                $keyColumn->flatPath() => null,
                $valueColumn->flatPath() => null,
            ];
        }

        if ($valueColumn instanceof FlatColumn) {
            $this->validator->validate($keyColumn, \array_keys($columnData));
            $this->validator->validate($valueColumn, \array_values($columnData));

            return [
                $keyColumn->flatPath() => \array_keys($columnData),
                $valueColumn->flatPath() => \array_values($columnData),
            ];
        }

        if ($valueColumn->isList()) {
            $data = [
                $keyColumn->flatPath() => \array_keys($columnData),
            ];

            foreach ($columnData as $listElement) {
                foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => $listElement]) as $key => $value) {
                    $data[$key][] = $value;
                }
            }

            return $data;
        }

        if ($valueColumn->isMap()) {
            $data = [
                $keyColumn->flatPath() => \array_keys($columnData),
            ];

            foreach ($columnData as $mapValue) {
                foreach ($this->flattenColumn($valueColumn, [$valueColumn->name() => $mapValue]) as $key => $value) {
                    $data[$key][] = $value;
                }
            }

            return $data;
        }

        $data = [];

        foreach ($columnData as $listElement) {
            $data[] = $this->flattenColumn($valueColumn, [$valueColumn->name() => $listElement]);
        }

        return \array_merge(
            [
                $keyColumn->flatPath() => \array_keys($columnData),
            ],
            \array_merge_recursive(...$data)
        );
    }

    private function flattenStructure(NestedColumn $column, mixed $columnData) : array
    {
        $data = [];

        foreach ($column->children() as $child) {
            $data = \array_merge($data, $this->flattenColumn($child, $columnData ?? [$child->name() => null]));
        }

        return $data;
    }
}
