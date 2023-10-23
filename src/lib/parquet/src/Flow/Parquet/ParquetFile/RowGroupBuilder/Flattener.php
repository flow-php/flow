<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

final class Flattener
{
    public function flattenColumn(Column $column, array $row) : array
    {
        if (!\array_key_exists($column->name(), $row)) {
            return [];
        }

        /**
         * @var mixed $columnData
         */
        $columnData = $row[$column->name()];

        if ($column instanceof FlatColumn) {
            return [
                $column->flatPath() => $columnData,
            ];
        }

        /** @var NestedColumn $column */
        if (!\is_array($columnData) && ($column->isMap() || $column->isList())) {
            return [];
        }

        if ($column->isList()) {
            $listElementColumn = $column->getListElement();

            if ($listElementColumn instanceof FlatColumn) {
                return [
                    $listElementColumn->flatPath() => $columnData,
                ];
            }

            $data = [];

            foreach ($columnData as $listElement) {
                $data[] = $this->flattenColumn($listElementColumn, [$listElementColumn->name() => $listElement]);
            }

            return \array_merge_recursive(...$data);
        }

        if ($column->isMap()) {
            $keyColumn = $column->getMapKeyColumn();
            $valueColumn = $column->getMapValueColumn();

            if ($valueColumn instanceof FlatColumn) {
                return [
                    $keyColumn->flatPath() => \array_keys($columnData),
                    $valueColumn->flatPath() => \array_values($columnData),
                ];
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

        if ($column->isStruct()) {
            $data = [];

            foreach ($column->children() as $child) {
                $data = \array_merge($data, $this->flattenColumn($child, $columnData));
            }

            return $data;
        }

        throw new RuntimeException('Unknown column type');
    }
}
