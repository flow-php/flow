<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\FlatData\FlatValue;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final class Dremel
{
    public function __construct(private readonly Validator $validator)
    {
    }

    /**
     * @param array<array<string,mixed>> $row
     */
    public function shredRow(Column $column, array $row, int $definitionLevel = 0, int $repetitionLevel = 0) : FlatData
    {
        $flatData = new FlatData($column);

        if ($column instanceof FlatColumn) {
            $this->flattenFlat($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData);

            return $flatData;
        }

        /** @var NestedColumn $column */
        if ($column->isList()) {
            $this->flattenList($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData);

            return $flatData;
        }

        if ($column->isMap()) {
            $this->flattenMap($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData);

            return $flatData;
        }

        $this->flattenStructure($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData);

        return $flatData;
    }

    private function flattenFlat(FlatColumn $column, mixed $value, int $definitionLevel, int $repetitionLevel, FlatData $data) : void
    {
        $this->validator->validate($column, $value);

        if (!$column->repetition()?->isRequired() && $value !== null) {
            $definitionLevel++;
        }

        $data->add(
            new FlatValue(
                $column,
                [$data->isEmpty($column) ? 0 : $repetitionLevel],
                [$definitionLevel],
                $value === null ? [] : [$value]
            )
        );
    }

    /**
     * @return array<FlatData>
     */
    private function flattenList(NestedColumn $column, ?array $listValue, int $definitionLevel, int $repetitionLevel, FlatData $data) : void
    {
        $this->validator->validate($column, $listValue);
        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            if ($listValue === null) {
                $data->add(
                    new FlatValue($listElementColumn, [$data->isEmpty($listElementColumn) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $data->add(
                    new FlatValue($listElementColumn, [$data->isEmpty($listElementColumn) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($listValue as $value) {
                $this->flattenFlat($listElementColumn, $value, $definitionLevel, $index === 0 ? $repetitionLevel : $repetitionLevel + 1, $data);
                $index++;
            }

            return;
        }

        /** @var NestedColumn $listElementColumn */
        if ($listElementColumn->isList()) {
            if ($listValue === null) {
                $data->add(
                    new FlatValue($listElementColumn->getListElement(), [$data->isEmpty($listElementColumn->getListElement()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $data->add(
                    new FlatValue($listElementColumn->getListElement(), [$data->isEmpty($listElementColumn->getListElement()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $value) {
                $this->flattenList($listElementColumn, $value, $definitionLevel, $repetitionLevel + 1, $data);
            }

            return;
        }

        if ($listElementColumn->isMap()) {
            if ($listValue === null) {
                $data->add(
                    new FlatValue($listElementColumn->getMapKeyColumn(), [$data->isEmpty($listElementColumn->getMapKeyColumn()) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($listElementColumn->getMapValueColumn(), [$data->isEmpty($listElementColumn->getMapValueColumn()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $data->add(
                    new FlatValue($listElementColumn->getMapKeyColumn(), [$data->isEmpty($listElementColumn->getMapKeyColumn()) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($listElementColumn->getMapValueColumn(), [$data->isEmpty($listElementColumn->getMapValueColumn()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $mapValue) {
                $this->flattenMap($listElementColumn, $mapValue, $definitionLevel, $repetitionLevel + 1, $data);
            }

            return;
        }

        if ($listValue === null) {
            foreach ($listElementColumn->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->flattenFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->flattenList($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                if ($child->isMap()) {
                    $this->flattenMap($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                $this->flattenStructure($child, null, $definitionLevel, $repetitionLevel, $data);
            }

            return;
        }

        if (!$listElementColumn->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($listValue)) {
            foreach ($listElementColumn->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->flattenFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->flattenList($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                if ($child->isMap()) {
                    $this->flattenMap($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                $this->flattenStructure($child, null, $definitionLevel, $repetitionLevel, $data);
            }

            return;
        }

        $definitionLevel++;

        foreach ($listValue as $listElementValue) {
            $this->flattenStructure($listElementColumn, $listElementValue, $definitionLevel, $repetitionLevel + 1, $data);
        }
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     * @psalm-suppress NamedArgumentNotAllowed
     *
     * @return array<mixed>
     */
    private function flattenMap(NestedColumn $column, ?array $mapValue, int $definitionLevel, int $repetitionLevel, FlatData $data) : void
    {
        $this->validator->validate($column, $mapValue);

        $keyColumn = $column->getMapKeyColumn();
        $valueColumn = $column->getMapValueColumn();

        if ($valueColumn instanceof FlatColumn) {
            if ($mapValue === null) {
                $data->add(
                    new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn, [$data->isEmpty($valueColumn) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $data->add(
                    new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn, [$data->isEmpty($valueColumn) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $this->flattenFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel : $repetitionLevel + 1, $data);
                $this->flattenFlat($valueColumn, $value, $definitionLevel, $index === 0 ? $repetitionLevel : $repetitionLevel + 1, $data);
                $index++;
            }

            return;
        }

        /** @var NestedColumn $valueColumn */
        if ($valueColumn->isList()) {
            if ($mapValue === null) {
                $data->add(
                    new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn->getListElement(), [$data->isEmpty($valueColumn->getListElement()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $data->add(
                    new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn->getListElement(), [$data->isEmpty($valueColumn->getListElement()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            $definitionLevel++;

            foreach ($mapValue as $key => $value) {
                $this->flattenFlat($keyColumn, $key, $definitionLevel, $repetitionLevel + 1, $data);
                $this->flattenList($valueColumn, $value, $definitionLevel, $repetitionLevel + 1, $data);
            }

            return;
        }

        if ($valueColumn->isMap()) {
            if ($mapValue === null) {
                $data->add(
                    new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn->getMapKeyColumn(), [$data->isEmpty($valueColumn->getMapKeyColumn()) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn->getMapValueColumn(), [$data->isEmpty($valueColumn->getMapValueColumn()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $data->add(
                    new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn->getMapKeyColumn(), [$data->isEmpty($valueColumn->getMapKeyColumn()) ? 0 : $repetitionLevel], [$definitionLevel], []),
                    new FlatValue($valueColumn->getMapValueColumn(), [$data->isEmpty($valueColumn->getMapValueColumn()) ? 0 : $repetitionLevel], [$definitionLevel], [])
                );

                return;
            }

            $definitionLevel++;

            foreach ($mapValue as $key => $value) {
                $this->flattenFlat($keyColumn, $key, $definitionLevel, $repetitionLevel + 1, $data);
                $this->flattenMap($valueColumn, $value, $definitionLevel, $repetitionLevel + 1, $data);
            }

            return;
        }

        if ($mapValue === null) {
            $data->add(new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []));

            foreach ($valueColumn->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->flattenFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->flattenList($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                if ($child->isMap()) {
                    $this->flattenMap($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                $this->flattenStructure($child, null, $definitionLevel, $repetitionLevel, $data);
            }

            return;
        }

        if (!$column->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($mapValue)) {
            $data->add(new FlatValue($keyColumn, [$data->isEmpty($keyColumn) ? 0 : $repetitionLevel], [$definitionLevel], []));

            foreach ($valueColumn->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->flattenFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->flattenList($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                if ($child->isMap()) {
                    $this->flattenMap($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                $this->flattenStructure($child, null, $definitionLevel, $repetitionLevel, $data);
            }

            return;
        }

        $definitionLevel++;

        foreach ($mapValue as $key => $value) {
            $this->flattenFlat($keyColumn, $key, $definitionLevel, $repetitionLevel + 1, $data);
            $this->flattenStructure($valueColumn, $value, $definitionLevel, $repetitionLevel + 1, $data);
        }
    }

    private function flattenStructure(NestedColumn $column, mixed $structureData, int $definitionLevel, int $repetitionLevel, FlatData $data) : void
    {
        $this->validator->validate($column, $structureData);

        if ($structureData === null) {
            foreach ($column->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->flattenFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->flattenList($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                if ($child->isMap()) {
                    $this->flattenMap($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                $this->flattenStructure($child, null, $definitionLevel, $repetitionLevel, $data);
            }

            return;
        }

        if (!$column->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($structureData)) {
            foreach ($column->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->flattenFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->flattenList($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                if ($child->isMap()) {
                    $this->flattenMap($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                $this->flattenStructure($child, null, $definitionLevel, $repetitionLevel, $data);
            }

            return;
        }

        foreach ($column->children() as $child) {
            if ($child instanceof FlatColumn) {
                $this->flattenFlat($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data);

                continue;
            }

            /**
             * @var NestedColumn $child
             */
            if ($child->isList()) {
                $this->flattenList($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data);

                continue;
            }

            if ($child->isMap()) {
                $this->flattenMap($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data);

                continue;
            }

            $this->flattenStructure($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data);
        }
    }
}
