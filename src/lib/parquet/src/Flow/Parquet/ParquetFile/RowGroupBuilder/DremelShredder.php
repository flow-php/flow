<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\{FlatValue};
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class DremelShredder
{
    public function __construct(
        private Validator $validator,
        private DataConverter $dataConverter,
    ) {
    }

    /**
     * @param array<array<string,mixed>> $row
     */
    public function shred(Column $column, array $row) : FlatColumnData
    {
        $flatData = FlatColumnData::initialize($column);
        $definitionLevel = 0;
        $repetitionLevel = 0;
        $depth = 0;

        if ($column instanceof FlatColumn) {
            $this->shredFlat($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData);

            return $flatData;
        }

        /** @var NestedColumn $column */
        if ($column->isList()) {
            $this->shredList($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData, $depth);

            return $flatData;
        }

        if ($column->isMap()) {
            $this->shredMap($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData, $depth);

            return $flatData;
        }

        $this->shredStructure($column, $row[$column->name()] ?? null, $definitionLevel, $repetitionLevel, $flatData, $depth);

        return $flatData;
    }

    private function shredFlat(FlatColumn $column, mixed $value, int $definitionLevel, int $repetitionLevel, FlatColumnData $data, bool $validate = true) : void
    {
        if ($validate) {
            $this->validator->validate($column, $value);
        }

        if (!$column->repetition()?->isRequired() && $value !== null) {
            $definitionLevel++;
        }

        /**
         * We can do that since DremelShredder is meant to shred only one row at Time, so there is no risk that Data
         * will carry previous rows.
         * In other words, whenever $data for a given column is empty we can safely assume that it's the first
         * value in the Row and set repetitionLevel to 0.
         */
        $repetitionLevel = $data->isEmpty($column) ? 0 : $repetitionLevel;
        $data->addValue(
            new FlatValue(
                $column,
                $repetitionLevel,
                $definitionLevel,
                $this->dataConverter->toParquetType($column, $value)
            )
        );
    }

    private function shredList(NestedColumn $column, ?array $listValue, int $definitionLevel, int $repetitionLevel, FlatColumnData $data, int $depth) : void
    {
        $repetitionLevel++;
        $depth++;
        $this->validator->validate($column, $listValue);
        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            if ($listValue === null) {
                $this->shredFlat($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, false);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $this->shredFlat($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, false);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $value) {
                $this->shredFlat($listElementColumn, $value, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $data);
            }

            return;
        }

        /** @var NestedColumn $listElementColumn */
        if ($listElementColumn->isList()) {
            if ($listValue === null) {
                $this->shredList($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $this->shredList($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $value) {
                $this->shredList($listElementColumn, $value, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
            }

            return;
        }

        if ($listElementColumn->isMap()) {
            if ($listValue === null) {
                $this->shredMap($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $this->shredMap($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $mapValue) {
                $this->shredMap($listElementColumn, $mapValue, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
            }

            return;
        }

        // List Element is a Structure
        if ($listValue === null) {
            $this->shredStructure($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

            return;
        }

        if (!$listElementColumn->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($listValue)) {
            $this->shredStructure($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

            return;
        }

        $definitionLevel++;

        foreach ($listValue as $i => $listElementValue) {
            $this->shredStructure($listElementColumn, $listElementValue, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
        }
    }

    private function shredMap(NestedColumn $column, ?array $mapValue, int $definitionLevel, int $repetitionLevel, FlatColumnData $data, int $depth) : void
    {
        $repetitionLevel++;
        $depth++;
        $this->validator->validate($column, $mapValue);

        $keyColumn = $column->getMapKeyColumn();
        $valueColumn = $column->getMapValueColumn();

        if ($valueColumn instanceof FlatColumn) {
            if ($mapValue === null) {
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredFlat($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredFlat($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, false);

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $this->shredFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
                $this->shredFlat($valueColumn, $value, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
                $index++;
            }

            return;
        }

        /** @var NestedColumn $valueColumn */
        if ($valueColumn->isList()) {
            if ($mapValue === null) {
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredList($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredList($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $this->shredFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
                $this->shredList($valueColumn, $value, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
                $index++;
            }

            return;
        }

        if ($valueColumn->isMap()) {
            if ($mapValue === null) {
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredMap($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($mapValue)) {
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredMap($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $this->shredFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
                $this->shredMap($valueColumn, $value, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
                $index++;
            }

            return;
        }

        // Map Value is a Structure

        if ($mapValue === null) {
            $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
            $this->shredStructure($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

            return;
        }

        if (!$column->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($mapValue)) {
            $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
            $this->shredStructure($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

            return;
        }

        $definitionLevel++;

        $index = 0;

        foreach ($mapValue as $key => $value) {
            $this->shredFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
            $this->shredStructure($valueColumn, $value, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
            $index++;
        }
    }

    private function shredStructure(NestedColumn $column, mixed $structureData, int $definitionLevel, int $repetitionLevel, FlatColumnData $data, int $depth) : void
    {
        $this->validator->validate($column, $structureData);

        if ($structureData === null) {
            foreach ($column->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->shredFlat($child->makeOptional(), null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->shredList($child, null, $definitionLevel, $repetitionLevel, $data, $depth);

                    continue;
                }

                if ($child->isMap()) {
                    $this->shredMap($child, null, $definitionLevel, $repetitionLevel, $data, $depth);

                    continue;
                }

                $this->shredStructure($child, null, $definitionLevel, $repetitionLevel, $data, $depth);
            }

            return;
        }

        if (!$column->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($structureData)) {
            foreach ($column->children() as $child) {
                if ($child instanceof FlatColumn) {
                    $this->shredFlat($child, null, $definitionLevel, $repetitionLevel, $data);

                    continue;
                }

                /**
                 * @var NestedColumn $child
                 */
                if ($child->isList()) {
                    $this->shredList($child, null, $definitionLevel, $repetitionLevel, $data, $depth);

                    continue;
                }

                if ($child->isMap()) {
                    $this->shredMap($child, null, $definitionLevel, $repetitionLevel, $data, $depth);

                    continue;
                }

                $this->shredStructure($child, null, $definitionLevel, $repetitionLevel, $data, $depth);
            }

            return;
        }

        foreach ($column->children() as $child) {
            if ($child instanceof FlatColumn) {
                $this->shredFlat($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data);

                continue;
            }

            /**
             * @var NestedColumn $child
             */
            if ($child->isList()) {
                $this->shredList($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data, $depth);

                continue;
            }

            if ($child->isMap()) {
                $this->shredMap($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data, $depth);

                continue;
            }

            $this->shredStructure($child, $structureData[$child->name()] ?? null, $definitionLevel, $repetitionLevel, $data, $depth);
        }
    }
}
