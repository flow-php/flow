<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\FlatValue;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, NestedColumn};

final readonly class DremelShredder
{
    public function __construct(
        private Validator $validator,
        private DataConverter $dataConverter,
    ) {
    }

    /**
     * @param array<string,mixed> $row
     */
    public function shred(Column $column, array $row) : WriteColumnData
    {
        $value = $row[$column->name()] ?? null;
        $this->validator->validate($column, $value);

        $flatData = WriteColumnData::initialize($column);
        $definitionLevel = 0;
        $repetitionLevel = 0;
        $depth = 0;

        if ($column instanceof FlatColumn) {
            $this->shredFlat($column, $value, $definitionLevel, $repetitionLevel, $flatData);

            return $flatData;
        }

        /** @var NestedColumn $column */
        if ($column->isList()) {
            /** @phpstan-ignore-next-line */
            $this->shredList($column, $value, $definitionLevel, $repetitionLevel, $flatData, $depth);

            return $flatData;
        }

        if ($column->isMap()) {
            /** @phpstan-ignore-next-line */
            $this->shredMap($column, $value, $definitionLevel, $repetitionLevel, $flatData, $depth);

            return $flatData;
        }

        $this->shredStructure($column, $value, $definitionLevel, $repetitionLevel, $flatData, $depth);

        return $flatData;
    }

    private function shredFlat(FlatColumn $column, mixed $value, int $definitionLevel, int $repetitionLevel, WriteColumnData $data) : void
    {
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
                /** @phpstan-ignore-next-line */
                $this->dataConverter->toParquetType($column, $value)
            )
        );
    }

    /**
     * @param null|array<mixed> $listValue
     */
    private function shredList(NestedColumn $column, ?array $listValue, int $definitionLevel, int $repetitionLevel, WriteColumnData $data, int $depth) : void
    {
        $repetitionLevel++;
        $depth++;
        $listElementColumn = $column->getListElement();

        if ($listElementColumn instanceof FlatColumn) {
            if ($listValue === null) {
                $this->shredFlat($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data);

                return;
            }

            if (!$column->repetition()?->isRequired()) {
                $definitionLevel++;
            }

            if (!\count($listValue)) {
                $this->shredFlat($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data);

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
                $this->validator->validate($listElementColumn, null);
                $this->shredList($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $value) {
                /** @phpstan-ignore-next-line */
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
                $this->validator->validate($listElementColumn, null);
                $this->shredMap($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            foreach ($listValue as $i => $mapValue) {
                /** @phpstan-ignore-next-line */
                $this->shredMap($listElementColumn, $mapValue, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
            }

            return;
        }

        // List Element is a Structure
        if ($listValue === null) {
            $this->shredStructure($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

            return;
        }

        if (!$column->repetition()?->isRequired()) {
            $definitionLevel++;
        }

        if (!\count($listValue)) {
            $this->validator->validate($listElementColumn, null);
            $this->shredStructure($listElementColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

            return;
        }

        $definitionLevel++;

        foreach ($listValue as $i => $listElementValue) {
            $this->shredStructure($listElementColumn, $listElementValue, $definitionLevel, $i === 0 ? $repetitionLevel - 1 : $depth, $data, $depth);
        }
    }

    /**
     * @param null|array<mixed> $mapValue
     */
    private function shredMap(NestedColumn $column, ?array $mapValue, int $definitionLevel, int $repetitionLevel, WriteColumnData $data, int $depth) : void
    {
        $repetitionLevel++;
        $depth++;
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
                $this->shredFlat($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data);

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
                $this->validator->validate($valueColumn, null);
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredList($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $this->shredFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
                /** @phpstan-ignore-next-line */
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
                $this->validator->validate($valueColumn, null);
                $this->shredFlat($keyColumn->makeOptional(), null, $definitionLevel, $repetitionLevel - 1, $data);
                $this->shredMap($valueColumn, null, $definitionLevel, $repetitionLevel - 1, $data, $depth);

                return;
            }

            $definitionLevel++;

            $index = 0;

            foreach ($mapValue as $key => $value) {
                $this->shredFlat($keyColumn, $key, $definitionLevel, $index === 0 ? $repetitionLevel - 1 : $depth, $data);
                /** @phpstan-ignore-next-line */
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
            $this->validator->validate($valueColumn, null);
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

    private function shredStructure(NestedColumn $column, mixed $structureData, int $definitionLevel, int $repetitionLevel, WriteColumnData $data, int $depth) : void
    {
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

        if (!\is_array($structureData) || !\count($structureData)) {
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
