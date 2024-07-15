<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Dremel\Dremel;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Page\ColumnData;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class DataBuilder
{
    public function __construct(
        private readonly DataConverter $dataConverter
    ) {
    }

    public function build(ColumnData $columnData, FlatColumn $column) : \Generator
    {
        $dremel = new Dremel();

        //        if ($column->flatPath() === 'int_map_array.list.element.map.key') {
        //            $columnData = new ColumnData(
        //                $columnData->type,
        //                $columnData->logicalType,
        //                $columnData->repetitions,
        //                $columnData->definitions,
        //                [$columnData->values[0]->toArray()]
        //            );
        //        }

        //        if ($column->flatPath() === 'int_map_array.list.element.map.key') {
        //            dd($columnData);
        //        }

        foreach ($dremel->assemble($columnData->repetitions, $columnData->definitions, $columnData->values, $column->maxDefinitionsLevel(), $column->maxRepetitionsLevel()) as $value) {
            //            if ($column->flatPath() === 'int_map_array.list.element.map.key') {
            //                dd($value);
            //            }
            yield $this->enrichData($value, $column);
        }
    }

    private function enrichData(mixed $value, FlatColumn $column) : mixed
    {
        if ($value === null) {
            return null;
        }

        if (\is_array($value)) {
            $enriched = [];

            foreach ($value as $val) {
                $enriched[] = $this->dataConverter->fromParquetType($column, $val);
            }

            return $enriched;
        }

        return $this->dataConverter->fromParquetType($column, $value);
    }
}
