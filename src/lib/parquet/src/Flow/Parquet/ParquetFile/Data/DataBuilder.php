<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Dremel\{DataAssembled, DataShredded, Dremel};
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, Repetition};

final class DataBuilder
{
    public function __construct(
        private readonly DataConverter $dataConverter,
    ) {
    }

    public function build(DataShredded $data, FlatColumn $column) : DataAssembled
    {
        //        $repetitions = \array_map(
        //            static fn (Repetition $repetition) => $repetition->toDremel(),
        //            $column->repetitions()
        //        );

        $dremel = new Dremel();

        dj([
            'flat_path' => $column->flatPath(),
            'repetition levels' => $data->repetitionLevels,
            'definition levels' => $data->definitionLevels,
            'values' => $data->values,
            //            'max definition level' => $column->maxDefinitionsLevel(),
            //            'max repetition level' => $column->maxRepetitionsLevel(),
            'repetitions' => $column->repetitions()->__toString(),
        ]);

        return new DataAssembled([], $data); // $this->enrichData($dremel->assemble($data, $repetitions, $column->maxDefinitionsLevel()), $column);
    }

    private function enrichData(DataAssembled $assembled, FlatColumn $column) : DataAssembled
    {
        $enriched = [];

        foreach ($assembled->rows as $value) {
            if ($value === null) {
                $enriched[] = null;

                continue;
            }

            if (\is_array($value)) {
                $enrichedRow = [];

                foreach ($value as $val) {
                    $enrichedRow[] = $this->dataConverter->fromParquetType($column, $val);
                }

                $enriched[] = $enrichedRow;

                continue;
            }

            $enriched[] = $this->dataConverter->fromParquetType($column, $value);
        }

        return new DataAssembled($enriched, $assembled->shredded);
    }
}
