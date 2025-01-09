<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class FlatValue
{
    public function __construct(
        public FlatColumn $column,
        public int $repetitionLevel,
        public int $definitionLevel,
        public int|float|string|bool|null $value = null,
    ) {
    }

    public function __debugInfo() : array
    {
        return [
            'column' => [
                'name' => $this->column->name(),
                'flat_path' => $this->column->flatPath(),
            ],
            'repetitionLevel' => $this->repetitionLevel,
            'definitionLevel' => $this->definitionLevel,
            'value' => $this->value,
        ];
    }
}
