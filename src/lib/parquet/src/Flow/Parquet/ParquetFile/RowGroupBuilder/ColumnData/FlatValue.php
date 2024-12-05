<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class FlatValue
{
    public function __construct(
        public readonly FlatColumn $column,
        public readonly int $repetitionLevel,
        public readonly int $definitionLevel,
        public readonly int|float|string|bool|null $value = null,
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
