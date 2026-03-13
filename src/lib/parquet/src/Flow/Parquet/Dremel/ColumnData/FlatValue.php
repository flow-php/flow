<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\Binary\Bytes;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class FlatValue
{
    public string $flatPath;

    public function __construct(
        public FlatColumn $column,
        public int $repetitionLevel,
        public int $definitionLevel,
        public int|float|string|bool|Bytes|null $value = null,
    ) {
        $this->flatPath = $column->flatPath();
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
