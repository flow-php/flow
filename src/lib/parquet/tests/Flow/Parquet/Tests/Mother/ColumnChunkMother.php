<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class ColumnChunkMother
{
    /**
     * A chunk of two values of the column, holding only its statistics.
     */
    public static function withStatistics(FlatColumn $column, ?Statistics $statistics): ColumnChunk
    {
        return new ColumnChunk(
            $column->type(),
            Compressions::UNCOMPRESSED,
            2,
            4,
            [$column->flatPath()],
            [],
            0,
            0,
            null,
            4,
            null,
            $statistics,
        );
    }
}
