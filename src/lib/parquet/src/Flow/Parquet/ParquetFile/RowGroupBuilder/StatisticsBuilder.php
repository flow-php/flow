<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsBuilder
{
    public function __construct()
    {

    }

    public function build(FlatColumn $column, ColumnChunkStatistics $chunkStatistics) : Statistics
    {
        $minBuffer = '';
        $maxBuffer = '';

        (new PlainValuesPacker(new BinaryBufferWriter($minBuffer)))->packValues($column, [$chunkStatistics->min()]);
        (new PlainValuesPacker(new BinaryBufferWriter($maxBuffer)))->packValues($column, [$chunkStatistics->max()]);

        return new Statistics(
            max: $maxBuffer !== '' ? $maxBuffer : null,
            min: $minBuffer !== '' ? $minBuffer : null,
            nullCount: $chunkStatistics->nullCount(),
            distinctCount: $chunkStatistics->distinctCount(),
            maxValue: $maxBuffer !== '' ? $maxBuffer : null,
            minValue: $minBuffer !== '' ? $minBuffer : null,
        );
    }
}
