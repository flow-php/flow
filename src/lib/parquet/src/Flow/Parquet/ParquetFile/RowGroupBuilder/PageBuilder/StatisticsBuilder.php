<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsBuilder
{
    public function __construct()
    {

    }

    public function build(FlatColumn $column, DataPageV2Statistics $chunkStatistics) : Statistics
    {
        $minBuffer = '';
        $maxBuffer = '';

        (new PlainValuesPacker(new BinaryBufferWriter($minBuffer)))->packValues($column, [$chunkStatistics->min()]);
        (new PlainValuesPacker(new BinaryBufferWriter($maxBuffer)))->packValues($column, [$chunkStatistics->max()]);

        return new Statistics(
            max: $maxBuffer,
            min: $minBuffer,
            nullCount: $chunkStatistics->nullCount(),
            distinctCount: $chunkStatistics->distinctCount(),
            maxValue: $maxBuffer,
            minValue: $minBuffer,
        );
    }
}
