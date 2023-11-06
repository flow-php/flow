<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsBuilder
{
    public function __construct(private readonly DataConverter $dataConverter)
    {

    }

    public function build(FlatColumn $column, ColumnChunkStatistics $chunkStatistics) : Statistics
    {
        $minBuffer = '';
        $maxBuffer = '';

        (new PlainValuesPacker(new BinaryBufferWriter($minBuffer), $this->dataConverter))->packValues($column, [$chunkStatistics->min()]);
        (new PlainValuesPacker(new BinaryBufferWriter($maxBuffer), $this->dataConverter))->packValues($column, [$chunkStatistics->max()]);

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
