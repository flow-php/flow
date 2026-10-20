<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Index\IndexLocation;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class ColumnChunkMother
{
    /**
     * The chunk as old parquet-mr writes it (PARQUET-816): no dictionary page offset, the data page offset at the
     * dictionary page.
     */
    public static function parquet816(ColumnChunk $chunk): ColumnChunk
    {
        $thrift = $chunk->toThrift();
        $metadata = $thrift->meta_data ?? throw new InvalidArgumentException('No column metadata');
        $metadata->data_page_offset = $chunk->pageOffset();
        $metadata->dictionary_page_offset = null;

        return ColumnChunk::fromThrift($thrift);
    }

    public static function withBloomFilter(FlatColumn $column, int $offset, ?int $length): ColumnChunk
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
            null,
            bloomFilterOffset: $offset,
            bloomFilterLength: $length,
        );
    }

    /**
     * A chunk of two values of the column without statistics, its pages in bytes 4 to 24, its page indexes at the given
     * locations.
     */
    public static function withIndexes(
        FlatColumn $column,
        ?IndexLocation $columnIndex,
        ?IndexLocation $offsetIndex,
    ): ColumnChunk {
        return new ColumnChunk(
            $column->type(),
            Compressions::UNCOMPRESSED,
            2,
            4,
            [$column->flatPath()],
            [],
            20,
            20,
            null,
            4,
            null,
            null,
            $columnIndex,
            $offsetIndex,
        );
    }

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

    /**
     * A chunk of the column whose pages take $bytes bytes from $offset.
     */
    public static function spanning(FlatColumn $column, int $offset, int $bytes): ColumnChunk
    {
        return new ColumnChunk(
            $column->type(),
            Compressions::UNCOMPRESSED,
            2,
            $offset,
            [$column->flatPath()],
            [],
            $bytes,
            $bytes,
            null,
            $offset,
            null,
            null,
        );
    }
}
