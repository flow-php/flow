<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroup;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ThriftModel\ColumnChunk as ThriftColumnChunk;
use PHPUnit\Framework\TestCase;

final class ColumnChunkTest extends TestCase
{
    public function test_a_column_chunk_without_metadata_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column chunk at offset 4 has no metadata (an encrypted column is not supported)',
        );

        ColumnChunk::fromThrift(new ThriftColumnChunk(['file_offset' => 4]));
    }
}
