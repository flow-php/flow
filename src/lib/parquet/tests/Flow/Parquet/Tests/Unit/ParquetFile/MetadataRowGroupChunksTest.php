<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\Parquet\Exception\OutOfBoundsException;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use PHPUnit\Framework\TestCase;

final class MetadataRowGroupChunksTest extends TestCase
{
    public function test_the_chunks_and_orders_are_the_footer_ones(): void
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();
        $id = $metadata->schema()->getFlat('id');
        $chunks = new MetadataRowGroupChunks($metadata);

        static::assertSame(3, $chunks->count());
        static::assertSame(2, $chunks->rowCount(2));
        static::assertEquals($metadata->rowGroups()->all()[2]->getColumnChunk($id), $chunks->chunk(2, $id));
        static::assertSame($metadata->columnOrder($id), $chunks->columnOrder($id));
    }

    public function test_a_row_group_the_file_lacks_is_refused(): void
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();

        $this->expectException(OutOfBoundsException::class);
        $this->expectExceptionMessage('Row group 3 does not exist');

        (new MetadataRowGroupChunks($metadata))->rowCount(3);
    }
}
