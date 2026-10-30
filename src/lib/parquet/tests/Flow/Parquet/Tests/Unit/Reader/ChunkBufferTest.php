<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ChunkBuffer;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use PHPUnit\Framework\TestCase;

use function stream_get_contents;
use function strlen;

final class ChunkBufferTest extends TestCase
{
    public function test_a_chunk_is_buffered_from_its_first_page(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($file->schema()->getFlat('word'));

        $buffer = (new ChunkBuffer(new Options()))->of($chunk, new MemorySourceStream($bytes));

        static::assertSame($chunk->totalCompressedSize(), strlen((string) stream_get_contents($buffer)));
    }

    public function test_a_parquet_816_chunk_is_extended_by_its_dictionary_page_header(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $chunk = ColumnChunkMother::parquet816($file
            ->metadata()
            ->rowGroups()
            ->all()[0]->getColumnChunk($file->schema()->getFlat('word')));

        $buffer = (new ChunkBuffer(new Options()))->of($chunk, new MemorySourceStream($bytes));

        static::assertGreaterThan($chunk->totalCompressedSize(), strlen((string) stream_get_contents($buffer)));
    }
}
