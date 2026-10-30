<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ColumnChunkViewer;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use PHPUnit\Framework\TestCase;

use function count;
use function iterator_to_array;

final class ColumnChunkViewerTest extends TestCase
{
    public function test_the_viewer_lists_the_pages_of_a_parquet_816_chunk(): void
    {
        $bytes = MemoryParquetFile::pagedColumns();
        $file = MemoryParquetFile::read(Reader::php(), $bytes);
        $chunk = $file->metadata()->rowGroups()->all()[0]->getColumnChunk($file->schema()->getFlat('word'));
        $viewer = new ColumnChunkViewer(new Options());

        static::assertCount(
            count(iterator_to_array($viewer->view($chunk, new MemorySourceStream($bytes)), false)),
            iterator_to_array(
                $viewer->view(ColumnChunkMother::parquet816($chunk), new MemorySourceStream($bytes)),
                false,
            ),
        );
    }
}
