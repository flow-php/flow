<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use PHPUnit\Framework\TestCase;

use function array_values;

final class PhpParquetFooterTest extends TestCase
{
    public function test_the_metadata_is_built_once_and_answers_the_schema_and_row_count(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups());
        $footer = $file->reader()->footer();

        $metadata = $footer->metadata();

        static::assertSame($metadata, $footer->metadata());
        static::assertSame($metadata->schema(), $footer->schema());
        static::assertSame(6, $footer->rowsNumber());
    }

    public function test_total_byte_size_sums_the_row_groups(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups());
        $footer = $file->reader()->footer();
        $expected = 0;

        foreach ($footer->metadata()->rowGroups()->all() as $rowGroup) {
            $expected += $rowGroup->totalByteSize();
        }

        static::assertCount(3, $footer->metadata()->rowGroups()->all());
        static::assertGreaterThan(0, $expected);
        static::assertSame($expected, $footer->totalByteSize());
    }

    public function test_row_group_chunks_answer_from_the_metadata(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups());
        $footer = $file->reader()->footer();
        $id = $footer->schema()->getFlat('id');
        $chunks = $footer->rowGroupChunks([$id]);

        static::assertSame([2, 2, 2], $chunks->rowCounts());
        static::assertSame(3, $chunks->chunk(1, $id)->statistics()?->minValue($id));
        static::assertSame($footer->metadata()->columnOrder($id), $chunks->columnOrder($id));
    }

    public function test_one_row_group_chunks_answers_every_column_set(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups());
        $footer = $file->reader()->footer();

        static::assertSame(
            $footer->rowGroupChunks([]),
            $footer->rowGroupChunks(array_values($footer->schema()->columnsFlat())),
        );
    }
}
