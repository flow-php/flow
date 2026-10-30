<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Parquet\Engine\Arrow\RustRowGroupChunks;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\OutOfBoundsException;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ThriftBytes;
use Flow\Parquet\ThriftModel\ColumnChunk as ThriftColumnChunk;
use PHPUnit\Framework\TestCase;

final class RustRowGroupChunksTest extends TestCase
{
    public function test_a_chunk_is_decoded_from_its_bytes_on_first_access_only(): void
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();
        $id = $metadata->schema()->getFlat('id');
        $chunks = RustRowGroupChunks::fromThrift(
            [2, 2],
            ['id' => [
                ThriftBytes::of($metadata->rowGroups()->all()[0]->getColumnChunk($id)->toThrift()),
                ThriftBytes::of(new ThriftColumnChunk(['file_offset' => 7])),
            ]],
            ['id' => ThriftBytes::of(ColumnOrder::TypeDefined->toThrift())],
        );

        static::assertSame([2, 2], $chunks->rowCounts());
        static::assertEquals($metadata->rowGroups()->all()[0]->getColumnChunk($id), $chunks->chunk(0, $id));
        static::assertSame($chunks->chunk(0, $id), $chunks->chunk(0, $id));
        static::assertSame(ColumnOrder::TypeDefined, $chunks->columnOrder($id));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column chunk at offset 7 has no metadata (an encrypted column is not supported)',
        );

        $chunks->chunk(1, $id);
    }

    public function test_a_column_without_column_orders_has_an_absent_order(): void
    {
        static::assertSame(
            ColumnOrder::Absent,
            RustRowGroupChunks::fromThrift([], ['id' => []], ['id' => null])->columnOrder(FlatColumn::int64('id')),
        );
    }

    public function test_a_column_that_was_not_read_from_the_footer_is_refused(): void
    {
        $chunks = RustRowGroupChunks::fromThrift([1], ['id' => ['']], ['id' => null]);

        try {
            $chunks->columnOrder(FlatColumn::int64('name'));
            static::fail('An unknown column order must be refused');
        } catch (InvalidArgumentException $exception) {
            static::assertSame('Column "name" was not read from the footer', $exception->getMessage());
        }

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "name" was not read from the footer');

        $chunks->chunk(0, FlatColumn::int64('name'));
    }

    public function test_a_row_group_the_file_lacks_is_refused(): void
    {
        $chunks = RustRowGroupChunks::fromThrift([1], ['id' => ['']], ['id' => null]);

        $this->expectException(OutOfBoundsException::class);
        $this->expectExceptionMessage('Row group 1 does not exist');

        $chunks->chunk(1, FlatColumn::int64('id'));
    }

    public function test_decoded_chunks_are_answered_as_given(): void
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::threeRowGroups())->metadata();
        $id = $metadata->schema()->getFlat('id');
        $chunk = $metadata->rowGroups()->all()[0]->getColumnChunk($id);
        $chunks = RustRowGroupChunks::decoded([2], ['id' => [$chunk]], ['id' => ColumnOrder::TypeDefined]);

        static::assertSame($chunk, $chunks->chunk(0, $id));
        static::assertSame(ColumnOrder::TypeDefined, $chunks->columnOrder($id));
        static::assertSame([2], $chunks->rowCounts());

        $this->expectException(OutOfBoundsException::class);

        $chunks->chunk(1, $id);
    }
}
