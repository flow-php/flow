<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\ColumnDoor;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\TestCase;

use function array_slice;
use function extension_loaded;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class ArrowParquetFileWriterTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow') || extension_loaded('flow_php')) {
            static::markTestSkipped('ArrowParquetEngine writes through arrow-ext only without flow_php');
        }
    }

    public function test_a_close_that_throws_leaves_the_writer_closed(): void
    {
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            new FailingCloseDestinationStream(memory_filesystem()->writeTo(path('memory://file.parquet'))),
        );

        try {
            $file->close();
            static::fail('close() was expected to throw');
        } catch (FilesystemRuntimeException $failure) {
            static::assertSame('Closing "memory://file.parquet" failed', $failure->getMessage());
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $file->close();
    }

    public function test_closing_twice_throws(): void
    {
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            memory_filesystem()->writeTo(path('memory://file.parquet')),
        );
        $file->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $file->close();
    }

    public function test_closing_writes_a_file_the_reader_reads_back(): void
    {
        $memory = memory_filesystem();
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            $memory->writeTo(path('memory://file.parquet')),
        );

        $file->writeRow(['id' => 1]);
        $file->writeBatch([['id' => 2]]);
        $file->close();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            iterator_to_array(
                (new Reader(engine: new PhpParquetEngine()))
                    ->readStream($memory->readFrom(path('memory://file.parquet')))
                    ->values(),
                false,
            ),
        );
    }

    public function test_writing_a_batch_after_close_throws(): void
    {
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            memory_filesystem()->writeTo(path('memory://file.parquet')),
        );
        $file->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $file->writeBatch([['id' => 1]]);
    }

    public function test_writing_a_row_after_close_throws(): void
    {
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            memory_filesystem()->writeTo(path('memory://file.parquet')),
        );
        $file->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $file->writeRow(['id' => 1]);
    }

    public function test_rows_beyond_the_batch_size_are_written_once(): void
    {
        $memory = memory_filesystem();
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(Options::default()->set(Option::ARROW_WRITE_BATCH_SIZE, 2)),
            $memory->writeTo(path('memory://file.parquet')),
        );

        $file->writeRow(['id' => 1]);
        $file->writeRow(['id' => 2]);
        $file->writeRow(['id' => 3]);
        $file->close();

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            iterator_to_array(
                (new Reader(engine: new PhpParquetEngine()))
                    ->readStream($memory->readFrom(path('memory://file.parquet')))
                    ->values(),
                false,
            ),
        );
    }

    public function test_columns_of_different_lengths_are_refused(): void
    {
        $file = ColumnDoor::open(new ArrowParquetEngine(), ColumnDoor::stream(), Schema::with(FlatColumn::int32('id')));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('writeColumns() takes lists of one length, got "id": 2, "other": 1');

        $file->writeColumns(['id' => [1, 2], 'other' => [1]]);
    }

    public function test_a_column_the_array_lacks_is_written_as_nulls_and_an_unknown_key_is_ignored(): void
    {
        $stream = ColumnDoor::stream();
        $file = ColumnDoor::open(
            new ArrowParquetEngine(),
            $stream,
            Schema::with(FlatColumn::int32('id'), FlatColumn::string('name')),
        );

        $file->writeColumns(['id' => [1, 2], 'unknown' => ['a', 'b']]);
        $file->writeColumns([]);
        $file->close();

        static::assertSame([['id' => 1, 'name' => null], ['id' => 2, 'name' => null]], ColumnDoor::read($stream));
    }

    public function test_writing_columns_after_close_throws(): void
    {
        $file = ColumnDoor::open(new ArrowParquetEngine(), ColumnDoor::stream(), Schema::with(FlatColumn::int32('id')));
        $file->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $file->writeColumns(['id' => [1]]);
    }

    public function test_columns_write_the_bytes_a_batch_writes(): void
    {
        $options = Options::default()
            ->set(Option::ROW_GROUP_SIZE_BYTES, 2 * 1024)
            ->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 140)
            ->set(Option::PAGE_SIZE_BYTES, 256)
            ->set(Option::PAGE_SIZE_CHECK_INTERVAL, 26);
        $rows = ColumnDoor::rows(600);
        $batch = ColumnDoor::stream();
        $columns = ColumnDoor::stream();

        $file = ColumnDoor::open(
            new ArrowParquetEngine($options->set(Option::ARROW_WRITE_BATCH_SIZE, 200)),
            $batch,
            ColumnDoor::schema(),
            $options,
        );
        $file->writeRow($rows[0]);
        $file->writeBatch(array_slice($rows, 1));
        $file->close();

        $file = ColumnDoor::open(
            new ArrowParquetEngine($options->set(Option::ARROW_WRITE_BATCH_SIZE, 200)),
            $columns,
            ColumnDoor::schema(),
            $options,
        );
        $file->writeRow($rows[0]);
        $file->writeColumns(ColumnDoor::columns(array_slice($rows, 1)));
        $file->close();

        static::assertTrue($batch->content() === $columns->content());
        static::assertSame($rows, ColumnDoor::read($columns));
    }
}
