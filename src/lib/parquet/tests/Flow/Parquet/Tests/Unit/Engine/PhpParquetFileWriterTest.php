<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use ArrayIterator;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\ColumnDoor;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_slice;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function range;

final class PhpParquetFileWriterTest extends TestCase
{
    public function test_a_close_that_throws_leaves_the_writer_closed(): void
    {
        $file = ParquetFileWriterMother::open(
            new PhpParquetEngine(),
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
            new PhpParquetEngine(),
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
        $file = ParquetFileWriterMother::open(new PhpParquetEngine(), $memory->writeTo(path('memory://file.parquet')));

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
            new PhpParquetEngine(),
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
            new PhpParquetEngine(),
            memory_filesystem()->writeTo(path('memory://file.parquet')),
        );
        $file->close();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $file->writeRow(['id' => 1]);
    }

    public function test_an_array_batch_is_split_into_row_groups(): void
    {
        $memory = memory_filesystem();
        $file = ParquetFileWriterMother::open(
            new PhpParquetEngine(options: Options::default()->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 1)),
            $memory->writeTo(path('memory://file.parquet')),
            Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(Option::PAGE_SIZE_CHECK_INTERVAL, 1),
        );

        $file->writeBatch([['id' => 1], ['id' => 2], ['id' => 3]]);
        $file->close();

        $parquet = (new Reader(engine: new PhpParquetEngine()))->readStream($memory->readFrom(path(
            'memory://file.parquet',
        )));
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], iterator_to_array($parquet->values(), false));
        static::assertCount(3, $parquet->metadata()->rowGroups()->all());
    }

    public function test_a_non_array_batch_is_split_into_row_groups(): void
    {
        $memory = memory_filesystem();
        $file = ParquetFileWriterMother::open(
            new PhpParquetEngine(options: Options::default()->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 1)),
            $memory->writeTo(path('memory://file.parquet')),
            Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(Option::PAGE_SIZE_CHECK_INTERVAL, 1),
        );

        $file->writeBatch(new ArrayIterator([['id' => 1], ['id' => 2], ['id' => 3]]));
        $file->close();

        $parquet = (new Reader(engine: new PhpParquetEngine()))->readStream($memory->readFrom(path(
            'memory://file.parquet',
        )));
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], iterator_to_array($parquet->values(), false));
        static::assertCount(3, $parquet->metadata()->rowGroups()->all());
    }

    public function test_columns_of_different_lengths_are_refused(): void
    {
        $file = ColumnDoor::open(new PhpParquetEngine(), ColumnDoor::stream(), Schema::with(FlatColumn::int32('id')));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('writeColumns() takes lists of one length, got "id": 2, "other": 1');

        $file->writeColumns(['id' => [1, 2], 'other' => [1]]);
    }

    public function test_a_column_the_array_lacks_is_written_as_nulls_and_an_unknown_key_is_ignored(): void
    {
        $stream = ColumnDoor::stream();
        $file = ColumnDoor::open(
            new PhpParquetEngine(),
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
        $file = ColumnDoor::open(new PhpParquetEngine(), ColumnDoor::stream(), Schema::with(FlatColumn::int32('id')));
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

        $file = ColumnDoor::open(new PhpParquetEngine(options: $options), $batch, ColumnDoor::schema(), $options);
        $file->writeRow($rows[0]);
        $file->writeBatch(array_slice($rows, 1));
        $file->close();

        $file = ColumnDoor::open(new PhpParquetEngine(options: $options), $columns, ColumnDoor::schema(), $options);
        $file->writeRow($rows[0]);
        $file->writeColumns(ColumnDoor::columns(array_slice($rows, 1)));
        $file->close();

        static::assertTrue($batch->content() === $columns->content());
        static::assertSame($rows, ColumnDoor::read($columns));
    }

    public function test_columns_refuse_the_cell_a_batch_refuses_and_keep_the_same_rows(): void
    {
        $schema = Schema::with(FlatColumn::int32('a'), FlatColumn::int32('b'), FlatColumn::int32('c'));
        $rows = array_map(static fn(int $i): array => ['a' => $i, 'b' => $i, 'c' => $i], range(0, 7));
        $rows[5]['a'] = 'five';
        $rows[2]['c'] = 'two';
        $outcomes = [];

        foreach (['batch', 'columns'] as $door) {
            $stream = ColumnDoor::stream();
            $file = ColumnDoor::open(new PhpParquetEngine(), $stream, $schema);

            try {
                $door === 'batch' ? $file->writeBatch($rows) : $file->writeColumns(ColumnDoor::columns($rows));
                static::fail('a string was written into INT32');
            } catch (ValidationException $refusal) {
                $file->close();
                $outcomes[$door] = [$refusal->getMessage(), ColumnDoor::read($stream)];
            }
        }

        static::assertSame($outcomes['batch'], $outcomes['columns']);
        static::assertSame('Column "a" require integer as value, got: string instead', $outcomes['columns'][0]);
    }
}
