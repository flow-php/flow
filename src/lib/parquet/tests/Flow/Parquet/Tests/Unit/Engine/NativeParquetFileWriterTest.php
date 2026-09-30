<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use ArrayIterator;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\NativeParquetFileWriter;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

#[RequiresPhpExtension('flow_php')]
final class NativeParquetFileWriterTest extends TestCase
{
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

    public function test_it_is_the_writer_arrow_opens_with_flow_php(): void
    {
        static::assertInstanceOf(NativeParquetFileWriter::class, ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            memory_filesystem()->writeTo(path('memory://file.parquet')),
        ));
    }

    public function test_a_refused_row_leaves_the_rows_before_it_written(): void
    {
        $memory = memory_filesystem();
        $file = ParquetFileWriterMother::open(
            new ArrowParquetEngine(),
            $memory->writeTo(path('memory://file.parquet')),
        );

        $file->writeRow(['id' => 1]);

        try {
            $file->writeRow(['id' => 'two']);
            static::fail('a string was written into INT32');
        } catch (ValidationException $refusal) {
            static::assertSame('Column "id" row 1: expected int, got string', $refusal->getMessage());
        }

        $file->writeBatch(new ArrayIterator([['id' => 3]]));
        $file->close();

        static::assertSame(
            [['id' => 1], ['id' => 3]],
            iterator_to_array(
                (new Reader(engine: new PhpParquetEngine()))
                    ->readStream($memory->readFrom(path('memory://file.parquet')))
                    ->values(),
                false,
            ),
        );
    }
}
