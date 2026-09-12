<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use ArrayIterator;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

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
}
