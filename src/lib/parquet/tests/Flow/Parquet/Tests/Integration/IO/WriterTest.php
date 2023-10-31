<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Faker\Factory;
use Flow\Parquet\Consts;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

final class WriterTest extends ParquetIntegrationTestCase
{
    public function test_closing_not_open_writer() : void
    {
        $writer = new Writer();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->close();
    }

    public function test_opening_already_open_writer() : void
    {
        $writer = new Writer();

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = $this->createSchema();

        $writer->open($path, $schema);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Writer is already open');

        $writer->open($path, $schema);
    }

    public function test_writing_batch_to_not_open_stream() : void
    {
        $writer = new Writer();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->writeBatch([$this->createRow()]);
    }

    public function test_writing_batch_to_not_writable_stream() : void
    {
        $writer = new Writer();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given stream is not opened in write mode, expected wb, got: rb+');

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';
        \file_put_contents($path, 'test');
        $stream = \fopen($path, 'rb+');

        $writer->openForStream($stream, $this->createSchema());
        $writer->writeBatch([$this->createRow()]);
        \unlink($path);
    }

    public function test_writing_in_batches_to_file() : void
    {
        $writer = new Writer();

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = $this->createSchema();

        $row = $this->createRow();

        $writer->open($path, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        $writer->close();

        $this->assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_in_batches_to_file_without_explicit_close() : void
    {
        $writer = new Writer();

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = $this->createSchema();

        $row = $this->createRow();
        $writer->open($path, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        unset($writer);

        $this->assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_in_batches_to_stream() : void
    {
        $writer = new Writer();

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = $this->createSchema();

        $row = $this->createRow();

        $stream = \fopen($path, 'wb+');
        $writer->openForStream($stream, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        $writer->close();

        $this->assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_row_to_not_open_stream() : void
    {
        $writer = new Writer();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->writeRow($this->createRow());
    }

    public function test_writing_to_file() : void
    {
        $writer = new Writer();

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = $this->createSchema();
        $row = $this->createRow();

        $writer->write($path, $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        $this->assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_to_stream() : void
    {
        $writer = new Writer();

        $path = \sys_get_temp_dir() . '/test-writer' . \uniqid('parquet-test-', true) . '.parquet';

        $schema = $this->createSchema();
        $row = $this->createRow();

        $stream = \fopen($path, 'wb+');

        $writer->writeStream($stream, $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        $this->assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        $this->assertFileExists($path);
        \unlink($path);
    }

    private function createRow() : array
    {
        $faker = Factory::create();

        return [
            'struct' => [
                'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                'boolean' => $faker->boolean,
                'string' => $faker->text(150),
                'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                'list_of_int' => \array_map(
                    static fn ($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    \range(1, \random_int(2, 10))
                ),
                'list_of_string' => \array_map(
                    static fn ($i) => $faker->text(10),
                    \range(1, \random_int(2, 10))
                ),
            ],
        ];
    }

    private function createSchema() : Schema
    {
        return Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int64('int64'),
            FlatColumn::boolean('boolean'),
            FlatColumn::string('string'),
            FlatColumn::int32('int32'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));
    }
}
