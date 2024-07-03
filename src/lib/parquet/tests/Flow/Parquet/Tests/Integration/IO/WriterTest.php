<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Composer\InstalledVersions;
use Faker\Factory;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\{Path};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use Flow\Parquet\{Consts, Option, Options, Reader, Writer};
use PHPUnit\Framework\TestCase;

final class WriterTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_closing_not_open_writer() : void
    {
        $writer = new Writer();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->close();
    }

    public function test_created_by_metadata() : void
    {
        $writer = new Writer();

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();
        $writer->open($path, $schema);
        $writer->close();

        $metadata = (new Reader())->read($path)->metadata();

        self::assertSame('flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version'], $metadata->createdBy());
    }

    public function test_opening_already_open_writer() : void
    {
        $writer = new Writer();

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

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

    public function test_writing_column_statistics() : void
    {
        $writer = new Writer(
            options: Options::default()
                ->set(Option::WRITER_VERSION, 1)
        );

        $path = __DIR__ . '/var/test-writer-parquet-test-v2-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with($column = FlatColumn::int32('int32'));

        $writer->write($path, $schema, \array_map(
            static fn ($i) => ['int32' => $i],
            \range(1, 100)
        ));

        $statistics = (new Reader())->read($path)->metadata()->columnChunks()[0]->statistics();

        self::assertSame(1, $statistics->min($column));
        self::assertSame(100, $statistics->max($column));
        self::assertSame(1, $statistics->minValue($column));
        self::assertSame(100, $statistics->maxValue($column));
        self::assertSame(100, $statistics->distinctCount());
        self::assertSame(0, $statistics->nullCount());

        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_data_page_v2_statistics() : void
    {
        $writer = new Writer(
            options: Options::default()
                ->set(Option::WRITER_VERSION, 2)
        );

        $path = __DIR__ . '/var/test-writer-parquet-test-v2-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = Schema::with($column = FlatColumn::int32('int32'));

        $writer->write($path, $schema, \array_map(
            static fn ($i) => ['int32' => $i],
            \range(1, 100)
        ));

        foreach ((new Reader())->read($path)->pageHeaders() as $pageHeader) {
            $statistics = $pageHeader->pageHeader->dataPageHeaderV2()->statistics();

            self::assertSame(1, $statistics->min($column));
            self::assertSame(100, $statistics->max($column));
            self::assertSame(1, $statistics->minValue($column));
            self::assertSame(100, $statistics->maxValue($column));
            self::assertSame(100, $statistics->distinctCount());
            self::assertSame(0, $statistics->nullCount());
        }

        \unlink($path);
    }

    public function test_writing_in_batches_to_file() : void
    {
        $writer = new Writer();

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();

        $row = $this->createRow();

        $writer->open($path, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        $writer->close();

        self::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_in_batches_to_file_without_explicit_close() : void
    {
        $writer = new Writer();

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();

        $row = $this->createRow();
        $writer->open($path, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        unset($writer);

        self::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_in_batches_to_stream() : void
    {
        $writer = new Writer();

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();

        $row = $this->createRow();

        $stream = \fopen($path, 'wb+');
        $writer->openForStream(new NativeLocalDestinationStream(new Path($path), $stream), $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        $writer->close();

        self::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
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

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();
        $row = $this->createRow();

        $writer->write($path, $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        self::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_to_file_v2() : void
    {
        $writer = new Writer(
            options: Options::default()
                ->set(Option::WRITER_VERSION, 2)
        );

        $path = __DIR__ . '/var/test-writer-parquet-test-v2-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();
        $row = $this->createRow();

        $writer->write($path, $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        self::assertSame(2, (new Reader())->read($path)->metadata()->version());
        self::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
        \unlink($path);
    }

    public function test_writing_to_stream() : void
    {
        $writer = new Writer();

        $path = __DIR__ . '/var/test-writer-parquet-test-' . bin2hex(random_bytes(16)) . '.parquet';

        $schema = $this->createSchema();
        $row = $this->createRow();

        $stream = \fopen($path, 'wb+');

        $writer->writeStream(new NativeLocalDestinationStream(new Path($path), $stream), $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        self::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            \iterator_to_array((new Reader())->read($path)->values())
        );
        self::assertFileExists($path);
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
