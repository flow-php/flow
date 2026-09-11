<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Composer\InstalledVersions;
use Faker\Factory;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Parquet\Consts;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function array_map;
use function Flow\ETL\DSL\generate_random_int;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function iterator_to_array;
use function range;

class WriterTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_closing_not_open_writer(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->close();
    }

    public function test_created_by_metadata(): void
    {
        $writer = Writer::php();

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();
        $writer->open($path, $schema);
        $writer->close();

        $metadata = Reader::php()->read($path)->metadata();

        static::assertSame(
            'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version'],
            $metadata->createdBy(),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_opening_already_open_writer(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();

        $writer->open($path, $schema);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is already open');

        $writer->open($path, $schema);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_all_column_types(ParquetEngine $engine): void
    {
        $schema = Schema::with(
            FlatColumn::int32('int32_col'),
            FlatColumn::int64('int64_col'),
            FlatColumn::string('string_col'),
            FlatColumn::boolean('bool_col'),
            FlatColumn::double('double_col'),
            FlatColumn::float('float_col'),
            NestedColumn::structure('struct_col', [
                FlatColumn::int32('a'),
                FlatColumn::string('b'),
            ]),
            NestedColumn::list('list_col', ListElement::int32()),
            NestedColumn::map('map_col', MapKey::string(), MapValue::int32()),
        );

        $rows = [
            [
                'int32_col' => 1,
                'int64_col' => 100,
                'string_col' => 'hello',
                'bool_col' => true,
                'double_col' => 1.5,
                'float_col' => 2.5,
                'struct_col' => ['a' => 10, 'b' => 'x'],
                'list_col' => [1, 2],
                'map_col' => ['k' => 1],
            ],
            [
                'int32_col' => null,
                'int64_col' => null,
                'string_col' => null,
                'bool_col' => null,
                'double_col' => null,
                'float_col' => null,
                'struct_col' => null,
                'list_col' => null,
                'map_col' => null,
            ],
            [
                'int32_col' => 3,
                'int64_col' => 300,
                'string_col' => 'world',
                'bool_col' => false,
                'double_col' => 3.5,
                'float_col' => 4.5,
                'struct_col' => ['a' => 30, 'b' => 'z'],
                'list_col' => [3],
                'map_col' => ['a' => 1, 'b' => 2],
            ],
        ];

        $path = TestParquetFile::path($this);

        (new Writer(engine: $engine))->write($path, $schema, $rows);

        static::assertSame(
            $rows,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_batch_to_not_open_stream(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->writeBatch([$this->createRow()]);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_column_statistics(ParquetEngine $engine): void
    {
        $writer = new Writer(options: Options::default()->set(Option::WRITER_VERSION, 1), engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = Schema::with($column = FlatColumn::int32('int32'));

        $writer->write($path, $schema, array_map(static fn($i) => ['int32' => $i], range(1, 100)));

        $statistics = (new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->columnChunks()[0]->statistics();

        static::assertNotNull($statistics);
        static::assertSame(1, $statistics->min($column));
        static::assertSame(100, $statistics->max($column));
        static::assertSame(1, $statistics->minValue($column));
        static::assertSame(100, $statistics->maxValue($column));
        static::assertNull($statistics->distinctCount());
        static::assertSame(0, $statistics->nullCount());

        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_column_statistics_with_null_values(ParquetEngine $engine): void
    {
        $schema = Schema::with(
            FlatColumn::string('all_null'),
            FlatColumn::string('all_string'),
            FlatColumn::string('mixed'),
        );

        $rows = [
            ['all_null' => null, 'all_string' => 'a', 'mixed' => 'x'],
            ['all_null' => null, 'all_string' => 'b', 'mixed' => null],
            ['all_null' => null, 'all_string' => 'c', 'mixed' => 'z'],
        ];

        $path = TestParquetFile::path($this);

        (new Writer(engine: $engine))->write($path, $schema, $rows);

        $chunks = [];

        foreach ((new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->columnChunks() as $chunk) {
            $chunks[$chunk->flatPath()] = $chunk;
        }

        static::assertSame(3, $chunks['all_null']->statistics()?->nullCount());
        static::assertSame(0, $chunks['all_string']->statistics()?->nullCount());
        static::assertSame(1, $chunks['mixed']->statistics()?->nullCount());
    }

    public function test_writing_data_page_v2_statistics(): void
    {
        $options = Options::default()->set(Option::WRITER_VERSION, 2);
        $writer = Writer::php(options: $options);

        $path = TestParquetFile::path($this);

        $schema = Schema::with($column = FlatColumn::int32('int32'));

        $writer->write($path, $schema, array_map(static fn($i) => ['int32' => $i], range(1, 100)));

        foreach ((new Reader(options: $options, engine: new PhpParquetEngine()))
            ->read($path)
            ->pageHeaders() as $pageHeader) {
            $dataPageHeaderV2 = $pageHeader->pageHeader->dataPageHeaderV2();
            static::assertNotNull($dataPageHeaderV2);
            $statistics = $dataPageHeaderV2->statistics($options);
            static::assertNotNull($statistics);

            static::assertSame(1, $statistics->min($column));
            static::assertSame(100, $statistics->max($column));
            static::assertSame(1, $statistics->minValue($column));
            static::assertSame(100, $statistics->maxValue($column));
            static::assertNull($statistics->distinctCount());
            static::assertSame(0, $statistics->nullCount());
        }
    }

    #[DataProvider('engine_provider')]
    public function test_writing_in_batches_to_file(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();

        $row = $this->createRow();

        $writer->open($path, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        $writer->close();

        static::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_in_batches_to_file_without_explicit_close(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();

        $row = $this->createRow();
        $writer->open($path, $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        unset($writer);

        static::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_in_batches_to_stream(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();

        $row = $this->createRow();

        $stream = fopen($path, 'wb+');
        $writer->openForStream(new NativeLocalDestinationStream(path($path), $stream), $schema);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);
        $writer->writeBatch([$row, $row]);

        $writer->close();

        static::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_one_row_that_is_nullable(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $schema = Schema::with($column = FlatColumn::int32('id'));

        $path = TestParquetFile::path($this);

        $writer->write($path, $schema, [
            [
                'id' => null,
            ],
        ]);

        static::assertNull((new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->columnChunks()[0]->statistics()?->max($column));
        static::assertNull((new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->columnChunks()[0]->statistics()?->min($column));
        static::assertNull(
            (new Reader(engine: $engine))
                ->read($path)
                ->metadata()
                ->columnChunks()[0]
                ->statistics()
                ?->maxValue($column),
        );
        static::assertNull(
            (new Reader(engine: $engine))
                ->read($path)
                ->metadata()
                ->columnChunks()[0]
                ->statistics()
                ?->minValue($column),
        );

        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_row_to_not_open_stream(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->writeRow($this->createRow());
    }

    #[DataProvider('engine_provider')]
    public function test_writing_to_file(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();
        $row = $this->createRow();

        $writer->write($path, $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        static::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_to_file_v2(ParquetEngine $engine): void
    {
        $writer = new Writer(options: Options::default()->set(Option::WRITER_VERSION, 2), engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();
        $row = $this->createRow();

        $writer->write($path, $schema, [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row]);

        static::assertSame(
            2,
            (new Reader(engine: $engine))
                ->read($path)
                ->metadata()
                ->version(),
        );
        static::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_to_stream(ParquetEngine $engine): void
    {
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = $this->createSchema();
        $row = $this->createRow();

        $stream = fopen($path, 'wb+');

        $writer->writeStream(new NativeLocalDestinationStream(path($path), $stream), $schema, [
            $row,
            $row,
            $row,
            $row,
            $row,
            $row,
            $row,
            $row,
            $row,
            $row,
        ]);

        static::assertSame(
            [$row, $row, $row, $row, $row, $row, $row, $row, $row, $row],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
        static::assertFileExists($path);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_with_all_nullable_columns(ParquetEngine $engine): void
    {
        $schema = Schema::with(FlatColumn::int32('a'), FlatColumn::string('b'), FlatColumn::boolean('c'));

        $rows = [
            ['a' => null, 'b' => null, 'c' => null],
            ['a' => null, 'b' => null, 'c' => null],
            ['a' => 1, 'b' => 'x', 'c' => true],
            ['a' => null, 'b' => null, 'c' => null],
        ];

        $path = TestParquetFile::path($this);

        (new Writer(engine: $engine))->write($path, $schema, $rows);

        static::assertSame(
            $rows,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_with_dictionary_encoding(ParquetEngine $engine): void
    {
        $schema = Schema::with(FlatColumn::int32('id')->makeRequired(), FlatColumn::string('category'));

        $rows = [];

        for ($i = 0; $i < 100; $i++) {
            $rows[] = ['id' => $i, 'category' => 'cat_' . ($i % 5)];
        }

        $path = TestParquetFile::path($this);

        (new Writer(engine: $engine))->write($path, $schema, $rows);

        static::assertSame(
            $rows,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_with_empty_lists_and_maps(ParquetEngine $engine): void
    {
        $schema = Schema::with(
            FlatColumn::int32('id')->makeRequired(),
            NestedColumn::list('items', ListElement::string()),
            NestedColumn::map('props', MapKey::string(), MapValue::int32()),
        );

        $rows = [
            ['id' => 1, 'items' => [], 'props' => []],
            ['id' => 2, 'items' => ['a'], 'props' => ['x' => 1]],
            ['id' => 3, 'items' => [], 'props' => []],
            ['id' => 4, 'items' => null, 'props' => null],
        ];

        $path = TestParquetFile::path($this);

        (new Writer(engine: $engine))->write($path, $schema, $rows);

        static::assertSame(
            $rows,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function createRow(): array
    {
        $faker = Factory::create();

        return [
            'struct' => [
                'int64' => $faker->numberBetween(0, Consts::PHP_INT64_MAX),
                'boolean' => $faker->boolean(),
                'string' => $faker->text(150),
                'int32' => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                'list_of_int' => array_map(
                    static fn($i) => $faker->numberBetween(0, Consts::PHP_INT32_MAX),
                    range(1, generate_random_int(2, 10)),
                ),
                'list_of_string' => array_map(static fn($i) => $faker->text(10), range(1, generate_random_int(2, 10))),
            ],
        ];
    }

    private function createSchema(): Schema
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
