<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Composer\InstalledVersions;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Tests\Context\StructWithLists;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use Flow\Parquet\Writer\StatisticsLevel;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function array_combine;
use function array_map;
use function array_unique;
use function array_values;
use function count;
use function extension_loaded;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function iterator_to_array;
use function pack;
use function range;
use function str_repeat;
use function unpack;
use function var_export;

use const NAN;

class WriterTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_a_close_that_throws_leaves_the_writer_closed(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);
        $writer->openForStream(
            new FailingCloseDestinationStream(memory_filesystem()->writeTo(path('memory://file.parquet'))),
            Schema::with(FlatColumn::int32('id')),
        );
        $writer->writeRow(['id' => 1]);

        try {
            $writer->close();
            static::fail('close() was expected to throw');
        } catch (FilesystemRuntimeException $failure) {
            static::assertSame('Closing "memory://file.parquet" failed', $failure->getMessage());
        }

        static::assertFalse($writer->isOpen());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->close();
    }

    #[DataProvider('engine_provider')]
    public function test_closing_not_open_writer(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->close();
    }

    public function test_created_by_metadata(): void
    {
        $writer = Writer::php();

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();
        $writer->open($path, $schema);
        $writer->close();

        $metadata = Reader::php()->read($path)->metadata();

        static::assertSame(
            'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version'],
            $metadata->createdBy(),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_opening_already_open_writer(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();

        $writer->open($path, $schema);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is already open');

        $writer->open($path, $schema);
    }

    #[DataProvider('engine_provider')]
    public function test_writers_sharing_one_engine_write_independent_files(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $memory = memory_filesystem();
        $schema = Schema::with(FlatColumn::int32('id'));
        $first = new Writer(engine: $engine);
        $second = new Writer(engine: $engine);

        $first->openForStream($memory->writeTo(path('memory://first.parquet')), $schema);
        $second->openForStream($memory->writeTo(path('memory://second.parquet')), $schema);
        $first->writeRow(['id' => 1]);
        $second->writeRow(['id' => 2]);
        $first->writeRow(['id' => 3]);
        $first->close();
        $second->close();

        static::assertSame(
            [['id' => 1], ['id' => 3]],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->readStream($memory->readFrom(path('memory://first.parquet')))
                    ->values(),
                false,
            ),
        );
        static::assertSame(
            [['id' => 2]],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->readStream($memory->readFrom(path('memory://second.parquet')))
                    ->values(),
                false,
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_all_column_types(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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
    public function test_writing_batch_to_not_open_stream(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->writeBatch([StructWithLists::row()]);
    }

    #[DataProvider('engine_provider')]
    public function test_writing_column_statistics(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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
    public function test_writing_column_orders_per_leaf(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $path = TestParquetFile::path($this);
        (new Writer(engine: $engine))->write(
            $path,
            Schema::with(
                FlatColumn::string('string'),
                FlatColumn::double('double'),
                FlatColumn::float('float'),
                FlatColumn::int64('int64'),
                NestedColumn::list('list', ListElement::double()),
                NestedColumn::map('map', MapKey::string(), MapValue::int32()),
                NestedColumn::struct('struct', [FlatColumn::double('x'), FlatColumn::boolean('flag')]),
            ),
            [[
                'string' => 'a',
                'double' => 1.5,
                'float' => 2.5,
                'int64' => 1,
                'list' => [1.5],
                'map' => ['a' => 1],
                'struct' => ['x' => 1.5, 'flag' => true],
            ]],
        );

        $metadata = (new Reader(engine: $engine))
            ->read($path)
            ->metadata();

        static::assertSame(
            [
                'string' => ColumnOrder::TypeDefined,
                'double' => ColumnOrder::Ieee754TotalOrder,
                'float' => ColumnOrder::Ieee754TotalOrder,
                'int64' => ColumnOrder::TypeDefined,
                'list.list.element' => ColumnOrder::Ieee754TotalOrder,
                'map.key_value.key' => ColumnOrder::TypeDefined,
                'map.key_value.value' => ColumnOrder::TypeDefined,
                'struct.x' => ColumnOrder::Ieee754TotalOrder,
                'struct.flag' => ColumnOrder::TypeDefined,
            ],
            array_combine(
                array_map(
                    static fn(FlatColumn $column): string => $column->flatPath(),
                    $metadata->schema()->columnsFlat(),
                ),
                array_map($metadata->columnOrder(...), $metadata->schema()->columnsFlat()),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_float_statistics_in_total_order_with_nan_count(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $path = TestParquetFile::path($this);
        $column = FlatColumn::double('double');

        (new Writer(engine: $engine))->write(
            $path,
            Schema::with($column),
            array_map(static fn(float $value): array => [
                'double' => $value,
            ], [NAN, 0.0, -0.0, 1.5, NAN]),
        );

        $statistics = (new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->columnChunks()[0]->statistics();

        static::assertNotNull($statistics);
        static::assertSame('-0.0', var_export($statistics->minValue($column), true));
        static::assertSame(1.5, $statistics->maxValue($column));
        static::assertSame(2, $statistics->nanCount());
        static::assertNull($statistics->min($column));
        static::assertNull($statistics->max($column));
        static::assertTrue($statistics->isMinValueExact());
        static::assertTrue($statistics->isMaxValueExact());
    }

    #[DataProvider('engine_pair_provider')]
    public function test_writing_uint64_reads_back_as_decimal_strings_on_both_engines(
        string $writerEngine,
        string $readerEngine,
    ): void {
        $path = TestParquetFile::path($this);

        (new Writer(engine: ParquetEngineMother::create($writerEngine)))->write(
            $path,
            Schema::with(new FlatColumn('u', PhysicalType::INT64, logicalType: LogicalType::integer(64, false))),
            [['u' => 0], ['u' => '9223372036854775808'], ['u' => '18446744073709551615']],
        );

        static::assertSame(
            [['u' => '0'], ['u' => '9223372036854775808'], ['u' => '18446744073709551615']],
            ParquetRows::read((new Reader(engine: ParquetEngineMother::create($readerEngine)))->read($path), 10),
        );
    }

    /**
     * arrow-rs 60 basic.rs `ConvertedType::from(Option<LogicalType>)`, copied by value.
     *
     * @return Generator<string, array{FlatColumn, ?ConvertedType}>
     */
    public static function converted_types(): Generator
    {
        $logical = static fn(
            PhysicalType $physical,
            LogicalType $type,
            ?int $length = null,
        ): FlatColumn => new FlatColumn('c', $physical, logicalType: $type, typeLength: $length);

        yield 'string' => [$logical(PhysicalType::BYTE_ARRAY, LogicalType::string()), ConvertedType::UTF8];
        yield 'enum' => [$logical(PhysicalType::BYTE_ARRAY, LogicalType::enum()), ConvertedType::ENUM];
        yield 'json' => [$logical(PhysicalType::BYTE_ARRAY, LogicalType::json()), ConvertedType::JSON];
        yield 'bson' => [$logical(PhysicalType::BYTE_ARRAY, LogicalType::bson()), ConvertedType::BSON];
        yield 'decimal' => [
            new FlatColumn('c', PhysicalType::INT64, logicalType: LogicalType::decimal(2, 10), precision: 10, scale: 2),
            ConvertedType::DECIMAL,
        ];
        yield 'date' => [$logical(PhysicalType::INT32, LogicalType::date()), ConvertedType::DATE];
        yield 'time millis' => [
            $logical(
                PhysicalType::INT32,
                new LogicalType(LogicalType::TIME, time: new Time(true, TimeUnit::MILLISECONDS)),
            ),
            ConvertedType::TIME_MILLIS,
        ];
        yield 'time micros' => [$logical(PhysicalType::INT64, LogicalType::time()), ConvertedType::TIME_MICROS];
        yield 'time nanos' => [
            $logical(
                PhysicalType::INT64,
                new LogicalType(LogicalType::TIME, time: new Time(true, TimeUnit::NANOSECONDS)),
            ),
            null,
        ];
        yield 'timestamp millis' => [
            $logical(
                PhysicalType::INT64,
                new LogicalType(LogicalType::TIMESTAMP, timestamp: new Timestamp(true, TimeUnit::MILLISECONDS)),
            ),
            ConvertedType::TIMESTAMP_MILLIS,
        ];
        yield 'timestamp micros' => [
            $logical(PhysicalType::INT64, LogicalType::timestamp()),
            ConvertedType::TIMESTAMP_MICROS,
        ];
        yield 'timestamp nanos' => [
            $logical(
                PhysicalType::INT64,
                new LogicalType(LogicalType::TIMESTAMP, timestamp: new Timestamp(true, TimeUnit::NANOSECONDS)),
            ),
            null,
        ];
        yield 'int8' => [$logical(PhysicalType::INT32, LogicalType::integer(8, true)), ConvertedType::INT_8];
        yield 'int16' => [$logical(PhysicalType::INT32, LogicalType::integer(16, true)), ConvertedType::INT_16];
        yield 'int32' => [$logical(PhysicalType::INT32, LogicalType::integer(32, true)), ConvertedType::INT_32];
        yield 'int64' => [$logical(PhysicalType::INT64, LogicalType::integer(64, true)), ConvertedType::INT_64];
        yield 'uint8' => [$logical(PhysicalType::INT32, LogicalType::integer(8, false)), ConvertedType::UINT_8];
        yield 'uint16' => [$logical(PhysicalType::INT32, LogicalType::integer(16, false)), ConvertedType::UINT_16];
        yield 'uint32' => [$logical(PhysicalType::INT32, LogicalType::integer(32, false)), ConvertedType::UINT_32];
        yield 'uint64' => [$logical(PhysicalType::INT64, LogicalType::integer(64, false)), ConvertedType::UINT_64];
        yield 'uuid' => [$logical(PhysicalType::FIXED_LEN_BYTE_ARRAY, LogicalType::uuid(), 16), null];
        yield 'float16' => [
            $logical(PhysicalType::FIXED_LEN_BYTE_ARRAY, new LogicalType(LogicalType::FLOAT16), 2),
            null,
        ];
    }

    #[DataProvider('converted_types')]
    public function test_writing_the_converted_type_of_a_logical_type(
        FlatColumn $column,
        ?ConvertedType $expected,
    ): void {
        $path = TestParquetFile::path($this);

        (new Writer(engine: new PhpParquetEngine()))->write($path, Schema::with($column), []);

        static::assertSame(
            $expected?->value,
            Reader::php()->read($path)->metadata()->toThrift()->schema[1]->converted_type,
        );
    }

    #[DataProvider('engine_pair_provider')]
    public function test_an_int16_column_round_trips(string $writerEngine, string $readerEngine): void
    {
        $path = TestParquetFile::path($this);

        (new Writer(engine: ParquetEngineMother::create($writerEngine)))->write(
            $path,
            Schema::with(new FlatColumn('s', PhysicalType::INT32, ConvertedType::INT_16)),
            [['s' => 1], ['s' => -2], ['s' => 32_767], ['s' => null]],
        );

        static::assertSame(
            [['s' => 1], ['s' => -2], ['s' => 32_767], ['s' => null]],
            ParquetRows::read((new Reader(engine: ParquetEngineMother::create($readerEngine)))->read($path), 10),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_writing_column_statistics_with_null_values(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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
        $options = Options::default()->set(Option::WRITER_VERSION, 2)->set(Option::PAGE_HEADER_STATISTICS, true);
        $writer = Writer::php(options: $options);

        $path = TestParquetFile::path($this);

        $schema = Schema::with($column = FlatColumn::int32('int32'));

        $writer->write($path, $schema, array_map(static fn($i) => ['int32' => $i], range(1, 100)));

        foreach ((new Reader(options: $options, engine: new PhpParquetEngine()))
            ->read($path)
            ->pageHeaders() as $pageHeader) {
            $dataPageHeaderV2 = $pageHeader->pageHeader->dataPageHeaderV2();
            static::assertNotNull($dataPageHeaderV2);
            $statistics = $dataPageHeaderV2->statistics();
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
    public function test_writing_in_batches_to_file(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();

        $row = StructWithLists::row();

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
    public function test_writing_in_batches_to_file_without_explicit_close(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();

        $row = StructWithLists::row();
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
    public function test_writing_in_batches_to_stream(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();

        $row = StructWithLists::row();

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
    public function test_writing_one_row_that_is_nullable(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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
    public function test_writing_row_to_not_open_stream(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Writer is not open');

        $writer->writeRow(StructWithLists::row());
    }

    #[DataProvider('engine_provider')]
    public function test_writing_to_file(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();
        $row = StructWithLists::row();

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
    public function test_writing_to_file_v2(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(options: Options::default()->set(Option::WRITER_VERSION, 2), engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();
        $row = StructWithLists::row();

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
    public function test_writing_to_stream(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $writer = new Writer(engine: $engine);

        $path = TestParquetFile::path($this);

        $schema = StructWithLists::schema();
        $row = StructWithLists::row();

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
    public function test_writing_with_all_nullable_columns(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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
    public function test_writing_with_dictionary_encoding(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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
    public function test_writing_with_empty_lists_and_maps(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
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

    public function test_the_page_index_is_written_and_reads_back_on_both_engines(): void
    {
        $bytes = MemoryParquetFile::written(
            Writer::php(options: (new Options())->set(Option::PAGE_SIZE_BYTES, 8)->set(
                Option::PAGE_SIZE_CHECK_INTERVAL,
                2,
            )),
            Schema::with($id = FlatColumn::int64('id'), $name = FlatColumn::string('name')),
            array_map(
                static fn(int $i): array => ['id' => $i, 'name' => ($i % 3) === 0 ? null : 'n' . $i],
                range(0, 5),
            ),
        );
        $file = MemoryParquetFile::read(Reader::php(), $bytes);

        static::assertEquals(
            new ColumnIndex(
                [false, false, false],
                [pack('P', 0), pack('P', 2), pack('P', 4)],
                [pack('P', 1), pack('P', 3), pack('P', 5)],
                BoundaryOrder::ASCENDING,
                [0, 0, 0],
            ),
            $file->indexes()->columnIndex(0, $id),
        );
        static::assertEquals(
            new ColumnIndex(
                [false, false, false],
                ['n1', 'n2', 'n4'],
                ['n1', 'n2', 'n5'],
                BoundaryOrder::ASCENDING,
                [1, 1, 0],
            ),
            $file->indexes()->columnIndex(0, $name),
        );
        static::assertSame(
            [0, 2, 4],
            array_map(
                static fn(PageLocation $page): int => $page->firstRowIndex,
                $file->indexes()->offsetIndex(0, $name)->pages ?? [],
            ),
        );

        if (extension_loaded('arrow')) {
            static::assertSame(
                iterator_to_array($file->values(), false),
                iterator_to_array(
                    MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes)->values(),
                    false,
                ),
            );
        }
    }

    public function test_bloom_filters_are_written_after_each_row_group_for_the_columns_asked(): void
    {
        $bytes = MemoryParquetFile::written(Writer::php(options: (new Options())
            ->set(Option::ROW_GROUP_SIZE_BYTES, 1)
            ->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 3)
            ->set(Option::PAGE_SIZE_BYTES, 1)
            ->set(Option::PAGE_SIZE_CHECK_INTERVAL, 3)
            ->set(Option::COLUMNS_BLOOM_FILTERS, [
                'id' => new BloomFilterOptions(),
            ])), Schema::with($id = FlatColumn::int64('id'), $name = FlatColumn::string('name')), array_map(static fn(int $i): array => ['id' => $i, 'name' => 'n' . $i], range(0, 5)));
        $file = MemoryParquetFile::read(Reader::php(), $bytes);

        foreach ([[0, 1, 2], [3, 4, 5]] as $rowGroup => $ids) {
            $chunks = $file->metadata()->rowGroups()->all()[$rowGroup]->columnChunks();
            $bloomFilter = $file->indexes()->bloomFilter($rowGroup, $id);

            static::assertNotNull($bloomFilter);
            static::assertGreaterThan($chunks[1]->fileOffset(), $chunks[0]->bloomFilterOffset());

            foreach ($ids as $value) {
                static::assertTrue($bloomFilter->check((new BloomHash())->of($id, $value)));
            }

            static::assertNull($file->indexes()->bloomFilter($rowGroup, $name));
        }
    }

    public function test_both_engines_write_the_same_bloom_filter_bytes(): void
    {
        if (!extension_loaded('arrow')) {
            static::markTestSkipped('arrow is not loaded');
        }

        $options = (new Options())->set(Option::COLUMNS_BLOOM_FILTERS, [
            'id' => new BloomFilterOptions(0.05, 5000),
            'name' => new BloomFilterOptions(0.01),
        ]);
        $schema = Schema::with($id = FlatColumn::int64('id'), $name = FlatColumn::string('name'));
        $rows = array_map(static fn(int $i): array => ['id' => $i * 7, 'name' => 'n' . $i], range(0, 2999));
        $php = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
            Writer::php(options: $options),
            $schema,
            $rows,
        ));
        $arrow = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
            new Writer(options: $options, engine: new RustParquetEngine()),
            $schema,
            $rows,
        ));

        foreach ([$id, $name] as $column) {
            static::assertSame(
                $php->indexes()->bloomFilter(0, $column)?->bytes(),
                $arrow->indexes()->bloomFilter(0, $column)?->bytes(),
            );
        }
    }

    public function test_pages_of_a_list_column_start_at_row_boundaries(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
            Writer::php(options: (new Options())->set(Option::PAGE_SIZE_BYTES, 16)->set(
                Option::PAGE_SIZE_CHECK_INTERVAL,
                1,
            )),
            Schema::with($id = FlatColumn::int64('id'), NestedColumn::list('tags', ListElement::int64())),
            array_map(static fn(int $i): array => ['id' => $i, 'tags' => range(0, $i % 4)], range(0, 19)),
        ));
        $element = $file->schema()->getFlat('tags.list.element');
        $pages = $file->indexes()->offsetIndex(0, $element)->pages ?? [];
        $firstRows = array_map(static fn(PageLocation $page): int => $page->firstRowIndex, $pages);
        $ids = $file->indexes()->columnIndex(0, $id);

        static::assertGreaterThan(1, count($pages));
        static::assertSame($firstRows, array_values(array_unique($firstRows)));
        static::assertSame($firstRows, array_map(
            static fn(PageLocation $page): int => $page->firstRowIndex,
            $file->indexes()->offsetIndex(0, $id)->pages ?? [],
        ));
        static::assertSame($firstRows, array_map(
            static fn(string $min): int => unpack('P', $min)[1],
            $ids->minValues ?? [],
        ));
    }

    public function test_column_index_bounds_are_truncated_to_64_bytes(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
            Writer::php(),
            Schema::with($name = FlatColumn::string('name')),
            [['name' => str_repeat('a', 100)], ['name' => str_repeat('b', 100)]],
        ));

        static::assertSame([str_repeat('a', 64)], $file->indexes()->columnIndex(0, $name)?->minValues);
        static::assertSame([str_repeat('b', 63) . 'c'], $file->indexes()->columnIndex(0, $name)?->maxValues);
    }

    public function test_the_statistics_level_decides_chunk_statistics_and_the_column_index(): void
    {
        $written =
            static fn(StatisticsLevel $level): ParquetFile => MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
                Writer::php(options: (new Options())->set(Option::STATISTICS_LEVEL, $level)),
                Schema::with(FlatColumn::int64('id')),
                [['id' => 1], ['id' => 2]],
            ));
        $id = FlatColumn::int64('id');

        foreach ([
            [StatisticsLevel::NONE,  false, false],
            [StatisticsLevel::CHUNK, true,  false],
            [StatisticsLevel::PAGE,  true,  true],
        ] as [$level, $chunk, $page]) {
            $file = $written($level);

            static::assertSame(
                $chunk,
                $file->metadata()->rowGroups()->all()[0]->columnChunks()[0]->statistics() !== null,
            );
            static::assertSame($page, $file->indexes()->columnIndex(0, $id) !== null);
            static::assertNotNull($file->indexes()->offsetIndex(0, $id));
        }
    }

    public function test_page_header_statistics_are_written_only_when_asked(): void
    {
        $headers = static fn(Options $options): array => array_map(
            static fn(ColumnPageHeader $header) => $header->pageHeader->dataPageHeader()?->statistics(),
            iterator_to_array(
                MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
                    Writer::php(options: $options),
                    Schema::with(FlatColumn::int64('id')),
                    [['id' => 1], ['id' => 2]],
                ))->pageHeaders(),
                false,
            ),
        );

        static::assertSame([null], $headers(new Options()));
        static::assertSame(
            2,
            $headers((new Options())->set(Option::PAGE_HEADER_STATISTICS, true))[0]?->maxValue(FlatColumn::int64('id')),
        );
        static::assertSame(
            [null],
            $headers((new Options())->set(Option::PAGE_HEADER_STATISTICS, true)->set(
                Option::STATISTICS_LEVEL,
                StatisticsLevel::CHUNK,
            )),
        );
    }
}
