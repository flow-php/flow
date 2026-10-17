<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Composer\InstalledVersions;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\Tests\Double\FailingCloseDestinationStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\ColumnOrder;
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
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Tests\Context\StructWithLists;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function array_combine;
use function array_map;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function iterator_to_array;
use function range;
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
}
