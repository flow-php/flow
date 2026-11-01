<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function iterator_to_array;
use function range;

final class DeltaEncodingsTest extends ParquetIntegrationTestCase
{
    public static function engine_pair_writer_version_provider(): Generator
    {
        foreach (self::engine_pair_provider() as $pair => [$writerEngine, $readerEngine]) {
            yield "{$pair} v1" => [$writerEngine, $readerEngine, 1];
            yield "{$pair} v2" => [$writerEngine, $readerEngine, 2];
        }
    }

    #[DataProvider('engine_pair_provider')]
    public function test_a_dictionary_column_of_nulls_round_trips(string $writerEngine, string $readerEngine): void
    {
        $path = TestParquetFile::path($this);
        $rows = [['s' => null], ['s' => null]];

        (new Writer(options: Options::default()->set(Option::COLUMNS_ENCODINGS, [
            's' => Encodings::RLE_DICTIONARY,
        ]), engine: ParquetEngineMother::create($writerEngine)))->write(
            $path,
            Schema::with(FlatColumn::string('s')),
            $rows,
        );

        static::assertSame($rows, iterator_to_array(
            (new Reader(engine: ParquetEngineMother::create($readerEngine)))
                ->read($path)
                ->values(),
            false,
        ));
    }

    #[DataProvider('engine_pair_writer_version_provider')]
    public function test_explicit_delta_string_encodings_round_trip(
        string $writerEngine,
        string $readerEngine,
        int $writerVersion,
    ): void {
        $path = TestParquetFile::path($this);
        $rows = [
            ['dlba' => 'axis', 'dba' => 'axis', 'dbp' => 1, 'required' => 3],
            ['dlba' => null, 'dba' => null, 'dbp' => null, 'required' => -2],
            ['dlba' => '', 'dba' => 'axle', 'dbp' => -5, 'required' => 7],
        ];

        (new Writer(options: Options::default()->set(
            Option::WRITER_VERSION,
            $writerVersion,
        )->set(Option::COLUMNS_ENCODINGS, [
            'dlba' => Encodings::DELTA_LENGTH_BYTE_ARRAY,
            'dba' => Encodings::DELTA_BYTE_ARRAY,
            'dbp' => Encodings::DELTA_BINARY_PACKED,
            'required' => Encodings::DELTA_BINARY_PACKED,
        ]), engine: ParquetEngineMother::create($writerEngine)))->write(
            $path,
            Schema::with(
                FlatColumn::string('dlba'),
                FlatColumn::string('dba'),
                FlatColumn::int32('dbp'),
                FlatColumn::int64('required', Repetition::REQUIRED),
            ),
            $rows,
        );

        $file = (new Reader(engine: ParquetEngineMother::create($readerEngine)))->read($path);

        static::assertSame($rows, iterator_to_array($file->values(), false));
        [$dlba, $dba, $dbp, $required] = $file->metadata()->columnChunks();
        static::assertContains(Encodings::DELTA_LENGTH_BYTE_ARRAY, $dlba->encodings());
        static::assertContains(Encodings::DELTA_BYTE_ARRAY, $dba->encodings());
        static::assertContains(Encodings::DELTA_BINARY_PACKED, $dbp->encodings());
        static::assertContains(Encodings::DELTA_BINARY_PACKED, $required->encodings());
    }

    #[DataProvider('engine_provider')]
    public function test_reading_delta_byte_array_written_by_arrow_rs(string $engineClass): void
    {
        static::assertSame(
            [
                ['s' => 'axis', 'f' => 'aaaa'],
                ['s' => 'axle', 'f' => null],
                ['s' => 'babble', 'f' => 'aabb'],
                ['s' => 'babyhood', 'f' => 'abcd'],
                ['s' => null, 'f' => 'zzzz'],
            ],
            iterator_to_array(
                (new Reader(engine: ParquetEngineMother::create($engineClass)))
                    ->read(__DIR__ . '/Fixtures/delta_byte_array_arrow_v2.parquet')
                    ->values(),
                false,
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_delta_length_byte_array_written_by_an_external_writer(string $engineClass): void
    {
        static::assertSame(
            [
                ['s' => 'axis', 'n' => 'a'],
                ['s' => 'axle', 'n' => null],
                ['s' => 'babble', 'n' => ''],
                ['s' => 'babyhood', 'n' => 'b'],
                ['s' => '', 'n' => null],
            ],
            iterator_to_array(
                (new Reader(engine: ParquetEngineMother::create($engineClass)))
                    ->read(__DIR__ . '/Fixtures/delta_length_byte_array_external_v2.parquet')
                    ->values(),
                false,
            ),
        );
    }

    #[DataProvider('engine_pair_provider')]
    public function test_writer_version_2_round_trips_strings_fixed_length_and_ints(
        string $writerEngine,
        string $readerEngine,
    ): void {
        $path = TestParquetFile::path($this);
        $rows = [
            ['s' => null, 'f' => null, 'i' => null],
            ['s' => null, 'f' => null, 'i' => null],
            ['s' => 'axis', 'f' => 'aaaa', 'i' => 1],
            ['s' => 'axle', 'f' => 'aabb', 'i' => 2],
            ['s' => 'babble', 'f' => 'abcd', 'i' => 3],
        ];

        (new Writer(
            options: Options::default()
                ->set(Option::DICTIONARY_ENABLED, false)
                ->set(Option::WRITER_VERSION, 2)
                ->set(Option::PAGE_MAXIMUM_ROWS_COUNT, 2)
                ->set(Option::PAGE_SIZE_CHECK_INTERVAL, 2),
            engine: ParquetEngineMother::create($writerEngine),
        ))->write(
            $path,
            Schema::with(FlatColumn::string('s'), FlatColumn::fixedSizeByteArray('f', 4), FlatColumn::int64('i')),
            $rows,
        );

        $file = (new Reader(engine: ParquetEngineMother::create($readerEngine)))->read($path);

        static::assertSame($rows, iterator_to_array($file->values(), false));

        if ($writerEngine === PhpParquetEngine::class) {
            static::assertSame(
                [2, 2, 1, 2, 2, 1, 2, 2, 1],
                array_map(
                    static fn(ColumnPageHeader $page): int => $page->pageHeader->dataValuesCount() ?? 0,
                    iterator_to_array($file->pageHeaders(), false),
                ),
            );
            static::assertSame(
                [
                    [Encodings::RLE, Encodings::DELTA_BYTE_ARRAY],
                    [Encodings::RLE, Encodings::DELTA_BYTE_ARRAY],
                    [Encodings::RLE, Encodings::DELTA_BINARY_PACKED],
                ],
                array_map(
                    static fn(ColumnChunk $chunk): array => $chunk->encodings(),
                    $file->metadata()->columnChunks(),
                ),
            );
        }
    }

    #[DataProvider('engine_pair_provider')]
    public function test_int64_values_with_deltas_near_large_powers_of_two_round_trip(
        string $writerEngine,
        string $readerEngine,
    ): void {
        $path = TestParquetFile::path($this);
        $rows = array_map(static fn(int $i): array => ['i' => ($i % 2) === 0 ? 0 : (1 << 59) + $i], range(0, 129));

        (new Writer(
            options: Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(Option::WRITER_VERSION, 2),
            engine: ParquetEngineMother::create($writerEngine),
        ))->write($path, Schema::with(FlatColumn::int64('i')), $rows);

        static::assertSame($rows, iterator_to_array(
            (new Reader(engine: ParquetEngineMother::create($readerEngine)))
                ->read($path)
                ->values(),
            false,
        ));
    }

    #[DataProvider('engine_pair_provider')]
    public function test_negative_and_sequential_ints_round_trip_gzip_compressed(
        string $writerEngine,
        string $readerEngine,
    ): void {
        $path = TestParquetFile::path($this);
        $rows = array_map(
            static fn(int $i): array => ['i32' => -$i * 7, 'i64' => 1_700_000_000_000 + $i],
            range(0, 199),
        );

        (new Writer(
            compression: Compressions::GZIP,
            options: Options::default()->set(Option::DICTIONARY_ENABLED, false)->set(Option::WRITER_VERSION, 2),
            engine: ParquetEngineMother::create($writerEngine),
        ))->write($path, Schema::with(FlatColumn::int32('i32'), FlatColumn::int64('i64')), $rows);

        static::assertSame($rows, iterator_to_array(
            (new Reader(engine: ParquetEngineMother::create($readerEngine)))
                ->read($path)
                ->values(),
            false,
        ));
    }
}
