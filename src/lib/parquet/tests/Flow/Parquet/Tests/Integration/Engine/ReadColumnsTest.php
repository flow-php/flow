<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Reader\SkippedMatches;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Integration\IO\ParquetIntegrationTestCase;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_fill;
use function array_map;
use function count;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Parquet\DSL\parquet_column;
use function iterator_to_array;
use function range;

final class ReadColumnsTest extends ParquetIntegrationTestCase
{
    /**
     * @return Generator<string, array{class-string<ParquetEngine>, int, ?int, ?int, list<int>, list<list<int>>}>
     */
    public static function chunkings(): Generator
    {
        foreach (self::engine_provider() as $name => [$engine]) {
            yield "{$name}: batch size 2" => [$engine, 2, null, null, [2, 2, 1], [[1, 2], [3, 4], [5]]];
            yield "{$name}: offset 3" => [$engine, 2, null, 3, [2], [[4, 5]]];
            yield "{$name}: limit 3" => [$engine, 2, 3, null, [2, 1], [[1, 2], [3]]];
            yield "{$name}: offset 1, limit 3" => [$engine, 2, 3, 1, [2, 1], [[2, 3], [4]]];
            yield "{$name}: batch size above the row count" => [$engine, 10, null, null, [5], [[1, 2, 3, 4, 5]]];
        }
    }

    /**
     * @return Generator<string, array{class-string<ParquetEngine>, int, list<array{skipped: int}|list<int>>}>
     */
    public static function matched_offsets(): Generator
    {
        foreach (self::engine_provider() as $name => [$engine]) {
            yield "{$name}: no offset yields no skip" => [$engine, 0, [[3, 4, 5, 6, 7, 8, 9]]];
            yield "{$name}: an offset inside a row group" => [$engine, 2, [['skipped' => 2], [5, 6, 7, 8, 9]]];
            yield "{$name}: an offset across row groups" => [$engine, 4, [['skipped' => 4], [7, 8, 9]]];
            yield "{$name}: an offset covering the file" => [$engine, 100, [['skipped' => 7]]];
        }
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     * @param list<array{skipped: int}|list<int>> $expected
     */
    #[DataProvider('matched_offsets')]
    public function test_an_offset_counts_the_rows_the_row_filter_keeps_and_is_announced_before_the_first_chunk(
        string $engineClass,
        int $offset,
        array $expected,
    ): void {
        $path = TestParquetFile::path($this);
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            3,
        );
        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED));
        (new Writer(options: $options, engine: new PhpParquetEngine(options: $options)))->write(
            $path,
            $schema,
            array_map(static fn(int $id): array => ['id' => $id], range(1, 9)),
        );

        static::assertSame($expected, array_map(
            /** @param array<string, list<mixed>>|SkippedMatches $chunk */
            static fn(array|SkippedMatches $chunk): array => (
                $chunk instanceof SkippedMatches ? ['skipped' => $chunk->count] : $chunk['id']
            ),
            iterator_to_array(
                ParquetEngineMother::create($engineClass)
                    ->openForRead(NativeLocalSourceStream::open(path_real($path)))
                    ->readColumns(
                        ['id'],
                        100,
                        ReadPlanMother::rowFiltered(9, parquet_column('id')->greaterThan(2), $schema, offset: $offset),
                    ),
                false,
            ),
        ));
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_a_row_filter_reads_a_column_the_read_does_not_return(string $engineClass): void
    {
        $path = TestParquetFile::path($this);
        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::int32('amount', Repetition::REQUIRED),
        );
        (new Writer())->write(
            $path,
            $schema,
            array_map(static fn(int $id): array => ['id' => $id, 'amount' => $id * 10], range(1, 5)),
        );

        static::assertSame(
            [['id' => [3, 4, 5]]],
            iterator_to_array(
                ParquetEngineMother::create($engineClass)
                    ->openForRead(NativeLocalSourceStream::open(path_real($path)))
                    ->readColumns(
                        ['id'],
                        100,
                        ReadPlanMother::rowFiltered(5, parquet_column('amount')->greaterThan(20), $schema),
                    ),
                false,
            ),
        );
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_reading_no_column_yields_at_most_one_chunk_without_a_column(string $engineClass): void
    {
        $path = TestParquetFile::path($this);
        (new Writer())->write(
            $path,
            Schema::with(FlatColumn::int32('id', Repetition::REQUIRED)),
            array_map(static fn(int $id): array => ['id' => $id], range(1, 5)),
        );

        $chunks = iterator_to_array(
            ParquetEngineMother::create($engineClass)
                ->openForRead(NativeLocalSourceStream::open(path_real($path)))
                ->readColumns([], 100, ReadPlanMother::window(5)),
            false,
        );

        static::assertLessThanOrEqual(1, count($chunks));
        static::assertSame(array_fill(0, count($chunks), []), $chunks);
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_stored_columns_hold_temporal_and_uuid_values_as_they_are_stored(string $engineClass): void
    {
        $path = TestParquetFile::path($this);
        $instant = new DateTimeImmutable('2009-02-13 23:31:30.123456', new DateTimeZone('UTC'));
        $beforeEpoch = new DateTimeImmutable('1969-12-31 23:59:59.999999', new DateTimeZone('UTC'));
        (new Writer())->write(
            $path,
            Schema::with(
                FlatColumn::date('d'),
                FlatColumn::dateTime('us'),
                new FlatColumn(
                    'ms',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(
                        LogicalType::TIMESTAMP,
                        timestamp: new Timestamp(true, TimeUnit::MILLISECONDS),
                    ),
                ),
                new FlatColumn(
                    'ns',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(
                        LogicalType::TIMESTAMP,
                        timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                    ),
                ),
                FlatColumn::time('t'),
                NestedColumn::list('instants', ListElement::datetime()),
                NestedColumn::struct('s', [FlatColumn::date('day')]),
                FlatColumn::uuid('u'),
                NestedColumn::list('uuids', ListElement::uuid()),
            ),
            [
                [
                    'd' => new DateTimeImmutable('2024-02-29', new DateTimeZone('UTC')),
                    'us' => $instant,
                    'ms' => $instant,
                    'ns' => $instant,
                    't' => (new DateTimeImmutable('@0'))->diff(new DateTimeImmutable('@3723.000004')),
                    'instants' => [$instant, $beforeEpoch],
                    's' => ['day' => new DateTimeImmutable('1969-12-31', new DateTimeZone('UTC'))],
                    'u' => '00112233-4455-6677-8899-aabbccddeeff',
                    'uuids' => ['00112233-4455-6677-8899-aabbccddeeff'],
                ],
                [
                    'd' => null,
                    'us' => null,
                    'ms' => null,
                    'ns' => null,
                    't' => null,
                    'instants' => null,
                    's' => null,
                    'u' => null,
                    'uuids' => null,
                ],
            ],
        );

        static::assertSame(
            [[
                'd' => [19_782, null],
                'us' => [1_234_567_890_123_456, null],
                'ms' => [1_234_567_890_123_000, null],
                'ns' => [1_234_567_890_123_456, null],
                't' => [3_723_000_004, null],
                'instants' => [[1_234_567_890_123_456, -1], null],
                's' => [['day' => -1], null],
                'u' => ["\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff", null],
                'uuids' => [["\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff"], null],
            ]],
            iterator_to_array(
                ParquetEngineMother::create($engineClass)
                    ->openForRead(NativeLocalSourceStream::open(path_real($path)))
                    ->readStoredColumns(
                        ['d', 'us', 'ms', 'ns', 't', 'instants', 's', 'u', 'uuids'],
                        100,
                        ReadPlanMother::window(2),
                    ),
                false,
            ),
        );
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_stored_int96_timestamps_hold_the_microseconds_their_datetimes_read_as(string $engineClass): void
    {
        $reader = ParquetEngineMother::create($engineClass, Options::default()->set(Option::INT_96_AS_DATETIME, true));
        $fixture = __DIR__ . '/../IO/Fixtures/EdgeCases/int96.parquet';
        $plan = ReadPlanMother::window(
            $reader
                ->openForRead(NativeLocalSourceStream::open(path_real($fixture)))
                ->footer()
                ->rowsNumber(),
        );
        /** @var list<array<string, list<?DateTimeInterface>>> $read */
        $read = iterator_to_array(
            $reader->openForRead(NativeLocalSourceStream::open(path_real($fixture)))->readColumns(['ts'], 100, $plan),
            false,
        );
        /** @var list<array<string, list<?int>>> $stored */
        $stored = iterator_to_array(
            $reader->openForRead(NativeLocalSourceStream::open(path_real($fixture)))->readStoredColumns(
                ['ts'],
                100,
                $plan,
            ),
            false,
        );

        static::assertSame(
            array_map(static fn(?DateTimeInterface $instant): ?int => $instant === null
                ? null
                : ($instant->getTimestamp() * 1_000_000) + (int) $instant->format('u'), $read[0]['ts']),
            $stored[0]['ts'],
        );
    }

    /**
     * @param int<1, max> $batchSize
     * @param list<int> $sizes
     * @param list<list<int>> $ids
     */
    #[DataProvider('chunkings')]
    public function test_chunks_follow_batch_size_limit_and_offset(
        string $engineClass,
        int $batchSize,
        ?int $limit,
        ?int $offset,
        array $sizes,
        array $ids,
    ): void {
        $engine = ParquetEngineMother::create($engineClass);
        $path = TestParquetFile::path($this);

        (new Writer())->write(
            $path,
            Schema::with(
                FlatColumn::int32('id', Repetition::REQUIRED),
                NestedColumn::list('tags', ListElement::string()),
            ),
            array_map(static fn(int $i): array => ['id' => $i, 'tags' => ['t' . $i]], [1, 2, 3, 4, 5]),
        );

        /** @var list<array<string, list<mixed>>> $chunks */
        $chunks = iterator_to_array(
            $engine->openForRead(NativeLocalSourceStream::open(path_real($path)))->readColumns(
                ['id', 'tags'],
                $batchSize,
                ReadPlanMother::window(5, $offset ?? 0, $limit),
            ),
            false,
        );

        static::assertSame($sizes, array_map(static fn(array $chunk): int => count($chunk['id']), $chunks));
        static::assertSame($ids, array_map(static fn(array $chunk): array => $chunk['id'], $chunks));
        static::assertSame(
            array_map(static fn(array $chunk): array => array_map(static fn(int $i): array => [
                't' . $i,
            ], $chunk), $ids),
            array_map(static fn(array $chunk): array => $chunk['tags'], $chunks),
        );
    }
}
