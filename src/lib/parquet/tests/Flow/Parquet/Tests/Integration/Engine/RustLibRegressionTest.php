<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

use function array_column;
use function array_map;
use function file_get_contents;
use function filesize;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;
use function range;
use function strlen;
use function substr;
use function unpack;
use function var_export;

#[Group('native-extension')]
#[RequiresPhpExtension('arrow')]
final class RustLibRegressionTest extends TestCase
{
    public const string PAGINATION = __DIR__ . '/../IO/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

    public function test_an_offset_read_reads_only_the_footer_and_the_row_group_it_needs(): void
    {
        $bytes = (string) file_get_contents(self::PAGINATION);
        $footer = (int) unpack('V', substr($bytes, -8, 4))[1];
        $group = 0;

        foreach (Reader::php()->read(self::PAGINATION)->metadata()->rowGroups()->all()[49]->columnChunks() as $chunk) {
            $group += $chunk->totalCompressedSize();
        }

        $stream = new ReadCountingSourceStream(NativeLocalSourceStream::open(path_real(self::PAGINATION)));
        $rows = iterator_to_array(
            (new Reader(engine: new RustParquetEngine()))
                ->readStream($stream)
                ->values([], 10, 4990),
            false,
        );

        static::assertCount(10, $rows);
        static::assertLessThanOrEqual($footer + 8 + $group, $stream->bytes);
    }

    public function test_a_full_read_reads_the_file_about_once(): void
    {
        $stream = new ReadCountingSourceStream(NativeLocalSourceStream::open(path_real(self::PAGINATION)));

        static::assertCount(5000, iterator_to_array(
            (new Reader(engine: new RustParquetEngine()))
                ->readStream($stream)
                ->values(),
            false,
        ));
        static::assertLessThanOrEqual((int) filesize(self::PAGINATION) + (64 * 1024), $stream->bytes);
    }

    public function test_invalid_utf8_is_refused_naming_column_and_row(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Parquet column "name" row 1 holds a string that is not valid UTF-8');

        MemoryParquetFile::written(
            new Writer(engine: new RustParquetEngine()),
            Schema::with(FlatColumn::string('name')),
            [
                ['name' => 'ok'],
                ['name' => "\xFF\xFE"],
            ],
        );
    }

    public function test_a_time_of_a_day_and_an_hour_keeps_its_day(): void
    {
        $interval = (new DateTimeImmutable('2024-01-01 00:00:00'))->diff(new DateTimeImmutable('2024-01-02 01:00:00'));
        $bytes = MemoryParquetFile::written(
            new Writer(engine: new RustParquetEngine()),
            Schema::with(FlatColumn::time('t')),
            [['t' => $interval]],
        );
        static::assertSame(
            var_export($interval, true),
            var_export(
                ParquetRows::read(
                    MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes),
                    10,
                )[0]['t'],
                true,
            ),
        );
    }

    public function test_int96_reads_exactly_outside_the_nanosecond_range(): void
    {
        $values = array_column(
            ParquetRows::read(
                (new Reader(engine: new RustParquetEngine()))->read(__DIR__
                . '/../IO/Fixtures/EdgeCases/int96.parquet'),
                10,
            ),
            'ts',
        );

        static::assertEquals(new DateTimeImmutable('0001-01-01 00:00:00.000001', new DateTimeZone('UTC')), $values[3]);
        static::assertEquals(new DateTimeImmutable('1960-01-01 23:59:59.999999', new DateTimeZone('UTC')), $values[2]);
    }

    public function test_dates_are_utc_and_times_carry_their_day_count(): void
    {
        $bytes = MemoryParquetFile::written(
            Writer::php(),
            Schema::with(FlatColumn::date('d'), FlatColumn::time('t')),
            [[
                'd' => new DateTimeImmutable('2024-02-29'),
                't' => (new DateTimeImmutable('@0'))->diff(new DateTimeImmutable('@90000')),
            ]],
        );

        static::assertSame(
            var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $bytes), 1), true),
            var_export(
                ParquetRows::read(MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes), 1),
                true,
            ),
        );
        static::assertStringContainsString("'timezone' => 'UTC'", var_export(
            ParquetRows::read(MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes), 1)[0]['d'],
            true,
        ));
    }

    public function test_a_logical_only_unsigned_integer_reads_as_itself_through_both_engines(): void
    {
        $bytes = MemoryParquetFile::written(
            Writer::php(),
            Schema::with(new FlatColumn('u', PhysicalType::INT32, logicalType: LogicalType::integer(32, false))),
            [['u' => 4_000_000_000]],
        );

        static::assertSame(
            [['u' => 4_000_000_000]],
            ParquetRows::read(MemoryParquetFile::read(Reader::php(), $bytes), 1),
        );
        static::assertSame(
            [['u' => 4_000_000_000]],
            ParquetRows::read(MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes), 1),
        );
    }

    public function test_a_schema_read_from_an_arrow_rs_file_rewritten_by_the_php_engine_opens_in_arrow(): void
    {
        $arrow = MemoryParquetFile::written(
            new Writer(engine: new RustParquetEngine()),
            Schema::with(
                new FlatColumn('i8', PhysicalType::INT32, ConvertedType::INT_8),
                new FlatColumn('u32', PhysicalType::INT32, ConvertedType::UINT_32),
            ),
            [['i8' => -5, 'u32' => 4_000_000_000]],
        );
        $schema = MemoryParquetFile::read(Reader::php(), $arrow)->schema();
        $rewritten = MemoryParquetFile::written(Writer::php(), $schema, [['i8' => -5, 'u32' => 4_000_000_000]]);

        static::assertSame(8, $schema->get('i8')->logicalType()?->integerData()?->bitWidth());
        static::assertGreaterThan(0, strlen($rewritten));
        static::assertSame(
            [['i8' => -5, 'u32' => 4_000_000_000]],
            ParquetRows::read(MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $rewritten), 1),
        );
    }

    public function test_times_of_day_and_longer_read_as_the_php_engine_reads_them(): void
    {
        $base = new DateTimeImmutable('1970-01-01 00:00:00.000000', new DateTimeZone('UTC'));
        $rows = array_map(static fn(int $micros): array => [
            't' => $base->diff($base->modify("+{$micros} microseconds")),
        ], [
            0,
            1,
            999_999,
            13_298_519_488,
            45_296_789_012,
            86_399_999_999,
            86_400_000_000,
            90_000_000_001,
            ...range(1, 86_399_999_999, 172_799_999),
        ]);
        $bytes = MemoryParquetFile::written(Writer::php(), Schema::with(FlatColumn::time('t')), $rows);

        static::assertSame(
            var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $bytes), 1_000), true),
            var_export(
                ParquetRows::read(MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes), 1_000),
                true,
            ),
        );
    }
}
