<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\EveryType;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function basename;
use function glob;
use function in_array;
use function str_starts_with;
use function var_export;

#[Group('native-extension')]
#[RequiresPhpExtension('arrow')]
final class RustLibParityTest extends TestCase
{
    /**
     * The 5000-row pagination fixture is read through a window crossing 5 row groups, skipping inside the first.
     *
     * @return Generator<string, array{string, int<1, max>, ?int, ?int}>
     */
    public static function fixtures(): Generator
    {
        $fixtures = __DIR__ . '/../IO/Fixtures';

        foreach ([
            ...(glob($fixtures . '/*.parquet') ?: []),
            ...(glob($fixtures . '/EdgeCases/*.parquet') ?: []),
        ] as $path) {
            if (in_array(basename($path), ['interval.parquet', 'uint64_overflow.parquet', 'map_keys.parquet'], true)) {
                continue;
            }

            [$limit, $offset] = str_starts_with(basename($path), 'pagination') ? [400, 4550] : [null, null];

            foreach ([100, 10_000] as $batchSize) {
                yield basename($path) . " batch {$batchSize}" => [$path, $batchSize, $limit, $offset];
            }
        }
    }

    /**
     * @param int<1, max> $batchSize
     */
    #[DataProvider('fixtures')]
    public function test_a_fixture_reads_the_values_of_the_php_engine(
        string $path,
        int $batchSize,
        ?int $limit,
        ?int $offset,
    ): void {
        static::assertSame(
            var_export(ParquetRows::read(Reader::php()->read($path), $batchSize, [], $limit, $offset), true),
            var_export(
                ParquetRows::read(
                    (new Reader(engine: new RustParquetEngine()))->read($path),
                    $batchSize,
                    [],
                    $limit,
                    $offset,
                ),
                true,
            ),
        );
    }

    public function test_every_type_reads_the_values_of_the_php_engine(): void
    {
        $bytes = MemoryParquetFile::written(Writer::php(), EveryType::schema(), EveryType::rows());

        static::assertSame(
            var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $bytes), 2), true),
            var_export(
                ParquetRows::read(MemoryParquetFile::read(new Reader(engine: new RustParquetEngine()), $bytes), 2),
                true,
            ),
        );
    }

    public function test_an_interval_column_is_refused_naming_it_and_the_php_reader(): void
    {
        $path = __DIR__ . '/../IO/Fixtures/EdgeCases/interval.parquet';

        static::assertSame(
            ParquetRows::read(Reader::php()->read($path), 100, ['id']),
            ParquetRows::read((new Reader(engine: new RustParquetEngine()))->read($path), 100, ['id']),
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Parquet column "iv" (Interval(DayTime)) is not supported by the arrow Parquet reader; read the file '
            . 'with \Flow\Parquet\Reader::php()',
        );

        ParquetRows::read((new Reader(engine: new RustParquetEngine()))->read($path), 100);
    }

    public function test_a_uint64_above_php_int_max_is_refused_by_both_engines(): void
    {
        $path = __DIR__ . '/../IO/Fixtures/EdgeCases/uint64_overflow.parquet';

        try {
            ParquetRows::read(Reader::php()->read($path), 100);
            static::fail('PhpParquetEngine must refuse a UINT_64 above PHP_INT_MAX');
        } catch (DataConversionException $exception) {
            static::assertStringContainsString('holds a UINT_64 value above PHP_INT_MAX', $exception->getMessage());
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Parquet column "u64" row 0 holds a value out of the range arrow stores it in');

        ParquetRows::read((new Reader(engine: new RustParquetEngine()))->read($path), 100);
    }

    #[TestWith(['double_key', 'float'])]
    #[TestWith(['date_key', 'DateTimeImmutable'])]
    #[TestWith(['bool_key', 'bool'])]
    public function test_a_map_key_a_php_array_cannot_hold_is_refused_by_both_engines(
        string $column,
        string $type,
    ): void {
        $path = __DIR__ . '/../IO/Fixtures/EdgeCases/map_keys.parquet';

        static::assertSame(
            ParquetRows::read(Reader::php()->read($path), 10, ['id']),
            ParquetRows::read((new Reader(engine: new RustParquetEngine()))->read($path), 10, ['id']),
        );

        try {
            ParquetRows::read(Reader::php()->read($path), 10, [$column]);
            static::fail('PhpParquetEngine must refuse the map key');
        } catch (InvalidArgumentException $exception) {
            static::assertSame("Map key must be int or string, got {$type}", $exception->getMessage());
        }

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Map key of Parquet column \"{$column}\" must be int or string, got {$type}");

        ParquetRows::read((new Reader(engine: new RustParquetEngine()))->read($path), 10, [$column]);
    }
}
