<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\ColumnDoor;
use Flow\Parquet\Tests\Context\EveryType;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Context\ParquetRows;
use Flow\Parquet\Tests\Context\WriteAcceptance;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Throwable;

use function var_export;

#[Group('native-extension')]
#[RequiresPhpExtension('arrow')]
final class NativeLibWriteParityTest extends TestCase
{
    /**
     * @return Generator<string, array{string, string}>
     */
    public static function cells(): Generator
    {
        foreach (WriteAcceptance::targets() as $target) {
            foreach (WriteAcceptance::inputs() as $input) {
                yield "{$target} <- {$input}" => [$target, $input];
            }
        }
    }

    #[DataProvider('cells')]
    public function test_both_writers_accept_and_refuse_the_same(string $target, string $input): void
    {
        $written = [];

        foreach (['php' => Writer::php(), 'native' => Writer::arrow()] as $engine => $writer) {
            try {
                $written[$engine] = MemoryParquetFile::written(
                    $writer,
                    Schema::with(WriteAcceptance::target($target)),
                    [['c' => WriteAcceptance::input($input)]],
                );
            } catch (Throwable) {
                $written[$engine] = null;
            }
        }

        static::assertSame($written['php'] === null, $written['native'] === null);

        if ($written['php'] !== null && $written['native'] !== null) {
            static::assertSame(
                var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $written['php']), 1), true),
                var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $written['native']), 1), true),
            );
        }
    }

    #[DataProvider('cells')]
    public function test_the_column_door_accepts_and_refuses_what_the_row_door_does(string $target, string $input): void
    {
        foreach ([new PhpParquetEngine(), new ArrowParquetEngine()] as $engine) {
            $outcomes = [];

            foreach (['batch', 'columns'] as $door) {
                $stream = ColumnDoor::stream();

                try {
                    $file = ColumnDoor::open($engine, $stream, Schema::with(WriteAcceptance::target($target)));
                    $door === 'batch'
                        ? $file->writeBatch([['c' => WriteAcceptance::input($input)]])
                        : $file->writeColumns(['c' => [WriteAcceptance::input($input)]]);
                    $file->close();
                    $outcomes[$door] = var_export(ColumnDoor::read($stream), true);
                } catch (Throwable $refusal) {
                    $outcomes[$door] = $refusal::class . ': ' . $refusal->getMessage();
                }
            }

            static::assertSame($outcomes['batch'], $outcomes['columns'], $engine::class);
        }
    }

    /**
     * The values PhpParquetEngine used to write wrongly (#27, #28, #16).
     *
     * @return Generator<string, array{string, string}>
     */
    public static function wrongly_written(): Generator
    {
        yield 'INT32 out of range' => ['INT32', 'int 4000000000'];
        yield 'INT_8 out of range' => ['INT32 INT_8', 'int PHP_INT_MAX'];
        yield 'UINT_32 negative' => ['INT32 UINT_32', 'int -1'];
        yield 'UINT_64 negative' => ['INT64 UINT_64', 'int -1'];
        yield 'UUID of 16 raw bytes' => ['UUID', 'string 16 bytes'];
        yield 'FIXED(4) of 5 bytes' => ['FIXED(4)', 'string 5 bytes'];
        yield 'FIXED(4) of 36 bytes' => ['FIXED(4)', 'string 36 uuid'];
        yield 'invalid UTF-8 into STRING' => ['STRING', 'string \xFF\xFE'];
        yield 'invalid UTF-8 into JSON' => ['JSON', 'string \xFF\xFE'];
        yield 'invalid UTF-8 into ENUM' => ['ENUM', 'string \xFF\xFE'];
    }

    #[DataProvider('wrongly_written')]
    public function test_the_php_engine_refuses_what_it_used_to_write_wrongly(string $target, string $input): void
    {
        $this->expectException(ValidationException::class);
        $this->expectExceptionMessage('Column "c" row 0: ');

        MemoryParquetFile::written(Writer::php(), Schema::with(WriteAcceptance::target($target)), [[
            'c' => WriteAcceptance::input($input),
        ]]);
    }

    public function test_every_type_writes_the_same_through_both_writers(): void
    {
        $php = MemoryParquetFile::written(Writer::php(), EveryType::schema(), EveryType::rows());
        $native = MemoryParquetFile::written(Writer::arrow(), EveryType::schema(), EveryType::rows());

        static::assertSame(
            var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $php), 3), true),
            var_export(ParquetRows::read(MemoryParquetFile::read(Reader::php(), $native), 3), true),
        );
    }
}
