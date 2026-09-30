<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\Native\NativeParquetFile;
use Flow\Parquet\Engine\NativeParquetFileReader;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\ParquetRows;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;

#[Group('native-extension')]
#[RequiresPhpExtension('flow_php')]
final class NativeParquetFileReaderTest extends TestCase
{
    public const string FIXTURES = __DIR__ . '/../IO/Fixtures';

    public function test_schema_rows_and_bytes_agree_with_the_metadata(): void
    {
        $open = static fn() => new NativeParquetFileReader(
            new NativeParquetFile(NativeLocalSourceStream::open(path_real(self::FIXTURES . '/structs.parquet'))),
            new Options(),
        );
        $metadata = $open()->metadata();
        $bytes = 0;

        foreach ($metadata->rowGroups()->all() as $rowGroup) {
            $bytes += $rowGroup->totalByteSize();
        }

        static::assertEquals($metadata->schema(), $open()->schema());
        static::assertSame($metadata->rowsNumber(), $open()->rowsNumber());
        static::assertSame($bytes, $open()->totalByteSize());
        static::assertEquals(Reader::php()->read(self::FIXTURES . '/structs.parquet')->metadata(), $metadata);
    }

    public function test_every_call_after_close_throws(): void
    {
        $reader = new NativeParquetFileReader(
            new NativeParquetFile(NativeLocalSourceStream::open(path_real(self::FIXTURES . '/structs.parquet'))),
            new Options(),
        );
        $reader->metadata();
        $reader->close();

        foreach ([
            static fn() => $reader->metadata(),
            static fn() => $reader->schema(),
            static fn() => $reader->rowsNumber(),
            static fn() => $reader->totalByteSize(),
            static fn() => $reader->file(),
            static fn() => iterator_to_array($reader->readColumns(['struct_flat'], 1, null, null)),
            static fn() => $reader->close(),
        ] as $call) {
            try {
                $call();
                static::fail('A call on a closed reader must throw');
            } catch (RuntimeException $exception) {
                static::assertSame('Reader is not open', $exception->getMessage());
            }
        }
    }

    public function test_int96_is_refused_when_it_is_not_read_as_a_datetime(): void
    {
        $options = (new Options())->set(Option::INT_96_AS_DATETIME, false);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Parquet column "ts" holds INT96, which flow_php reads only as a datetime (Option::INT_96_AS_DATETIME = '
            . 'true); read the file with \Flow\Parquet\Reader::php()',
        );

        ParquetRows::read(Reader::arrow($options)->read(self::FIXTURES . '/EdgeCases/int96.parquet'), 10);
    }

    public function test_a_struct_path_reads_the_values_of_the_php_engine(): void
    {
        static::assertSame(
            ParquetRows::read(Reader::php()->read(self::FIXTURES . '/structs.parquet'), 30, ['struct_flat.int']),
            ParquetRows::read(Reader::arrow()->read(self::FIXTURES . '/structs.parquet'), 30, ['struct_flat.int']),
        );
    }

    public function test_a_path_into_a_list_is_refused_naming_the_php_reader(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Parquet column "list.list.element" (a path into LIST/MAP) is not supported by the flow_php Parquet '
            . 'reader; read the file with \Flow\Parquet\Reader::php()',
        );

        ParquetRows::read(Reader::arrow()->read(self::FIXTURES . '/lists.parquet'), 10, ['list.list.element']);
    }

    public function test_a_file_that_is_not_parquet_is_refused_when_it_is_opened(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given file is not valid Parquet file: Invalid Parquet file. Corrupt footer');

        Reader::arrow()->readStream(new MemorySourceStream('{"not": "parquet"}'));
    }
}
