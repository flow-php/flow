<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Double\SpyParquetFileReader;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class ParquetFileTest extends TestCase
{
    public function test_close_closes_the_reader_once(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')));
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        $file->close();
        $file->close();
        unset($file);

        static::assertSame(1, $reader->closes);
    }

    public function test_dropping_the_file_closes_the_reader(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id')));
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        unset($file);

        static::assertSame(1, $reader->closes);
    }

    public function test_metadata_schema_and_reader_come_from_the_reader(): void
    {
        $schema = Schema::with(FlatColumn::int64('id'));
        $reader = new SpyParquetFileReader($schema);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame($reader, $file->reader());
        static::assertSame($schema, $file->schema());
        static::assertSame($schema, $file->metadata()->schema());
    }

    public function test_columns_reads_the_asked_columns_through_the_reader(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')), [[
            'name' => ['a'],
            'id' => [1],
        ]]);
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        static::assertSame(
            [['name' => ['a'], 'id' => [1]]],
            iterator_to_array($file->columns(10, [3 => 'name', 7 => 'id'], 5, 2), false),
        );
        static::assertSame([[['name', 'id'], 10, 5, 2]], $reader->reads);
    }

    public function test_columns_without_names_reads_every_root_column(): void
    {
        $reader = new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')));
        $file = new ParquetFile(new MemorySourceStream('PAR1'), new Options(), $reader);

        iterator_to_array($file->columns(10));

        static::assertSame([[['id', 'name'], 10, null, null]], $reader->reads);
    }

    public function test_columns_refuses_a_column_the_schema_lacks(): void
    {
        $file = new ParquetFile(
            new MemorySourceStream('PAR1'),
            new Options(),
            new SpyParquetFileReader(Schema::with(FlatColumn::int64('id'))),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "missing" does not exist');

        iterator_to_array($file->columns(10, ['missing']));
    }
}
