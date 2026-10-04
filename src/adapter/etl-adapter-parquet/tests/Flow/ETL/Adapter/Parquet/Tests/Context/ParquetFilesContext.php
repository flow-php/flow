<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Adapter\Parquet\ParquetOpenSink;
use Flow\ETL\Adapter\Parquet\RustParquetOpenSink;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Rows;
use Flow\ETL\Schema as FlowSchema;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\Filesystem\Filesystem;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter as ArrowSchemaConverter;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

use function array_count_values;
use function array_map;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function range;

final class ParquetFilesContext
{
    /**
     * @return array<string>
     */
    public static function columnNames(Filesystem $filesystem, string $uri): array
    {
        return array_map(
            static fn(Column $column): string => $column->name(),
            (new Reader())
                ->readStream($filesystem->readFrom(path($uri)))
                ->schema()
                ->columns(),
        );
    }

    /**
     * @return list<array<array-key, mixed>>
     */
    public static function values(Filesystem $filesystem, string $uri): array
    {
        return iterator_to_array(
            (new Reader())
                ->readStream($filesystem->readFrom(path($uri)))
                ->values(),
            false,
        );
    }

    /**
     * @param array<string, Rows> $files - uri => the rows written to it
     */
    public static function write(Filesystem $filesystem, array $files): void
    {
        foreach ($files as $uri => $rows) {
            data_frame()
                ->read(from_rows($rows))
                ->write(to_parquet(path($uri), filesystem: $filesystem))
                ->run();
        }
    }

    /**
     * `id` 0..$rows-1 and `name` "n<id>", one row group per $rowsPerGroup rows.
     */
    public static function rowGroups(Filesystem $filesystem, string $uri, int $rows, int $rowsPerGroup): void
    {
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            $rowsPerGroup,
        );
        $writer = new Writer(options: $options, engine: new PhpParquetEngine(options: $options));
        $writer->openForStream(
            $filesystem->writeTo(path($uri)),
            Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
        );
        $writer->writeBatch(array_map(
            static fn(int $id): array => ['id' => $id, 'name' => 'n' . $id],
            range(0, $rows - 1),
        ));
        $writer->close();
    }

    /**
     * @return list<array<array-key, mixed>>
     */
    public static function phpEngineValues(Filesystem $filesystem, string $uri): array
    {
        return iterator_to_array(
            (new Reader(engine: new PhpParquetEngine()))
                ->readStream($filesystem->readFrom(path($uri)))
                ->values(),
            false,
        );
    }

    public static function nativeSink(Filesystem $filesystem, string $uri, FlowSchema $schema): ParquetOpenSink
    {
        return new RustParquetOpenSink(
            new RustParquetFileWriter(
                $filesystem->writeTo(path($uri)),
                ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
                Compressions::SNAPPY,
                OptionsConverter::toExtension(Options::default()),
                Options::default()->getInt(Option::ARROW_WRITE_BATCH_SIZE),
            ),
        );
    }

    /**
     * An int64 `id` column written by PhpParquetEngine with DELTA_BINARY_PACKED.
     *
     * @param list<int> $ids
     */
    public static function deltaEncoded(Filesystem $filesystem, string $uri, array $ids): void
    {
        $options = Options::default()->set(Option::COLUMNS_ENCODINGS, ['id' => Encodings::DELTA_BINARY_PACKED]);
        $writer = new Writer(options: $options, engine: new PhpParquetEngine(options: $options));
        $writer->openForStream($filesystem->writeTo(path($uri)), Schema::with(FlatColumn::int64('id')));
        $writer->writeBatch(array_map(static fn(int $id): array => ['id' => $id], $ids));
        $writer->close();
    }

    /**
     * How often each path was opened through the filesystem, by path.
     *
     * @return array<string, int>
     */
    public static function opensPerFile(CountingFilesystem $filesystem): array
    {
        return array_count_values(array_map(static fn(ClosingSpySourceStream $stream): string => $stream
            ->path()
            ->path(), $filesystem->openedStreams));
    }
}
