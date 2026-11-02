<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetSourceFileOpener;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetFileWriter;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetSchemaConformance;
use Flow\ETL\Adapter\Parquet\IO\Loader\RustParquetFileWriter;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Flow;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoSource;
use Flow\ETL\Tests\Double\ClosingSpySourceStream;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema as FlowSchema;
use Flow\Filesystem\Filesystem;
use Flow\Parquet\Engine\Arrow\OptionsConverter;
use Flow\Parquet\Engine\Arrow\SchemaConverter as ArrowSchemaConverter;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetFileWriter as ArrowRustParquetFileWriter;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter as ParquetFilter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Writer;
use Flow\Parquet\Writer\StatisticsLevel;
use Throwable;

use function array_count_values;
use function array_keys;
use function array_map;
use function array_sum;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function json_encode;
use function range;

use const JSON_THROW_ON_ERROR;

final class ParquetFilesContext
{
    public static function plan(
        Filesystem $filesystem,
        string $uri,
        int $offset = 0,
        ?int $limit = null,
        ?ParquetFilter $filter = null,
    ): ReadPlan {
        $file = (new Reader())->readStream($filesystem->readFrom(path($uri)));

        return ReadPlan::forFilter(
            $file->reader()->footer(),
            $file->indexes(),
            $filter === null ? null : BoundFilter::bind($filter, $file->schema(), $file->options()),
            $offset,
            $limit,
        );
    }

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

    // the PHP engine, because only it honours a row group size of half a kilobyte (90 rows)
    public static function writeRowGroups(Filesystem $filesystem, string $uri, int $rows): void
    {
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1024)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            10,
        );

        data_frame()
            ->read(from_sequence_number('id', 1, $rows))
            ->write(
                to_parquet(path($uri), filesystem: $filesystem)
                    ->withOptions($options)
                    ->withEngine(new PhpParquetEngine(options: $options)),
            )
            ->run();
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
     * @param iterable<Filter|Rows|SkippedRows> $batches
     *
     * @return list<Rows>
     */
    public static function rows(iterable $batches): array
    {
        $rows = [];

        foreach ($batches as $batch) {
            if ($batch instanceof Rows) {
                $rows[] = $batch;
            }
        }

        return $rows;
    }

    /**
     * @param iterable<Filter|Rows|SkippedRows> $batches
     *
     * @return list<int>
     */
    public static function skippedRows(iterable $batches): array
    {
        $skipped = [];

        foreach ($batches as $batch) {
            if ($batch instanceof SkippedRows) {
                $skipped[] = $batch->count;
            }
        }

        return $skipped;
    }

    /**
     * @param iterable<Filter|Rows|SkippedRows> $batches
     */
    public static function yieldedFilter(iterable $batches): ?Filter
    {
        foreach ($batches as $batch) {
            return $batch instanceof Filter ? $batch : null;
        }

        return null;
    }

    /**
     * @param list<float> $scores
     */
    public static function scores(
        Filesystem $filesystem,
        string $uri,
        array $scores,
        StatisticsLevel $statistics = StatisticsLevel::PAGE,
    ): void {
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            3,
        )->set(Option::STATISTICS_LEVEL, $statistics);
        $writer = new Writer(options: $options, engine: new PhpParquetEngine(options: $options));
        $writer->openForStream(
            $filesystem->writeTo(path($uri)),
            Schema::with(FlatColumn::int64('id'), FlatColumn::double('score')),
        );
        $writer->writeBatch(array_map(
            static fn(int $id, float $score): array => ['id' => $id, 'score' => $score],
            array_keys($scores),
            $scores,
        ));
        $writer->close();
    }

    /**
     * @return list<array<array-key, mixed>> the rows written through the loader's ParquetFileWriter, read back by the PHP engine
     */
    public static function writtenThroughTheLoader(Rows $rows): array
    {
        $filesystem = memory_filesystem();
        $parquetSchema = (new SchemaConverter())->toParquetSchema($rows->schema());
        $writer = new ParquetFileWriter(
            (new PhpParquetEngine())->openForWrite(
                $filesystem->writeTo(path('memory://loader.parquet')),
                $parquetSchema,
                Compressions::SNAPPY,
                Options::default(),
            ),
            new ParquetSchemaConformance($parquetSchema),
            new PhpBackend(),
        );
        $writer->write($rows);
        $writer->close();

        return self::phpEngineValues($filesystem, 'memory://loader.parquet');
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

    public static function nativeWriter(Filesystem $filesystem, string $uri, FlowSchema $schema): RustParquetFileWriter
    {
        $parquetSchema = (new SchemaConverter())->toParquetSchema($schema);

        return new RustParquetFileWriter(
            new ArrowRustParquetFileWriter(
                $filesystem->writeTo(path($uri)),
                ArrowSchemaConverter::toExtension($parquetSchema),
                Compressions::SNAPPY,
                OptionsConverter::toExtension(Options::default()),
                Options::default()->getInt(Option::ARROW_WRITE_BATCH_SIZE),
            ),
            new ParquetSchemaConformance($parquetSchema),
            new PhpBackend(),
        );
    }

    /**
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
     * @return array<string, int>
     */
    public static function opensPerFile(CountingFilesystem $filesystem): array
    {
        return array_count_values(array_map(static fn(ClosingSpySourceStream $stream): string => $stream
            ->path()
            ->path(), $filesystem->openedStreams));
    }

    public static function bytesRead(CountingFilesystem $filesystem): int
    {
        return array_sum(array_map(
            static fn(ClosingSpySourceStream $stream): int => $stream->readBytes,
            $filesystem->openedStreams,
        ));
    }

    /**
     * @param array<string, array<string, list<int>>> $groups
     *
     * @return list<ColumnChunk>
     */
    public static function chunks(Filesystem $filesystem, array $groups): array
    {
        $chunks = [];

        foreach ($groups as $uri => $columns) {
            $file = Reader::php()->readStream($filesystem->readFrom(path($uri)));
            $rowGroups = $file->metadata()->rowGroups()->all();

            foreach ($columns as $column => $indexes) {
                foreach ($indexes as $index) {
                    $chunks[] = $rowGroups[$index]->getColumnChunk($file->schema()->getFlat($column));
                }
            }
        }

        return $chunks;
    }

    /**
     * @param array<string, array<string, list<int>>> $groups
     */
    public static function offsetIndexBytes(Filesystem $filesystem, array $groups): int
    {
        return array_sum(array_map(
            static fn(ColumnChunk $chunk): int => (int) $chunk->offsetIndex()?->length,
            self::chunks($filesystem, $groups),
        ));
    }

    /**
     * @param array<string, array<string, list<int>>> $groups
     */
    public static function pageIndexBytes(Filesystem $filesystem, array $groups): int
    {
        return array_sum(array_map(
            static fn(ColumnChunk $chunk): int => (
                (int) $chunk->columnIndex()?->length + (int) $chunk->offsetIndex()?->length
            ),
            self::chunks($filesystem, $groups),
        ));
    }

    public static function opener(Filesystem $filesystem): ParquetSourceFileOpener
    {
        return new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );
    }

    /**
     * @param iterable<Filter|Rows|SkippedRows> $batches
     */
    public static function rowCount(iterable $batches): int
    {
        $count = 0;

        foreach ($batches as $rows) {
            if ($rows instanceof Rows) {
                $count += $rows->count();
            }
        }

        return $count;
    }

    /**
     * @param list<int|string> $values
     */
    public static function unsignedInt64(Filesystem $filesystem, string $uri, array $values): void
    {
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            1,
        );
        $writer = new Writer(options: $options, engine: new PhpParquetEngine(options: $options));
        $writer->openForStream(
            $filesystem->writeTo(path($uri)),
            Schema::with(new FlatColumn('big', PhysicalType::INT64, logicalType: LogicalType::integer(64, false))),
        );
        $writer->writeBatch(array_map(static fn(int|string $value): array => ['big' => $value], $values));
        $writer->close();
    }

    public static function orders(Filesystem $filesystem, string $uri): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'country' => 'PL', 'amount' => 10, 'note' => 'a'],
                ['id' => 2, 'country' => 'DE', 'amount' => 20, 'note' => 'b'],
                ['id' => 3, 'country' => 'PL', 'amount' => 30, 'note' => 'c'],
            ]))
            ->write(to_parquet(path($uri), filesystem: $filesystem))
            ->run();
    }

    public static function withoutProjectionPushdown(): Flow
    {
        return df(config_builder()->optimizer(Optimizer::default()->without(PushProjectionIntoSource::class)));
    }

    /**
     * @param callable(): array<array-key, mixed> $read
     */
    public static function outcome(callable $read): string
    {
        try {
            return 'returned ' . json_encode($read(), JSON_THROW_ON_ERROR);
        } catch (Throwable $e) {
            return $e::class . ': ' . $e->getMessage();
        }
    }
}
