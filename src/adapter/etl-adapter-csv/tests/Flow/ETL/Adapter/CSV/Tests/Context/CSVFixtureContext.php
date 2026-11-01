<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Context;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVDecoder;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVFileBatches;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVLineReader;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVOpenSource;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\IO\Extractor\PhpCSVOpenSource;
use Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource;
use Flow\ETL\Adapter\CSV\IO\Inference\CSVSniffer;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\IO\Inference\SniffedColumns;
use Flow\ETL\Tests\Double\FakeSniffer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\SourceStream;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Generator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function array_values;
use function Flow\ETL\Adapter\CSV\csv_detect_separator;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function in_array;
use function iterator_to_array;
use function range;
use function serialize;
use function sprintf;
use function str_ends_with;
use function str_repeat;
use function strlen;
use function substr;

/**
 * Resolves the adapter's test fixtures and builds readers over them, so no test needs a private helper.
 */
final class CSVFixtureContext
{
    /**
     * A sniffer over every file a glob under Fixtures/ lists, in listing order, each source carrying its listed size.
     */
    public static function globSniffer(
        string $glob,
        SchemaInference $inference = new SchemaInference(),
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVSniffer {
        $sources = [];

        foreach ((new FileListing($filesystem))->list(path(self::path($glob)), new OnlyFiles()) as $status) {
            $sources[] = new SourceFile($status->path, $status->size);
        }

        return new CSVSniffer(new CSVSourceOpener($filesystem, $options), $sources, $inference->candidates());
    }

    public static function open(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVOpenSource {
        return (new CSVSourceOpener($filesystem, $options))->open(self::source($fixture));
    }

    /**
     * Every *.csv under Fixtures/, keyed and valued by its path relative to Fixtures/.
     *
     * @return Generator<string, array{string}>
     */
    public static function fixtures(): Generator
    {
        $root = self::path('');

        /** @var SplFileInfo $file */
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(
            $root,
            RecursiveDirectoryIterator::SKIP_DOTS,
        )) as $file) {
            if (str_ends_with($file->getFilename(), '.csv')) {
                $fixture = substr($file->getPathname(), strlen($root));

                yield $fixture => [$fixture];
            }
        }
    }

    /**
     * fixtures() without the ones holding a corrupted record, which a read refuses.
     *
     * @return Generator<string, array{string}>
     */
    public static function wellFormedFixtures(): Generator
    {
        foreach (self::fixtures() as $name => $fixture) {
            if (!in_array(
                $name,
                [
                    'corrupted_row.csv',
                    'file_with_empty_columns.csv',
                    'file_with_empty_headers.csv',
                    'more_columns_than_headers.csv',
                    'more_headers_than_columns.csv',
                    'ragged.csv',
                    'single_quotes_csv.csv',
                ],
                true,
            )) {
                yield $name => $fixture;
            }
        }
    }

    /**
     * The dialect CSVSourceOpener resolves: pinned options first, detection for the rest.
     */
    public static function dialect(SourceStream $stream, CSVReadOptions $options = new CSVReadOptions()): CSVDialect
    {
        $detected = csv_detect_separator($stream);

        return new CSVDialect(
            $options->separator ?? $detected->separator,
            $options->enclosure ?? $detected->enclosure,
            $options->escape ?? $detected->escape,
        );
    }

    /**
     * Requires the extension.
     */
    public static function openNative(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): RustCSVOpenSource {
        return self::openNativeStream($filesystem->readFrom(path_real(self::path($fixture))), $options);
    }

    /**
     * The native open source over an already-open stream - requires the extension.
     */
    public static function openNativeStream(
        SourceStream $stream,
        CSVReadOptions $options = new CSVReadOptions(),
    ): RustCSVOpenSource {
        $dialect = self::dialect($stream, $options);

        return new RustCSVOpenSource(
            $stream,
            $dialect->separator,
            $dialect->enclosure,
            $dialect->escape,
            $options->withHeader,
            $options->emptyToNull,
            $options->removeBOM,
            $options->charactersReadInLine,
            missingColumnsAsNull: $options->missingColumnsAsNull,
        );
    }

    public static function openPhp(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): PhpCSVOpenSource {
        return self::openPhpStream($filesystem->readFrom(path_real(self::path($fixture))), $options);
    }

    public static function openPhpStream(
        SourceStream $stream,
        CSVReadOptions $options = new CSVReadOptions(),
    ): PhpCSVOpenSource {
        $dialect = self::dialect($stream, $options);

        return new PhpCSVOpenSource(
            $stream,
            new CSVDecoder(
                withHeader: $options->withHeader,
                separator: $dialect->separator,
                enclosure: $dialect->enclosure,
                escape: $dialect->escape,
                emptyToNull: $options->emptyToNull,
                missingColumnsAsNull: $options->missingColumnsAsNull,
            ),
            new CSVLineReader(
                $dialect->enclosure,
                $dialect->separator,
                $dialect->escape,
                $options->charactersReadInLine,
                $options->removeBOM,
            ),
        );
    }

    /**
     * An open stream over `$content`.
     */
    public static function content(string $content): SourceStream
    {
        return self::memory($content)->readFrom(self::memorySource()->path);
    }

    /**
     * Every batch's rows, or the refusal that stopped them - consumes and closes the source.
     *
     * @param int<1, max> $batchSize
     *
     * @return InferredSchemaException|list<array{corrupted: list<mixed>}|array<int, array<array-key, mixed>>>|SchemaMismatchException
     *         rows, and a corrupted record as ['corrupted' => its identity]
     */
    public static function batches(
        CSVOpenSource $open,
        Schema $schema,
        int $batchSize,
        Backend $backend = new PhpBackend(),
        ?SchemaInference $inference = null,
    ): array|InferredSchemaException|SchemaMismatchException {
        $batches = [];

        try {
            foreach ($open->batches($schema, $batchSize, $backend, $inference) as $batch) {
                $batches[] = $batch instanceof CorruptedRecordException
                    ? ['corrupted' => [
                        $batch->uri,
                        $batch->offset,
                        $batch->lineNumber,
                        $batch->reason,
                        $batch->record,
                        $batch->fields,
                    ]]
                    : $batch->toArray();
            }

            return $batches;
        } catch (InferredSchemaException|SchemaMismatchException $e) {
            return $e;
        } finally {
            $open->close();
        }
    }

    /**
     * `batches()` as one string: the serialized rows, or the refusal's class, message, row and previous.
     *
     * @param int<1, max> $batchSize
     */
    public static function batchesOutcome(
        CSVOpenSource $open,
        Schema $schema,
        int $batchSize,
        Backend $backend = new PhpBackend(),
        ?SchemaInference $inference = null,
    ): string {
        $batches = self::batches($open, $schema, $batchSize, $backend, $inference);

        if ($batches instanceof InferredSchemaException) {
            return $batches::class . ': ' . $batches->getMessage();
        }

        return $batches instanceof SchemaMismatchException
            ? $batches::class
            . ': '
            . $batches->getMessage()
            . ' @'
            . $batches->rowIndex
            . ' <- '
            . $batches->cause::class
            . ': '
            . $batches->cause->getMessage()
            : serialize($batches);
    }

    /**
     * Every record, for strict comparison across paths - consumes the source.
     *
     * @return list<array<array-key, ?string>>
     */
    public static function records(CSVOpenSource $open): array
    {
        $records = [];

        foreach ($open->records() as $record) {
            $records[] = $record;
        }

        return $records;
    }

    /**
     * The schema CSVExtractor infers - through CSVSniffer, native when the extension is loaded.
     */
    public static function infer(SchemaInference $inference, string ...$fixtures): Schema
    {
        return (new SchemaInferrer($inference))->infer(self::sniffer(array_values($fixtures), $inference))->schema;
    }

    /**
     * The canonical PHP fold: the PHP open source's records observed row by row, whatever the opener would pick.
     */
    public static function inferPhp(SchemaInference $inference, string ...$fixtures): Schema
    {
        $names = [];

        foreach ($fixtures as $fixture) {
            $open = self::openPhp($fixture);
            $names = $open->columns();
            $open->close();

            if ($names !== []) {
                break;
            }
        }

        $units = [];

        foreach ($fixtures as $fixture) {
            $units[] = iterator_to_array(self::phpRecords($fixture), false);
        }

        return (new SchemaInferrer($inference))->infer(
            new FakeSniffer($units, new StringTypeNarrower($inference->candidates()->toArray()), $names),
        )->schema;
    }

    /**
     * The PHP open source's records, opened on the first advance and closed when abandoned - one sniffed unit.
     *
     * @return Generator<int, array<array-key, mixed>>
     */
    public static function phpRecords(string $fixture): Generator
    {
        $open = self::openPhp($fixture);

        try {
            yield from $open->records();
        } finally {
            $open->close();
        }
    }

    /**
     * A 1000-row file whose first data row is 4 bytes and every other one 101 - 100 914 bytes in total.
     */
    public static function narrowFirstRow(): string
    {
        $content = "id,payload\n1,a\n";

        foreach (range(2, 1000) as $id) {
            $content .= sprintf("%04d,%s\n", $id, str_repeat('x', 95));
        }

        return $content;
    }

    /**
     * An 8-byte header and $rows rows of exactly 10 bytes each.
     *
     * @param int<1, 999> $rows
     */
    public static function tenByteRows(int $rows): string
    {
        $content = "id,name\n";

        foreach (range(1, $rows) as $id) {
            $content .= sprintf("%03d,abcde\n", $id);
        }

        return $content;
    }

    /**
     * Records built to trap a resync: quoted values holding newlines, separators, doubled enclosures and whole lines
     * that look like records of the right width.
     */
    public static function quotedMultilineRows(int $rows): string
    {
        $content = "id,text,note\n";

        foreach (range(1, $rows) as $id) {
            $content .= match ($id % 4) {
                0 => sprintf("%d,\"line one\nline, two\nline three\",plain\n", $id),
                1 => sprintf("%d,\"a,b,c\n1,2,3\nx,y,z\",\"said \"\"hi\"\"\"\n", $id),
                2 => sprintf("%d,short,\"\"\n", $id),
                default => sprintf("%d,\"\n\n\",\"tail\nend\"\n", $id),
            };
        }

        return $content;
    }

    public static function memory(string $content): MemoryFilesystem
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(self::memorySource()->path);
        $stream->append($content);
        $stream->close();

        return $filesystem;
    }

    public static function memorySource(): SourceFile
    {
        return new SourceFile(path('memory://source.csv'));
    }

    public static function path(string $fixture): string
    {
        return __DIR__ . '/../Fixtures/' . $fixture;
    }

    /**
     * Batches under no inferred columns; a read's Criteria split narrows them to its window.
     */
    public static function fileBatches(
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVFileBatches {
        return new CSVFileBatches(new CSVSourceOpener($filesystem, $options), null);
    }

    /**
     * @param list<string> $fixtures
     * @param SchemaInference $inference its candidates build the string ladder, as CSVExtractor builds it
     */
    public static function sniffer(
        array $fixtures,
        SchemaInference $inference = new SchemaInference(),
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVSniffer {
        return new CSVSniffer(
            new CSVSourceOpener($filesystem, $options),
            self::sources(...$fixtures),
            $inference->candidates(),
        );
    }

    /**
     * The fixture's fold observed row by row over the PHP records with $inference's string ladder, and sniffed by
     * CSVSniffer - natively when the extension is loaded.
     *
     * @param null|int<0, max> $rowBudget
     *
     * @return array{SniffedColumns, SniffedColumns}
     */
    public static function sniffBothWays(string $fixture, ?int $rowBudget, SchemaInference $inference): array
    {
        $open = self::openPhp($fixture);
        $names = $open->columns();
        $open->close();

        return [
            ColumnTypes::sniff(
                $names,
                self::phpRecords($fixture),
                $rowBudget,
                new StringTypeNarrower($inference->candidates()->toArray()),
            )->folded(),
            self::sniffer([$fixture], $inference)->sniff(0, $rowBudget)->columns,
        ];
    }

    public static function source(string $fixture): SourceFile
    {
        return new SourceFile(path_real(self::path($fixture)));
    }

    /**
     * @return list<SourceFile>
     */
    public static function sources(string ...$fixtures): array
    {
        $sources = [];

        foreach ($fixtures as $fixture) {
            $sources[] = self::source($fixture);
        }

        return $sources;
    }
}
