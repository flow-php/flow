<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Context;

use Flow\ETL\Adapter\CSV\CSVDialect;
use Flow\ETL\Adapter\CSV\CSVEncoder;
use Flow\ETL\Adapter\CSV\CSVFileReader;
use Flow\ETL\Adapter\CSV\CSVFileSample;
use Flow\ETL\Adapter\CSV\CSVLineReader;
use Flow\ETL\Adapter\CSV\CSVOpenSource;
use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\NativeCSVOpenSource;
use Flow\ETL\Adapter\CSV\PhpCSVOpenSource;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\MemoryFilesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\SourceStream;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Flow\Types\Type\TypeNarrower;
use Generator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function Flow\ETL\Adapter\CSV\csv_detect_separator;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function str_ends_with;
use function strlen;
use function substr;

/**
 * Resolves the adapter's test fixtures and builds readers over them, so no test needs a private helper.
 */
final class CSVFixtureContext
{
    /**
     * A reader over every file a glob under Fixtures/ lists, in listing order.
     */
    public static function globReader(
        string $glob,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVFileReader {
        $sources = [];

        foreach ((new FileListing($filesystem))->list(path(self::path($glob)), new OnlyFiles()) as $status) {
            $sources[] = new SourceFile($status->path);
        }

        return new CSVFileReader(new CSVSourceOpener($filesystem, $options), $sources);
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
     * The dialect CSVSourceOpener would resolve: pinned options first, detection for the rest.
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
    ): NativeCSVOpenSource {
        return self::openNativeStream($filesystem->readFrom(path_real(self::path($fixture))), $options);
    }

    /**
     * The native open source over an already-open stream - requires the extension.
     */
    public static function openNativeStream(
        SourceStream $stream,
        CSVReadOptions $options = new CSVReadOptions(),
    ): NativeCSVOpenSource {
        $dialect = self::dialect($stream, $options);

        return new NativeCSVOpenSource(
            $stream,
            new RustCSVReaderNative(
                $dialect->separator,
                $dialect->enclosure,
                $dialect->escape,
                $options->withHeader,
                $options->emptyToNull,
                $options->removeBOM,
            ),
            $options->charactersReadInLine,
        );
    }

    public static function openPhp(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): PhpCSVOpenSource {
        $stream = $filesystem->readFrom(path_real(self::path($fixture)));
        $dialect = self::dialect($stream, $options);

        return new PhpCSVOpenSource(
            $stream,
            new CSVEncoder(
                withHeader: $options->withHeader,
                separator: $dialect->separator,
                enclosure: $dialect->enclosure,
                escape: $dialect->escape,
                emptyToNull: $options->emptyToNull,
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
     * Every record's values and metadata, for strict comparison across paths - consumes the source.
     *
     * @return list<array{array<array-key, mixed>, array<array-key, mixed>}>
     */
    public static function records(CSVOpenSource $open): array
    {
        $records = [];

        foreach ($open->records() as $record) {
            $records[] = [$record->values, $record->metadata];
        }

        return $records;
    }

    /**
     * The schema CSVExtractor infers - through CSVFileReader::samples(), native when the extension is loaded.
     */
    public static function infer(SchemaInference $inference, string ...$fixtures): Schema
    {
        $reader = self::reader(sources: self::sources(...$fixtures));

        return (new SchemaInferrer($inference, new StringTypeNarrower($inference->candidates()->toArray())))->infer(
            $reader->header()->names,
            $reader->samples($inference->sampleSize),
        );
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

        $samples = [];

        foreach ($fixtures as $fixture) {
            $samples[] = self::phpRecords($fixture);
        }

        return (new SchemaInferrer($inference, new StringTypeNarrower($inference->candidates()->toArray())))->infer(
            $names,
            $samples,
        );
    }

    /**
     * The PHP open source's records, opened on the first advance and closed when abandoned - a SchemaSampler unit.
     *
     * @return Generator<int, RawRowValues>
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
     * A reader with an EMPTY listing - for the per-source methods (columns/sample/batches), which take the
     * source as an argument and never walk $sources.
     *
     * @param list<SourceFile> $sources
     */
    public static function reader(
        Filesystem $filesystem = new NativeLocalFilesystem(),
        array $sources = [],
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVFileReader {
        return new CSVFileReader(new CSVSourceOpener($filesystem, $options), $sources);
    }

    public static function sample(
        string $fixture,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        CSVReadOptions $options = new CSVReadOptions(),
    ): CSVFileSample {
        return new CSVFileSample(new CSVSourceOpener($filesystem, $options), self::source($fixture));
    }

    /**
     * The fixture's partial ColumnTypes observed row by row over the PHP records, and sniffed by CSVFileSample -
     * natively when the extension is loaded.
     *
     * @param int<0, max>|-1 $rowBudget
     *
     * @return array{ColumnTypes, ColumnTypes}
     */
    public static function sniffBothWays(
        string $fixture,
        int $rowBudget,
        SchemaInference $inference,
        TypeNarrower $typer,
    ): array {
        $open = self::openPhp($fixture);
        $names = $open->columns();
        $open->close();

        return [
            (new SchemaInferrer($inference, $typer))->sniff($names, self::phpRecords($fixture), $rowBudget),
            self::sample($fixture)->sniffColumnTypes($names, $rowBudget, $inference, $typer),
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
