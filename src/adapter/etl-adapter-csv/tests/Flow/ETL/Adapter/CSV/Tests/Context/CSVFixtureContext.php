<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Context;

use Flow\ETL\Adapter\CSV\CSVFileReader;
use Flow\ETL\Adapter\CSV\CSVOpenSource;
use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\CSVSourceOpener;
use Flow\ETL\Extractor\SourceFile;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

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
