<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Context;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileReader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonLinesExtractor;
use Flow\ETL\Extractor\SourceFile;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function file_get_contents;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

/**
 * Resolves the adapter's test fixtures and builds readers over them, so no test needs a private helper.
 */
final class JsonFixtureContext
{
    /**
     * A reader over every file a glob under Fixtures/ lists, in listing order.
     */
    public static function globReader(
        string $glob,
        JsonFormat $format = JsonFormat::Document,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ?string $pointer = null,
        bool $pointerToEntryName = false,
    ): JsonFileReader {
        $sources = [];

        foreach ((new FileListing($filesystem))->list(path(self::path($glob)), new OnlyFiles()) as $status) {
            $sources[] = new SourceFile($status->path);
        }

        return new JsonFileReader($filesystem, $format, $pointer, $pointerToEntryName, $sources);
    }

    /**
     * @param 'memory'|'native' $filesystem
     */
    public static function linesExtractor(string $fixture, string $filesystem): JsonLinesExtractor
    {
        if ($filesystem === 'native') {
            return from_json_lines(self::path($fixture));
        }

        $memory = memory_filesystem();
        $stream = $memory->writeTo(path('memory://' . $fixture));
        $stream->append((string) file_get_contents(self::path($fixture)));
        $stream->close();

        return from_json_lines(path('memory://' . $fixture), filesystem: $memory);
    }

    public static function path(string $fixture): string
    {
        return __DIR__ . '/../Fixtures/' . $fixture;
    }

    /**
     * @param list<SourceFile> $sources
     */
    public static function reader(
        JsonFormat $format = JsonFormat::Document,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        array $sources = [],
        ?string $pointer = null,
        bool $pointerToEntryName = false,
    ): JsonFileReader {
        return new JsonFileReader($filesystem, $format, $pointer, $pointerToEntryName, $sources);
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
