<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Context;

use Flow\ETL\Adapter\JSON\AdaptiveJsonOpenSource;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileReader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonSamples;
use Flow\ETL\Adapter\JSON\JsonOpenSource;
use Flow\ETL\Column\Backend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Generator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

use function count;
use function file_get_contents;
use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function in_array;
use function serialize;
use function str_ends_with;
use function str_starts_with;
use function strlen;
use function substr;

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
     * Every *.json and *.jsonl under Fixtures/ outside json-schema/, keyed and valued by its path relative to Fixtures/.
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
            $fixture = substr($file->getPathname(), strlen($root));

            if (
                !str_starts_with($fixture, 'json-schema/') && in_array($file->getExtension(), ['json', 'jsonl'], true)
            ) {
                yield $fixture => [$fixture];
            }
        }
    }

    public static function format(string $fixture): JsonFormat
    {
        return str_ends_with($fixture, '.jsonl') ? JsonFormat::Lines : JsonFormat::Document;
    }

    /**
     * The schema JsonExtractor infer from the fixture.
     */
    public static function infer(string $fixture, SchemaInference $inference = new SchemaInference()): Schema
    {
        $reader = self::reader(self::format($fixture), sources: [self::source($fixture)]);

        return (new SchemaInferrer($inference, new InstanceOfTypeNarrower()))->infer(
            [],
            new JsonSamples($reader->samples($inference->sampleSize)),
        );
    }

    /**
     * @param 'memory'|'native' $filesystem
     */
    public static function linesExtractor(string $fixture, string $filesystem): JsonExtractor
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

    /**
     * The source JsonExtractor (Document or Lines) opens, over a reader of the same filesystem.
     */
    public static function open(
        JsonFormat $format,
        SourceFile $source,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ?string $pointer = null,
    ): AdaptiveJsonOpenSource {
        return new AdaptiveJsonOpenSource(
            $filesystem,
            self::reader($format, $filesystem, pointer: $pointer),
            $format,
            $pointer,
            $source,
        );
    }

    /**
     * The rows of every batch (of the first $batches when given) as one string, or the refusal's class, message, row
     * and cause - consumes and closes the source.
     *
     * @param int<1, max> $batchSize
     * @param null|int<1, max> $batches
     */
    public static function outcome(
        JsonOpenSource $open,
        Schema $schema,
        int $batchSize,
        Backend $backend = new PhpBackend(),
        ?int $batches = null,
    ): string {
        $read = [];

        try {
            foreach ($open->batches($schema, $batchSize, $backend) as $batch) {
                $read[] = $batch->toArray();

                if (count($read) === $batches) {
                    break;
                }
            }

            return serialize($read);
        } catch (SchemaMismatchException $e) {
            return (
                $e::class
                . ': '
                . $e->getMessage()
                . ' @'
                . $e->rowIndex
                . ' <- '
                . $e->cause::class
                . ': '
                . $e->cause->getMessage()
            );
        } finally {
            $open->close();
        }
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
