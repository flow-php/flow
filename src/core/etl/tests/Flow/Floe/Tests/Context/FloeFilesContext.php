<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Options;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;

final class FloeFilesContext
{
    public static function phpWriter(Filesystem $filesystem, Schema $schema, Codec $codec = new NoopCodec()): FloeWriter
    {
        return new FloeWriter($filesystem, $schema, new AdaptiveBackend(), new Options(codec: $codec));
    }

    public static function phpReader(Filesystem $filesystem, Codec $codec = new NoopCodec()): FloeReader
    {
        return new FloeReader($filesystem, new AdaptiveBackend(), $codec);
    }

    /**
     * A two-file Hive tree whose bodies carry only `id`, so `country` can only come from the path.
     */
    public static function writePartitionedFiles(Filesystem $filesystem, string $root = 'memory://parts'): void
    {
        $schema = schema(int_schema('id'));

        foreach (['PL' => 1, 'DE' => 2] as $country => $id) {
            self::writeAll(
                self::phpWriter($filesystem, $schema),
                path($root . '/country=' . $country . '/data.floe'),
                [array_to_rows([['id' => $id]], $schema)],
            );
        }
    }

    /**
     * The same tree with a numerically-valued partition, so a declared partition type has something
     * to convert - `country=PL` is already a string and would prove nothing.
     */
    public static function writeYearPartitionedFiles(Filesystem $filesystem, string $root = 'memory://years'): void
    {
        $schema = schema(int_schema('id'));

        foreach (['2024' => 1, '2025' => 2] as $year => $id) {
            self::writeAll(
                self::phpWriter($filesystem, $schema),
                path($root . '/year=' . $year . '/data.floe'),
                [array_to_rows([['id' => $id]], $schema)],
            );
        }
    }

    /**
     * @param array<string, Rows> $files - uri => the rows written to it
     */
    public static function writeFiles(Filesystem $filesystem, array $files): void
    {
        foreach ($files as $uri => $rows) {
            self::writeAll(self::phpWriter($filesystem, $rows->schema()), path($uri), [$rows]);
        }
    }

    /**
     * @param array<int, Rows> $batches
     */
    public static function writeAll(FloeWriter $writer, Path $path, array $batches): void
    {
        $writer->create($path);

        foreach ($batches as $batch) {
            $writer->write($batch);
        }

        $writer->close();
    }
}
