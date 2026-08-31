<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Options;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;

final class FloeEngineContext
{
    public static function phpWriter(Filesystem $filesystem, Schema $schema, Codec $codec = new NoopCodec()): FloeWriter
    {
        return new FloeWriter($filesystem, $schema, new Options(codec: $codec), new PhpRowHydrator());
    }

    public static function nativeWriter(
        Filesystem $filesystem,
        Schema $schema,
        Codec $codec = new NoopCodec(),
    ): FloeWriter {
        return new FloeWriter($filesystem, $schema, new Options(codec: $codec), new NativeRowHydrator());
    }

    public static function phpReader(Filesystem $filesystem, Codec $codec = new NoopCodec()): FloeReader
    {
        return new FloeReader($filesystem, $codec, hydrator: new PhpRowHydrator());
    }

    public static function nativeReader(Filesystem $filesystem, Codec $codec = new NoopCodec()): FloeReader
    {
        return new FloeReader($filesystem, $codec, hydrator: new NativeRowHydrator());
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
                [rows($schema, row(['id' => $id]))],
            );
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
