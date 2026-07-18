<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;

final class FloeEngineContext
{
    public static function phpWriter(Filesystem $filesystem, Codec $codec = new NoopCodec()): FloeWriter
    {
        return new FloeWriter($filesystem, $codec, new PhpRowHydrator());
    }

    public static function nativeWriter(Filesystem $filesystem, Codec $codec = new NoopCodec()): FloeWriter
    {
        return new FloeWriter($filesystem, $codec, new NativeRowHydrator());
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
