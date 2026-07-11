<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use Flow\Floe\FrameReader;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FrameReaderMother
{
    public static function overBytes(string $bytes, int $expectedFlags = 0x00, int $chunkSize = 65536): FrameReader
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://frames.floe'));
        $stream->append($bytes);
        $stream->close();

        return new FrameReader($filesystem->readFrom(path('memory://frames.floe')), $expectedFlags, $chunkSize);
    }
}
