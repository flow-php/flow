<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\Filesystem\Local\MemoryFilesystem;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class MemoryFiles
{
    /**
     * @param array<string, string> $contents file content keyed by memory:// URI
     */
    public static function with(array $contents): MemoryFilesystem
    {
        $filesystem = memory_filesystem();

        foreach ($contents as $uri => $content) {
            $stream = $filesystem->writeTo(path($uri));
            $stream->append($content);
            $stream->close();
        }

        return $filesystem;
    }
}
