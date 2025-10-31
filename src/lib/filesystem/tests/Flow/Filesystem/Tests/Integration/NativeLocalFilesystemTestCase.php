<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

abstract class NativeLocalFilesystemTestCase extends TestCase
{
    protected function setUp() : void
    {
        $fs = new NativeLocalFilesystem();

        $fs->rm(Path::from(__DIR__ . '/var/*'));
    }

    protected function givenFileExists(string $path, string $content) : void
    {
        $fs = new NativeLocalFilesystem();

        $stream = $fs->writeTo(Path::from($path));
        $stream->append($content);
        $stream->close();
    }
}
