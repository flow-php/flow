<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\TestCase;

abstract class NativeLocalFilesystemTestCase extends TestCase
{
    #[\Override]
    protected function setUp() : void
    {
        $fs = new NativeLocalFilesystem();

        $fs->rm(path(__DIR__ . '/var/*'));
    }

    protected function givenFileExists(string $path, string $content) : void
    {
        $fs = new NativeLocalFilesystem();

        $stream = $fs->writeTo(path($path));
        $stream->append($content);
        $stream->close();
    }
}
