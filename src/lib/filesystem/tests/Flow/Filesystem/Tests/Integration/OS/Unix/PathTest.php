<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix;

use Flow\Filesystem\Path\UnixPath;
use Flow\Filesystem\Tests\Context\OperatingSystem;
use PHPUnit\Framework\TestCase;

final class PathTest extends TestCase
{
    protected function setUp(): void
    {
        if (OperatingSystem::isWindows()) {
            self::markTestSkipped('Unix-specific filesystem tests should only run on Unix systems');
        }
    }

    public function test_unix_home_directory_resolution(): void
    {
        $path = UnixPath::realpath('~/test.txt');

        static::assertStringContainsString('test.txt', $path->path());
        static::assertStringStartsWith('/', $path->path());
    }
}
