<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Path\UnixPath;
use Flow\Filesystem\Tests\OperatingSystem;

final class PathTest extends FlowIntegrationTestCase
{
    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('Unix-specific filesystem tests should only run on Unix systems');
        }
    }

    public function test_unix_home_directory_resolution() : void
    {
        $path = UnixPath::realpath('~/test.txt');

        self::assertStringContainsString('test.txt', $path->path());
        self::assertStringStartsWith('/', $path->path());
    }
}
