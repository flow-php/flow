<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use function Flow\Filesystem\DSL\path_real;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

final class PathTest extends FlowIntegrationTestCase
{
    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific tests should only run on Windows');
        }
    }

    public function test_windows_home_directory_resolution() : void
    {
        $homePath = path_real('~/test_windows.txt');

        self::assertStringContainsString('test_windows.txt', $homePath->path());
        self::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $homePath->path());
    }
}
