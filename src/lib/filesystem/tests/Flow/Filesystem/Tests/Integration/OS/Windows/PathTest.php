<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function Flow\Filesystem\DSL\path_real;

final class PathTest extends FlowIntegrationTestCase
{
    use OperatingSystem;

    protected function setUp(): void
    {
        parent::setUp();

        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific tests should only run on Windows');
        }
    }

    public function test_windows_home_directory_resolution(): void
    {
        $homePath = path_real('~/test_windows.txt');

        static::assertStringContainsString('test_windows.txt', $homePath->path());
        static::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $homePath->path());
    }
}
