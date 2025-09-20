<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Tests\Integration\NativeLocalFilesystemTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

final class NativeLocalFilesystemTest extends NativeLocalFilesystemTestCase
{
    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific tests should only run on Windows');
        }
    }

    public function test_file_status_on_pattern_windows() : void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/../var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/../../Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/../var/some_path_to/*.txt'))->isFile());

        $expectedUri = 'file://' . \str_replace('\\', '/', __DIR__ . '/../var/some_path_to/file.txt');
        self::assertSame(
            $expectedUri,
            $fs->status(new Path(__DIR__ . '/../var/some_path_to/*.txt'))->path->uri()
        );

        $fs->rm(new Path(__DIR__ . '/../var/some_path_to'));
    }

    public function test_tmp_dir_windows() : void
    {
        $fs = native_local_filesystem();

        $expectedTmpDir = 'file://' . \str_replace('\\', '/', \sys_get_temp_dir());
        self::assertSame($expectedTmpDir, $fs->getSystemTmpDir()->uri());
    }

    public function test_windows_absolute_path_behavior() : void
    {
        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        \file_put_contents($tempFile, 'test content');

        $path = new Path($tempFile);
        self::assertTrue($fs->status($path)->isFile());

        self::assertMatchesRegularExpression('/^file:\/\/[a-zA-Z]:\//', $path->uri());
        self::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $path->path());

        \unlink($tempFile);
    }

    public function test_windows_drive_path_handling() : void
    {
        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        \file_put_contents($tempFile, 'test content');

        $path = new Path($tempFile);
        self::assertTrue($fs->status($path)->isFile());

        self::assertMatchesRegularExpression('/^file:\/\/[a-zA-Z]:\//', $path->uri());

        \unlink($tempFile);
    }

    public function test_windows_unc_path_support() : void
    {
        // Test UNC path handling (if accessible)
        $uncPath = '//localhost/C$/Windows/System32';

        if (!\is_dir($uncPath)) {
            self::markTestSkipped('UNC path not accessible on this system');
        }

        $path = new Path($uncPath);
        self::assertStringStartsWith('//', $path->path());
    }
}
