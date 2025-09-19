<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Tests\Integration\NativeLocalFilesystemTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

final class NativeLocalFilesystemWindowsTest extends NativeLocalFilesystemTestCase
{
    use OperatingSystem;

    protected function setUp(): void
    {
        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific tests should only run on Windows');
        }

        parent::setUp();
    }

    public function test_file_status_on_pattern_windows(): void
    {
        $fs = native_local_filesystem();

        $fs->writeTo(new Path(__DIR__ . '/../var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/../../Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/../var/some_path_to/*.txt'))->isFile());

        // Windows expects file:// format (not file:/)
        $expectedUri = 'file://' . \str_replace('\\', '/', __DIR__ . '/../var/some_path_to/file.txt');
        self::assertSame(
            $expectedUri,
            $fs->status(new Path(__DIR__ . '/../var/some_path_to/*.txt'))->path->uri()
        );

        $fs->rm(new Path(__DIR__ . '/../var/some_path_to'));
    }

    public function test_tmp_dir_windows(): void
    {
        $fs = native_local_filesystem();

        // Windows expects file:// format and forward slashes
        $expectedTmpDir = 'file://' . \str_replace('\\', '/', \sys_get_temp_dir());
        self::assertSame($expectedTmpDir, $fs->getSystemTmpDir()->uri());
    }

    public function test_windows_drive_path_handling(): void
    {
        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        \file_put_contents($tempFile, 'test content');

        $path = new Path($tempFile);
        self::assertTrue($fs->status($path)->isFile());

        // Verify Windows drive letter is preserved
        self::assertMatchesRegularExpression('/^file:\/\/[a-zA-Z]:\//', $path->uri());

        \unlink($tempFile);
    }

    public function test_windows_backslash_normalization(): void
    {
        $fs = native_local_filesystem();

        // Create a path with Windows backslashes
        $windowsPath = \sys_get_temp_dir() . '\\flow_test_dir\\test_file.txt';
        $normalizedPath = new Path($windowsPath);

        // Path should be normalized to forward slashes internally
        self::assertStringNotContainsString('\\', $normalizedPath->path());
        self::assertStringContainsString('/', $normalizedPath->path());
    }

    public function test_windows_unc_path_support(): void
    {
        // Test UNC path handling (if accessible)
        $uncPath = '//localhost/C$/Windows/System32';

        if (!\is_dir($uncPath)) {
            self::markTestSkipped('UNC path not accessible on this system');
        }

        $path = new Path($uncPath);
        self::assertStringStartsWith('//', $path->path());
    }

    public function test_windows_home_directory_resolution(): void
    {
        if (!\getenv('USERPROFILE')) {
            self::markTestSkipped('USERPROFILE environment variable not available');
        }

        $homePath = Path::realpath('~/test_windows.txt');

        self::assertStringContainsString('test_windows.txt', $homePath->path());
        self::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $homePath->path());
    }

    public function test_windows_absolute_path_behavior(): void
    {
        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        \file_put_contents($tempFile, 'test content');

        $path = new Path($tempFile);
        self::assertTrue($fs->status($path)->isFile());

        // Verify Windows path format with drive letter
        self::assertMatchesRegularExpression('/^file:\/\/[a-zA-Z]:\//', $path->uri());
        self::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $path->path());

        \unlink($tempFile);
    }

    public function test_windows_permissions_handling(): void
    {
        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        \file_put_contents($tempFile, 'test content');

        $path = new Path($tempFile);

        // Windows has different permission model, but basic read/write should work
        self::assertTrue(\is_readable($tempFile));
        self::assertTrue(\is_writable($tempFile));

        \unlink($tempFile);
    }
}