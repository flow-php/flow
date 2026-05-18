<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use Flow\Filesystem\Tests\Integration\NativeLocalFilesystemTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function file_put_contents;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function is_dir;
use function str_replace;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

final class NativeLocalFilesystemTest extends NativeLocalFilesystemTestCase
{
    use OperatingSystem;

    protected function setUp(): void
    {
        parent::setUp();

        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific tests should only run on Windows');
        }
    }

    public function test_file_status_on_pattern_windows(): void
    {
        $fs = native_local_filesystem();

        $resource = fopen(__DIR__ . '/../../Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/../var/some_path_to/file.txt'))->fromResource($resource);

        $status = $fs->status(path(__DIR__ . '/../var/some_path_to/*.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        $expectedUri = 'file://' . str_replace('\\', '/', __DIR__ . '/../var/some_path_to/file.txt');
        $statusForUri = $fs->status(path(__DIR__ . '/../var/some_path_to/*.txt'));
        static::assertNotNull($statusForUri);
        static::assertSame($expectedUri, $statusForUri->path->uri());

        $fs->rm(path(__DIR__ . '/../var/some_path_to'));
    }

    public function test_tmp_dir_windows(): void
    {
        $fs = native_local_filesystem();

        $expectedTmpDir = 'file://' . str_replace('\\', '/', sys_get_temp_dir());
        static::assertSame($expectedTmpDir, $fs->getSystemTmpDir()->uri());
    }

    public function test_windows_absolute_path_behavior(): void
    {
        $fs = native_local_filesystem();

        $tempFile = tempnam(sys_get_temp_dir(), 'flow_test_');
        static::assertIsString($tempFile);
        file_put_contents($tempFile, 'test content');

        $path = path($tempFile);
        $status = $fs->status($path);
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        static::assertMatchesRegularExpression('/^file:\/\/[a-zA-Z]:\//', $path->uri());
        static::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $path->path());

        unlink($tempFile);
    }

    public function test_windows_drive_path_handling(): void
    {
        $fs = native_local_filesystem();

        $tempFile = tempnam(sys_get_temp_dir(), 'flow_test_');
        static::assertIsString($tempFile);
        file_put_contents($tempFile, 'test content');

        $path = path($tempFile);
        $status = $fs->status($path);
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        static::assertMatchesRegularExpression('/^file:\/\/[a-zA-Z]:\//', $path->uri());

        unlink($tempFile);
    }

    public function test_windows_unc_path_support(): void
    {
        // Test UNC path handling (if accessible)
        $uncPath = '//localhost/C$/Windows/System32';

        if (!is_dir($uncPath)) {
            static::markTestSkipped('UNC path not accessible on this system');
        }

        $path = path($uncPath);
        static::assertStringStartsWith('//', $path->path());
    }
}
