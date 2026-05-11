<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix;

use Flow\Filesystem\Tests\Integration\NativeLocalFilesystemTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

/**
 * Tests that are specific to Unix file system behavior and will fail on Windows
 * due to URI format differences or Unix-specific functionality.
 */
final class NativeLocalFilesystemTest extends NativeLocalFilesystemTestCase
{
    use OperatingSystem;

    protected function setUp(): void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('Unix-specific filesystem tests should only run on Unix systems');
        }
    }

    public function test_file_status_on_pattern_unix_uri_format(): void
    {
        $fs = native_local_filesystem();

        $resource = \fopen(__DIR__ . '/../../Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/../var/some_path_to/file.txt'))->fromResource($resource);

        $status = $fs->status(path(__DIR__ . '/../var/some_path_to/*.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        $statusForUri = $fs->status(path(__DIR__ . '/../var/some_path_to/*.txt'));
        static::assertNotNull($statusForUri);
        static::assertSame(
            'file://' . ltrim(__DIR__, '/') . '/../var/some_path_to/file.txt',
            $statusForUri->path->uri(),
        );

        $fs->rm(path(__DIR__ . '/../var/some_path_to'));
    }

    public function test_status_on_non_existent_path_does_not_walk_parent_directory(): void
    {
        if (!\function_exists('chmod')) {
            static::markTestSkipped('chmod functionality not available');
        }

        if (\function_exists('posix_geteuid') && \posix_geteuid() === 0) {
            static::markTestSkipped('Cannot test permission restrictions as root');
        }

        $parent = \sys_get_temp_dir() . '/flow_status_regression_' . \bin2hex(\random_bytes(4));
        $unreadable = $parent . '/unreadable_sibling';

        \mkdir($parent, 0755, true);
        \mkdir($unreadable, 0755);
        \chmod($unreadable, 0000);

        try {
            $status = native_local_filesystem()->status(path($parent . '/does_not_exist'));

            static::assertNull($status);
        } finally {
            \chmod($unreadable, 0755);
            \rmdir($unreadable);
            \rmdir($parent);
        }
    }

    public function test_tmp_dir_unix_uri_format(): void
    {
        $fs = native_local_filesystem();

        static::assertSame('file://' . ltrim(\sys_get_temp_dir(), '/'), $fs->getSystemTmpDir()->uri());
    }

    public function test_unix_absolute_path_behavior(): void
    {
        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        static::assertIsString($tempFile);
        \file_put_contents($tempFile, 'test content');

        $path = path($tempFile);
        $status = $fs->status($path);
        static::assertNotNull($status);
        static::assertTrue($status->isFile());

        static::assertStringStartsWith('file://', $path->uri());
        static::assertStringStartsWith('/', $path->path());

        \unlink($tempFile);
    }

    public function test_unix_home_directory_resolution(): void
    {
        if (!\getenv('HOME')) {
            static::markTestSkipped('HOME environment variable not available');
        }

        $homePath = path_real('~/test_unix.txt');

        static::assertStringContainsString('test_unix.txt', $homePath->path());
        static::assertStringStartsWith('/', $homePath->path());
    }

    public function test_unix_path_normalization(): void
    {
        native_local_filesystem();

        // Test path normalization on Unix (forward slashes should remain)
        $unixPath = \sys_get_temp_dir() . '/flow_test_dir/test_file.txt';
        $normalizedPath = path($unixPath);

        // Path should maintain forward slashes on Unix
        static::assertStringContainsString('/', $normalizedPath->path());
        static::assertStringNotContainsString('\\', $normalizedPath->path());
    }

    public function test_unix_permissions(): void
    {
        if (!\function_exists('chmod')) {
            static::markTestSkipped('chmod functionality not available');
        }

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        static::assertIsString($tempFile);
        \file_put_contents($tempFile, 'test content');

        // Test file permissions on Unix
        \chmod($tempFile, 0644);
        path($tempFile);

        static::assertTrue(\is_readable($tempFile));
        static::assertTrue(\is_writable($tempFile));

        \unlink($tempFile);
    }

    public function test_unix_symlink_handling(): void
    {
        if (!\function_exists('symlink')) {
            static::markTestSkipped('Symlink functionality not available');
        }

        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        static::assertIsString($tempFile);
        $symlinkPath = $tempFile . '_symlink';

        \file_put_contents($tempFile, 'test content');

        if (\symlink($tempFile, $symlinkPath)) {
            $path = path($symlinkPath);
            $status = $fs->status($path);
            static::assertNotNull($status);
            static::assertTrue($status->isFile());

            \unlink($symlinkPath);
        }

        \unlink($tempFile);
    }
}
