<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix;

use function Flow\Filesystem\DSL\{native_local_filesystem, path, path_real};
use Flow\Filesystem\Tests\Integration\NativeLocalFilesystemTestCase;
use Flow\Filesystem\Tests\OperatingSystem;

/**
 * Tests that are specific to Unix file system behavior and will fail on Windows
 * due to URI format differences or Unix-specific functionality.
 */
final class NativeLocalFilesystemTest extends NativeLocalFilesystemTestCase
{
    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('Unix-specific filesystem tests should only run on Unix systems');
        }
    }

    public function test_file_status_on_pattern_unix_uri_format() : void
    {
        $fs = native_local_filesystem();

        $resource = \fopen(__DIR__ . '/../../Fixtures/orders.csv', 'rb');
        self::assertIsResource($resource);
        $fs->writeTo(path(__DIR__ . '/../var/some_path_to/file.txt'))->fromResource($resource);

        $status = $fs->status(path(__DIR__ . '/../var/some_path_to/*.txt'));
        self::assertNotNull($status);
        self::assertTrue($status->isFile());

        $statusForUri = $fs->status(path(__DIR__ . '/../var/some_path_to/*.txt'));
        self::assertNotNull($statusForUri);
        self::assertSame(
            'file://' . ltrim(__DIR__, '/') . '/../var/some_path_to/file.txt',
            $statusForUri->path->uri()
        );

        $fs->rm(path(__DIR__ . '/../var/some_path_to'));
    }

    public function test_status_on_non_existent_path_does_not_walk_parent_directory() : void
    {
        if (!\function_exists('chmod')) {
            self::markTestSkipped('chmod functionality not available');
        }

        if (\function_exists('posix_geteuid') && \posix_geteuid() === 0) {
            self::markTestSkipped('Cannot test permission restrictions as root');
        }

        $parent = \sys_get_temp_dir() . '/flow_status_regression_' . \bin2hex(\random_bytes(4));
        $unreadable = $parent . '/unreadable_sibling';

        \mkdir($parent, 0755, true);
        \mkdir($unreadable, 0755);
        \chmod($unreadable, 0000);

        try {
            $status = native_local_filesystem()->status(path($parent . '/does_not_exist'));

            self::assertNull($status);
        } finally {
            \chmod($unreadable, 0755);
            \rmdir($unreadable);
            \rmdir($parent);
        }
    }

    public function test_tmp_dir_unix_uri_format() : void
    {
        $fs = native_local_filesystem();

        self::assertSame('file://' . ltrim(\sys_get_temp_dir(), '/'), $fs->getSystemTmpDir()->uri());
    }

    public function test_unix_absolute_path_behavior() : void
    {
        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        self::assertIsString($tempFile);
        \file_put_contents($tempFile, 'test content');

        $path = path($tempFile);
        $status = $fs->status($path);
        self::assertNotNull($status);
        self::assertTrue($status->isFile());

        self::assertStringStartsWith('file://', $path->uri());
        self::assertStringStartsWith('/', $path->path());

        \unlink($tempFile);
    }

    public function test_unix_home_directory_resolution() : void
    {
        if (!\getenv('HOME')) {
            self::markTestSkipped('HOME environment variable not available');
        }

        $homePath = path_real('~/test_unix.txt');

        self::assertStringContainsString('test_unix.txt', $homePath->path());
        self::assertStringStartsWith('/', $homePath->path());
    }

    public function test_unix_path_normalization() : void
    {
        $fs = native_local_filesystem();

        // Test path normalization on Unix (forward slashes should remain)
        $unixPath = \sys_get_temp_dir() . '/flow_test_dir/test_file.txt';
        $normalizedPath = path($unixPath);

        // Path should maintain forward slashes on Unix
        self::assertStringContainsString('/', $normalizedPath->path());
        self::assertStringNotContainsString('\\', $normalizedPath->path());
    }

    public function test_unix_permissions() : void
    {
        if (!\function_exists('chmod')) {
            self::markTestSkipped('chmod functionality not available');
        }

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        self::assertIsString($tempFile);
        \file_put_contents($tempFile, 'test content');

        // Test file permissions on Unix
        \chmod($tempFile, 0644);
        $path = path($tempFile);

        self::assertTrue(\is_readable($tempFile));
        self::assertTrue(\is_writable($tempFile));

        \unlink($tempFile);
    }

    public function test_unix_symlink_handling() : void
    {
        if (!\function_exists('symlink')) {
            self::markTestSkipped('Symlink functionality not available');
        }

        $fs = native_local_filesystem();

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_test_');
        self::assertIsString($tempFile);
        $symlinkPath = $tempFile . '_symlink';

        \file_put_contents($tempFile, 'test content');

        if (\symlink($tempFile, $symlinkPath)) {
            $path = path($symlinkPath);
            $status = $fs->status($path);
            self::assertNotNull($status);
            self::assertTrue($status->isFile());

            \unlink($symlinkPath);
        }

        \unlink($tempFile);
    }
}
