<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix;

use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\Filesystem\Path;
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

        $fs->writeTo(new Path(__DIR__ . '/../var/some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/../../Fixtures/orders.csv', 'rb'));

        self::assertTrue($fs->status(new Path(__DIR__ . '/../var/some_path_to/*.txt'))->isFile());

        self::assertSame(
            'file://' . ltrim(__DIR__, '/') . '/../var/some_path_to/file.txt',
            $fs->status(new Path(__DIR__ . '/../var/some_path_to/*.txt'))->path->uri()
        );

        $fs->rm(new Path(__DIR__ . '/../var/some_path_to'));
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
        \file_put_contents($tempFile, 'test content');

        $path = new Path($tempFile);
        self::assertTrue($fs->status($path)->isFile());

        self::assertStringStartsWith('file://', $path->uri());
        self::assertStringStartsWith('/', $path->path());

        \unlink($tempFile);
    }

    public function test_unix_home_directory_resolution() : void
    {
        if (!\getenv('HOME')) {
            self::markTestSkipped('HOME environment variable not available');
        }

        $homePath = Path::realpath('~/test_unix.txt');

        self::assertStringContainsString('test_unix.txt', $homePath->path());
        self::assertStringStartsWith('/', $homePath->path());
    }

    public function test_unix_path_normalization() : void
    {
        $fs = native_local_filesystem();

        // Test path normalization on Unix (forward slashes should remain)
        $unixPath = \sys_get_temp_dir() . '/flow_test_dir/test_file.txt';
        $normalizedPath = new Path($unixPath);

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
        \file_put_contents($tempFile, 'test content');

        // Test file permissions on Unix
        \chmod($tempFile, 0644);
        $path = new Path($tempFile);

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
        $symlinkPath = $tempFile . '_symlink';

        \file_put_contents($tempFile, 'test content');

        if (\symlink($tempFile, $symlinkPath)) {
            $path = new Path($symlinkPath);
            self::assertTrue($fs->status($path)->isFile());

            \unlink($symlinkPath);
        }

        \unlink($tempFile);
    }
}
