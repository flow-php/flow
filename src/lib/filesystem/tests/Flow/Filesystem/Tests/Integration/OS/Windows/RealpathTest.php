<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use Flow\Filesystem\Path;
use Flow\Filesystem\Tests\OperatingSystem;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RealpathTest extends TestCase
{
    use OperatingSystem;

    public static function windows_backslash_normalization() : \Generator
    {
        yield ['C:\\path\\to\\file.txt', 'C:/path/to/file.txt'];
        yield ['C:\\Windows\\System32', 'C:/Windows/System32'];
        yield ['D:\\Documents\\Projects', 'D:/Documents/Projects'];
    }

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific realpath tests should only run on Windows');
        }
    }

    #[DataProvider('windows_backslash_normalization')]
    public function test_windows_backslash_to_forward_slash(string $windowsPath, string $normalizedPath) : void
    {
        $path = Path::realpath($windowsPath);
        self::assertEquals($normalizedPath, $path->path());
    }

    public function test_windows_drive_letter_case_handling() : void
    {
        // Test that Windows drive letters are handled consistently
        $pathLower = Path::realpath('c:/path/file.txt');
        $pathUpper = Path::realpath('C:/path/file.txt');

        // Drive letters preserve their original case
        self::assertEquals('C:/path/file.txt', $pathUpper->path());
        self::assertEquals('c:/path/file.txt', $pathLower->path());

        // But both represent the same logical path on Windows
        self::assertStringContainsString(':/path/file.txt', $pathLower->path());
        self::assertStringContainsString(':/path/file.txt', $pathUpper->path());
    }

    public function test_windows_home_directory_expansion() : void
    {
        if (!\getenv('USERPROFILE')) {
            self::markTestSkipped('USERPROFILE environment variable not available');
        }

        $homePath = Path::realpath('~/test_file.txt');

        // Should expand to user profile directory with forward slashes
        self::assertStringContainsString('test_file.txt', $homePath->path());
        self::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $homePath->path());
        self::assertStringNotContainsString('~', $homePath->path());
    }

    public function test_windows_relative_to_absolute_path() : void
    {
        // Get current working directory
        $cwd = \str_replace('\\', '/', \getcwd());

        $relativePath = Path::realpath('./test_file.txt');

        // Should resolve to current working directory
        self::assertStringStartsWith($cwd, $relativePath->path());
        self::assertStringEndsWith('test_file.txt', $relativePath->path());
    }
}
