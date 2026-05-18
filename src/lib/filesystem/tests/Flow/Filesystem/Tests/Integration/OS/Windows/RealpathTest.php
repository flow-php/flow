<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Windows;

use Flow\Filesystem\Tests\OperatingSystem;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path_real;
use function getcwd;
use function getenv;
use function str_replace;

final class RealpathTest extends TestCase
{
    use OperatingSystem;

    public static function windows_backslash_normalization(): Generator
    {
        yield ['C:\\path\\to\\file.txt', 'C:/path/to/file.txt'];
        yield ['C:\\Windows\\System32', 'C:/Windows/System32'];
        yield ['D:\\Documents\\Projects', 'D:/Documents/Projects'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        if ($this->isUnix()) {
            self::markTestSkipped('Windows-specific realpath tests should only run on Windows');
        }
    }

    #[DataProvider('windows_backslash_normalization')]
    public function test_windows_backslash_to_forward_slash(string $windowsPath, string $normalizedPath): void
    {
        $path = path_real($windowsPath);
        static::assertEquals($normalizedPath, $path->path());
    }

    public function test_windows_drive_letter_case_handling(): void
    {
        // Test that Windows drive letters are handled consistently
        $pathLower = path_real('c:/path/file.txt');
        $pathUpper = path_real('C:/path/file.txt');

        // Drive letters preserve their original case
        static::assertEquals('C:/path/file.txt', $pathUpper->path());
        static::assertEquals('c:/path/file.txt', $pathLower->path());

        // But both represent the same logical path on Windows
        static::assertStringContainsString(':/path/file.txt', $pathLower->path());
        static::assertStringContainsString(':/path/file.txt', $pathUpper->path());
    }

    public function test_windows_home_directory_expansion(): void
    {
        if (!getenv('USERPROFILE')) {
            static::markTestSkipped('USERPROFILE environment variable not available');
        }

        $homePath = path_real('~/test_file.txt');

        // Should expand to user profile directory with forward slashes
        static::assertStringContainsString('test_file.txt', $homePath->path());
        static::assertMatchesRegularExpression('/^[a-zA-Z]:\//', $homePath->path());
        static::assertStringNotContainsString('~', $homePath->path());
    }

    public function test_windows_relative_to_absolute_path(): void
    {
        $currentDir = getcwd();
        static::assertIsString($currentDir);
        $cwd = str_replace('\\', '/', $currentDir);

        if ($cwd === '') {
            static::fail('Current working directory is empty');
        }

        $relativePath = path_real('./test_file.txt');

        static::assertStringStartsWith($cwd, $relativePath->path());
        static::assertStringEndsWith('test_file.txt', $relativePath->path());
    }
}
