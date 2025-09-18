<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use function Flow\Filesystem\DSL\{partition, path};
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\{Partition};
use Flow\Filesystem\Path\{Options, WindowsPath};
use Flow\Filesystem\Tests\Unit\PathTestCase;

final class WindowsPathTest extends PathTestCase
{
    public static function partitionProvider() : \Generator
    {
        yield 'single partition' => [
            '/file.txt',
            [['name' => 'group', 'value' => 'a']],
            '/group=a/file.txt',
        ];

        yield 'multiple partitions' => [
            '/file.txt',
            [
                ['name' => 'country', 'value' => 'US'],
                ['name' => 'region', 'value' => 'west'],
            ],
            '/country=US/region=west/file.txt',
        ];

        yield 'subdirectory' => [
            '/path/to/file.txt',
            [['name' => 'group', 'value' => 'a']],
            '/path/to/group=a/file.txt',
        ];
    }

    public static function pathProvider() : \Generator
    {
        yield 'file scheme' => ['file://path/to/file.txt', '/path/to/file.txt', 'file'];
        yield 'custom scheme' => ['flow-file://path/to/file.txt', '/path/to/file.txt', 'flow-file'];
        yield 'no scheme' => ['/path/to/file.txt', '/path/to/file.txt', 'file'];
        yield 'relative path' => ['path/to/file.txt', '/path/to/file.txt', 'file'];
    }

    public static function patternProvider() : \Generator
    {
        yield 'exact match' => ['/file.csv', '/file.csv', true];
        yield 'wildcard match' => ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield 'wildcard no match' => ['/nested/folder/*/file.csv', '/nested/folder/any/other.csv', false];
        yield 'recursive wildcard' => ['/nested/**/file.csv', '/nested/very/deep/file.csv', true];
        yield 'question mark' => ['/nested/fil?.csv', '/nested/file.csv', true];
    }

    public function test_options_handling() : void
    {
        $options = new Options(['key' => 'value']);
        $path = new WindowsPath('/file.txt', $options);

        self::assertEquals(['key' => 'value'], $path->options()->toArray());
    }

    public function test_pattern_methods_throw_exception() : void
    {
        $patternPath = new WindowsPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $patternPath->addPartitions(partition('group', 'a'));
    }

    public function test_protocol_operations() : void
    {
        $path = new WindowsPath('custom://path/to/file.txt');

        self::assertEquals('custom', $path->protocol()->name);
        self::assertTrue($path->protocol()->is('custom'));
        self::assertFalse($path->protocol()->is('file'));
    }

    public function test_shared_basename_operations() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());

        $prefixed = $path->basenamePrefix('prefix_');
        self::assertEquals('/path/to/prefix_file.txt', $prefixed->path());
    }

    public function test_shared_extension_operations() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        self::assertEquals('txt', $path->extension());

        $newExt = $path->setExtension('csv');
        self::assertEquals('/path/to/file.csv', $newExt->path());
        self::assertEquals('csv', $newExt->extension());
    }

    /**
     * @dataProvider pathProvider
     */
    public function test_shared_os_agnostic_logic(string $input, string $expectedPath, string $expectedScheme) : void
    {
        $path = new WindowsPath($input);

        self::assertEquals($expectedPath, $path->path());
        self::assertEquals($expectedScheme, $path->protocol()->name);
    }

    /**
     * @dataProvider partitionProvider
     */
    public function test_shared_partition_logic(string $input, array $partitionData, string $expected) : void
    {
        $path = new WindowsPath($input);
        $partitions = array_map(fn ($p) => partition($p['name'], $p['value']), $partitionData);

        $result = $path->addPartitions(...$partitions);

        self::assertEquals($expected, $result->path());
    }

    public function test_shared_path_manipulation() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        // Test suffix
        $suffixed = $path->suffix('subdir/newfile.csv');
        self::assertEquals('/path/to/file.txt/subdir/newfile.csv', $suffixed->path());

        // Test parent directory
        $parent = $path->parentDirectory();
        self::assertEquals('/path/to', $parent->path());

        // Test root directory name
        self::assertEquals('path', $path->rootDirectoryName());
    }

    /**
     * @dataProvider patternProvider
     */
    public function test_shared_pattern_logic(string $pattern, string $filename, bool $expected) : void
    {
        $patternPath = new WindowsPath($pattern);
        $filePath = new WindowsPath($filename);

        self::assertEquals($expected, $patternPath->matches($filePath));
    }

    public function test_shared_randomization() : void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $randomized = $path->randomize();

        self::assertStringStartsWith('/path/to/file_', $randomized->path());
        self::assertStringEndsWith('.txt', $randomized->path());
        self::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_windows_backslash_normalization() : void
    {
        $path = new WindowsPath('C:\\path\\to\\file.txt');

        self::assertEquals('C:/path/to/file.txt', $path->path());
        self::assertEquals('file://C:/path/to/file.txt', $path->uri());
    }

    public function test_windows_drive_partition_handling() : void
    {
        $path = new WindowsPath('C:/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        self::assertEquals('C:/group=a/file.txt', $partitioned->path());
        self::assertEquals('file://C:/group=a/file.txt', $partitioned->uri());
    }

    public function test_windows_drive_root_handling() : void
    {
        $path = new WindowsPath('C:/file.txt');

        self::assertEquals('C:/file.txt', $path->path());
        self::assertEquals('file://C:/file.txt', $path->uri());
        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());
        self::assertEquals('txt', $path->extension());
    }

    public function test_windows_drive_skip_directories() : void
    {
        $path = new WindowsPath('C:/var/www/index.html');

        $skipped1 = $path->skipDirectories(1);
        self::assertNotNull($skipped1);
        self::assertEquals('C:/www/index.html', $skipped1->path());

        $skipped2 = $path->skipDirectories(2);
        self::assertNotNull($skipped2);
        self::assertEquals('C:/index.html', $skipped2->path());

        $skipped3 = $path->skipDirectories(3);
        self::assertNull($skipped3);
    }

    public function test_windows_home_directory_resolution() : void
    {
        if (!getenv('USERPROFILE') && !getenv('HOMEDRIVE')) {
            self::markTestSkipped('Windows home directory environment variables not available');
        }

        $path = WindowsPath::realpath('~/test.txt');

        self::assertStringContainsString('test.txt', $path->path());
        // Windows paths start with drive letter, not /
        self::assertMatchesRegularExpression('/^[a-zA-Z]:/', $path->path());
    }

    public function test_windows_pathinfo_backslash_edge_case() : void
    {
        // Test the specific edge case where Windows pathinfo returns backslash for root
        $path = new WindowsPath('/file.txt');
        $parent = $path->parentDirectory();

        self::assertEquals('/', $parent->path());
        self::assertEquals('file://', $parent->uri());
    }

    public function test_windows_root_partition_edge_case() : void
    {
        // Test the specific root path partition issue from V3
        $path = new WindowsPath('/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        self::assertEquals('/group=a/file.txt', $partitioned->path());
        self::assertEquals('file://group=a/file.txt', $partitioned->uri());
    }

    public function test_windows_unc_path_handling() : void
    {
        $path = new WindowsPath('//server/share/file.txt');

        self::assertEquals('//server/share/file.txt', $path->path());
        self::assertEquals('file://server/share/file.txt', $path->uri());
        self::assertEquals('server', $path->rootDirectoryName());
    }
}
