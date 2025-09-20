<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use function Flow\Filesystem\DSL\{partition};
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path\{Options, UnixPath};
use Flow\Filesystem\Tests\Unit\PathTestCase;

final class UnixPathTest extends PathTestCase
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
        $path = new UnixPath('/file.txt', $options);

        self::assertEquals(['key' => 'value'], $path->options()->toArray());
    }

    public function test_partitions_extraction() : void
    {
        $path = new UnixPath('/path/country=US/region=west/file.txt');
        $partitions = $path->partitions();

        self::assertEquals(2, $partitions->count());

        $partitionArray = $partitions->toArray();
        self::assertEquals('country', $partitionArray[0]->name);
        self::assertEquals('US', $partitionArray[0]->value);
        self::assertEquals('region', $partitionArray[1]->name);
        self::assertEquals('west', $partitionArray[1]->value);
    }

    public function test_partitions_paths() : void
    {
        $path = new UnixPath('/path/country=US/region=west/file.txt');
        $partitionPaths = $path->partitionsPaths();

        self::assertCount(2, $partitionPaths);
        self::assertEquals('/path/country=US', $partitionPaths[0]->path());
        self::assertEquals('/path/country=US/region=west', $partitionPaths[1]->path());
    }

    public function test_pattern_methods_throw_exception() : void
    {
        $patternPath = new UnixPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $patternPath->addPartitions(partition('group', 'a'));
    }

    public function test_protocol_operations() : void
    {
        $path = new UnixPath('custom://path/to/file.txt');

        self::assertEquals('custom', $path->protocol()->name);
        self::assertTrue($path->protocol()->is('custom'));
        self::assertFalse($path->protocol()->is('file'));
    }

    public function test_basename_operations() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());

        $prefixed = $path->basenamePrefix('prefix_');
        self::assertEquals('/path/to/prefix_file.txt', $prefixed->path());
    }

    public function test_extension_operations() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertEquals('txt', $path->extension());

        $newExt = $path->setExtension('csv');
        self::assertEquals('/path/to/file.csv', $newExt->path());
        self::assertEquals('csv', $newExt->extension());
    }

    /**
     * @dataProvider pathProvider
     */
    public function test_os_agnostic_logic(string $input, string $expectedPath, string $expectedScheme) : void
    {
        $path = new UnixPath($input);

        self::assertEquals($expectedPath, $path->path());
        self::assertEquals($expectedScheme, $path->protocol()->name);
    }

    /**
     * @dataProvider partitionProvider
     */
    public function test_shared_partition_logic(string $input, array $partitionData, string $expected) : void
    {
        $path = new UnixPath($input);
        $partitions = array_map(fn ($p) => partition($p['name'], $p['value']), $partitionData);

        $result = $path->addPartitions(...$partitions);

        self::assertEquals($expected, $result->path());
    }

    public function test_path_manipulation() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        $suffixed = $path->suffix('subdir/newfile.csv');
        self::assertEquals('/path/to/file.txt/subdir/newfile.csv', $suffixed->path());

        $parent = $path->parentDirectory();
        self::assertEquals('/path/to', $parent->path());

        self::assertEquals('path', $path->rootDirectoryName());
    }

    /**
     * @dataProvider patternProvider
     */
    public function test_pattern_logic(string $pattern, string $filename, bool $expected) : void
    {
        $patternPath = new UnixPath($pattern);
        $filePath = new UnixPath($filename);

        self::assertEquals($expected, $patternPath->matches($filePath));
    }

    public function test_randomization() : void
    {
        $path = new UnixPath('/path/to/file.txt');
        $randomized = $path->randomize();

        self::assertStringStartsWith('/path/to/file_', $randomized->path());
        self::assertStringEndsWith('.txt', $randomized->path());
        self::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_static_part_extraction() : void
    {
        $pattern = new UnixPath('/static/part/*/dynamic/part');
        $staticPart = $pattern->staticPart();

        self::assertEquals('/static/part', $staticPart->path());
    }

    public function test_absolute_path_handling() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertEquals('/path/to/file.txt', $path->path());
        self::assertEquals('file://path/to/file.txt', $path->uri());
        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());
        self::assertEquals('txt', $path->extension());
    }

    public function test_current_directory_handling() : void
    {
        $path = new UnixPath('./file.txt');

        self::assertEquals('/./file.txt', $path->path());

        $parent = $path->parentDirectory();
        self::assertEquals('/.', $parent->path());
    }

    public function test_relative_path_normalization() : void
    {
        $path = new UnixPath('relative/path/file.txt');

        self::assertEquals('/relative/path/file.txt', $path->path());
        self::assertEquals('file://relative/path/file.txt', $path->uri());
    }

    public function test_root_directory_cases() : void
    {
        $rootCases = ['/', '/file.txt'];

        foreach ($rootCases as $case) {
            $path = new UnixPath($case);
            $parent = $path->parentDirectory();

            self::assertEquals('/', $parent->path(), "Failed for case: {$case}");
        }
    }

    public function test_root_partition_handling() : void
    {
        $path = new UnixPath('/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        self::assertEquals('/group=a/file.txt', $partitioned->path());
        self::assertEquals('file://group=a/file.txt', $partitioned->uri());
    }

    public function test_skip_directories() : void
    {
        $path = new UnixPath('/var/www/index.html');

        $skipped1 = $path->skipDirectories(1);
        self::assertNotNull($skipped1);
        self::assertEquals('file://www/index.html', $skipped1->uri());

        $skipped2 = $path->skipDirectories(2);
        self::assertNotNull($skipped2);
        self::assertEquals('file://index.html', $skipped2->uri());

        $skipped3 = $path->skipDirectories(3);
        self::assertNull($skipped3);
    }
}
