<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use function Flow\Filesystem\DSL\{partition};
use Flow\Filesystem\Exception\{InvalidArgumentException};
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

    public function test_absolute_path_detection() : void
    {
        $path1 = new WindowsPath('C:\\test\\file.txt');
        self::assertEquals('C:/test/file.txt', $path1->path());

        $path2 = new WindowsPath('\\\\server\\share\\file.txt');
        self::assertEquals('//server/share/file.txt', $path2->path());
    }

    public function test_add_partitions_complex_drive_path() : void
    {
        $path = new WindowsPath('C:/some/path/file.txt');
        $partitioned = $path->addPartitions(partition('year', '2023'));

        self::assertEquals('C:/some/path/year=2023/file.txt', $partitioned->path());
    }

    public function test_basename_operations() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());

        $prefixed = $path->basenamePrefix('prefix_');
        self::assertEquals('/path/to/prefix_file.txt', $prefixed->path());
    }

    public function test_bracket_pattern_matching() : void
    {
        $pattern = new WindowsPath('/path/file[123].txt');
        $file1 = new WindowsPath('/path/file1.txt');
        $file2 = new WindowsPath('/path/file2.txt');
        $fileA = new WindowsPath('/path/fileA.txt');

        self::assertTrue($pattern->matches($file1));
        self::assertTrue($pattern->matches($file2));
        self::assertFalse($pattern->matches($fileA));
    }

    public function test_complex_pattern_matching() : void
    {
        $pattern = new WindowsPath('/path/file*.txt');
        $file1 = new WindowsPath('/path/file1.txt');
        $file2 = new WindowsPath('/path/file2.txt');
        $file3 = new WindowsPath('/path/other.txt');

        self::assertTrue($pattern->matches($file1));
        self::assertTrue($pattern->matches($file2));
        self::assertFalse($pattern->matches($file3));
    }

    public function test_constructor_with_options_object() : void
    {
        $options = new Options(['test' => 'value']);
        $path = new WindowsPath('/file.txt', $options);

        self::assertEquals(['test' => 'value'], $path->options()->toArray());
    }

    public function test_drive_partition_handling() : void
    {
        $path = new WindowsPath('C:/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        self::assertEquals('C:/group=a/file.txt', $partitioned->path());
        self::assertEquals('file://C:/group=a/file.txt', $partitioned->uri());
    }

    public function test_drive_root_handling() : void
    {
        $path = new WindowsPath('C:/file.txt');

        self::assertEquals('C:/file.txt', $path->path());
        self::assertEquals('file://C:/file.txt', $path->uri());
        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());
        self::assertEquals('txt', $path->extension());
    }

    public function test_drive_skip_directories() : void
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

    public function test_empty_path_normalization() : void
    {
        $path = new WindowsPath('');
        self::assertEquals('/', $path->path());
    }

    public function test_ends_with() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        self::assertTrue($path->endsWith('.txt'));
        self::assertTrue($path->endsWith('file.txt'));
        self::assertTrue($path->endsWith('/file.txt'));
        self::assertFalse($path->endsWith('.csv'));
        self::assertFalse($path->endsWith('other.txt'));
    }

    public function test_extension_case_insensitive() : void
    {
        $path = new WindowsPath('/path/to/file.TXT');

        self::assertEquals('txt', $path->extension());
    }

    public function test_extension_operations() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        self::assertEquals('txt', $path->extension());

        $newExt = $path->setExtension('csv');
        self::assertEquals('/path/to/file.csv', $newExt->path());
        self::assertEquals('csv', $newExt->extension());
    }

    public function test_extension_with_no_extension_returns_false() : void
    {
        $path = new WindowsPath('/path/to/file');

        self::assertFalse($path->extension());
    }

    public function test_fnmatch_with_hidden_files() : void
    {
        $pattern = new WindowsPath('/*');
        $hidden = new WindowsPath('/.hidden');
        $normal = new WindowsPath('/visible');

        self::assertTrue($pattern->matches($normal));
        self::assertTrue($pattern->matches($hidden));
    }

    public function test_is_equal() : void
    {
        $path1 = new WindowsPath('/path/to/file.txt');
        $path2 = new WindowsPath('/path/to/file.txt');
        $path3 = new WindowsPath('/path/to/other.txt');

        self::assertTrue($path1->isEqual($path2));
        self::assertFalse($path1->isEqual($path3));
    }

    public function test_is_pattern_detection() : void
    {
        self::assertTrue((new WindowsPath('/path/*/file.txt'))->isPattern());
        self::assertTrue((new WindowsPath('/path/**/file.txt'))->isPattern());
        self::assertTrue((new WindowsPath('/path/file?.txt'))->isPattern());
        self::assertTrue((new WindowsPath('/path/file[abc].txt'))->isPattern());
        self::assertTrue((new WindowsPath('/path/file{a,b}.txt'))->isPattern());
        self::assertFalse((new WindowsPath('/path/file.txt'))->isPattern());
    }

    public function test_matches_non_pattern_exact_match() : void
    {
        $path1 = new WindowsPath('/path/to/file.txt');
        $path2 = new WindowsPath('/path/to/file.txt');
        $path3 = new WindowsPath('/path/to/other.txt');

        self::assertTrue($path1->matches($path2));
        self::assertFalse($path1->matches($path3));
    }

    public function test_matches_pattern_against_pattern_returns_false() : void
    {
        $pattern1 = new WindowsPath('/path/*/file.txt');
        $pattern2 = new WindowsPath('/path/*/other.txt');

        self::assertFalse($pattern1->matches($pattern2));
    }

    public function test_options_from_array() : void
    {
        $path = new WindowsPath('/file.txt', ['option1' => 'value1', 'option2' => 'value2']);

        self::assertEquals(['option1' => 'value1', 'option2' => 'value2'], $path->options()->toArray());
    }

    public function test_options_handling() : void
    {
        $options = new Options(['key' => 'value']);
        $path = new WindowsPath('/file.txt', $options);

        self::assertEquals(['key' => 'value'], $path->options()->toArray());
    }

    /**
     * @dataProvider pathProvider
     */
    public function test_os_agnostic_logic(string $input, string $expectedPath, string $expectedScheme) : void
    {
        $path = new WindowsPath($input);

        self::assertEquals($expectedPath, $path->path());
        self::assertEquals($expectedScheme, $path->protocol()->name);
    }

    /**
     * @dataProvider partitionProvider
     */
    public function test_partition_logic(string $input, array $partitionData, string $expected) : void
    {
        $path = new WindowsPath($input);
        $partitions = array_map(fn ($p) => partition($p['name'], $p['value']), $partitionData);

        $result = $path->addPartitions(...$partitions);

        self::assertEquals($expected, $result->path());
    }

    public function test_partitions_extraction() : void
    {
        $path = new WindowsPath('/path/country=US/region=west/city=LA/file.txt');
        $partitions = $path->partitions();

        self::assertCount(3, $partitions);

        $partitionArray = [];

        foreach ($partitions as $partition) {
            $partitionArray[$partition->name] = $partition->value;
        }

        self::assertEquals('US', $partitionArray['country']);
        self::assertEquals('west', $partitionArray['region']);
        self::assertEquals('LA', $partitionArray['city']);
    }

    public function test_partitions_paths() : void
    {
        $path = new WindowsPath('/base/country=US/region=west/city=LA/file.txt');
        $partitionPaths = $path->partitionsPaths();

        self::assertCount(3, $partitionPaths);
        self::assertEquals('/base/country=US', $partitionPaths[0]->path());
        self::assertEquals('/base/country=US/region=west', $partitionPaths[1]->path());
        self::assertEquals('/base/country=US/region=west/city=LA', $partitionPaths[2]->path());
    }

    public function test_partitions_paths_with_root_directory() : void
    {
        $path = new WindowsPath('/country=US/region=west/file.txt');
        $partitionPaths = $path->partitionsPaths();

        self::assertCount(2, $partitionPaths);
        self::assertEquals('/country=US', $partitionPaths[0]->path());
        self::assertEquals('/country=US/region=west', $partitionPaths[1]->path());
    }

    public function test_partitions_paths_without_partitions() : void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $partitionPaths = $path->partitionsPaths();

        self::assertCount(0, $partitionPaths);
    }

    public function test_partitions_with_pattern_returns_empty() : void
    {
        $path = new WindowsPath('/path/*/file.txt');
        $partitions = $path->partitions();

        self::assertCount(0, $partitions);
    }

    public function test_path_manipulation() : void
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

    public function test_path_method() : void
    {
        $path = new WindowsPath('/path/file.txt');

        self::assertEquals('/path/file.txt', $path->path());
    }

    public function test_pathinfo_backslash_edge_case() : void
    {
        $path = new WindowsPath('/file.txt');
        $parent = $path->parentDirectory();

        self::assertEquals('/', $parent->path());
        self::assertEquals('file://', $parent->uri());
    }

    /**
     * @dataProvider patternProvider
     */
    public function test_pattern_logic(string $pattern, string $filename, bool $expected) : void
    {
        $patternPath = new WindowsPath($pattern);
        $filePath = new WindowsPath($filename);

        self::assertEquals($expected, $patternPath->matches($filePath));
    }

    public function test_pattern_methods_throw_exception() : void
    {
        $patternPath = new WindowsPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $patternPath->addPartitions(partition('group', 'a'));
    }

    public function test_pattern_parent_directory_throws_exception() : void
    {
        $patternPath = new WindowsPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't take directory from path pattern.");

        $patternPath->parentDirectory();
    }

    public function test_pattern_with_double_wildcard() : void
    {
        $pattern = new WindowsPath('/path/**/file.txt');
        $file = new WindowsPath('/path/deeply/nested/file.txt');

        self::assertTrue($pattern->matches($file));
    }

    public function test_protocol_operations() : void
    {
        $path = new WindowsPath('custom://path/to/file.txt');

        self::assertEquals('custom', $path->protocol()->name);
        self::assertTrue($path->protocol()->is('custom'));
        self::assertFalse($path->protocol()->is('file'));
    }

    public function test_protocol_scheme_method() : void
    {
        $path = new WindowsPath('s3://bucket/file.txt');

        self::assertEquals('s3://', $path->protocol()->scheme());
    }

    public function test_randomization() : void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $randomized = $path->randomize();

        self::assertStringStartsWith('/path/to/file_', $randomized->path());
        self::assertStringEndsWith('.txt', $randomized->path());
        self::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_randomize_without_extension() : void
    {
        $path = new WindowsPath('/path/to/file');
        $randomized = $path->randomize();

        self::assertStringStartsWith('/path/to/file_', $randomized->path());
        self::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_realpath_with_absolute_path() : void
    {
        $path = WindowsPath::realpath('C:/absolute/path/file.txt');
        self::assertEquals('C:/absolute/path/file.txt', $path->path());
    }

    public function test_realpath_with_non_file_scheme() : void
    {
        $path = WindowsPath::realpath('s3://bucket/key.txt');

        self::assertEquals('s3://bucket/key.txt', $path->uri());
    }

    public function test_root_directory_name_with_drive() : void
    {
        $path = new WindowsPath('C:/folder/file.txt');
        self::assertEquals('folder', $path->rootDirectoryName());
    }

    public function test_root_directory_name_with_single_file() : void
    {
        $path1 = new WindowsPath('/file.txt');
        self::assertNull($path1->rootDirectoryName());

        $path2 = new WindowsPath('C:/file.txt');
        self::assertEquals('file.txt', $path2->rootDirectoryName());
    }

    public function test_root_directory_name_with_unc() : void
    {
        $path = new WindowsPath('//server/share/folder/file.txt');
        self::assertEquals('server', $path->rootDirectoryName());
    }

    public function test_root_partition_edge_case() : void
    {
        $path = new WindowsPath('/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        self::assertEquals('/group=a/file.txt', $partitioned->path());
        self::assertEquals('file://group=a/file.txt', $partitioned->uri());
    }

    public function test_set_extension_edge_case() : void
    {
        $path = new WindowsPath('file');
        $newPath = $path->setExtension('txt');

        self::assertEquals('//file.txt', $newPath->path());
    }

    public function test_set_extension_without_existing_extension() : void
    {
        $path = new WindowsPath('/path/to/file');
        $newPath = $path->setExtension('txt');

        self::assertEquals('/path/to/file.txt', $newPath->path());
    }

    public function test_skip_directories() : void
    {
        $path = new WindowsPath('C:/var/www/index.html');

        $skipped1 = $path->skipDirectories(1);
        self::assertNotNull($skipped1);
        self::assertEquals('file://C:/www/index.html', $skipped1->uri());

        $skipped2 = $path->skipDirectories(2);
        self::assertNotNull($skipped2);
        self::assertEquals('file://C:/index.html', $skipped2->uri());

        $skipped3 = $path->skipDirectories(3);
        self::assertNull($skipped3);
    }

    public function test_skip_directories_with_empty_path_after_drive() : void
    {
        $path = new WindowsPath('C:/');

        $skipped = $path->skipDirectories(1);
        self::assertNull($skipped);
    }

    public function test_skip_directories_with_negative_count_throws_exception() : void
    {
        $path = new WindowsPath('/path/to/file.txt');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The number of folders to skip must be non-negative.');

        $path->skipDirectories(-1);
    }

    public function test_skip_directories_without_drive() : void
    {
        $path = new WindowsPath('/var/www/html/index.html');

        $skipped1 = $path->skipDirectories(1);
        self::assertNotNull($skipped1);
        self::assertEquals('/www/html/index.html', $skipped1->path());

        $skipped2 = $path->skipDirectories(2);
        self::assertNotNull($skipped2);
        self::assertEquals('/html/index.html', $skipped2->path());

        $skipped3 = $path->skipDirectories(3);
        self::assertNotNull($skipped3);
        self::assertEquals('/index.html', $skipped3->path());

        $skipped4 = $path->skipDirectories(4);
        self::assertNull($skipped4);
    }

    public function test_skip_directories_zero_count() : void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $result = $path->skipDirectories(0);

        self::assertNotNull($result);
        self::assertEquals('/path/to/file.txt', $result->path());
    }

    public function test_static_part_at_root_with_pattern() : void
    {
        $pattern = new WindowsPath('/*');
        $staticPart = $pattern->staticPart();

        self::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_empty_after_root() : void
    {
        $path = new WindowsPath('/*');
        $staticPart = $path->staticPart();

        self::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_with_pattern() : void
    {
        $path = new WindowsPath('/path/to/*/file.txt');
        $staticPart = $path->staticPart();

        self::assertEquals('/path/to', $staticPart->path());
    }

    public function test_static_part_with_pattern_at_start() : void
    {
        $path = new WindowsPath('/*/to/file.txt');
        $staticPart = $path->staticPart();

        self::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_without_pattern() : void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $staticPart = $path->staticPart();

        self::assertEquals('/path/to/file.txt', $staticPart->path());
        self::assertTrue($path->isEqual($staticPart));
    }

    public function test_suffix_with_root_path() : void
    {
        $path = new WindowsPath('/');
        $suffixed = $path->suffix('file.txt');

        self::assertEquals('/file.txt', $suffixed->path());
    }

    public function test_unc_path_handling() : void
    {
        $path = new WindowsPath('//server/share/file.txt');

        self::assertEquals('//server/share/file.txt', $path->path());
        self::assertEquals('file://server/share/file.txt', $path->uri());
        self::assertEquals('server', $path->rootDirectoryName());
    }

    public function test_uri_method() : void
    {
        $path = new WindowsPath('/path/file.txt');

        self::assertEquals('file://path/file.txt', $path->uri());
    }

    public function test_windows_backslash_normalization() : void
    {
        $path = new WindowsPath('C:\\path\\to\\file.txt');

        self::assertEquals('C:/path/to/file.txt', $path->path());
        self::assertEquals('file://C:/path/to/file.txt', $path->uri());
    }
}
