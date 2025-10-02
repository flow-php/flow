<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use function Flow\Filesystem\DSL\{partition};
use Flow\Filesystem\Exception\{InvalidArgumentException};
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

    public function test_absolute_path_handling() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertEquals('/path/to/file.txt', $path->path());
        self::assertEquals('file://path/to/file.txt', $path->uri());
        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());
        self::assertEquals('txt', $path->extension());
    }

    public function test_basename_operations() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertEquals('file.txt', $path->basename());
        self::assertEquals('file', $path->filename());

        $prefixed = $path->basenamePrefix('prefix_');
        self::assertEquals('/path/to/prefix_file.txt', $prefixed->path());
    }

    public function test_bracket_pattern_matching() : void
    {
        $pattern = new UnixPath('/path/file[123].txt');
        $file1 = new UnixPath('/path/file1.txt');
        $file2 = new UnixPath('/path/file2.txt');
        $fileA = new UnixPath('/path/fileA.txt');

        self::assertTrue($pattern->matches($file1));
        self::assertTrue($pattern->matches($file2));
        self::assertFalse($pattern->matches($fileA));
    }

    public function test_complex_pattern_matching() : void
    {
        $pattern = new UnixPath('/path/file*.txt');
        $file1 = new UnixPath('/path/file1.txt');
        $file2 = new UnixPath('/path/file2.txt');
        $file3 = new UnixPath('/path/other.txt');

        self::assertTrue($pattern->matches($file1));
        self::assertTrue($pattern->matches($file2));
        self::assertFalse($pattern->matches($file3));
    }

    public function test_constructor_with_options_object() : void
    {
        $options = new Options(['test' => 'value']);
        $path = new UnixPath('/file.txt', $options);

        self::assertEquals(['test' => 'value'], $path->options()->toArray());
    }

    public function test_current_directory_handling() : void
    {
        $path = new UnixPath('./file.txt');

        self::assertEquals('/./file.txt', $path->path());

        $parent = $path->parentDirectory();
        self::assertEquals('/.', $parent->path());
    }

    public function test_empty_path_normalization() : void
    {
        $path = new UnixPath('');
        self::assertEquals('/', $path->path());
    }

    public function test_ends_with() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertTrue($path->endsWith('.txt'));
        self::assertTrue($path->endsWith('file.txt'));
        self::assertTrue($path->endsWith('/file.txt'));
        self::assertFalse($path->endsWith('.csv'));
        self::assertFalse($path->endsWith('other.txt'));
    }

    public function test_extension_case_insensitive() : void
    {
        $path = new UnixPath('/path/to/file.TXT');

        self::assertEquals('txt', $path->extension());
    }

    public function test_extension_operations() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        self::assertEquals('txt', $path->extension());

        $newExt = $path->setExtension('csv');
        self::assertEquals('/path/to/file.csv', $newExt->path());
        self::assertEquals('csv', $newExt->extension());
    }

    public function test_extension_with_no_extension_returns_false() : void
    {
        $path = new UnixPath('/path/to/file');

        self::assertFalse($path->extension());
    }

    public function test_fnmatch_with_hidden_files() : void
    {
        $pattern = new UnixPath('/*');
        $hidden = new UnixPath('/.hidden');
        $normal = new UnixPath('/visible');

        self::assertTrue($pattern->matches($normal));
        self::assertTrue($pattern->matches($hidden));
    }

    public function test_is_equal() : void
    {
        $path1 = new UnixPath('/path/to/file.txt');
        $path2 = new UnixPath('/path/to/file.txt');
        $path3 = new UnixPath('/path/to/other.txt');

        self::assertTrue($path1->isEqual($path2));
        self::assertFalse($path1->isEqual($path3));
    }

    public function test_is_pattern_detection() : void
    {
        self::assertTrue((new UnixPath('/path/*/file.txt'))->isPattern());
        self::assertTrue((new UnixPath('/path/**/file.txt'))->isPattern());
        self::assertTrue((new UnixPath('/path/file?.txt'))->isPattern());
        self::assertTrue((new UnixPath('/path/file[abc].txt'))->isPattern());
        self::assertTrue((new UnixPath('/path/file{a,b}.txt'))->isPattern());
        self::assertFalse((new UnixPath('/path/file.txt'))->isPattern());
    }

    public function test_matches_non_pattern_exact_match() : void
    {
        $path1 = new UnixPath('/path/to/file.txt');
        $path2 = new UnixPath('/path/to/file.txt');
        $path3 = new UnixPath('/path/to/other.txt');

        self::assertTrue($path1->matches($path2));
        self::assertFalse($path1->matches($path3));
    }

    public function test_matches_pattern_against_pattern_returns_false() : void
    {
        $pattern1 = new UnixPath('/path/*/file.txt');
        $pattern2 = new UnixPath('/path/*/other.txt');

        self::assertFalse($pattern1->matches($pattern2));
    }

    public function test_options_from_array() : void
    {
        $path = new UnixPath('/file.txt', ['option1' => 'value1', 'option2' => 'value2']);

        self::assertEquals(['option1' => 'value1', 'option2' => 'value2'], $path->options()->toArray());
    }

    public function test_options_handling() : void
    {
        $options = new Options(['key' => 'value']);
        $path = new UnixPath('/file.txt', $options);

        self::assertEquals(['key' => 'value'], $path->options()->toArray());
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

    public function test_parent_directory_edge_cases() : void
    {
        $path1 = new UnixPath('.');
        self::assertEquals('/', $path1->parentDirectory()->path());

        $path2 = new UnixPath('\\');
        self::assertEquals('/', $path2->parentDirectory()->path());
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

    public function test_partitions_paths_with_root_directory() : void
    {
        $path = new UnixPath('/country=US/region=west/file.txt');
        $partitionPaths = $path->partitionsPaths();

        self::assertCount(2, $partitionPaths);
        self::assertEquals('file://country=US', $partitionPaths[0]->uri());
        self::assertEquals('file://country=US/region=west', $partitionPaths[1]->uri());
    }

    public function test_partitions_paths_without_partitions() : void
    {
        $path = new UnixPath('/path/to/file.txt');
        $partitionPaths = $path->partitionsPaths();

        self::assertCount(0, $partitionPaths);
    }

    public function test_partitions_with_pattern_returns_empty() : void
    {
        $path = new UnixPath('/path/*/file.txt');
        $partitions = $path->partitions();

        self::assertCount(0, $partitions);
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

    public function test_path_method() : void
    {
        $path = new UnixPath('/path/file.txt');

        self::assertEquals('/path/file.txt', $path->path());
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

    public function test_pattern_methods_throw_exception() : void
    {
        $patternPath = new UnixPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $patternPath->addPartitions(partition('group', 'a'));
    }

    public function test_pattern_parent_directory_throws_exception() : void
    {
        $patternPath = new UnixPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't take directory from path pattern.");

        $patternPath->parentDirectory();
    }

    public function test_pattern_with_double_wildcard() : void
    {
        $pattern = new UnixPath('/path/**/file.txt');
        $file = new UnixPath('/path/deeply/nested/file.txt');

        self::assertTrue($pattern->matches($file));
    }

    public function test_protocol_operations() : void
    {
        $path = new UnixPath('custom://path/to/file.txt');

        self::assertEquals('custom', $path->protocol()->name);
        self::assertTrue($path->protocol()->is('custom'));
        self::assertFalse($path->protocol()->is('file'));
    }

    public function test_protocol_scheme_method() : void
    {
        $path = new UnixPath('s3://bucket/file.txt');

        self::assertEquals('s3://', $path->protocol()->scheme());
    }

    public function test_randomization() : void
    {
        $path = new UnixPath('/path/to/file.txt');
        $randomized = $path->randomize();

        self::assertStringStartsWith('/path/to/file_', $randomized->path());
        self::assertStringEndsWith('.txt', $randomized->path());
        self::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_randomize_without_extension() : void
    {
        $path = new UnixPath('/path/to/file');
        $randomized = $path->randomize();

        self::assertStringStartsWith('/path/to/file_', $randomized->path());
        self::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_realpath_multiple_parent_navigation() : void
    {
        $path = UnixPath::realpath('/a/b/c/../../d/../e/file.txt');
        self::assertEquals('/a/e/file.txt', $path->path());
    }

    public function test_realpath_too_many_parent_navigations() : void
    {
        $path = UnixPath::realpath('/a/../../../file.txt');
        self::assertEquals('/file.txt', $path->path());
    }

    public function test_realpath_with_absolute_path() : void
    {
        $path = UnixPath::realpath('/absolute/path/file.txt');
        self::assertEquals('/absolute/path/file.txt', $path->path());
    }

    public function test_realpath_with_current_directory_dots() : void
    {
        $path = UnixPath::realpath('/path/./to/./file.txt');
        self::assertEquals('/path/to/file.txt', $path->path());
    }

    public function test_realpath_with_non_file_scheme() : void
    {
        $path = UnixPath::realpath('s3://bucket/key.txt');

        self::assertEquals('s3://bucket/key.txt', $path->uri());
    }

    public function test_realpath_with_path_resolution() : void
    {
        $path = UnixPath::realpath('/path/to/../file.txt');
        self::assertEquals('/path/file.txt', $path->path());
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

    public function test_root_directory_name_with_single_file() : void
    {
        $path1 = new UnixPath('/file.txt');
        self::assertNull($path1->rootDirectoryName());

        $path2 = new UnixPath('/folder/file.txt');
        self::assertEquals('folder', $path2->rootDirectoryName());
    }

    public function test_root_partition_handling() : void
    {
        $path = new UnixPath('/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        self::assertEquals('/group=a/file.txt', $partitioned->path());
        self::assertEquals('file://group=a/file.txt', $partitioned->uri());
    }

    public function test_set_extension_without_existing_extension() : void
    {
        $path = new UnixPath('/path/to/file');
        $newPath = $path->setExtension('txt');

        self::assertEquals('/path/to/file.txt', $newPath->path());
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

    public function test_skip_directories_with_negative_count_throws_exception() : void
    {
        $path = new UnixPath('/path/to/file.txt');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The number of folders to skip must be non-negative.');

        $path->skipDirectories(-1);
    }

    public function test_skip_directories_zero_count() : void
    {
        $path = new UnixPath('/path/to/file.txt');
        $result = $path->skipDirectories(0);

        self::assertNotNull($result);
        self::assertEquals('/path/to/file.txt', $result->path());
    }

    public function test_static_part_at_root_with_pattern() : void
    {
        $pattern = new UnixPath('/*');
        $staticPart = $pattern->staticPart();

        self::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_extraction() : void
    {
        $pattern = new UnixPath('/static/part/*/dynamic/part');
        $staticPart = $pattern->staticPart();

        self::assertEquals('/static/part', $staticPart->path());
    }

    public function test_static_part_with_pattern_at_start() : void
    {
        $path = new UnixPath('/*/to/file.txt');
        $staticPart = $path->staticPart();

        self::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_without_pattern() : void
    {
        $path = new UnixPath('/path/to/file.txt');
        $staticPart = $path->staticPart();

        self::assertEquals('/path/to/file.txt', $staticPart->path());
        self::assertTrue($path->isEqual($staticPart));
    }

    public function test_suffix_with_root_path() : void
    {
        $path = new UnixPath('/');
        $suffixed = $path->suffix('file.txt');

        self::assertEquals('/file.txt', $suffixed->path());
    }

    public function test_uri_method() : void
    {
        $path = new UnixPath('/path/file.txt');

        self::assertEquals('file://path/file.txt', $path->uri());
    }
}
