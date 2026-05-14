<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path\Options;
use Flow\Filesystem\Path\WindowsPath;
use Flow\Filesystem\Tests\Unit\PathTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Filesystem\DSL\partition;

final class WindowsPathTest extends PathTestCase
{
    public static function partitionProvider(): \Generator
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

    public static function pathProvider(): \Generator
    {
        yield 'file scheme' => ['file://path/to/file.txt', '/path/to/file.txt', 'file'];
        yield 'custom scheme' => ['flow-file://path/to/file.txt', '/path/to/file.txt', 'flow-file'];
        yield 'no scheme' => ['/path/to/file.txt', '/path/to/file.txt', 'file'];
        yield 'relative path' => ['path/to/file.txt', '/path/to/file.txt', 'file'];
    }

    public static function patternProvider(): \Generator
    {
        yield 'exact match' => ['/file.csv', '/file.csv', true];
        yield 'wildcard match' => ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield 'wildcard no match' => ['/nested/folder/*/file.csv', '/nested/folder/any/other.csv', false];
        yield 'recursive wildcard' => ['/nested/**/file.csv', '/nested/very/deep/file.csv', true];
        yield 'question mark' => ['/nested/fil?.csv', '/nested/file.csv', true];
    }

    public function test_absolute_path_detection(): void
    {
        $path1 = new WindowsPath('C:\\test\\file.txt');
        static::assertEquals('C:/test/file.txt', $path1->path());

        $path2 = new WindowsPath('\\\\server\\share\\file.txt');
        static::assertEquals('//server/share/file.txt', $path2->path());
    }

    public function test_add_partitions_complex_drive_path(): void
    {
        $path = new WindowsPath('C:/some/path/file.txt');
        $partitioned = $path->addPartitions(partition('year', '2023'));

        static::assertEquals('C:/some/path/year=2023/file.txt', $partitioned->path());
    }

    public function test_basename_operations(): void
    {
        $path = new WindowsPath('/path/to/file.txt');

        static::assertEquals('file.txt', $path->basename());
        static::assertEquals('file', $path->filename());

        $prefixed = $path->basenamePrefix('prefix_');
        static::assertEquals('/path/to/prefix_file.txt', $prefixed->path());
    }

    public function test_bracket_pattern_matching(): void
    {
        $pattern = new WindowsPath('/path/file[123].txt');
        $file1 = new WindowsPath('/path/file1.txt');
        $file2 = new WindowsPath('/path/file2.txt');
        $fileA = new WindowsPath('/path/fileA.txt');

        static::assertTrue($pattern->matches($file1));
        static::assertTrue($pattern->matches($file2));
        static::assertFalse($pattern->matches($fileA));
    }

    public function test_complex_pattern_matching(): void
    {
        $pattern = new WindowsPath('/path/file*.txt');
        $file1 = new WindowsPath('/path/file1.txt');
        $file2 = new WindowsPath('/path/file2.txt');
        $file3 = new WindowsPath('/path/other.txt');

        static::assertTrue($pattern->matches($file1));
        static::assertTrue($pattern->matches($file2));
        static::assertFalse($pattern->matches($file3));
    }

    public function test_constructor_with_options_object(): void
    {
        $options = new Options(['test' => 'value']);
        $path = new WindowsPath('/file.txt', $options);

        static::assertEquals(['test' => 'value'], $path->options()->toArray());
    }

    public function test_drive_partition_handling(): void
    {
        $path = new WindowsPath('C:/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        static::assertEquals('C:/group=a/file.txt', $partitioned->path());
        static::assertEquals('file://C:/group=a/file.txt', $partitioned->uri());
    }

    public function test_drive_root_handling(): void
    {
        $path = new WindowsPath('C:/file.txt');

        static::assertEquals('C:/file.txt', $path->path());
        static::assertEquals('file://C:/file.txt', $path->uri());
        static::assertEquals('file.txt', $path->basename());
        static::assertEquals('file', $path->filename());
        static::assertEquals('txt', $path->extension());
    }

    public function test_drive_skip_directories(): void
    {
        $path = new WindowsPath('C:/var/www/index.html');

        $skipped1 = $path->skipDirectories(1);
        static::assertNotNull($skipped1);
        static::assertEquals('C:/www/index.html', $skipped1->path());

        $skipped2 = $path->skipDirectories(2);
        static::assertNotNull($skipped2);
        static::assertEquals('C:/index.html', $skipped2->path());

        $skipped3 = $path->skipDirectories(3);
        static::assertNull($skipped3);
    }

    public function test_empty_path_normalization(): void
    {
        $path = new WindowsPath('');
        static::assertEquals('/', $path->path());
    }

    public function test_ends_with(): void
    {
        $path = new WindowsPath('/path/to/file.txt');

        static::assertTrue($path->endsWith('.txt'));
        static::assertTrue($path->endsWith('file.txt'));
        static::assertTrue($path->endsWith('/file.txt'));
        static::assertFalse($path->endsWith('.csv'));
        static::assertFalse($path->endsWith('other.txt'));
    }

    public function test_extension_case_insensitive(): void
    {
        $path = new WindowsPath('/path/to/file.TXT');

        static::assertEquals('txt', $path->extension());
    }

    public function test_extension_operations(): void
    {
        $path = new WindowsPath('/path/to/file.txt');

        static::assertEquals('txt', $path->extension());

        $newExt = $path->setExtension('csv');
        static::assertEquals('/path/to/file.csv', $newExt->path());
        static::assertEquals('csv', $newExt->extension());
    }

    public function test_extension_with_no_extension_returns_false(): void
    {
        $path = new WindowsPath('/path/to/file');

        static::assertFalse($path->extension());
    }

    public function test_fnmatch_with_hidden_files(): void
    {
        $pattern = new WindowsPath('/*');
        $hidden = new WindowsPath('/.hidden');
        $normal = new WindowsPath('/visible');

        static::assertTrue($pattern->matches($normal));
        static::assertTrue($pattern->matches($hidden));
    }

    public function test_is_equal(): void
    {
        $path1 = new WindowsPath('/path/to/file.txt');
        $path2 = new WindowsPath('/path/to/file.txt');
        $path3 = new WindowsPath('/path/to/other.txt');

        static::assertTrue($path1->isEqual($path2));
        static::assertFalse($path1->isEqual($path3));
    }

    public function test_is_pattern_detection(): void
    {
        static::assertTrue((new WindowsPath('/path/*/file.txt'))->isPattern());
        static::assertTrue((new WindowsPath('/path/**/file.txt'))->isPattern());
        static::assertTrue((new WindowsPath('/path/file?.txt'))->isPattern());
        static::assertTrue((new WindowsPath('/path/file[abc].txt'))->isPattern());
        static::assertTrue((new WindowsPath('/path/file{a,b}.txt'))->isPattern());
        static::assertFalse((new WindowsPath('/path/file.txt'))->isPattern());
    }

    public function test_matches_non_pattern_exact_match(): void
    {
        $path1 = new WindowsPath('/path/to/file.txt');
        $path2 = new WindowsPath('/path/to/file.txt');
        $path3 = new WindowsPath('/path/to/other.txt');

        static::assertTrue($path1->matches($path2));
        static::assertFalse($path1->matches($path3));
    }

    public function test_matches_pattern_against_pattern_returns_false(): void
    {
        $pattern1 = new WindowsPath('/path/*/file.txt');
        $pattern2 = new WindowsPath('/path/*/other.txt');

        static::assertFalse($pattern1->matches($pattern2));
    }

    public function test_options_from_array(): void
    {
        $path = new WindowsPath('/file.txt', ['option1' => 'value1', 'option2' => 'value2']);

        static::assertEquals(['option1' => 'value1', 'option2' => 'value2'], $path->options()->toArray());
    }

    public function test_options_handling(): void
    {
        $options = new Options(['key' => 'value']);
        $path = new WindowsPath('/file.txt', $options);

        static::assertEquals(['key' => 'value'], $path->options()->toArray());
    }

    #[DataProvider('pathProvider')]
    public function test_os_agnostic_logic(string $input, string $expectedPath, string $expectedScheme): void
    {
        $path = new WindowsPath($input);

        static::assertEquals($expectedPath, $path->path());
        static::assertEquals($expectedScheme, $path->protocol());
    }

    /**
     * @param non-empty-array<int, array{name: string, value: string}> $partitionData
     */
    #[DataProvider('partitionProvider')]
    public function test_partition_logic(string $input, array $partitionData, string $expected): void
    {
        $path = new WindowsPath($input);
        $partitions = array_values(array_map(static fn($p) => partition($p['name'], $p['value']), $partitionData));
        $first = array_shift($partitions);
        static::assertNotNull($first);

        $result = $path->addPartitions($first, ...$partitions);

        static::assertEquals($expected, $result->path());
    }

    public function test_partitions_extraction(): void
    {
        $path = new WindowsPath('/path/country=US/region=west/city=LA/file.txt');
        $partitions = $path->partitions();

        static::assertCount(3, $partitions);

        $partitionArray = [];

        foreach ($partitions as $partition) {
            $partitionArray[$partition->name] = $partition->value;
        }

        static::assertEquals('US', $partitionArray['country']);
        static::assertEquals('west', $partitionArray['region']);
        static::assertEquals('LA', $partitionArray['city']);
    }

    public function test_partitions_paths(): void
    {
        $path = new WindowsPath('/base/country=US/region=west/city=LA/file.txt');
        $partitionPaths = $path->partitionsPaths();

        static::assertCount(3, $partitionPaths);
        static::assertEquals('/base/country=US', $partitionPaths[0]->path());
        static::assertEquals('/base/country=US/region=west', $partitionPaths[1]->path());
        static::assertEquals('/base/country=US/region=west/city=LA', $partitionPaths[2]->path());
    }

    public function test_partitions_paths_with_root_directory(): void
    {
        $path = new WindowsPath('/country=US/region=west/file.txt');
        $partitionPaths = $path->partitionsPaths();

        static::assertCount(2, $partitionPaths);
        static::assertEquals('/country=US', $partitionPaths[0]->path());
        static::assertEquals('/country=US/region=west', $partitionPaths[1]->path());
    }

    public function test_partitions_paths_without_partitions(): void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $partitionPaths = $path->partitionsPaths();

        static::assertCount(0, $partitionPaths);
    }

    public function test_partitions_with_pattern_returns_empty(): void
    {
        $path = new WindowsPath('/path/*/file.txt');
        $partitions = $path->partitions();

        static::assertCount(0, $partitions);
    }

    public function test_path_manipulation(): void
    {
        $path = new WindowsPath('/path/to/file.txt');

        // Test suffix
        $suffixed = $path->suffix('subdir/newfile.csv');
        static::assertEquals('/path/to/file.txt/subdir/newfile.csv', $suffixed->path());

        // Test parent directory
        $parent = $path->parentDirectory();
        static::assertEquals('/path/to', $parent->path());

        // Test root directory name
        static::assertEquals('path', $path->rootDirectoryName());
    }

    public function test_path_method(): void
    {
        $path = new WindowsPath('/path/file.txt');

        static::assertEquals('/path/file.txt', $path->path());
    }

    public function test_pathinfo_backslash_edge_case(): void
    {
        $path = new WindowsPath('/file.txt');
        $parent = $path->parentDirectory();

        static::assertEquals('/', $parent->path());
        static::assertEquals('file://', $parent->uri());
    }

    #[DataProvider('patternProvider')]
    public function test_pattern_logic(string $pattern, string $filename, bool $expected): void
    {
        $patternPath = new WindowsPath($pattern);
        $filePath = new WindowsPath($filename);

        static::assertEquals($expected, $patternPath->matches($filePath));
    }

    public function test_pattern_methods_throw_exception(): void
    {
        $patternPath = new WindowsPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $patternPath->addPartitions(partition('group', 'a'));
    }

    public function test_pattern_parent_directory_throws_exception(): void
    {
        $patternPath = new WindowsPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't take directory from path pattern.");

        $patternPath->parentDirectory();
    }

    public function test_pattern_with_double_wildcard(): void
    {
        $pattern = new WindowsPath('/path/**/file.txt');
        $file = new WindowsPath('/path/deeply/nested/file.txt');

        static::assertTrue($pattern->matches($file));
    }

    public function test_protocol_operations(): void
    {
        $path = new WindowsPath('custom://path/to/file.txt');

        static::assertSame('custom', $path->protocol());
    }

    public function test_randomization(): void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $randomized = $path->randomize();

        static::assertStringStartsWith('/path/to/file_', $randomized->path());
        static::assertStringEndsWith('.txt', $randomized->path());
        static::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_randomize_without_extension(): void
    {
        $path = new WindowsPath('/path/to/file');
        $randomized = $path->randomize();

        static::assertStringStartsWith('/path/to/file_', $randomized->path());
        static::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_realpath_with_absolute_path(): void
    {
        $path = WindowsPath::realpath('C:/absolute/path/file.txt');
        static::assertEquals('C:/absolute/path/file.txt', $path->path());
    }

    public function test_realpath_with_file_scheme_strips_protocol_prefix(): void
    {
        $path = WindowsPath::realpath('file:///C:/tmp/foo.txt');

        static::assertSame('file', $path->protocol());
        static::assertSame('C:/tmp/foo.txt', $path->path());
    }

    public function test_realpath_with_non_file_scheme(): void
    {
        $path = WindowsPath::realpath('s3://bucket/key.txt');

        static::assertEquals('s3://bucket/key.txt', $path->uri());
    }

    public function test_root_directory_name_with_drive(): void
    {
        $path = new WindowsPath('C:/folder/file.txt');
        static::assertEquals('folder', $path->rootDirectoryName());
    }

    public function test_root_directory_name_with_single_file(): void
    {
        $path1 = new WindowsPath('/file.txt');
        static::assertNull($path1->rootDirectoryName());

        $path2 = new WindowsPath('C:/file.txt');
        static::assertEquals('file.txt', $path2->rootDirectoryName());
    }

    public function test_root_directory_name_with_unc(): void
    {
        $path = new WindowsPath('//server/share/folder/file.txt');
        static::assertEquals('server', $path->rootDirectoryName());
    }

    public function test_root_partition_edge_case(): void
    {
        $path = new WindowsPath('/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        static::assertEquals('/group=a/file.txt', $partitioned->path());
        static::assertEquals('file://group=a/file.txt', $partitioned->uri());
    }

    public function test_set_extension_edge_case(): void
    {
        $path = new WindowsPath('file');
        $newPath = $path->setExtension('txt');

        static::assertEquals('//file.txt', $newPath->path());
    }

    public function test_set_extension_without_existing_extension(): void
    {
        $path = new WindowsPath('/path/to/file');
        $newPath = $path->setExtension('txt');

        static::assertEquals('/path/to/file.txt', $newPath->path());
    }

    public function test_skip_directories(): void
    {
        $path = new WindowsPath('C:/var/www/index.html');

        $skipped1 = $path->skipDirectories(1);
        static::assertNotNull($skipped1);
        static::assertEquals('file://C:/www/index.html', $skipped1->uri());

        $skipped2 = $path->skipDirectories(2);
        static::assertNotNull($skipped2);
        static::assertEquals('file://C:/index.html', $skipped2->uri());

        $skipped3 = $path->skipDirectories(3);
        static::assertNull($skipped3);
    }

    public function test_skip_directories_with_empty_path_after_drive(): void
    {
        $path = new WindowsPath('C:/');

        $skipped = $path->skipDirectories(1);
        static::assertNull($skipped);
    }

    public function test_skip_directories_with_negative_count_throws_exception(): void
    {
        $path = new WindowsPath('/path/to/file.txt');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('The number of folders to skip must be non-negative.');

        $path->skipDirectories(-1);
    }

    public function test_skip_directories_without_drive(): void
    {
        $path = new WindowsPath('/var/www/html/index.html');

        $skipped1 = $path->skipDirectories(1);
        static::assertNotNull($skipped1);
        static::assertEquals('/www/html/index.html', $skipped1->path());

        $skipped2 = $path->skipDirectories(2);
        static::assertNotNull($skipped2);
        static::assertEquals('/html/index.html', $skipped2->path());

        $skipped3 = $path->skipDirectories(3);
        static::assertNotNull($skipped3);
        static::assertEquals('/index.html', $skipped3->path());

        $skipped4 = $path->skipDirectories(4);
        static::assertNull($skipped4);
    }

    public function test_skip_directories_zero_count(): void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $result = $path->skipDirectories(0);

        static::assertNotNull($result);
        static::assertEquals('/path/to/file.txt', $result->path());
    }

    public function test_static_part_at_root_with_pattern(): void
    {
        $pattern = new WindowsPath('/*');
        $staticPart = $pattern->staticPart();

        static::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_empty_after_root(): void
    {
        $path = new WindowsPath('/*');
        $staticPart = $path->staticPart();

        static::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_with_pattern(): void
    {
        $path = new WindowsPath('/path/to/*/file.txt');
        $staticPart = $path->staticPart();

        static::assertEquals('/path/to', $staticPart->path());
    }

    public function test_static_part_with_pattern_at_start(): void
    {
        $path = new WindowsPath('/*/to/file.txt');
        $staticPart = $path->staticPart();

        static::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_without_pattern(): void
    {
        $path = new WindowsPath('/path/to/file.txt');
        $staticPart = $path->staticPart();

        static::assertEquals('/path/to/file.txt', $staticPart->path());
        static::assertTrue($path->isEqual($staticPart));
    }

    public function test_suffix_with_root_path(): void
    {
        $path = new WindowsPath('/');
        $suffixed = $path->suffix('file.txt');

        static::assertEquals('/file.txt', $suffixed->path());
    }

    public function test_unc_path_handling(): void
    {
        $path = new WindowsPath('//server/share/file.txt');

        static::assertEquals('//server/share/file.txt', $path->path());
        static::assertEquals('file://server/share/file.txt', $path->uri());
        static::assertEquals('server', $path->rootDirectoryName());
    }

    public function test_uri_method(): void
    {
        $path = new WindowsPath('/path/file.txt');

        static::assertEquals('file://path/file.txt', $path->uri());
    }

    public function test_windows_backslash_normalization(): void
    {
        $path = new WindowsPath('C:\\path\\to\\file.txt');

        static::assertEquals('C:/path/to/file.txt', $path->path());
        static::assertEquals('file://C:/path/to/file.txt', $path->uri());
    }
}
