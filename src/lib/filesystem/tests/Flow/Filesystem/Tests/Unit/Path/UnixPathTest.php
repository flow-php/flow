<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path\Options;
use Flow\Filesystem\Path\UnixPath;
use Flow\Filesystem\Tests\Unit\PathTestCase;
use Generator;
use InvalidArgumentException as BaseInvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Filesystem\DSL\partition;

final class UnixPathTest extends PathTestCase
{
    public static function partitionProvider(): Generator
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

    public static function pathProvider(): Generator
    {
        yield 'file scheme' => ['file://path/to/file.txt', '/path/to/file.txt', 'file'];
        yield 'custom scheme' => ['flow-file://path/to/file.txt', '/path/to/file.txt', 'flow-file'];
        yield 'no scheme' => ['/path/to/file.txt', '/path/to/file.txt', 'file'];
        yield 'relative path' => ['path/to/file.txt', '/path/to/file.txt', 'file'];
    }

    public static function patternProvider(): Generator
    {
        yield 'exact match' => ['/file.csv', '/file.csv', true];
        yield 'wildcard match' => ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield 'wildcard no match' => ['/nested/folder/*/file.csv', '/nested/folder/any/other.csv', false];
        yield 'recursive wildcard' => ['/nested/**/file.csv', '/nested/very/deep/file.csv', true];
        yield 'question mark' => ['/nested/fil?.csv', '/nested/file.csv', true];
    }

    public function test_absolute_path_handling(): void
    {
        $path = new UnixPath('/path/to/file.txt');

        static::assertEquals('/path/to/file.txt', $path->path());
        static::assertEquals('file://path/to/file.txt', $path->uri());
        static::assertEquals('file.txt', $path->basename());
        static::assertEquals('file', $path->filename());
        static::assertEquals('txt', $path->extension());
    }

    public function test_add_partitions_consuming_all_partitions_into_placeholders(): void
    {
        $path = new UnixPath('/output/{year}_{month}.csv');

        static::assertEquals(
            '/output/2024_03.csv',
            $path->addPartitions(partition('year', '2024'), partition('month', '03'))->path(),
        );
    }

    public function test_add_partitions_with_placeholder_and_remaining_partitions(): void
    {
        $path = new UnixPath('/output/{order-name}.csv');

        static::assertEquals(
            '/output/order-year=2024/123456-PL.csv',
            $path->addPartitions(partition('order-year', '2024'), partition('order-name', '123456-PL'))->path(),
        );
    }

    public function test_add_partitions_with_placeholder_in_directory(): void
    {
        $path = new UnixPath('/output/{year}/file.csv');

        static::assertEquals(
            '/output/2024/country=PL/file.csv',
            $path->addPartitions(partition('year', '2024'), partition('country', 'PL'))->path(),
        );
    }

    public function test_add_partitions_with_placeholder_next_to_glob_pattern(): void
    {
        $path = new UnixPath('/output/*/{order-name}.csv');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $path->addPartitions(partition('order-name', '123456-PL'));
    }

    public function test_add_partitions_with_repeated_placeholder(): void
    {
        $path = new UnixPath('/output/{name}/{name}.csv');

        static::assertEquals('/output/abc/abc.csv', $path->addPartitions(partition('name', 'abc'))->path());
    }

    public function test_add_partitions_with_unresolved_placeholder(): void
    {
        $path = new UnixPath('/output/{order-name}.csv');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "Path partition placeholder {order-name} does not match any partition, available partitions: 'order-year'",
        );

        $path->addPartitions(partition('order-year', '2024'));
    }

    public function test_basename_operations(): void
    {
        $path = new UnixPath('/path/to/file.txt');

        static::assertEquals('file.txt', $path->basename());
        static::assertEquals('file', $path->filename());

        $prefixed = $path->basenamePrefix('prefix_');
        static::assertEquals('/path/to/prefix_file.txt', $prefixed->path());
    }

    public function test_bracket_pattern_matching(): void
    {
        $pattern = new UnixPath('/path/file[123].txt');
        $file1 = new UnixPath('/path/file1.txt');
        $file2 = new UnixPath('/path/file2.txt');
        $fileA = new UnixPath('/path/fileA.txt');

        static::assertTrue($pattern->matches($file1));
        static::assertTrue($pattern->matches($file2));
        static::assertFalse($pattern->matches($fileA));
    }

    public function test_complex_pattern_matching(): void
    {
        $pattern = new UnixPath('/path/file*.txt');
        $file1 = new UnixPath('/path/file1.txt');
        $file2 = new UnixPath('/path/file2.txt');
        $file3 = new UnixPath('/path/other.txt');

        static::assertTrue($pattern->matches($file1));
        static::assertTrue($pattern->matches($file2));
        static::assertFalse($pattern->matches($file3));
    }

    public function test_constructor_with_options_object(): void
    {
        $options = new Options(['test' => 'value']);
        $path = new UnixPath('/file.txt', $options);

        static::assertEquals(['test' => 'value'], $path->options()->toArray());
    }

    public function test_current_directory_handling(): void
    {
        $path = new UnixPath('./file.txt');

        static::assertEquals('/./file.txt', $path->path());

        $parent = $path->parentDirectory();
        static::assertEquals('/.', $parent->path());
    }

    public function test_empty_path_normalization(): void
    {
        $path = new UnixPath('');
        static::assertEquals('/', $path->path());
    }

    public function test_ends_with(): void
    {
        $path = new UnixPath('/path/to/file.txt');

        static::assertTrue($path->endsWith('.txt'));
        static::assertTrue($path->endsWith('file.txt'));
        static::assertTrue($path->endsWith('/file.txt'));
        static::assertFalse($path->endsWith('.csv'));
        static::assertFalse($path->endsWith('other.txt'));
    }

    public function test_extension_case_insensitive(): void
    {
        $path = new UnixPath('/path/to/file.TXT');

        static::assertEquals('txt', $path->extension());
    }

    public function test_extension_operations(): void
    {
        $path = new UnixPath('/path/to/file.txt');

        static::assertEquals('txt', $path->extension());

        $newExt = $path->setExtension('csv');
        static::assertEquals('/path/to/file.csv', $newExt->path());
        static::assertEquals('csv', $newExt->extension());
    }

    public function test_extension_with_no_extension_returns_false(): void
    {
        $path = new UnixPath('/path/to/file');

        static::assertFalse($path->extension());
    }

    public function test_extract_placeholder_partitions(): void
    {
        $pattern = new UnixPath('/output/order-year=2024/{order-name}.csv');

        $partitions = $pattern->extractPlaceholderPartitions(new UnixPath('/output/order-year=2024/123456-PL.csv'));

        static::assertCount(1, $partitions);
        static::assertEquals('123456-PL', $partitions->get('order-name')->value);
    }

    public function test_extract_placeholder_partitions_from_not_matching_path(): void
    {
        $pattern = new UnixPath('/output/{order-name}.csv');

        static::assertCount(0, $pattern->extractPlaceholderPartitions(new UnixPath('/other/123456-PL.csv')));
    }

    public function test_extract_placeholder_partitions_from_path_without_placeholders(): void
    {
        $pattern = new UnixPath('/output/file.csv');

        static::assertCount(0, $pattern->extractPlaceholderPartitions(new UnixPath('/output/file.csv')));
    }

    public function test_extract_placeholder_partitions_keeps_a_value_carrying_a_reserved_character(): void
    {
        $pattern = new UnixPath('/output/{order-name}.csv');
        $partitions = $pattern->extractPlaceholderPartitions(new UnixPath('/output/foo=bar.csv'));

        // a reserved character used to make the value unusable; it is percent-encoded on the way out now
        static::assertCount(1, $partitions);
        static::assertSame('foo=bar', $partitions[0]->value);
        static::assertSame('order-name=foo%3Dbar', $partitions[0]->segment());
    }

    public function test_extract_placeholder_partitions_with_conflicting_values_of_repeated_placeholder(): void
    {
        $pattern = new UnixPath('/output/{name}/{name}.csv');

        static::assertCount(0, $pattern->extractPlaceholderPartitions(new UnixPath('/output/a/b.csv')));
        static::assertCount(1, $pattern->extractPlaceholderPartitions(new UnixPath('/output/a/a.csv')));
    }

    public function test_extract_placeholder_partitions_with_glob_and_placeholder(): void
    {
        $pattern = new UnixPath('/output/order-year=*/{order-name}.csv');

        $partitions = $pattern->extractPlaceholderPartitions(new UnixPath('/output/order-year=2024/123456-PL.csv'));

        static::assertCount(1, $partitions);
        static::assertEquals('123456-PL', $partitions->get('order-name')->value);
    }

    public function test_fnmatch_with_hidden_files(): void
    {
        $pattern = new UnixPath('/*');
        $hidden = new UnixPath('/.hidden');
        $normal = new UnixPath('/visible');

        static::assertTrue($pattern->matches($normal));
        static::assertTrue($pattern->matches($hidden));
    }

    public function test_glob(): void
    {
        static::assertEquals('/output/*.csv', (new UnixPath('/output/{order-name}.csv'))->glob());
        static::assertEquals('/output/*_*/file.csv', (new UnixPath('/output/{year}_{month}/file.csv'))->glob());
        static::assertEquals('/output/file.csv', (new UnixPath('/output/file.csv'))->glob());
    }

    public function test_is_equal(): void
    {
        $path1 = new UnixPath('/path/to/file.txt');
        $path2 = new UnixPath('/path/to/file.txt');
        $path3 = new UnixPath('/path/to/other.txt');

        static::assertTrue($path1->isEqual($path2));
        static::assertFalse($path1->isEqual($path3));
    }

    public function test_is_pattern_detection(): void
    {
        static::assertTrue((new UnixPath('/path/*/file.txt'))->isPattern());
        static::assertTrue((new UnixPath('/path/**/file.txt'))->isPattern());
        static::assertTrue((new UnixPath('/path/file?.txt'))->isPattern());
        static::assertTrue((new UnixPath('/path/file[abc].txt'))->isPattern());
        static::assertTrue((new UnixPath('/path/file{a,b}.txt'))->isPattern());
        static::assertFalse((new UnixPath('/path/file.txt'))->isPattern());
    }

    public function test_matches_non_pattern_exact_match(): void
    {
        $path1 = new UnixPath('/path/to/file.txt');
        $path2 = new UnixPath('/path/to/file.txt');
        $path3 = new UnixPath('/path/to/other.txt');

        static::assertTrue($path1->matches($path2));
        static::assertFalse($path1->matches($path3));
    }

    public function test_matches_pattern_against_pattern_returns_false(): void
    {
        $pattern1 = new UnixPath('/path/*/file.txt');
        $pattern2 = new UnixPath('/path/*/other.txt');

        static::assertFalse($pattern1->matches($pattern2));
    }

    public function test_matches_with_placeholders(): void
    {
        $pattern = new UnixPath('/output/{order-name}.csv');

        static::assertTrue($pattern->matches(new UnixPath('/output/123456-PL.csv')));
        static::assertFalse($pattern->matches(new UnixPath('/output/nested/123456-PL.csv')));
        static::assertFalse($pattern->matches(new UnixPath('/output/123456-PL.json')));
    }

    public function test_options_from_array(): void
    {
        $path = new UnixPath('/file.txt', ['option1' => 'value1', 'option2' => 'value2']);

        static::assertEquals(['option1' => 'value1', 'option2' => 'value2'], $path->options()->toArray());
    }

    public function test_options_handling(): void
    {
        $options = new Options(['key' => 'value']);
        $path = new UnixPath('/file.txt', $options);

        static::assertEquals(['key' => 'value'], $path->options()->toArray());
    }

    #[DataProvider('pathProvider')]
    public function test_os_agnostic_logic(string $input, string $expectedPath, string $expectedScheme): void
    {
        $path = new UnixPath($input);

        static::assertEquals($expectedPath, $path->path());
        static::assertEquals($expectedScheme, $path->protocol());
    }

    public function test_parent_directory_edge_cases(): void
    {
        $path1 = new UnixPath('.');
        static::assertEquals('/', $path1->parentDirectory()->path());

        $path2 = new UnixPath('\\');
        static::assertEquals('/', $path2->parentDirectory()->path());
    }

    public function test_partition_placeholders(): void
    {
        static::assertEquals([], (new UnixPath('/path/to/file.csv'))->partitionPlaceholders());
        static::assertEquals(['order-name'], (new UnixPath('/path/to/{order-name}.csv'))->partitionPlaceholders());
        static::assertEquals(
            ['year', 'month'],
            (new UnixPath('/path/{year}_{month}/file.csv'))->partitionPlaceholders(),
        );
        static::assertEquals(['name'], (new UnixPath('/path/{name}/{name}.csv'))->partitionPlaceholders());
        static::assertEquals([], (new UnixPath('/path/*/file.csv'))->partitionPlaceholders());
    }

    public function test_partitions_extraction(): void
    {
        $path = new UnixPath('/path/country=US/region=west/file.txt');
        $partitions = $path->partitions();

        static::assertEquals(2, $partitions->count());

        $partitionArray = $partitions->toArray();
        static::assertEquals('country', $partitionArray[0]->name);
        static::assertEquals('US', $partitionArray[0]->value);
        static::assertEquals('region', $partitionArray[1]->name);
        static::assertEquals('west', $partitionArray[1]->value);
    }

    public function test_partitions_paths(): void
    {
        $path = new UnixPath('/path/country=US/region=west/file.txt');
        $partitionPaths = $path->partitionsPaths();

        static::assertCount(2, $partitionPaths);
        static::assertEquals('/path/country=US', $partitionPaths[0]->path());
        static::assertEquals('/path/country=US/region=west', $partitionPaths[1]->path());
    }

    public function test_partitions_paths_with_root_directory(): void
    {
        $path = new UnixPath('/country=US/region=west/file.txt');
        $partitionPaths = $path->partitionsPaths();

        static::assertCount(2, $partitionPaths);
        static::assertEquals('file://country=US', $partitionPaths[0]->uri());
        static::assertEquals('file://country=US/region=west', $partitionPaths[1]->uri());
    }

    public function test_partitions_paths_without_partitions(): void
    {
        $path = new UnixPath('/path/to/file.txt');
        $partitionPaths = $path->partitionsPaths();

        static::assertCount(0, $partitionPaths);
    }

    public function test_partitions_with_pattern_returns_empty(): void
    {
        $path = new UnixPath('/path/*/file.txt');
        $partitions = $path->partitions();

        static::assertCount(0, $partitions);
    }

    public function test_path_manipulation(): void
    {
        $path = new UnixPath('/path/to/file.txt');

        $suffixed = $path->suffix('subdir/newfile.csv');
        static::assertEquals('/path/to/file.txt/subdir/newfile.csv', $suffixed->path());

        $parent = $path->parentDirectory();
        static::assertEquals('/path/to', $parent->path());

        static::assertEquals('path', $path->rootDirectoryName());
    }

    public function test_path_method(): void
    {
        $path = new UnixPath('/path/file.txt');

        static::assertEquals('/path/file.txt', $path->path());
    }

    #[DataProvider('patternProvider')]
    public function test_pattern_logic(string $pattern, string $filename, bool $expected): void
    {
        $patternPath = new UnixPath($pattern);
        $filePath = new UnixPath($filename);

        static::assertEquals($expected, $patternPath->matches($filePath));
    }

    public function test_pattern_methods_throw_exception(): void
    {
        $patternPath = new UnixPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        $patternPath->addPartitions(partition('group', 'a'));
    }

    public function test_pattern_parent_directory_throws_exception(): void
    {
        $patternPath = new UnixPath('/path/*/file.txt');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't take directory from path pattern.");

        $patternPath->parentDirectory();
    }

    public function test_pattern_with_double_wildcard(): void
    {
        $pattern = new UnixPath('/path/**/file.txt');
        $file = new UnixPath('/path/deeply/nested/file.txt');

        static::assertTrue($pattern->matches($file));
    }

    public function test_protocol_operations(): void
    {
        $path = new UnixPath('custom://path/to/file.txt');

        static::assertSame('custom', $path->protocol());
    }

    public function test_randomization(): void
    {
        $path = new UnixPath('/path/to/file.txt');
        $randomized = $path->randomize();

        static::assertStringStartsWith('/path/to/file_', $randomized->path());
        static::assertStringEndsWith('.txt', $randomized->path());
        static::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_randomize_without_extension(): void
    {
        $path = new UnixPath('/path/to/file');
        $randomized = $path->randomize();

        static::assertStringStartsWith('/path/to/file_', $randomized->path());
        static::assertNotEquals($path->path(), $randomized->path());
    }

    public function test_realpath_multiple_parent_navigation(): void
    {
        $path = UnixPath::realpath('/a/b/c/../../d/../e/file.txt');
        static::assertEquals('/a/e/file.txt', $path->path());
    }

    public function test_realpath_throws_on_empty_path(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/Empty path passed to UnixPath::realpath/');

        UnixPath::realpath('');
    }

    public function test_realpath_too_many_parent_navigations(): void
    {
        $path = UnixPath::realpath('/a/../../../file.txt');
        static::assertEquals('/file.txt', $path->path());
    }

    public function test_realpath_with_absolute_path(): void
    {
        $path = UnixPath::realpath('/absolute/path/file.txt');
        static::assertEquals('/absolute/path/file.txt', $path->path());
    }

    public function test_realpath_with_current_directory_dots(): void
    {
        $path = UnixPath::realpath('/path/./to/./file.txt');
        static::assertEquals('/path/to/file.txt', $path->path());
    }

    public function test_realpath_with_file_scheme_normalizes_dot_segments(): void
    {
        $path = UnixPath::realpath('file:///a/b/../c/./d.txt');

        static::assertSame('file', $path->protocol());
        static::assertSame('/a/c/d.txt', $path->path());
    }

    public function test_realpath_with_file_scheme_strips_protocol_prefix(): void
    {
        $path = UnixPath::realpath('file:///private/tmp/foo.txt');

        static::assertSame('file', $path->protocol());
        static::assertSame('/private/tmp/foo.txt', $path->path());
        static::assertSame('file://private/tmp/foo.txt', $path->uri());
    }

    public function test_realpath_with_non_file_scheme(): void
    {
        $path = UnixPath::realpath('s3://bucket/key.txt');

        static::assertEquals('s3://bucket/key.txt', $path->uri());
    }

    public function test_realpath_with_path_resolution(): void
    {
        $path = UnixPath::realpath('/path/to/../file.txt');
        static::assertEquals('/path/file.txt', $path->path());
    }

    public function test_relative_path_normalization(): void
    {
        $path = new UnixPath('relative/path/file.txt');

        static::assertEquals('/relative/path/file.txt', $path->path());
        static::assertEquals('file://relative/path/file.txt', $path->uri());
    }

    public function test_root_directory_cases(): void
    {
        $rootCases = ['/', '/file.txt'];

        foreach ($rootCases as $case) {
            $path = new UnixPath($case);
            $parent = $path->parentDirectory();

            static::assertEquals('/', $parent->path(), "Failed for case: {$case}");
        }
    }

    public function test_root_directory_name_with_single_file(): void
    {
        $path1 = new UnixPath('/file.txt');
        static::assertNull($path1->rootDirectoryName());

        $path2 = new UnixPath('/folder/file.txt');
        static::assertEquals('folder', $path2->rootDirectoryName());
    }

    public function test_root_partition_handling(): void
    {
        $path = new UnixPath('/file.txt');
        $partitioned = $path->addPartitions(partition('group', 'a'));

        static::assertEquals('/group=a/file.txt', $partitioned->path());
        static::assertEquals('file://group=a/file.txt', $partitioned->uri());
    }

    public function test_set_extension_without_existing_extension(): void
    {
        $path = new UnixPath('/path/to/file');
        $newPath = $path->setExtension('txt');

        static::assertEquals('/path/to/file.txt', $newPath->path());
    }

    /**
     * @param non-empty-array<int, array{name: string, value: string}> $partitionData
     */
    #[DataProvider('partitionProvider')]
    public function test_shared_partition_logic(string $input, array $partitionData, string $expected): void
    {
        $path = new UnixPath($input);
        $partitions = array_values(array_map(static fn($p) => partition($p['name'], $p['value']), $partitionData));
        $first = array_shift($partitions);
        static::assertNotNull($first);

        $result = $path->addPartitions($first, ...$partitions);

        static::assertEquals($expected, $result->path());
    }

    public function test_skip_directories(): void
    {
        $path = new UnixPath('/var/www/index.html');

        $skipped1 = $path->skipDirectories(1);
        static::assertNotNull($skipped1);
        static::assertEquals('file://www/index.html', $skipped1->uri());

        $skipped2 = $path->skipDirectories(2);
        static::assertNotNull($skipped2);
        static::assertEquals('file://index.html', $skipped2->uri());

        $skipped3 = $path->skipDirectories(3);
        static::assertNull($skipped3);
    }

    public function test_skip_directories_with_negative_count_throws_exception(): void
    {
        $path = new UnixPath('/path/to/file.txt');

        $this->expectException(BaseInvalidArgumentException::class);
        $this->expectExceptionMessage('The number of folders to skip must be non-negative.');

        $path->skipDirectories(-1);
    }

    public function test_skip_directories_zero_count(): void
    {
        $path = new UnixPath('/path/to/file.txt');
        $result = $path->skipDirectories(0);

        static::assertNotNull($result);
        static::assertEquals('/path/to/file.txt', $result->path());
    }

    public function test_static_part_at_root_with_pattern(): void
    {
        $pattern = new UnixPath('/*');
        $staticPart = $pattern->staticPart();

        static::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_extraction(): void
    {
        $pattern = new UnixPath('/static/part/*/dynamic/part');
        $staticPart = $pattern->staticPart();

        static::assertEquals('/static/part', $staticPart->path());
    }

    public function test_static_part_with_pattern_at_start(): void
    {
        $path = new UnixPath('/*/to/file.txt');
        $staticPart = $path->staticPart();

        static::assertEquals('/', $staticPart->path());
    }

    public function test_static_part_without_pattern(): void
    {
        $path = new UnixPath('/path/to/file.txt');
        $staticPart = $path->staticPart();

        static::assertEquals('/path/to/file.txt', $staticPart->path());
        static::assertTrue($path->isEqual($staticPart));
    }

    public function test_suffix_with_root_path(): void
    {
        $path = new UnixPath('/');
        $suffixed = $path->suffix('file.txt');

        static::assertEquals('/file.txt', $suffixed->path());
    }

    public function test_uri_method(): void
    {
        $path = new UnixPath('/path/file.txt');

        static::assertEquals('file://path/file.txt', $path->uri());
    }
}
