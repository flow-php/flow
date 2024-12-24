<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use function Flow\Filesystem\DSL\{partition, partitions, path, path_real};
use Flow\Filesystem\Partitions;
use PHPUnit\Framework\Attributes\{DataProvider, TestWith};
use PHPUnit\Framework\TestCase;

final class PathTest extends TestCase
{
    public static function directories() : \Generator
    {
        yield '/some_file.txt' => ['/some_file.txt', '/'];
        yield '/some/nested/file.csv' => ['/some/nested/file.csv', '/some/nested'];
        yield 'flow-file://nested/file/path/file.txt' => ['flow-file://nested/file/path/file.txt', '/nested/file/path'];
    }

    /**
     * @return \Generator<int, array<string>> - string $uri, string $schema, string $parsedUri
     */
    public static function paths() : \Generator
    {
        yield '/file.csv' => ['/file.csv', 'file', 'file://file.csv'];
        yield 'file://file.csv' => ['file://file.csv', 'file', 'file://file.csv'];
        yield 'file:///' => ['file:///', 'file', 'file://'];
        yield '/' => ['/', 'file', 'file://'];
        yield '/absolute/path/to/file.txt' => ['/absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield 'file://absolute/path/to/file.txt' => ['file://absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield 'file:///absolute/path/to/file.txt' => ['file:///absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield 'flow-file://' => ['flow-file://', 'flow-file', 'flow-file://'];
        yield 'flow-file:///' => ['flow-file:///', 'flow-file', 'flow-file://'];
        yield 'flow-file://folder/file.csv' => ['flow-file://folder/file.csv', 'flow-file', 'flow-file://folder/file.csv'];
    }

    public static function paths_pattern_matching() : \Generator
    {
        yield ['/file.csv', '/file.csv', true];
        yield ['/nested/folder/any/file.csv', '/nested/folder/*/file.csv', false];
        yield ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield ['/nested/folder/[a]*/file.csv', '/nested/folder/ab/file.csv', true];
        yield ['/nested/folder/**/file.csv', '/nested/folder/any/nested/file.csv', true];
        yield ['/nested/folder/**/fil?.csv', '/nested/folder/any/nested/file.csv', true];
    }

    public static function paths_with_partitions() : \Generator
    {
        yield '/' => ['/', partitions()];
        yield 'file://path/without/partitions/file.csv' => ['file://path/without/partitions/file.csv', partitions()];
        yield 'file://path/country=US/file.csv' => ['file://path/country=US/file.csv', partitions(partition('country', 'US'))];
        yield 'file://path/country=US/region=america/file.csv' => ['file://path/country=US/region=america/file.csv', partitions(partition('country', 'US'), partition('region', 'america'))];
        yield 'file://path/country=*/file.csv' => ['file://path/country=*/file.csv', partitions()];
    }

    public static function paths_with_static_parts() : \Generator
    {
        yield '/file.csv' => ['/file.csv', '/file.csv'];
        yield '/nested/folder/*/file.csv' => ['/nested/folder', '/nested/folder/*/file.csv'];
        yield '/nested/folder/path/{one|two}/file.csv' => ['/nested/folder/path', '/nested/folder/path/{one|two}/file.csv'];
        yield '/file*.csv' => ['/', '/file*.csv'];
        yield '/{one|two|tree}.csv' => ['/', '/{one|two|tree}.csv'];
        yield '/file.{parquet|csv}' => ['/', '/file.{parquet|csv}'];
        yield 'flow-file://nested/partition={one,two}/*.csv' => ['flow-file://nested', 'flow-file://nested/partition={one,two}/*.csv'];
        yield 'flow-file://nested/partition=[one]/*.csv' => ['flow-file://nested', 'flow-file://nested/partition=[one]/*.csv'];
        yield '/nested/partition=[one]/*.csv' => ['file://nested', '/nested/partition=[one]/*.csv'];
    }

    protected function setUp() : void
    {
        if (!\file_exists(__DIR__ . '/var')) {
            \mkdir(__DIR__ . '/var');
        }
    }

    public function test_add_partitions_to_path_pattern() : void
    {
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        (path('/path/to/group=*/file.txt'))->addPartitions(partition('group', 'a'));
    }

    public function test_add_partitions_to_path_with_extension() : void
    {
        self::assertEquals(
            path('/path/to/group=a/file.txt'),
            (path('/path/to/file.txt'))->addPartitions(partition('group', 'a'))
        );
    }

    public function test_add_partitions_to_path_without_extension() : void
    {
        self::assertEquals(
            path('/path/to/group=a/folder'),
            (path('/path/to/folder'))->addPartitions(partition('group', 'a'))
        );
    }

    public function test_add_partitions_to_root_path_with_extension() : void
    {
        self::assertEquals(
            path('/group=a/file.txt'),
            (path('/file.txt'))->addPartitions(partition('group', 'a'))
        );
    }

    public function test_add_partitions_to_root_path_without_extension() : void
    {
        self::assertEquals(
            path('/group=a/folder'),
            (path('/folder'))->addPartitions(partition('group', 'a'))
        );
    }

    #[DataProvider('directories')]
    public function test_directories(string $uri, string $dirPath) : void
    {
        self::assertSame($dirPath, (path($uri))->parentDirectory()->path());
    }

    public function test_extension() : void
    {
        self::assertSame('php', (path(__FILE__))->extension());
        self::assertFalse((path(__DIR__))->extension());
    }

    public function test_extension_uppercase() : void
    {
        self::assertSame('php', (path('/var/file/code.PhP'))->extension());
    }

    public function test_file_prefix() : void
    {
        $path = path('flow-file://var/dir/file.csv', []);

        self::assertSame(
            'flow-file://var/dir/._flow_tmp.file.csv',
            $path->basenamePrefix('._flow_tmp.')->uri()
        );
        self::assertSame('csv', $path->extension());
    }

    public function test_file_prefix_on_directory() : void
    {
        $path = path('flow-file://var/dir/', []);

        self::assertSame(
            'flow-file://var/._flow_tmp.dir',
            $path->basenamePrefix('._flow_tmp.')->uri()
        );
        self::assertFalse($path->extension());
    }

    public function test_file_prefix_on_root_directory() : void
    {
        $path = path('flow-file://', []);

        self::assertSame(
            'flow-file://._flow_tmp.',
            $path->basenamePrefix('._flow_tmp.')->uri()
        );
        self::assertFalse($path->extension());
    }

    #[DataProvider('paths_with_static_parts')]
    public function test_finding_static_part_of_the_path(string $staticPart, string $uri) : void
    {
        self::assertEquals(path($staticPart), (path($uri))->staticPart());
    }

    public function test_local_file() : void
    {
        self::assertNull((path(__FILE__))->context()->resource());
    }

    #[DataProvider('paths_pattern_matching')]
    public function test_matching_pattern_with_path(string $path, string $pattern, bool $result) : void
    {
        self::assertSame($result, (path($path))->matches(path($pattern)));
    }

    public function test_not_matching_items_under_directory_that_matches_pattern() : void
    {
        $path = path('flow-file://var/file/partition=*');

        self::assertTrue($path->matches(path('flow-file://var/file/partition=1')));
        self::assertFalse($path->matches(path('flow-file://var/file/partition=1/file.csv')));
    }

    #[DataProvider('paths')]
    public function test_parsing_path(string $uri, string $schema, string $parsedUri) : void
    {
        self::assertEquals($schema, (path($uri))->protocol()->name);
        self::assertEquals($parsedUri, (path($uri))->uri());
    }

    #[DataProvider('paths_with_partitions')]
    public function test_partitions_in_path(string $uri, Partitions $partitions) : void
    {
        self::assertEquals($partitions, (path($uri))->partitions());
    }

    public function test_partitions_paths() : void
    {
        $path = path('/var/path/partition_1=A/partition_2=B/file.csv', ['option' => true]);

        self::assertEquals(
            [
                path('/var/path/partition_1=A', ['option' => true]),
                path('/var/path/partition_1=A/partition_2=B', ['option' => true]),
            ],
            $path->partitionsPaths()
        );
    }

    public function test_randomization_file_path() : void
    {
        $path = path('flow-file://var/file/test.csv', []);

        self::assertStringStartsWith(
            'flow-file://var/file/test_',
            $path->randomize()->uri()
        );
        self::assertStringEndsWith(
            '.csv',
            $path->randomize()->uri()
        );
    }

    public function test_randomization_folder_path() : void
    {
        $path = path('flow-file://var/file/folder/', []);

        self::assertStringStartsWith(
            'flow-file://var/file/folder_',
            $path->randomize()->uri()
        );
    }

    public function test_real_path_on_custom_schema() : void
    {
        $path = path_real('azure-blob://var/dir/file.php');

        self::assertSame('azure-blob://var/dir/file.php', $path->uri());
    }

    #[TestWith(['file://var/www/index.html', 'var'])]
    #[TestWith(['file://index.html', null])]
    #[TestWith(['file://index', null])]
    #[TestWith(['file://var/www', 'var'])]
    #[TestWith(['/var/www', 'var'])]
    #[TestWith(['var/www', 'var'])]
    #[TestWith(['www', null])]
    public function test_root_directory_name(string $path, ?string $rootDirectory) : void
    {
        self::assertEquals($rootDirectory, path($path)->rootDirectoryName());
    }

    public function test_set_extension() : void
    {
        $path = path('flow-file://var/dir/file.csv', []);

        self::assertSame(
            'flow-file://var/dir/file.parquet',
            $path->setExtension('parquet')->uri()
        );
    }

    public function test_set_extension_on_directory() : void
    {
        $path = path('flow-file://var/dir/', []);

        self::assertSame(
            'flow-file://var/dir.parquet',
            $path->setExtension('parquet')->uri()
        );
    }

    public function test_set_extension_on_file_without_extension() : void
    {
        $path = path('flow-file://var/dir/file', []);

        self::assertSame(
            'flow-file://var/dir/file.parquet',
            $path->setExtension('parquet')->uri()
        );
    }

    #[TestWith(['file://var/www/index.html', 1,  'file://www/index.html'])]
    #[TestWith(['file://var/www/index.html', 2,  'file://index.html'])]
    #[TestWith(['file://var/www/index.html', 3,  null])]
    #[TestWith(['file://index.html', 1,  null])]
    public function test_skip_directories(string $path, int $count, ?string $newPath) : void
    {
        if ($newPath === null) {
            self::assertNull(path($path)->skipDirectories($count));
        } else {
            self::assertEquals(path($newPath), path($path)->skipDirectories($count));
        }
    }

    public function test_suffix() : void
    {
        $path = path('flow-file://var/dir', []);

        self::assertSame(
            'flow-file://var/dir/test.csv',
            $path->suffix('test.csv')->uri()
        );

        self::assertSame(
            'flow-file://var/dir/test.csv',
            $path->suffix('/test.csv')->uri()
        );
    }
}
