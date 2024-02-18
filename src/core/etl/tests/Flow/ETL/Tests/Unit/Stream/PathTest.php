<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Stream;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Partition;
use Flow\ETL\Partitions;
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
        yield 'flow-file://file.csv' => ['flow-file://folder/file.csv', 'flow-file', 'flow-file://folder/file.csv'];
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
        yield '/' => ['/', new Partitions()];
        yield 'file://path/without/partitions/file.csv' => ['file://path/without/partitions/file.csv', new Partitions()];
        yield 'file://path/country=US/file.csv' => ['file://path/country=US/file.csv', new Partitions(new Partition('country', 'US'))];
        yield 'file://path/country=US/region=america/file.csv' => ['file://path/country=US/region=america/file.csv', new Partitions(new Partition('country', 'US'), new Partition('region', 'america'))];
        yield 'file://path/country=*/file.csv' => ['file://path/country=*/file.csv', new Partitions()];
    }

    public static function paths_with_static_parts() : \Generator
    {
        yield '/file.csv' => ['/file.csv', '/file.csv'];
        yield '/nested/folder/*/file.csv' => ['/nested/folder', '/nested/folder/*/file.csv'];
        yield '/nested/folder/path/{one|two}/file.csv' => ['/nested/folder/path', '/nested/folder/path/{one|two}/file.csv'];
        yield '/file*.csv' => ['/file*.csv', '/file*.csv'];
        yield 'flow-file://nested/partition={one,two}/*.csv' => ['flow-file://nested', 'flow-file://nested/partition={one,two}/*.csv'];
        yield 'flow-file://nested/partition=[one]/*.csv' => ['flow-file://nested', 'flow-file://nested/partition=[one]/*.csv'];
        yield '/nested/partition=[one]/*.csv' => ['file://nested', '/nested/partition=[one]/*.csv'];
    }

    protected function setUp() : void
    {
        if (!\in_array('flow-file', \stream_get_wrappers(), true)) {
            \stream_wrapper_register('flow-file', self::class);
        }
    }

    protected function tearDown() : void
    {
        if (\in_array('flow-file', \stream_get_wrappers(), true)) {
            \stream_wrapper_unregister('flow-file');
        }
    }

    public function test_add_partitions_to_path_pattern() : void
    {
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        (new Path('/path/to/group=*/file.txt'))->addPartitions(new Partition('group', 'a'));
    }

    public function test_add_partitions_to_path_with_extension() : void
    {
        $this->assertEquals(
            new Path('/path/to/group=a/file.txt'),
            (new Path('/path/to/file.txt'))->addPartitions(new Partition('group', 'a'))
        );
    }

    public function test_add_partitions_to_path_without_extension() : void
    {
        $this->assertEquals(
            new Path('/path/to/group=a/folder'),
            (new Path('/path/to/folder'))->addPartitions(new Partition('group', 'a'))
        );
    }

    public function test_add_partitions_to_root_path_with_extension() : void
    {
        $this->assertEquals(
            new Path('/group=a/file.txt'),
            (new Path('/file.txt'))->addPartitions(new Partition('group', 'a'))
        );
    }

    public function test_add_partitions_to_root_path_without_extension() : void
    {
        $this->assertEquals(
            new Path('/group=a/folder'),
            (new Path('/folder'))->addPartitions(new Partition('group', 'a'))
        );
    }

    /**
     * @dataProvider directories
     */
    public function test_directories(string $uri, string $dirPath) : void
    {
        $this->assertSame($dirPath, (new Path($uri))->parentDirectory()->path());
    }

    public function test_equal_paths_starts_with() : void
    {
        $this->assertTrue(
            Path::realpath(\sys_get_temp_dir() . '/some/path/file.json')
                ->startsWith(Path::realpath(\sys_get_temp_dir() . '/some/path/file.json'))
        );
    }

    public function test_extension() : void
    {
        $this->assertSame('php', (new Path(__FILE__))->extension());
        $this->assertFalse((new Path(__DIR__))->extension());
    }

    /**
     * @dataProvider paths_with_static_parts
     */
    public function test_finding_static_part_of_the_path(string $staticPart, string $uri) : void
    {
        $this->assertEquals(new Path($staticPart), (new Path($uri))->staticPart());
    }

    public function test_local_file() : void
    {
        $this->assertNull((new Path(__FILE__))->context()->resource());
    }

    /**
     * @dataProvider paths_pattern_matching
     */
    public function test_matching_pattern_with_path(string $path, string $pattern, bool $result) : void
    {
        $this->assertSame($result, (new Path($path))->matches(new Path($pattern)));
    }

    public function test_not_matching_items_under_directory_that_matches_pattern() : void
    {
        $path = new Path('flow-file://var/file/partition=*');

        $this->assertTrue($path->matches(new Path('flow-file://var/file/partition=1')));
        $this->assertFalse($path->matches(new Path('flow-file://var/file/partition=1/file.csv')));
    }

    /**
     * @dataProvider paths
     */
    public function test_parsing_path(string $uri, string $schema, string $parsedUri) : void
    {
        $this->assertEquals($schema, (new Path($uri))->scheme());
        $this->assertEquals($parsedUri, (new Path($uri))->uri());
    }

    /**
     * @dataProvider paths_with_partitions
     */
    public function test_partitions_in_path(string $uri, Partitions $partitions) : void
    {
        $this->assertEquals($partitions, (new Path($uri))->partitions());
    }

    public function test_partitions_paths() : void
    {
        $path = new Path('/var/path/partition_1=A/partition_2=B/file.csv', ['option' => true]);

        $this->assertEquals(
            [
                new Path('/var/path/partition_1=A', ['option' => true]),
                new Path('/var/path/partition_1=A/partition_2=B', ['option' => true]),
            ],
            $path->partitionsPaths()
        );
    }

    public function test_path_starting_with_other_path() : void
    {
        $this->assertTrue(
            Path::realpath(\sys_get_temp_dir() . '/some/path/file.json')
                ->startsWith(Path::realpath(\sys_get_temp_dir() . '/some/path'))
        );
    }

    public function test_pattern_path_starting_with_realpath_path() : void
    {
        $this->assertTrue(
            Path::realpath(\sys_get_temp_dir() . '/some/path/*.json')
                ->startsWith(Path::realpath(\sys_get_temp_dir() . '/some/path'))
        );
    }

    public function test_randomization_file_path() : void
    {
        $path = new Path('flow-file://var/file/test.csv', []);

        $this->assertStringStartsWith(
            'flow-file://var/file/test_',
            $path->randomize()->uri()
        );
        $this->assertStringEndsWith(
            '.csv',
            $path->randomize()->uri()
        );
    }

    public function test_randomization_folder_path() : void
    {
        $path = new Path('flow-file://var/file/folder/', []);

        $this->assertStringStartsWith(
            'flow-file://var/file/folder_',
            $path->randomize()->uri()
        );
    }

    public function test_realpath_starting_with_non_realpath_path() : void
    {
        $this->assertFalse(
            Path::realpath(\sys_get_temp_dir() . '/some/path/file.json')
                ->startsWith(new Path('/some/path'))
        );
    }

    public function test_set_extension_different_than_existing_one() : void
    {
        $path = new Path('flow-file://var/file/folder/file.txt', ['option' => true]);

        $this->assertEquals(
            new Path('flow-file://var/file/folder/file.csv', ['option' => true]),
            $path->setExtension('csv')
        );
    }

    public function test_set_extension_when_is_not_set_yet() : void
    {
        $path = (new Path('flow-file://var/file/folder/file', ['option' => true]))->randomize();

        $this->assertEquals(
            new Path($path->uri() . '.csv', ['option' => true]),
            $path->setExtension('csv')
        );
    }

    public function test_set_same_extension() : void
    {
        $path = new Path('flow-file://var/file/folder/file.csv', ['option' => true]);

        $this->assertEquals(
            $path,
            $path->setExtension('csv')
        );
    }

    public function test_unknown_stream_scheme() : void
    {
        $this->expectExceptionMessage('Unknown scheme "flow-invalid"');
        $this->expectException(InvalidArgumentException::class);

        new Path('flow-invalid://some_file.txt');
    }
}
