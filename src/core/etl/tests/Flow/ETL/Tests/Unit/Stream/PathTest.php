<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Stream;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Partition;
use PHPUnit\Framework\TestCase;

final class PathTest extends TestCase
{
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

    public function directories() : \Generator
    {
        yield '/some_file.txt' => ['/some_file.txt', '/'];
        yield '/some/nested/file.csv' => ['/some/nested/file.csv', '/some/nested'];
        yield 'flow-file://nested/file/path/file.txt' => ['flow-file://nested/file/path/file.txt', '/nested/file/path'];
    }

    /**
     * @return \Generator<int, array<string>> - string $uri, string $schema, string $parsedUri
     */
    public function paths() : \Generator
    {
        yield '/file.csv' => ['/file.csv', 'file', 'file://file.csv'];
        yield 'file://file.csv' => ['file://file.csv', 'file', 'file://file.csv'];
        yield 'file:///' => ['file:///', 'file', 'file://'];
        yield '/' => ['/', 'file', 'file://'];
        yield 'flow-file://file.csv' => ['flow-file://folder/file.csv', 'flow-file', 'flow-file://folder/file.csv'];
    }

    public function paths_pattern_matching() : \Generator
    {
        yield ['/file.csv', '/file.csv', true];
        yield ['/nested/folder/any/file.csv', '/nested/folder/*/file.csv', false];
        yield ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield ['/nested/folder/[a]*/file.csv', '/nested/folder/ab/file.csv', true];
        yield ['/nested/folder/**/file.csv', '/nested/folder/any/nested/file.csv', true];
        yield ['/nested/folder/**/fil?.csv', '/nested/folder/any/nested/file.csv', true];
    }

    public function paths_with_partitions() : \Generator
    {
        yield '/' => ['/', []];
        yield 'file://path/without/partitions/file.csv' => ['file://path/without/partitions/file.csv', []];
        yield 'file://path/country=US/file.csv' => ['file://path/country=US/file.csv', [new Partition('country', 'US')]];
        yield 'file://path/country=US/region=america/file.csv' => ['file://path/country=US/region=america/file.csv', [new Partition('country', 'US'), new Partition('region', 'america')]];
        yield 'file://path/country=*/file.csv' => ['file://path/country=*/file.csv', []];
    }

    public function paths_with_static_parts() : \Generator
    {
        yield '/file.csv' => ['/file.csv', '/file.csv'];
        yield '/nested/folder/*/file.csv' => ['/nested/folder', '/nested/folder/*/file.csv'];
        yield '/nested/folder/path/{one|two}/file.csv' => ['/nested/folder/path', '/nested/folder/path/{one|two}/file.csv'];
        yield '/file*.csv' => ['/file*.csv', '/file*.csv'];
        yield 'flow-file://nested/partition={one,two}/*.csv' => ['flow-file://nested', 'flow-file://nested/partition={one,two}/*.csv'];
        yield 'flow-file://nested/partition=[one]/*.csv' => ['flow-file://nested', 'flow-file://nested/partition=[one]/*.csv'];
        yield '/nested/partition=[one]/*.csv' => ['file://nested', '/nested/partition=[one]/*.csv'];
    }

    public function test_add_partitions() : void
    {
        $this->assertEquals(
            new Path('/path/to/file.txt/group=a'),
            (new Path('/path/to/file.txt'))->addPartitions(new Partition('group', 'a'))
        );
    }

    /**
     * @dataProvider directories
     */
    public function test_directories(string $uri, string $dirPath) : void
    {
        $this->assertSame($dirPath, (new Path($uri))->parentDirectory()->path());
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
    public function test_partitions_in_path(string $uri, array $partitions) : void
    {
        $this->assertEquals($partitions, (new Path($uri))->partitions());
    }

    public function test_randomization_file_path() : void
    {
        $path = new Path('flow-file://var/file/test.csv', []);

        $this->assertStringStartsWith(
            'flow-file://var/file/test.csv/',
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
            'flow-file://var/file/folder/',
            $path->randomize()->uri()
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
