<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Generator;
use PHPUnit\Framework\TestCase;

use function file_exists;
use function file_put_contents;
use function Flow\Filesystem\DSL\partition;
use function Flow\Filesystem\DSL\partitions;
use function Flow\Filesystem\DSL\path;
use function mkdir;
use function tempnam;
use function uniqid;

abstract class PathTestCase extends TestCase
{
    /**
     * @return \Generator<int, array{string, string}>
     */
    public static function directories(): Generator
    {
        yield ['/some_file.txt', '/'];
        yield ['/some/nested/file.csv', '/some/nested'];
        yield ['flow-file://nested/file/path/file.txt', '/nested/file/path'];
    }

    /**
     * @return \Generator<int, array{string, string, string}> - string $uri, string $schema, string $parsedUri
     */
    public static function paths(): Generator
    {
        yield ['/file.csv', 'file', 'file://file.csv'];
        yield ['file://file.csv', 'file', 'file://file.csv'];
        yield ['file:///', 'file', 'file://'];
        yield ['/', 'file', 'file://'];
        yield ['/absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield ['file://absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield ['file:///absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield ['flow-file://', 'flow-file', 'flow-file://'];
        yield ['flow-file:///', 'flow-file', 'flow-file://'];
        yield ['flow-file://folder/file.csv', 'flow-file', 'flow-file://folder/file.csv'];
    }

    /**
     * @return \Generator<int, array{string, string, bool}>
     */
    public static function paths_pattern_matching(): Generator
    {
        yield ['/file.csv', '/file.csv', true];
        yield ['/nested/folder/any/file.csv', '/nested/folder/*/file.csv', false];
        yield ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield ['/nested/folder/[a]*/file.csv', '/nested/folder/ab/file.csv', true];
        yield ['/nested/folder/**/file.csv', '/nested/folder/any/nested/file.csv', true];
        yield ['/nested/folder/**/fil?.csv', '/nested/folder/any/nested/file.csv', true];
        yield ['/nested/folder/**/file.csv', '/nested/folder/file.csv', true];
        yield ['/nested/folder/**.csv', '/nested/folder/any/file.csv', false];
        yield ['/nested/folder/[!a]*.csv', '/nested/folder/b.csv', true];
        yield ['/nested/folder/[a-c].csv', '/nested/folder/b.csv', true];
    }

    /**
     * @return \Generator<int, array{string, \Flow\Filesystem\Partitions}>
     */
    public static function paths_with_partitions(): Generator
    {
        yield ['/', partitions()];
        yield ['file://path/without/partitions/file.csv', partitions()];
        yield ['file://path/country=US/file.csv', partitions(partition('country', 'US'))];
        yield [
            'file://path/country=US/region=america/file.csv',
            partitions(partition('country', 'US'), partition('region', 'america')),
        ];
        yield ['file://path/country=*/file.csv', partitions()];
    }

    /**
     * @return \Generator<int, array{string, string}>
     */
    public static function paths_with_static_parts(): Generator
    {
        yield ['/file.csv', '/file.csv'];
        yield ['/nested/folder', '/nested/folder/*/file.csv'];
        yield ['/nested/folder/path', '/nested/folder/path/{one|two}/file.csv'];
        yield ['/', '/file*.csv'];
        yield ['/', '/{one|two|tree}.csv'];
        yield ['/', '/file.{parquet|csv}'];
        yield ['flow-file://nested', 'flow-file://nested/partition={one,two}/*.csv'];
        yield ['flow-file://nested', 'flow-file://nested/partition=[one]/*.csv'];
        yield ['file://nested', '/nested/partition=[one]/*.csv'];
    }

    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    protected function assertPathEquals(string $expectedPath, string $actualPath, string $message = ''): void
    {
        static::assertEquals(path($expectedPath), path($actualPath), $message);
    }

    protected function createTempDir(): string
    {
        mkdir($tempDir = __DIR__ . '/var/' . uniqid('test_dir_'));

        return $tempDir;
    }

    protected function createTempFile(string $content = ''): string
    {
        if (($tempFile = tempnam(__DIR__ . '/var', 'test_')) === false) {
            static::fail('Could not create temporary file');
        }

        if ($content !== '') {
            file_put_contents($tempFile, $content);
        }

        return $tempFile;
    }
}
