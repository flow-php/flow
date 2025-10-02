<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use function Flow\Filesystem\DSL\{partition, partitions, path};
use PHPUnit\Framework\TestCase;

abstract class PathTestCase extends TestCase
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

    protected function assertPathEquals(string $expectedPath, string $actualPath, string $message = '') : void
    {
        static::assertEquals(path($expectedPath), path($actualPath), $message);
    }

    protected function createTempDir() : string
    {
        \mkdir($tempDir = __DIR__ . '/var/' . \uniqid('test_dir_'));

        return $tempDir;
    }

    protected function createTempFile(string $content = '') : string
    {
        if (($tempFile = \tempnam(__DIR__ . '/var', 'test_')) === false) {
            static::fail('Could not create temporary file');
        }

        if ($content !== '') {
            \file_put_contents($tempFile, $content);
        }

        return $tempFile;
    }
}
