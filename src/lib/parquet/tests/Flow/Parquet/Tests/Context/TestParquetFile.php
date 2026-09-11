<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use PHPUnit\Framework\TestCase;
use ReflectionClass;

use function dirname;
use function file_exists;
use function getmypid;
use function is_dir;
use function mkdir;
use function preg_replace;
use function unlink;

final class TestParquetFile
{
    public static function path(TestCase $test): string
    {
        $directory = dirname(__DIR__) . '/var';

        if (!is_dir($directory)) {
            mkdir($directory);
        }

        return (
            $directory
            . '/'
            . (string) preg_replace(
                '/[^A-Za-z0-9_-]+/',
                '_',
                (new ReflectionClass($test))->getShortName() . '-' . $test->nameWithDataSet(),
            )
            . '-'
            . (string) getmypid()
            . '.parquet'
        );
    }

    public static function remove(TestCase $test): void
    {
        $path = self::path($test);

        if (file_exists($path)) {
            unlink($path);
        }
    }
}
