<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix\Stream;

use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Stream\Blocks;
use Flow\Filesystem\Tests\OperatingSystem;
use PHPUnit\Framework\TestCase;

use function ceil;
use function chmod;
use function count;
use function file_put_contents;
use function filesize;
use function fopen;
use function function_exists;
use function str_repeat;
use function strlen;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

/**
 * Stream Blocks tests that are specific to Unix and may fail on Windows
 * due to file handling or resource management differences.
 */
final class BlocksUnixSpecificTest extends TestCase
{
    use OperatingSystem;

    protected function setUp(): void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('Unix-specific stream tests should only run on Unix systems');
        }
    }

    public function test_moving_resource_to_blocks_unix(): void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $file = fopen(__DIR__ . '/../../../Fixtures/orders.csv', 'rb');
        static::assertIsResource($file);
        $fileSize = filesize(__DIR__ . '/../../../Fixtures/orders.csv');
        static::assertIsInt($fileSize);

        $blocks->fromResource($file);

        static::assertSame($fileSize, $blocks->size());
        static::assertSame((int) ceil($fileSize / $blockSize), count($blocks->all()));
    }

    public function test_moving_resource_to_existing_blocks_unix(): void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $file = fopen(__DIR__ . '/../../../Fixtures/orders.csv', 'rb');
        static::assertIsResource($file);
        $fileSize = filesize(__DIR__ . '/../../../Fixtures/orders.csv');
        static::assertIsInt($fileSize);

        $blocks->append(str_repeat('a', 100));
        $blocks->fromResource($file);

        static::assertSame($fileSize + 100, $blocks->size());
        static::assertCount((int) ceil($fileSize / $blockSize), $blocks->all());
    }

    public function test_unix_file_permissions_during_streaming(): void
    {
        if (!function_exists('chmod')) {
            static::markTestSkipped('chmod functionality not available');
        }

        $tempFile = tempnam(sys_get_temp_dir(), 'flow_blocks_test_');
        static::assertIsString($tempFile);
        $content = str_repeat("Permission test content\n", 100);
        file_put_contents($tempFile, $content);

        // Set specific permissions
        chmod($tempFile, 0644);

        $blocks = new Blocks(SizeUnits::kbToBytes(1));
        $file = fopen($tempFile, 'rb');

        static::assertIsResource($file, 'Should be able to open file with 644 permissions');

        $blocks->fromResource($file);
        static::assertSame(strlen($content), $blocks->size());

        unlink($tempFile);
    }

    public function test_unix_large_file_streaming(): void
    {
        // Create a temporary large file
        $tempFile = tempnam(sys_get_temp_dir(), 'flow_blocks_test_');
        static::assertIsString($tempFile);
        $largeContent = str_repeat("Large file test content\n", 1000);
        file_put_contents($tempFile, $largeContent);

        $blocks = new Blocks(SizeUnits::kbToBytes(5));
        $file = fopen($tempFile, 'rb');
        static::assertIsResource($file);

        $blocks->fromResource($file);

        static::assertSame(strlen($largeContent), $blocks->size());
        static::assertGreaterThan(1, count($blocks->all()));

        // Cleanup
        unlink($tempFile);
    }

    public function test_unix_specific_stream_handling(): void
    {
        $blocks = new Blocks(SizeUnits::kbToBytes(1));

        // Test with Unix line endings
        $testContent = "Unix test content\nWith LF line endings\n";
        $blocks->append($testContent);

        static::assertSame(strlen($testContent), $blocks->size());
        static::assertGreaterThan(0, count($blocks->all()));

        // Verify blocks are created correctly
        static::assertGreaterThan(0, count($blocks->all()));
    }
}
