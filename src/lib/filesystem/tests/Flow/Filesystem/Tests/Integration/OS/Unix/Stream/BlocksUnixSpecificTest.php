<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration\OS\Unix\Stream;

use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Stream\Blocks;
use Flow\Filesystem\Tests\OperatingSystem;
use PHPUnit\Framework\TestCase;

/**
 * Stream Blocks tests that are specific to Unix and may fail on Windows
 * due to file handling or resource management differences.
 */
final class BlocksUnixSpecificTest extends TestCase
{
    use OperatingSystem;

    protected function setUp() : void
    {
        parent::setUp();

        if ($this->isWindows()) {
            self::markTestSkipped('Unix-specific stream tests should only run on Unix systems');
        }
    }

    public function test_moving_resource_to_blocks_unix() : void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $file = \fopen(__DIR__ . '/../../../Fixtures/orders.csv', 'rb');
        $fileSize = \filesize(__DIR__ . '/../../../Fixtures/orders.csv');

        $blocks->fromResource($file);

        self::assertSame($fileSize, $blocks->size());
        self::assertSame((int) \ceil($fileSize / $blockSize), \count($blocks->all()));
    }

    public function test_moving_resource_to_existing_blocks_unix() : void
    {
        $blocks = new Blocks($blockSize = SizeUnits::kbToBytes(10));

        $file = \fopen(__DIR__ . '/../../../Fixtures/orders.csv', 'rb');
        $fileSize = \filesize(__DIR__ . '/../../../Fixtures/orders.csv');

        $blocks->append(\str_repeat('a', 100));
        $blocks->fromResource($file);

        self::assertSame($fileSize + 100, $blocks->size());
        self::assertCount((int) \ceil($fileSize / $blockSize), $blocks->all());
    }

    public function test_unix_file_permissions_during_streaming() : void
    {
        if (!\function_exists('chmod')) {
            self::markTestSkipped('chmod functionality not available');
        }

        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_blocks_test_');
        $content = \str_repeat("Permission test content\n", 100);
        \file_put_contents($tempFile, $content);

        // Set specific permissions
        \chmod($tempFile, 0644);

        $blocks = new Blocks(SizeUnits::kbToBytes(1));
        $file = \fopen($tempFile, 'rb');

        self::assertNotFalse($file, 'Should be able to open file with 644 permissions');

        $blocks->fromResource($file);
        self::assertSame(\strlen($content), $blocks->size());

        \unlink($tempFile);
    }

    public function test_unix_large_file_streaming() : void
    {
        // Create a temporary large file
        $tempFile = \tempnam(\sys_get_temp_dir(), 'flow_blocks_test_');
        $largeContent = \str_repeat("Large file test content\n", 1000);
        \file_put_contents($tempFile, $largeContent);

        $blocks = new Blocks(SizeUnits::kbToBytes(5));
        $file = \fopen($tempFile, 'rb');

        $blocks->fromResource($file);

        self::assertSame(\strlen($largeContent), $blocks->size());
        self::assertGreaterThan(1, \count($blocks->all()));

        // Cleanup
        \unlink($tempFile);
    }

    public function test_unix_specific_stream_handling() : void
    {
        $blocks = new Blocks(SizeUnits::kbToBytes(1));

        // Test with Unix line endings
        $testContent = "Unix test content\nWith LF line endings\n";
        $blocks->append($testContent);

        self::assertSame(\strlen($testContent), $blocks->size());
        self::assertGreaterThan(0, \count($blocks->all()));

        // Verify blocks are created correctly
        self::assertGreaterThan(0, \count($blocks->all()));
    }
}
