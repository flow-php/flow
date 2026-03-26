<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block;
use PHPUnit\Framework\TestCase;

final class BlockTest extends TestCase
{
    public function test_append_throws_on_closed_handle() : void
    {
        $filePath = \sys_get_temp_dir() . '/flow_block_test_' . \uniqid() . '.bin';
        $path = Path::realpath($filePath);
        $block = new Block('test-block', 1024, $path);

        $block->append('initial data');

        $reflection = new \ReflectionClass($block);
        $handleProperty = $reflection->getProperty('handle');
        $handle = $handleProperty->getValue($block);
        \fclose($handle);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Failed to write to block');

        $block->append('more data');
    }

    public function test_append_throws_when_block_is_full() : void
    {
        $path = Path::realpath(\sys_get_temp_dir() . '/flow_block_test_' . \uniqid() . '.bin');
        $block = new Block('test-block', 10, $path);

        $this->expectException(RuntimeException::class);

        $block->append(\str_repeat('a', 11));
    }

    public function test_append_writes_data_to_block() : void
    {
        $path = Path::realpath(\sys_get_temp_dir() . '/flow_block_test_' . \uniqid() . '.bin');
        $block = new Block('test-block', 1024, $path);

        $block->append('hello');
        $block->append(' world');

        self::assertSame(11, $block->size());
        self::assertSame(1013, $block->spaceLeft());
    }
}
