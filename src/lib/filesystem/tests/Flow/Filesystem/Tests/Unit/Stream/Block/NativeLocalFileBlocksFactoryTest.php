<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream\Block;

use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function is_dir;
use function random_bytes;
use function rmdir;
use function sys_get_temp_dir;

final class NativeLocalFileBlocksFactoryTest extends TestCase
{
    public function test_the_default_block_location_is_the_system_temp_dir(): void
    {
        static::assertSame(sys_get_temp_dir(), (new NativeLocalFileBlocksFactory())->blockLocation());
    }

    public function test_an_explicit_block_location_is_created_and_reported(): void
    {
        $location = sys_get_temp_dir() . '/flow-blocks-' . bin2hex(random_bytes(4));

        static::assertSame($location, (new NativeLocalFileBlocksFactory($location))->blockLocation());
        static::assertTrue(is_dir($location));

        rmdir($location);
    }

    public function test_a_block_is_created_under_the_block_location(): void
    {
        $block = (new NativeLocalFileBlocksFactory())->create(16);

        static::assertStringStartsWith(sys_get_temp_dir(), $block->path()->path());
        static::assertSame(16, $block->spaceLeft());
    }
}
