<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;
use PHPUnit\Framework\TestCase;

use function sys_get_temp_dir;

final class OptionsTest extends TestCase
{
    public function test_block_factory_can_be_replaced(): void
    {
        $blockFactory = new NativeLocalFileBlocksFactory();

        static::assertSame(
            $blockFactory,
            (new Options())
                ->withBlockFactory($blockFactory)
                ->blockFactory(),
        );
    }

    public function test_normalize_round_trips(): void
    {
        $options = (new Options())
            ->withFileFastPath(false)
            ->withBlockSize(6 * 1024 * 1024);

        $normalized = $options->normalize();

        static::assertSame(
            ['block_location' => sys_get_temp_dir(), 'file_fast_path' => false, 'part_size' => 6 * 1024 * 1024],
            $normalized,
        );
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_the_default_part_size_round_trips(): void
    {
        $options = new Options();
        $normalized = $options->normalize();

        static::assertNotNull($normalized);
        static::assertEquals($options, Options::fromArray($normalized));
    }

    public function test_a_custom_block_factory_does_not_normalize(): void
    {
        static::assertNull(
            (new Options())
                ->withBlockFactory($this->createStub(BlockFactory::class))
                ->normalize(),
        );
    }

    public function test_from_array_refuses_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Options::fromArray(['block_location' => sys_get_temp_dir()]);
    }
}
