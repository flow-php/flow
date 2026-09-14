<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use PHPUnit\Framework\TestCase;

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
}
