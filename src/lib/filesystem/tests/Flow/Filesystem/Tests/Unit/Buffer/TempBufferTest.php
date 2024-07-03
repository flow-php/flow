<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Buffer;

use Flow\Filesystem\Buffer\TempBuffer;
use PHPUnit\Framework\TestCase;

final class TempBufferTest extends TestCase
{
    public function test_writing_to_buffer() : void
    {
        $buffer = new TempBuffer();
        $buffer->append('Hello, World!');

        self::assertSame(13, $buffer->size());
        self::assertSame('Hello, World!', $buffer->dump());

        $buffer->append('Hello, World!');

        self::assertSame(26, $buffer->size());
        self::assertSame('Hello, World!Hello, World!', $buffer->dump());
    }
}
