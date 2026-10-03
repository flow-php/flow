<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Layout;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class BuffersTest extends TestCase
{
    public function test_hands_out_buffers_in_order(): void
    {
        $buffers = new Buffers(['a', 'b']);

        static::assertSame(2, $buffers->remaining());
        static::assertSame('a', $buffers->next());
        static::assertSame(1, $buffers->remaining());
        static::assertSame('b', $buffers->next());
        static::assertSame(0, $buffers->remaining());
    }

    public function test_refuses_when_exhausted(): void
    {
        $buffers = new Buffers(['a']);
        $buffers->next();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column buffers exhausted');

        $buffers->next();
    }
}
