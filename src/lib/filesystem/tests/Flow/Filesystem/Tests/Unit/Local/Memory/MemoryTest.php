<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Local\Memory;

use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Local\Memory\Memory;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;

final class MemoryTest extends TestCase
{
    public function test_exclusive_stream_is_registered_at_close(): void
    {
        $memory = new Memory();
        $stream = $memory->exclusive(path('memory://file.txt'));
        $stream->append('whole');

        static::assertFalse($memory->has(path('memory://file.txt')));

        $stream->close();

        static::assertSame('whole', $memory->for(path('memory://file.txt'))->content());
    }

    public function test_register_refuses_an_existing_path(): void
    {
        $memory = new Memory();
        $memory->for(path('memory://file.txt'))->append('first');

        $this->expectException(FileExistsException::class);

        $memory->register(path('memory://file.txt'), $memory->exclusive(path('memory://file.txt')));
    }
}
