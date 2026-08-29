<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream\WriteOffset;
use Flow\Filesystem\Exception\InvalidArgumentException;

final class WriteOffsetTest extends FlowTestCase
{
    public function test_advancing_accumulates_written_bytes(): void
    {
        $offset = new WriteOffset();

        $offset->advance(10);
        $offset->advance(5);

        static::assertSame(15, $offset->current());
    }

    public function test_advancing_by_a_negative_value_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot advance write offset by a negative value, got: -1');

        (new WriteOffset())->advance(-1);
    }

    public function test_negative_starting_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Write offset cannot be negative, got: -5');

        new WriteOffset(-5);
    }
}
