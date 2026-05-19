<?php

declare(strict_types=1);

namespace Flow\Snappy\Tests\Unit;

use Flow\Snappy\SnappyDecompressor;
use PHPUnit\Framework\TestCase;

final class SnappyDecompressorTest extends TestCase
{
    public function test_uncompress_to_buffer_returns_false_on_truncated_varint_length(): void
    {
        $decompressor = new SnappyDecompressor([0xFF]);
        $outBuffer = [];

        static::assertFalse($decompressor->uncompressToBuffer($outBuffer));
    }
}
