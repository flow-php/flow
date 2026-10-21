<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Data\DeltaLengthByteArrayDecoder;
use Flow\Parquet\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

final class DeltaLengthByteArrayDecoderTest extends TestCase
{
    public function test_decode_spec_example_leaves_the_bytes_after_the_stream(): void
    {
        $reader = new BinaryBufferReader((new DeltaBinaryPackedEncoder())->encode([5, 5, 6, 6])
            . 'HelloWorldFoobarABCDEFtail');

        static::assertSame(
            ['Hello', 'World', 'Foobar', 'ABCDEF'],
            (new DeltaLengthByteArrayDecoder())->decode($reader, 4),
        );
        static::assertSame('tail', $reader->readBytes(4));
    }

    public function test_decode_throws_on_a_negative_length(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Invalid DELTA_LENGTH_BYTE_ARRAY length -1 of value 0');

        (new DeltaLengthByteArrayDecoder())->decode(
            new BinaryBufferReader((new DeltaBinaryPackedEncoder())->encode([-1]) . 'x'),
            1,
        );
    }

    public function test_decode_throws_when_the_data_ends_early(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('DELTA_LENGTH_BYTE_ARRAY data ends before value 0');

        (new DeltaLengthByteArrayDecoder())->decode(
            new BinaryBufferReader((new DeltaBinaryPackedEncoder())->encode([5]) . 'abc'),
            1,
        );
    }
}
