<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Data\DeltaByteArrayDecoder;
use Flow\Parquet\Data\DeltaByteArrayEncoder;
use Flow\Parquet\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

use function array_map;
use function range;
use function sprintf;

final class DeltaByteArrayDecoderTest extends TestCase
{
    public function test_decode_leaves_the_bytes_after_the_stream(): void
    {
        $reader = new BinaryBufferReader((new DeltaByteArrayEncoder())->encode(['ab', 'abc']) . 'tail');

        static::assertSame(['ab', 'abc'], (new DeltaByteArrayDecoder())->decode($reader, 2));
        static::assertSame('tail', $reader->readBytes(4));
    }

    public function test_decode_round_trips_values_crossing_block_and_miniblock_edges(): void
    {
        $values = array_map(static fn(int $i): string => sprintf('key_%05d', $i), range(1, 300));

        static::assertSame($values, (new DeltaByteArrayDecoder())->decode(
            new BinaryBufferReader((new DeltaByteArrayEncoder())->encode($values)),
            300,
        ));
    }

    public function test_decode_spec_example(): void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        static::assertSame(
            ['axis', 'axle', 'babble', 'babyhood'],
            (new DeltaByteArrayDecoder())->decode(new BinaryBufferReader($encoder->encode([0, 2, 0, 3])
            . $encoder->encode([4, 2, 6, 5])
            . 'axislebabbleyhood'), 4),
        );
    }

    public function test_decode_throws_on_a_prefix_longer_than_the_previous_value(): void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Invalid DELTA_BYTE_ARRAY prefix length 5 of value 1, the previous value has 2 bytes',
        );

        (new DeltaByteArrayDecoder())->decode(
            new BinaryBufferReader($encoder->encode([0, 5]) . $encoder->encode([2, 1]) . 'abc'),
            2,
        );
    }

    public function test_decode_throws_on_a_negative_prefix_length(): void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Invalid DELTA_BYTE_ARRAY prefix length -1 of value 0, the previous value has 0 bytes',
        );

        (new DeltaByteArrayDecoder())->decode(
            new BinaryBufferReader($encoder->encode([-1]) . $encoder->encode([1]) . 'a'),
            1,
        );
    }
}
