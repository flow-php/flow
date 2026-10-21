<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Data\DeltaLengthByteArrayEncoder;
use PHPUnit\Framework\TestCase;

final class DeltaLengthByteArrayEncoderTest extends TestCase
{
    public function test_encode_empty_array_writes_an_empty_length_stream(): void
    {
        static::assertSame("\x80\x01\x04\x00\x00", (new DeltaLengthByteArrayEncoder())->encode([]));
    }

    public function test_encode_empty_strings(): void
    {
        static::assertSame(
            (new DeltaBinaryPackedEncoder())->encode([0, 1, 0]) . 'a',
            (new DeltaLengthByteArrayEncoder())->encode(['', 'a', '']),
        );
    }

    public function test_encode_spec_example(): void
    {
        static::assertSame(
            (new DeltaBinaryPackedEncoder())->encode([5, 5, 6, 6]) . 'HelloWorldFoobarABCDEF',
            (new DeltaLengthByteArrayEncoder())->encode(['Hello', 'World', 'Foobar', 'ABCDEF']),
        );
    }
}
