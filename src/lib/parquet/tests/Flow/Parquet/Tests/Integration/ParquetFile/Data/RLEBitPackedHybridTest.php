<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\BitWidth;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use PHPUnit\Framework\TestCase;

final class RLEBitPackedHybridTest extends TestCase
{
    public function test_bit_packing_reading_with_extra_bytes_in_the_buffer() : void
    {
        $values = [3, 3, 3, 3];
        $buffer = '';
        (new RLEBitPackedHybrid())->encodeHybrid($writer = new BinaryBufferWriter($buffer), $values);
        $writer->writeBytes([1, 2, 3]);
        $this->assertSame(
            $values,
            (new RLEBitPackedHybrid())->decodeHybrid(
                new BinaryBufferReader($buffer),
                BitWidth::fromArray($values),
                \count($values)
            )
        );
    }

    public function test_bit_packing_with_not_a_full_sequence() : void
    {
        $values = [1, 2, 3, 1234];

        $buffer = '';
        (new RLEBitPackedHybrid())->encodeHybrid(new BinaryBufferWriter($buffer), $values);

        $this->assertSame(
            $values,
            (new RLEBitPackedHybrid())->decodeHybrid(
                new BinaryBufferReader($buffer),
                BitWidth::fromArray($values),
                \count($values)
            )
        );
    }

    public function test_bit_packing_with_two_sequences() : void
    {
        $values = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
        ];

        $buffer = '';
        (new RLEBitPackedHybrid())->encodeHybrid(new BinaryBufferWriter($buffer), $values);

        $this->assertSame(
            $values,
            (new RLEBitPackedHybrid())->decodeHybrid(
                new BinaryBufferReader($buffer),
                BitWidth::fromArray($values),
                \count($values)
            )
        );
    }

    public function test_both_rle_and_bit_packed() : void
    {
        $values = [1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 13, 412, 5];

        $buffer = '';
        (new RLEBitPackedHybrid())->encodeHybrid(new BinaryBufferWriter($buffer), $values);

        $this->assertSame(
            $values,
            (new RLEBitPackedHybrid())->decodeHybrid(
                new BinaryBufferReader($buffer),
                BitWidth::fromArray($values),
                \count($values)
            )
        );
    }

    public function test_plain_rle_with_two_sequences() : void
    {
        $values = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1];

        $buffer = '';
        (new RLEBitPackedHybrid())->encodeHybrid(new BinaryBufferWriter($buffer), $values);

        $this->assertSame(
            $values,
            (new RLEBitPackedHybrid())->decodeHybrid(
                new BinaryBufferReader($buffer),
                BitWidth::fromArray($values),
                \count($values)
            )
        );
    }

    public function test_with_dynamically_generated_values() : void
    {
        for ($iteration = 0; $iteration < 100; $iteration++) {
            $values = [];
            $maxValues = \random_int(10, 1000);

            for ($i = 0; $i < $maxValues; $i++) {
                $values[] = \random_int(0, 1000);
            }

            $buffer = '';
            (new RLEBitPackedHybrid())->encodeHybrid(new BinaryBufferWriter($buffer), $values);

            $this->assertSame(
                $values,
                (new RLEBitPackedHybrid())->decodeHybrid(
                    new BinaryBufferReader($buffer),
                    BitWidth::fromArray($values),
                    \count($values)
                ),
                'Failed to encode and decode RLEBitPackedHybrid: Iteration: ' . $iteration . ', values: ' . \json_encode($values, JSON_THROW_ON_ERROR)
            );
        }
    }
}
