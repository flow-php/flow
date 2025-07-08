<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\PageBuilder;

use Flow\Parquet\BinaryReader\{BinaryBufferReader};
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\ParquetFile\Data\{BitWidth, RLEBitPackedHybrid};
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RLEBitPackedPackerTest extends TestCase
{
    private RLEBitPackedHybrid $hybrid;

    private RLEBitPackedPacker $packer;

    public static function pack_basic_values_provider() : \Generator
    {
        yield '1-bit single value' => [1, [1], '1-bit single value'];
        yield '1-bit multiple values' => [1, [0, 1, 0, 1], '1-bit multiple values'];
        yield '2-bit values' => [2, [0, 1, 2, 3], '2-bit values'];
        yield '3-bit values' => [3, [0, 1, 2, 3, 4, 5, 6, 7], '3-bit values'];
        yield '4-bit values' => [4, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], '4-bit values'];
        yield '8-bit values' => [8, [0, 1, 127, 128, 255], '8-bit values'];
        yield '16-bit values' => [16, [0, 1, 255, 256, 65535], '16-bit values'];
        yield 'mixed values for bit packing' => [4, [1, 2, 3, 4, 5, 6, 7, 8], 'mixed values for bit packing'];
        yield 'alternating pattern' => [2, [0, 3, 0, 3, 0, 3, 0, 3], 'alternating pattern'];
        yield 'sequential values' => [8, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'sequential values'];
    }

    public static function pack_with_bit_width_provider() : \Generator
    {
        yield '1-bit single value' => [1, [1], '1-bit single value with bit width'];
        yield '1-bit multiple values' => [1, [0, 1, 0, 1], '1-bit multiple values with bit width'];
        yield '2-bit values' => [2, [0, 1, 2, 3], '2-bit values with bit width'];
        yield '3-bit values' => [3, [0, 1, 2, 3, 4, 5, 6, 7], '3-bit values with bit width'];
        yield '4-bit values' => [4, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], '4-bit values with bit width'];
        yield '8-bit values' => [8, [0, 1, 127, 128, 255], '8-bit values with bit width'];
        yield '16-bit values' => [16, [0, 1, 255, 256, 65535], '16-bit values with bit width'];
        yield 'mixed values for bit packing' => [4, [1, 2, 3, 4, 5, 6, 7, 8], 'mixed values for bit packing with bit width'];
        yield 'alternating pattern' => [2, [0, 3, 0, 3, 0, 3, 0, 3], 'alternating pattern with bit width'];
        yield 'sequential values' => [8, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'sequential values with bit width'];
    }

    public static function pack_with_length_provider() : \Generator
    {
        yield '1-bit single value' => [1, [1], '1-bit single value with length'];
        yield '1-bit multiple values' => [1, [0, 1, 0, 1], '1-bit multiple values with length'];
        yield '2-bit values' => [2, [0, 1, 2, 3], '2-bit values with length'];
        yield '3-bit values' => [3, [0, 1, 2, 3, 4, 5, 6, 7], '3-bit values with length'];
        yield '4-bit values' => [4, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], '4-bit values with length'];
        yield '8-bit values' => [8, [0, 1, 127, 128, 255], '8-bit values with length'];
        yield '16-bit values' => [16, [0, 1, 255, 256, 65535], '16-bit values with length'];
        yield 'mixed values for bit packing' => [4, [1, 2, 3, 4, 5, 6, 7, 8], 'mixed values for bit packing with length'];
        yield 'alternating pattern' => [2, [0, 3, 0, 3, 0, 3, 0, 3], 'alternating pattern with length'];
        yield 'sequential values' => [8, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'sequential values with length'];
    }

    protected function setUp() : void
    {
        $this->hybrid = new RLEBitPackedHybrid();
        $this->packer = new RLEBitPackedPacker($this->hybrid);
    }

    #[DataProvider('pack_basic_values_provider')]
    public function test_pack_basic_values(int $bitWidth, array $values, string $description) : void
    {
        $result = $this->packer->pack($bitWidth, $values);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, $bitWidth, count($values));

        self::assertEquals($values, $decoded, $description);
    }

    public function test_pack_empty_array() : void
    {
        $result = $this->packer->pack(1, []);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, 1, 0);
        self::assertEquals([], $decoded);
    }

    public function test_pack_large_array() : void
    {
        $values = array_fill(0, 1000, 123);
        $result = $this->packer->pack(8, $values);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, 8, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_maximum_values_for_bit_width() : void
    {
        $bitWidth = 4;
        $maxValue = (1 << $bitWidth) - 1; // 15 for 4-bit
        $values = [$maxValue, $maxValue, $maxValue];
        $result = $this->packer->pack($bitWidth, $values);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, $bitWidth, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_repeated_values_for_rle() : void
    {
        $values = [5, 5, 5, 5, 5, 5, 5, 5, 5, 5];
        $result = $this->packer->pack(4, $values);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, 4, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_single_value() : void
    {
        $values = [42];
        $result = $this->packer->pack(8, $values);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, 8, count($values));
        self::assertEquals($values, $decoded);
    }

    #[DataProvider('pack_with_bit_width_provider')]
    public function test_pack_with_bit_width(int $bitWidth, array $values, string $description) : void
    {
        $result = $this->packer->packWithBitWidth($bitWidth, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        // Verify the bit width is prepended
        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        $expectedBitWidth = BitWidth::fromArray($values);
        self::assertEquals($expectedBitWidth, $decodedBitWidth, 'Bit width should be prepended');

        // Verify we can decode the remaining data
        $decoded = $this->hybrid->decodeHybrid($reader, $bitWidth, count($values));
        self::assertEquals($values, $decoded, $description);
    }

    public function test_pack_with_bit_width_empty_array() : void
    {
        $result = $this->packer->packWithBitWidth(1, []);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        self::assertEquals(0, $decodedBitWidth);

        $decoded = $this->hybrid->decodeHybrid($reader, 1, 0);
        self::assertEquals([], $decoded);
    }

    public function test_pack_with_bit_width_large_array() : void
    {
        $values = array_fill(0, 1000, 123);
        $result = $this->packer->packWithBitWidth(8, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        self::assertEquals(BitWidth::fromArray($values), $decodedBitWidth);

        $decoded = $this->hybrid->decodeHybrid($reader, 8, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_bit_width_maximum_values_for_bit_width() : void
    {
        $bitWidth = 4;
        $maxValue = (1 << $bitWidth) - 1; // 15 for 4-bit
        $values = [$maxValue, $maxValue, $maxValue];
        $result = $this->packer->packWithBitWidth($bitWidth, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        self::assertEquals(BitWidth::fromArray($values), $decodedBitWidth);

        $decoded = $this->hybrid->decodeHybrid($reader, $bitWidth, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_bit_width_repeated_values_for_rle() : void
    {
        $values = [5, 5, 5, 5, 5, 5, 5, 5, 5, 5];
        $result = $this->packer->packWithBitWidth(4, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        self::assertEquals(BitWidth::fromArray($values), $decodedBitWidth);

        $decoded = $this->hybrid->decodeHybrid($reader, 4, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_bit_width_single_value() : void
    {
        $values = [42];
        $result = $this->packer->packWithBitWidth(8, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        self::assertEquals(BitWidth::fromArray($values), $decodedBitWidth);

        $decoded = $this->hybrid->decodeHybrid($reader, 8, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_bit_width_zero_bit_width() : void
    {
        $values = [0, 0, 0, 0];
        $result = $this->packer->packWithBitWidth(0, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedBitWidth = $reader->readVarInt();
        self::assertEquals(0, $decodedBitWidth);

        $decoded = $this->hybrid->decodeHybrid($reader, 0, count($values));
        self::assertEquals($values, $decoded);
    }

    #[DataProvider('pack_with_length_provider')]
    public function test_pack_with_length(int $bitWidth, array $values, string $description) : void
    {
        $result = $this->packer->packWithLength($bitWidth, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThan(0, $decodedLength, 'Length should be prepended and positive');

        $dataBuffer = '';
        $this->hybrid->encodeHybrid(new BinaryBufferWriter($dataBuffer), $bitWidth, $values);
        self::assertEquals(strlen($dataBuffer), $decodedLength, 'Length should match encoded data size');

        $decoded = $this->hybrid->decodeHybrid($reader, $bitWidth, count($values));
        self::assertEquals($values, $decoded, $description);
    }

    public function test_pack_with_length_empty_array() : void
    {
        $result = $this->packer->packWithLength(1, []);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThanOrEqual(0, $decodedLength);

        $decoded = $this->hybrid->decodeHybrid($reader, 1, 0);
        self::assertEquals([], $decoded);
    }

    public function test_pack_with_length_large_array() : void
    {
        $values = array_fill(0, 1000, 123);
        $result = $this->packer->packWithLength(8, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThan(0, $decodedLength);

        $decoded = $this->hybrid->decodeHybrid($reader, 8, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_length_maximum_values_for_bit_width() : void
    {
        $bitWidth = 4;
        $maxValue = (1 << $bitWidth) - 1; // 15 for 4-bit
        $values = [$maxValue, $maxValue, $maxValue];
        $result = $this->packer->packWithLength($bitWidth, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThan(0, $decodedLength);

        $decoded = $this->hybrid->decodeHybrid($reader, $bitWidth, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_length_repeated_values_for_rle() : void
    {
        $values = [5, 5, 5, 5, 5, 5, 5, 5, 5, 5];
        $result = $this->packer->packWithLength(4, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThan(0, $decodedLength);

        $decoded = $this->hybrid->decodeHybrid($reader, 4, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_length_single_value() : void
    {
        $values = [42];
        $result = $this->packer->packWithLength(8, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThan(0, $decodedLength);

        $decoded = $this->hybrid->decodeHybrid($reader, 8, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_with_length_zero_bit_width() : void
    {
        $values = [0, 0, 0, 0];
        $result = $this->packer->packWithLength(0, $values);

        self::assertIsString($result);
        self::assertGreaterThan(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decodedLength = iterator_to_array($reader->readInts32(1))[0];
        self::assertGreaterThanOrEqual(0, $decodedLength);

        $decoded = $this->hybrid->decodeHybrid($reader, 0, count($values));
        self::assertEquals($values, $decoded);
    }

    public function test_pack_zero_bit_width() : void
    {
        $values = [0, 0, 0, 0];
        $result = $this->packer->pack(0, $values);

        self::assertIsString($result);
        self::assertGreaterThanOrEqual(0, strlen($result));

        $reader = new BinaryBufferReader($result);
        $decoded = $this->hybrid->decodeHybrid($reader, 0, count($values));
        self::assertEquals($values, $decoded);
    }
}
