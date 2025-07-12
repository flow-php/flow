<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BinaryWriter;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class VarIntsTest extends TestCase
{
    /**
     * @return array<string, array{int}>
     */
    public static function negativeValuesProvider() : array
    {
        return [
            'negative_one' => [-1],
            'negative_small' => [-42],
            'negative_large' => [-1000000],
            'int_min' => [PHP_INT_MIN],
        ];
    }

    /**
     * @return array<string, array{int, array<int>}>
     */
    public static function positiveValuesProvider() : array
    {
        return [
            'zero' => [0, [0x00]],
            'small_value' => [42, [0x2A]],
            'one_byte_max' => [127, [0x7F]],
            'two_byte_min' => [128, [0x80, 0x01]],
            'two_byte_value' => [300, [0xAC, 0x02]], // 300 = 0x12C -> 0xAC 0x02
            'larger_value' => [16384, [0x80, 0x80, 0x01]], // 16384 = 0x4000
        ];
    }

    /**
     * @return array<string, array{int}>
     */
    public static function uleb128ValuesProvider() : array
    {
        return [
            'zero' => [0],
            'small_positive' => [42],
            'one_byte_max' => [127],
            'two_byte_min' => [128],
            'two_byte_value' => [300],
            'two_byte_max' => [16383],
            'three_byte_min' => [16384],
            'large_positive' => [1000000],
            'medium_large' => [100000000],
            'very_large' => [1000000000000],
        ];
    }

    #[DataProvider('positiveValuesProvider')]
    public function test_uleb128_encoding_format_for_positive_values(int $value, array $expectedBytes) : void
    {
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        $writer->writeVarInts([$value]);

        $actualBytes = array_values(unpack('C*', $buffer));

        self::assertSame($expectedBytes, $actualBytes, "Value {$value} should encode to specific byte sequence");
    }

    #[DataProvider('negativeValuesProvider')]
    public function test_uleb128_handles_negative_values_as_unsigned(int $negativeValue) : void
    {
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        $writer->writeVarInts([$negativeValue]);

        $reader = new BinaryBufferReader($buffer);
        $decoded = $reader->readVarInt();

        self::assertSame($negativeValue, $decoded, "Negative value {$negativeValue} should roundtrip correctly when treated as unsigned");
    }

    public function test_uleb128_handles_zigzag_encoded_values() : void
    {
        // These are ZigZag encoded values that may appear negative after encoding
        $zigzagValues = [
            0,    // 0 -> 0
            1,    // -1 -> 1
            2,    // 1 -> 2
            3,    // -2 -> 3
            4,    // 2 -> 4
        ];

        foreach ($zigzagValues as $zigzagEncoded) {
            $buffer = '';
            $writer = new BinaryBufferWriter($buffer);

            $writer->writeVarInts([$zigzagEncoded]);

            $reader = new BinaryBufferReader($buffer);
            $decoded = $reader->readVarInt();

            self::assertSame($zigzagEncoded, $decoded, "ZigZag encoded value {$zigzagEncoded} should roundtrip correctly");
        }
    }

    public function test_uleb128_is_same_as_varint_for_positive_values() : void
    {
        $testValues = [0, 1, 127, 128, 255, 16383, 16384, 65535, 65536];

        foreach ($testValues as $value) {
            $uleb128Buffer = '';
            $uleb128Writer = new BinaryBufferWriter($uleb128Buffer);
            $uleb128Writer->writeVarInts([$value]);

            $varintBuffer = '';
            $varintWriter = new BinaryBufferWriter($varintBuffer);
            $varintWriter->writeVarInts([$value]);

            self::assertSame($varintBuffer, $uleb128Buffer, "ULEB128 and VarInt should produce same encoding for value {$value}");
        }
    }

    #[DataProvider('uleb128ValuesProvider')]
    public function test_uleb128_roundtrip(int $value) : void
    {
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        $writer->writeVarInts([$value]);

        $reader = new BinaryBufferReader($buffer);
        $decoded = $reader->readVarInt();

        self::assertSame($value, $decoded, "Value {$value} should roundtrip correctly through ULEB128");
    }
}
