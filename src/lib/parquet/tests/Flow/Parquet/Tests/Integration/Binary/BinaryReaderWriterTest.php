<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Binary;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_values;
use function ceil;
use function count;
use function Flow\Parquet\Binary\decode_decimal;
use function Flow\Parquet\Binary\decode_f32;
use function Flow\Parquet\Binary\decode_i16;
use function Flow\Parquet\Binary\decode_i32;
use function Flow\Parquet\Binary\decode_i64;
use function Flow\Parquet\Binary\decode_u32;
use function Flow\Parquet\Binary\encode_decimal;
use function Flow\Parquet\Binary\encode_f32;
use function Flow\Parquet\Binary\encode_i16;
use function Flow\Parquet\Binary\encode_i32;
use function Flow\Parquet\Binary\encode_i64;
use function Flow\Parquet\Binary\encode_u32;
use function log;
use function pack;
use function strlen;
use function unpack;

final class BinaryReaderWriterTest extends TestCase
{
    public static function decimalProvider(): array
    {
        return [
            ['decimals' => [10.24, 10.25], 'precision' => 10, 'scale' => 2],
            ['decimals' => [0.1, 0.2], 'precision' => 2, 'scale' => 1],
            ['decimals' => [1.2, 3.4], 'precision' => 2, 'scale' => 1],
            ['decimals' => [0.01, 0.02], 'precision' => 3, 'scale' => 2],
            ['decimals' => [1.234, 5.678], 'precision' => 4, 'scale' => 3],
            ['decimals' => [12.345, 67.890], 'precision' => 5, 'scale' => 3],
            ['decimals' => [0.00012, 0.00034], 'precision' => 6, 'scale' => 5],
            ['decimals' => [123.456, 789.012], 'precision' => 6, 'scale' => 3],
            ['decimals' => [0.0000001, 0.0000002], 'precision' => 8, 'scale' => 7],
            ['decimals' => [12345678.9, 98765432.1], 'precision' => 9, 'scale' => 1],
            ['decimals' => [12.3456789, 98.7654321], 'precision' => 9, 'scale' => 7],
            ['decimals' => [0.123456789, 0.987654321], 'precision' => 10, 'scale' => 9],
            ['decimals' => [1234567.89, 9876543.21], 'precision' => 10, 'scale' => 2],
            ['decimals' => [123456.7890, 987654.3210], 'precision' => 11, 'scale' => 4],
            ['decimals' => [0.000123456789, 0.000987654321], 'precision' => 12, 'scale' => 12],
            ['decimals' => [12345.67890, 98765.43210], 'precision' => 12, 'scale' => 5],
            ['decimals' => [0.0000000123456, 0.0000000987654], 'precision' => 16, 'scale' => 16],
            ['decimals' => [1234567890.12, 9876543210.98], 'precision' => 12, 'scale' => 2],
            ['decimals' => [1.234567890123456, 9.876543210987654], 'precision' => 17, 'scale' => 16],
            ['decimals' => [123456789012.345, 987654321098.765], 'precision' => 15, 'scale' => 3],
        ];
    }

    public function test_reading_and_writing_bytes(): void
    {
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $writer->writeBytes([1, 2, 3, 4, 5]);

        $reader = new BinaryBufferReader($buffer);

        static::assertEquals([1, 2, 3, 4, 5], array_values(unpack('C*', $reader->readBytes(5))));
    }

    public function test_reading_and_writing_varint(): void
    {
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $writer->writeVarInts([300, 1, 0, 127, 128]);

        $reader = new BinaryBufferReader($buffer);
        static::assertEquals(300, $reader->readVarInt());
        static::assertEquals(1, $reader->readVarInt());
        static::assertEquals(0, $reader->readVarInt());
        static::assertEquals(127, $reader->readVarInt());
        static::assertEquals(128, $reader->readVarInt());
    }

    public function test_writing_and_reading_big_integers_with_functions(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $ints = [];

        for ($i = 0; $i < 10000; $i++) {
            $ints[] = $i;
            $ints[] = -$i;
        }

        foreach ($ints as $int) {
            $writer->append(encode_i64($byteOrder, [$int]));
        }

        $reader = new BinaryBufferReader($buffer);

        static::assertEquals($ints, decode_i64($byteOrder, $reader->readBytes(count($ints) * 8)));
    }

    #[DataProvider('decimalProvider')]
    public function test_writing_and_reading_decimals_with_functions(array $decimals, int $precision, int $scale): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $bitsNeeded = ceil(log(10 ** $precision, 2));
        $byteLength = (int) ceil($bitsNeeded / 8);

        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        // @mago-ignore analysis:mixed-assignment
        foreach ($decimals as $decimal) {
            static::assertIsFloat($decimal);
            $writer->append(encode_decimal($byteOrder, $decimal, $byteLength, $precision, $scale));
        }

        $reader = new BinaryBufferReader($buffer);
        $decoded = [];

        foreach ($decimals as $_ignored) {
            $decoded[] = decode_decimal($byteOrder, $reader->readBytes($byteLength), $precision, $scale);
        }

        static::assertSame($decimals, $decoded);
    }

    public function test_writing_and_reading_floats_with_functions(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $floats = [1.1, 2.2, 3.3, 4.4, 9.1];

        foreach ($floats as $float) {
            $writer->append(encode_f32($byteOrder, [$float]));
        }

        $reader = new BinaryBufferReader($buffer);
        $widened = [];

        foreach ($floats as $float) {
            $widened[] = unpack('g', pack('g', $float))[1];
        }

        static::assertSame($widened, decode_f32($byteOrder, $reader->readBytes(count($floats) * 4)));
    }

    public function test_writing_and_reading_integers_with_functions(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $ints = [];

        for ($i = 0; $i < 10000; $i++) {
            $ints[] = $i;
            $ints[] = -$i;
        }

        foreach ($ints as $int) {
            $writer->append(encode_i32($byteOrder, [$int]));
        }

        $reader = new BinaryBufferReader($buffer);

        static::assertEquals($ints, decode_i32($byteOrder, $reader->readBytes(count($ints) * 4)));
    }

    public function test_writing_and_reading_small_integers_with_functions(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $ints = [];

        for ($i = 0; $i < 1000; $i++) {
            $ints[] = $i;
            $ints[] = -$i;
        }

        foreach ($ints as $int) {
            $writer->append(encode_i16($byteOrder, [$int]));
        }

        $reader = new BinaryBufferReader($buffer);

        static::assertEquals($ints, decode_i16($byteOrder, $reader->readBytes(count($ints) * 2)));
    }

    public function test_writing_and_reading_strings_with_functions(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);
        $strings = ['some_string_01', 'some_string_02', 'some_string_02', 'ĄCZXCĄŚQWRQW'];

        foreach ($strings as $string) {
            $writer->append(encode_u32($byteOrder, [strlen($string)]));
            $writer->append($string);
        }

        $reader = new BinaryBufferReader($buffer);
        $decoded = [];

        foreach ($strings as $_ignored) {
            $length = decode_u32($byteOrder, $reader->readBytes(4))[0];
            $decoded[] = $reader->readBytes($length);
        }

        static::assertSame($strings, $decoded);
    }
}
