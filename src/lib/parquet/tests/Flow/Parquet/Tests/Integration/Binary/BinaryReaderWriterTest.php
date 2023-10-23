<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Binary;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use PHPUnit\Framework\TestCase;

final class BinaryReaderWriterTest extends TestCase
{
    public function decimalProvider() : array
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
            ['decimals' => [1.234567890123456, 9.876543210987654], 'precision' => 17, 'scale' => 15],
            ['decimals' => [123456789012.345, 987654321098.765], 'precision' => 15, 'scale' => 3],
        ];
    }

    /**
     * @dataProvider decimalProvider
     */
    public function test_writing_and_reading_decimals(array $decimals, int $precision, int $scale) : void
    {
        $bitsNeeded = \ceil(\log(10 ** $precision, 2));
        $byteLength = (int) \ceil($bitsNeeded / 8);

        $buffer = '';
        (new BinaryBufferWriter($buffer))->writeDecimals($decimals, $byteLength, $precision, $scale);
        $this->assertSame(
            $decimals,
            (new BinaryBufferReader($buffer))->readDecimals(\count($decimals), $byteLength, $precision, $scale)
        );
    }

    public function test_writing_and_reading_strings() : void
    {
        $buffer = '';
        (new BinaryBufferWriter($buffer))->writeStrings($strings = ['some_string_01', 'some_string_02', 'some_string_02', 'ĄCZXCĄŚQWRQW']);
        $this->assertSame(
            $strings,
            (new BinaryBufferReader($buffer))->readStrings(\count($strings))
        );
    }
}
