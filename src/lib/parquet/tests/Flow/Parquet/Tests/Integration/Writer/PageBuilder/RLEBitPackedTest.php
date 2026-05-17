<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Writer\PageBuilder;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\BitWidth;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\Writer\PageBuilder\RLEBitPackedPacker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\Binary\decode_i32;

final class RLEBitPackedTest extends TestCase
{
    public static function values_provider(): \Generator
    {
        yield [
            [0, 0, 0, 0, 0, 0, 0, 0, 0],
            1,
        ];

        yield [
            [0, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            4,
        ];

        yield [
            [5, 5, 5, 5, 5, 5, 4, 4, 4, 4],
            8,
        ];
    }

    #[DataProvider('values_provider')]
    public function test_packing_and_unpacking_with_length(array $values, int $length): void
    {
        $values = \Flow\Types\DSL\type_list(\Flow\Types\DSL\type_integer())->assert($values);
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $packer = new RLEBitPackedPacker($rleBitPackedHybrid, $byteOrder);

        $buffer = $packer->packWithLength(BitWidth::fromArray($values), $values);
        $reader = new BinaryBufferReader($buffer);
        static::assertSame($length, decode_i32($byteOrder, $reader->readBytes(4))[0]);
        $unpacked = $rleBitPackedHybrid->decodeHybrid($reader, BitWidth::fromArray($values), \count($values));

        static::assertSame($values, $unpacked);
    }
}
