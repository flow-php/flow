<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ParquetFile\Data\BitWidth;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\RLEBitPackedPacker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class RLEBitPackedTest extends TestCase
{
    public static function values_provider() : \Generator
    {
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
    public function test_packing_and_unpacking_with_length(array $values, int $length) : void
    {
        $packer = new RLEBitPackedPacker($rleBitPackedHybrid = new RLEBitPackedHybrid());

        $buffer = $packer->packWithLength($values);
        $reader = new BinaryBufferReader($buffer);
        $this->assertSame($length, $reader->readInts32(1)[0]);
        $unpacked = $rleBitPackedHybrid->decodeHybrid($reader, BitWidth::fromArray($values), \count($values));

        $this->assertSame(
            $values,
            $unpacked
        );
    }
}
