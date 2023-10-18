<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

final class BitWidth
{
    public static function calculate(int $value) : int
    {
        return (int) \ceil(\log($value + 1, 2));
    }

    /**
     * @param array<int> $ints
     */
    public static function fromArray(array $ints) : int
    {
        if (!\count($ints)) {
            return 0;
        }

        $maxInt = \max($ints);

        if ($maxInt === 0) {
            return 0;
        }

        return self::calculate($maxInt);
    }

    /**
     * @return array<int>
     */
    public static function toBytes(int $value, int $bitWidth) : array
    {
        $bytes = [];
        $width = (int) (($bitWidth + 7) / 8);

        for ($i = 0; $i < $width; $i++) {
            $bytes[] = ($value >> ($i * 8)) & 0xFF;
        }

        return $bytes;
    }
}
