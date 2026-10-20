<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;

use function pack;
use function unpack;

/**
 * A FLOAT decodes to the exact double of its 32 bits: a literal between two of them equals neither, and one beyond the
 * largest float32 lies between it and the infinity.
 */
final readonly class Float32PreImage implements PreImage
{
    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        $value = DoublePreImage::value($literal);

        if ($value === null) {
            return null;
        }

        /** @var array{1: float} $unpacked */
        $unpacked = unpack('g', pack('g', $value));
        $rounded = $unpacked[1];

        if ($rounded === $value) {
            return EqualRange::point($value);
        }

        $key = (int) PhysicalOrder::Float->key($rounded);

        return $rounded > $value
            ? new EqualRange($rounded, self::fromKey($key - 1))
            : new EqualRange(self::fromKey($key + 1), $rounded);
    }

    private static function fromKey(int $key): float
    {
        /** @var array{1: float} $unpacked */
        $unpacked = unpack('g', pack('V', ($key ^ (($key >> 31) & 0x7FFFFFFF)) & 0xFFFFFFFF));

        return $unpacked[1];
    }
}
