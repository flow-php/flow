<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\OffsetOverflow;

use function array_values;
use function count;
use function pack;
use function sprintf;
use function strlen;
use function unpack;

final readonly class Offsets
{
    /**
     * @param list<int> $offsets
     */
    public function pack(array $offsets): string
    {
        $last = $offsets[count($offsets) - 1];

        if ($last > 2_147_483_647) {
            throw new OffsetOverflow(sprintf(
                'Offsets exceed the i32 range of a batch buffer: the last offset is %d',
                $last,
            ));
        }

        return pack('V*', ...$offsets);
    }

    /**
     * @return list<int> $count + 1 offsets, the first 0, monotonic
     */
    public function unpack(string $buffer, int $count, string $layout): array
    {
        if (strlen($buffer) !== (($count + 1) * 4)) {
            throw new InvalidArgumentException(sprintf(
                '%s offsets buffer of %d bytes, expected %d for %d rows',
                $layout,
                strlen($buffer),
                ($count + 1) * 4,
                $count,
            ));
        }

        /** @var list<int> $offsets */
        $offsets = array_values((array) unpack('V*', $buffer));

        if ($offsets[0] !== 0) {
            throw new InvalidArgumentException(sprintf('%s offsets start at %d, not 0', $layout, $offsets[0]));
        }

        for ($i = 1; $i <= $count; $i++) {
            if ($offsets[$i] < $offsets[$i - 1]) {
                throw new InvalidArgumentException(sprintf(
                    '%s offsets are not monotonic: offset %d is %d, offset %d is %d',
                    $layout,
                    $i - 1,
                    $offsets[$i - 1],
                    $i,
                    $offsets[$i],
                ));
            }
        }

        return $offsets;
    }
}
