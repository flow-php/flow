<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function array_fill;
use function chr;
use function intdiv;
use function ord;
use function sprintf;
use function str_repeat;
use function strlen;

final readonly class Validity
{
    /**
     * @param list<mixed> $values
     */
    public function fromValues(array $values): string
    {
        $bitmap = '';
        $byte = 0;
        $bit = 0;
        $hasNull = false;

        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if ($value === null) {
                $hasNull = true;
            } else {
                $byte |= 1 << $bit;
            }

            if (++$bit === 8) {
                $bitmap .= chr($byte);
                $byte = 0;
                $bit = 0;
            }
        }

        if (!$hasNull) {
            return '';
        }

        return $bit === 0 ? $bitmap : $bitmap . chr($byte);
    }

    /**
     * @param array<int, true> $nulls
     */
    public function fromNulls(array $nulls, int $count): string
    {
        if ($nulls === []) {
            return '';
        }

        // @mago-ignore analysis:possibly-invalid-argument
        $bitmap = str_repeat("\xFF", intdiv($count, 8));
        $tail = $count % 8;

        if ($tail !== 0) {
            $bitmap .= chr((1 << $tail) - 1);
        }

        foreach ($nulls as $index => $_) {
            $byte = intdiv($index, 8);
            $bitmap[$byte] = chr(ord($bitmap[$byte]) & ~(1 << ($index % 8)) & 0xFF);
        }

        return $bitmap;
    }

    /**
     * @return list<bool>
     */
    public function valid(string $bitmap, int $count): array
    {
        if ($bitmap === '') {
            // @mago-ignore analysis:possibly-invalid-argument
            return $count === 0 ? [] : array_fill(0, $count, true);
        }

        if (strlen($bitmap) < intdiv($count + 7, 8)) {
            throw new InvalidArgumentException(sprintf(
                'Validity bitmap of %d bytes is too short for %d rows',
                strlen($bitmap),
                $count,
            ));
        }

        $valid = [];

        for ($i = 0; $i < $count; $i++) {
            $valid[] = ((ord($bitmap[$i >> 3]) >> ($i & 7)) & 1) === 1;
        }

        return $valid;
    }

    public function nullCount(string $bitmap, int $count): int
    {
        if ($bitmap === '') {
            return 0;
        }

        $nulls = 0;

        foreach ($this->valid($bitmap, $count) as $valid) {
            if (!$valid) {
                $nulls++;
            }
        }

        return $nulls;
    }

    /**
     * @param list<bool> $valid
     *
     * @return array<int, true>
     */
    public function nulls(array $valid): array
    {
        $nulls = [];

        foreach ($valid as $index => $isValid) {
            if (!$isValid) {
                $nulls[$index] = true;
            }
        }

        return $nulls;
    }
}
