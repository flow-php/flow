<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

enum Truth
{
    case Always;
    case Maybe;
    case Never;

    /**
     * AND: Never as soon as one is Never, Always only when all are.
     *
     * @param iterable<Truth> $truths
     */
    public static function every(iterable $truths): self
    {
        $truth = self::Always;

        foreach ($truths as $child) {
            if ($child === self::Never) {
                return self::Never;
            }

            if ($child === self::Maybe) {
                $truth = self::Maybe;
            }
        }

        return $truth;
    }

    /**
     * OR: Always as soon as one is Always, Never only when all are.
     *
     * @param iterable<Truth> $truths
     */
    public static function some(iterable $truths): self
    {
        $truth = self::Never;

        foreach ($truths as $child) {
            if ($child === self::Always) {
                return self::Always;
            }

            if ($child === self::Maybe) {
                $truth = self::Maybe;
            }
        }

        return $truth;
    }
}
