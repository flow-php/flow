<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Round implements ScalarFunction
{
    /**
     * @param ScalarFunction $entry
     * @param ScalarFunction $precision
     * @param int<0, max> $mode
     */
    public function __construct(
        private readonly ScalarFunction $entry,
        private readonly ScalarFunction $precision,
        private readonly int $mode = PHP_ROUND_HALF_UP,
    ) {
    }

    public function eval(Row $row) : ?float
    {
        /** @var mixed $value */
        $value = $this->entry->eval($row);

        if (!\is_float($value) && !\is_int($value)) {
            return null;
        }

        /** @phpstan-ignore-next-line */
        return \round($value, (int) $this->precision->eval($row), $this->mode);
    }
}
