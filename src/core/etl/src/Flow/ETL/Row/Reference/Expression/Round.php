<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Round implements Expression
{
    /**
     * @param Expression $entry
     * @param Expression $precision
     * @param int<0, max> $mode
     */
    public function __construct(
        private readonly Expression $entry,
        private readonly Expression $precision,
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
