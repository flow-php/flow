<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class LessThanEqual implements Expression
{
    public function __construct(
        private readonly Expression $base,
        private readonly Expression $next
    ) {
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    public function eval(Row $row) : bool
    {
        $base = $this->base->eval($row);
        $next = $this->next->eval($row);

        return $base <= $next;
    }
}
