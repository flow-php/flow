<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Plus implements Expression
{
    public function __construct(
        private readonly Expression $leftRef,
        private readonly Expression $rightRef
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $left = $this->leftRef->eval($row);
        $right = $this->rightRef->eval($row);

        if (!\is_numeric($left) || !\is_numeric($right)) {
            return null;
        }

        return $left + $right;
    }
}
