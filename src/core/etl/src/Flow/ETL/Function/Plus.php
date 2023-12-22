<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Plus implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly ScalarFunction $leftRef,
        private readonly ScalarFunction $rightRef
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
