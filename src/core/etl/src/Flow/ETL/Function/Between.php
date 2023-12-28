<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Row;

final class Between extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction $lowerBoundRef,
        private readonly ScalarFunction $upperBoundRef,
        private readonly Boundary $boundary = Boundary::LEFT_INCLUSIVE,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = $this->ref->eval($row);
        $lowerBound = $this->lowerBoundRef->eval($row);
        $upperBound = $this->upperBoundRef->eval($row);

        return $this->boundary->compare($value, $lowerBound, $upperBound);
    }
}
